//! Exact small-scale Cloudcast policy planner for the WR substrate.
//!
//! The Cloudcast paper minimizes egress plus VM cost while selecting a distribution tree for
//! each equal-sized stripe subject to a completion budget. Wansim already fixes one VM/NIC budget
//! per overlay actor, so this planner minimizes egress cost under that normalized resource budget.
//! It exactly enumerates every two-relay tree executable by the W0b actor shape and every split of
//! equal stripes across two tree slots. This deliberately avoids reproducing the published
//! `skyplane/nsdi` implementation defects or adding a heavyweight ILP dependency.

use crate::protocol::proportional_quotas;

use super::{CloudScenario, CloudScenarioError};

const TREE_COUNT: usize = 2;
const RECEIVER_COUNT: usize = 3;
const TREE_EDGE_COUNT: usize = 5;
const NANOS_PER_SECOND: u128 = 1_000_000_000;
const BITS_PER_BYTE: u128 = 8;
const GIB_BYTES: u128 = 1_u128 << 30;

/// A deterministic region-to-region egress-price matrix.
///
/// Rates use nano-USD/GiB so planner comparisons remain exact integers. The matrix order must
/// match `CloudScenario::regions`; same-region entries must be zero.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CloudcastEgressPrices {
    nano_usd_per_gib: Vec<Vec<u64>>,
}

impl CloudcastEgressPrices {
    pub fn new(
        scenario: &CloudScenario,
        nano_usd_per_gib: Vec<Vec<u64>>,
    ) -> Result<Self, CloudcastPolicyError> {
        let region_count = scenario.regions.len();
        if nano_usd_per_gib.len() != region_count
            || nano_usd_per_gib.iter().any(|row| row.len() != region_count)
            || (0..region_count).any(|region| nano_usd_per_gib[region][region] != 0)
        {
            return Err(CloudcastPolicyError::PriceMatrix);
        }
        Ok(Self { nano_usd_per_gib })
    }

    pub fn rate(&self, from: usize, to: usize) -> Option<u64> {
        self.nano_usd_per_gib
            .get(from)
            .and_then(|row| row.get(to))
            .copied()
    }
}

#[derive(Clone, Copy, Debug)]
pub struct CloudcastPolicyRequest<'a> {
    pub scenario: &'a CloudScenario,
    pub egress_prices: &'a CloudcastEgressPrices,
    pub source_symbols: usize,
    pub symbol_payload_bytes: usize,
    pub stripe_count: usize,
    /// Transfer duration budget, excluding the WR background warm-up.
    pub completion_budget_ns: u64,
    /// Configured background-load profile supplied to the planner as a throughput profile.
    pub background_utilization_percent: u8,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CloudcastPolicyPlan {
    completion_budget_ns: u64,
    estimated_completion_ns: u64,
    estimated_egress_nano_usd: u64,
    stripe_count: usize,
    stripe_tree_ids: Vec<u8>,
    tree_stripe_counts: [usize; TREE_COUNT],
    tree_symbol_quotas: [usize; TREE_COUNT],
    relay_regions: [[String; 2]; TREE_COUNT],
    candidate_tree_count: usize,
    evaluated_assignment_count: usize,
}

impl CloudcastPolicyPlan {
    pub fn completion_budget_ns(&self) -> u64 {
        self.completion_budget_ns
    }

    pub fn estimated_completion_ns(&self) -> u64 {
        self.estimated_completion_ns
    }

    pub fn estimated_egress_nano_usd(&self) -> u64 {
        self.estimated_egress_nano_usd
    }

    pub fn stripe_count(&self) -> usize {
        self.stripe_count
    }

    pub fn stripe_tree_ids(&self) -> &[u8] {
        &self.stripe_tree_ids
    }

    pub fn tree_stripe_counts(&self) -> [usize; TREE_COUNT] {
        self.tree_stripe_counts
    }

    pub fn tree_symbol_quotas(&self) -> [usize; TREE_COUNT] {
        self.tree_symbol_quotas
    }

    pub fn relay_regions(&self) -> &[[String; 2]; TREE_COUNT] {
        &self.relay_regions
    }

    pub fn candidate_tree_count(&self) -> usize {
        self.candidate_tree_count
    }

    pub fn evaluated_assignment_count(&self) -> usize {
        self.evaluated_assignment_count
    }

    pub fn apply_to(&self, scenario: &mut CloudScenario) -> Result<(), CloudcastPolicyError> {
        scenario.placement.relay_regions = self.relay_regions.clone();
        scenario.validate()?;
        Ok(())
    }
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum CloudcastPolicyError {
    #[error("Cloudcast policy request has invalid geometry")]
    Geometry,
    #[error("Cloudcast policy egress-price matrix does not match the scenario")]
    PriceMatrix,
    #[error("Cloudcast policy arithmetic overflow")]
    Overflow,
    #[error("Cloudcast policy found no plan within the {completion_budget_ns} ns budget")]
    NoFeasiblePlan { completion_budget_ns: u64 },
    #[error(transparent)]
    Scenario(#[from] CloudScenarioError),
}

#[derive(Clone, Debug)]
struct CandidateTree {
    relay_a: usize,
    relay_b: usize,
    resource_multiplicity: Vec<u8>,
    critical_propagation_ns: u64,
    egress_rate_sum_nano_usd_per_gib: u64,
}

#[derive(Clone, Debug)]
struct CandidatePlan {
    cost_numerator: u128,
    estimated_completion_ns: u64,
    tree_indexes: [usize; TREE_COUNT],
    tree_stripe_counts: [usize; TREE_COUNT],
    tree_symbol_quotas: [usize; TREE_COUNT],
}

impl CandidatePlan {
    fn ordering_key(&self) -> (u128, u64, usize, [usize; 2], [usize; 2]) {
        (
            self.cost_numerator,
            self.estimated_completion_ns,
            self.tree_stripe_counts
                .iter()
                .filter(|count| **count != 0)
                .count(),
            self.tree_indexes,
            self.tree_stripe_counts,
        )
    }
}

pub fn plan_cloudcast_policy(
    request: CloudcastPolicyRequest<'_>,
) -> Result<CloudcastPolicyPlan, CloudcastPolicyError> {
    validate_request(request)?;
    let candidates = candidate_trees(request.scenario, request.egress_prices)?;
    let mut best: Option<CandidatePlan> = None;
    let mut evaluated_assignment_count = 0usize;
    for first in 0..candidates.len() {
        for second in 0..candidates.len() {
            for first_stripes in 0..=request.stripe_count {
                let second_stripes = request.stripe_count - first_stripes;
                if first == second && first_stripes != 0 && second_stripes != 0 {
                    continue;
                }
                evaluated_assignment_count = evaluated_assignment_count
                    .checked_add(1)
                    .ok_or(CloudcastPolicyError::Overflow)?;
                let stripe_counts = [first_stripes, second_stripes];
                let quotas = proportional_quotas(
                    request.source_symbols,
                    &[first_stripes as u64, second_stripes as u64],
                )
                .ok_or(CloudcastPolicyError::Geometry)?;
                let tree_symbol_quotas = [quotas[0], quotas[1]];
                let trees = [&candidates[first], &candidates[second]];
                let estimated_completion_ns =
                    estimate_completion_ns(request, trees, tree_symbol_quotas)?;
                if estimated_completion_ns > request.completion_budget_ns {
                    continue;
                }
                let cost_numerator = estimate_cost_numerator(request, trees, tree_symbol_quotas)?;
                let candidate = CandidatePlan {
                    cost_numerator,
                    estimated_completion_ns,
                    tree_indexes: [first, second],
                    tree_stripe_counts: stripe_counts,
                    tree_symbol_quotas,
                };
                if best
                    .as_ref()
                    .is_none_or(|current| candidate.ordering_key() < current.ordering_key())
                {
                    best = Some(candidate);
                }
            }
        }
    }
    let best = best.ok_or(CloudcastPolicyError::NoFeasiblePlan {
        completion_budget_ns: request.completion_budget_ns,
    })?;
    let selected = best.tree_indexes.map(|index| &candidates[index]);
    let relay_regions = selected.map(|tree| {
        [
            request.scenario.regions[tree.relay_a].id.clone(),
            request.scenario.regions[tree.relay_b].id.clone(),
        ]
    });
    let mut stripe_tree_ids = Vec::with_capacity(request.stripe_count);
    for (tree, count) in best.tree_stripe_counts.into_iter().enumerate() {
        let tree = u8::try_from(tree).map_err(|_| CloudcastPolicyError::Overflow)?;
        stripe_tree_ids.extend(std::iter::repeat_n(tree, count));
    }
    Ok(CloudcastPolicyPlan {
        completion_budget_ns: request.completion_budget_ns,
        estimated_completion_ns: best.estimated_completion_ns,
        estimated_egress_nano_usd: u64::try_from(best.cost_numerator / GIB_BYTES)
            .map_err(|_| CloudcastPolicyError::Overflow)?,
        stripe_count: request.stripe_count,
        stripe_tree_ids,
        tree_stripe_counts: best.tree_stripe_counts,
        tree_symbol_quotas: best.tree_symbol_quotas,
        relay_regions,
        candidate_tree_count: candidates.len(),
        evaluated_assignment_count,
    })
}

fn validate_request(request: CloudcastPolicyRequest<'_>) -> Result<(), CloudcastPolicyError> {
    request.scenario.validate()?;
    let region_count = request.scenario.regions.len();
    if request.source_symbols == 0
        || request.symbol_payload_bytes == 0
        || request.stripe_count == 0
        || request.completion_budget_ns == 0
        || request.background_utilization_percent >= 100
        || request.egress_prices.nano_usd_per_gib.len() != region_count
    {
        return Err(CloudcastPolicyError::Geometry);
    }
    Ok(())
}

fn candidate_trees(
    scenario: &CloudScenario,
    prices: &CloudcastEgressPrices,
) -> Result<Vec<CandidateTree>, CloudcastPolicyError> {
    let sender = scenario.region_index(&scenario.placement.sender_region)?;
    let receivers = scenario
        .placement
        .receiver_regions
        .each_ref()
        .map(|region| scenario.region_index(region))
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?;
    let receivers: [usize; RECEIVER_COUNT] = receivers
        .try_into()
        .map_err(|_| CloudcastPolicyError::Geometry)?;
    let candidates = (0..scenario.regions.len())
        .filter(|relay| *relay != sender)
        .collect::<Vec<_>>();
    let resource_count = scenario.regions.len() + scenario.trunks.len();
    let mut trees = Vec::new();
    for &relay_a in &candidates {
        for &relay_b in &candidates {
            if relay_a == relay_b {
                continue;
            }
            let edges = tree_edges(sender, receivers, relay_a, relay_b);
            let mut resource_multiplicity = vec![0_u8; resource_count];
            let mut edge_delays = [0_u64; TREE_EDGE_COUNT];
            let mut egress_rate_sum = 0_u64;
            for (edge_index, &(from, to)) in edges.iter().enumerate() {
                for resource in route_resources(scenario, from, to)? {
                    resource_multiplicity[resource] = resource_multiplicity[resource]
                        .checked_add(1)
                        .ok_or(CloudcastPolicyError::Overflow)?;
                }
                edge_delays[edge_index] = scenario.directed_region_delay_ns(from, to)?;
                egress_rate_sum = egress_rate_sum
                    .checked_add(
                        prices
                            .rate(from, to)
                            .ok_or(CloudcastPolicyError::PriceMatrix)?,
                    )
                    .ok_or(CloudcastPolicyError::Overflow)?;
            }
            let critical_propagation_ns = [
                edge_delays[0].saturating_add(edge_delays[1]),
                edge_delays[0]
                    .saturating_add(edge_delays[2])
                    .saturating_add(edge_delays[3]),
                edge_delays[0]
                    .saturating_add(edge_delays[2])
                    .saturating_add(edge_delays[4]),
            ]
            .into_iter()
            .max()
            .ok_or(CloudcastPolicyError::Geometry)?;
            trees.push(CandidateTree {
                relay_a,
                relay_b,
                resource_multiplicity,
                critical_propagation_ns,
                egress_rate_sum_nano_usd_per_gib: egress_rate_sum,
            });
        }
    }
    if trees.is_empty() {
        return Err(CloudcastPolicyError::Geometry);
    }
    Ok(trees)
}

fn tree_edges(
    sender: usize,
    receivers: [usize; RECEIVER_COUNT],
    relay_a: usize,
    relay_b: usize,
) -> [(usize, usize); TREE_EDGE_COUNT] {
    [
        (sender, relay_a),
        (relay_a, receivers[0]),
        (relay_a, relay_b),
        (relay_b, receivers[1]),
        (relay_b, receivers[2]),
    ]
}

fn route_resources(
    scenario: &CloudScenario,
    from: usize,
    to: usize,
) -> Result<Vec<usize>, CloudcastPolicyError> {
    if from == to {
        return Ok(vec![from]);
    }
    let source = scenario
        .regions
        .get(from)
        .ok_or(CloudcastPolicyError::Geometry)?;
    let destination = scenario
        .regions
        .get(to)
        .ok_or(CloudcastPolicyError::Geometry)?;
    let route = scenario.hub_route(&source.hub, &destination.hub)?;
    let mut resources = Vec::with_capacity(route.trunk_indexes.len() + 2);
    resources.push(from);
    resources.extend(
        route
            .trunk_indexes
            .into_iter()
            .map(|trunk| scenario.regions.len() + trunk),
    );
    resources.push(to);
    Ok(resources)
}

fn estimate_completion_ns(
    request: CloudcastPolicyRequest<'_>,
    trees: [&CandidateTree; TREE_COUNT],
    quotas: [usize; TREE_COUNT],
) -> Result<u64, CloudcastPolicyError> {
    let resource_count = request.scenario.regions.len() + request.scenario.trunks.len();
    let mut resource_bytes = vec![0_u128; resource_count];
    let mut maximum_propagation_ns = 0_u64;
    for (tree, quota) in trees.into_iter().zip(quotas) {
        if quota == 0 {
            continue;
        }
        let bytes = (quota as u128)
            .checked_mul(request.symbol_payload_bytes as u128)
            .ok_or(CloudcastPolicyError::Overflow)?;
        for (resource, multiplicity) in tree.resource_multiplicity.iter().copied().enumerate() {
            let demand = bytes
                .checked_mul(u128::from(multiplicity))
                .ok_or(CloudcastPolicyError::Overflow)?;
            resource_bytes[resource] = resource_bytes[resource]
                .checked_add(demand)
                .ok_or(CloudcastPolicyError::Overflow)?;
        }
        maximum_propagation_ns = maximum_propagation_ns.max(tree.critical_propagation_ns);
    }
    let mut maximum_service_ns = 0_u128;
    for (resource, bytes) in resource_bytes.into_iter().enumerate() {
        if bytes == 0 {
            continue;
        }
        let rate = effective_resource_rate_bps(request, resource);
        let numerator = bytes
            .checked_mul(BITS_PER_BYTE)
            .and_then(|value| value.checked_mul(NANOS_PER_SECOND))
            .ok_or(CloudcastPolicyError::Overflow)?;
        let service_ns = numerator.div_ceil(u128::from(rate.max(1)));
        maximum_service_ns = maximum_service_ns.max(service_ns);
    }
    u64::try_from(maximum_service_ns)
        .map_err(|_| CloudcastPolicyError::Overflow)?
        .checked_add(maximum_propagation_ns)
        .ok_or(CloudcastPolicyError::Overflow)
}

fn effective_resource_rate_bps(request: CloudcastPolicyRequest<'_>, resource: usize) -> u64 {
    if resource < request.scenario.regions.len() {
        return request.scenario.vm_nic_cap_bps;
    }
    let trunk = &request.scenario.trunks[resource - request.scenario.regions.len()];
    trunk
        .capacity_bps
        .saturating_mul(100 - u64::from(request.background_utilization_percent))
        / 100
}

fn estimate_cost_numerator(
    request: CloudcastPolicyRequest<'_>,
    trees: [&CandidateTree; TREE_COUNT],
    quotas: [usize; TREE_COUNT],
) -> Result<u128, CloudcastPolicyError> {
    trees
        .into_iter()
        .zip(quotas)
        .try_fold(0_u128, |total, (tree, quota)| {
            let bytes = (quota as u128)
                .checked_mul(request.symbol_payload_bytes as u128)
                .ok_or(CloudcastPolicyError::Overflow)?;
            let charge = bytes
                .checked_mul(u128::from(tree.egress_rate_sum_nano_usd_per_gib))
                .ok_or(CloudcastPolicyError::Overflow)?;
            total
                .checked_add(charge)
                .ok_or(CloudcastPolicyError::Overflow)
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scenario::CloudProfileKind;

    fn uniform_prices(scenario: &CloudScenario, rate: u64) -> CloudcastEgressPrices {
        let mut matrix = vec![vec![rate; scenario.regions.len()]; scenario.regions.len()];
        for (region, row) in matrix.iter_mut().enumerate() {
            row[region] = 0;
        }
        CloudcastEgressPrices::new(scenario, matrix).expect("valid synthetic prices")
    }

    fn request<'a>(
        scenario: &'a CloudScenario,
        prices: &'a CloudcastEgressPrices,
        budget: u64,
    ) -> CloudcastPolicyRequest<'a> {
        CloudcastPolicyRequest {
            scenario,
            egress_prices: prices,
            source_symbols: 8_192,
            symbol_payload_bytes: 508,
            stripe_count: 8,
            completion_budget_ns: budget,
            background_utilization_percent: 70,
        }
    }

    #[test]
    fn exact_small_scale_plan_is_deterministic_feasible_and_complete() {
        let scenario =
            CloudScenario::built_in(CloudProfileKind::DigitaloceanLike, 1).expect("cloud scenario");
        let prices = uniform_prices(&scenario, 10_000_000);
        let first = plan_cloudcast_policy(request(&scenario, &prices, 2_000_000_000))
            .expect("feasible plan");
        let second = plan_cloudcast_policy(request(&scenario, &prices, 2_000_000_000))
            .expect("same feasible plan");

        assert_eq!(first, second);
        assert_eq!(first.candidate_tree_count(), 20);
        assert!(first.evaluated_assignment_count() > first.candidate_tree_count());
        assert_eq!(first.stripe_tree_ids().len(), 8);
        assert_eq!(first.tree_stripe_counts().iter().sum::<usize>(), 8);
        assert_eq!(first.tree_symbol_quotas().iter().sum::<usize>(), 8_192);
        assert!(first.estimated_completion_ns() <= first.completion_budget_ns());

        let mut planned = scenario;
        first.apply_to(&mut planned).expect("plan applies cleanly");
        assert_eq!(&planned.placement.relay_regions, first.relay_regions());
    }

    #[test]
    fn impossible_completion_budget_is_rejected_cleanly() {
        let scenario =
            CloudScenario::built_in(CloudProfileKind::DigitaloceanLike, 1).expect("cloud scenario");
        let prices = uniform_prices(&scenario, 10_000_000);
        assert_eq!(
            plan_cloudcast_policy(request(&scenario, &prices, 1)),
            Err(CloudcastPolicyError::NoFeasiblePlan {
                completion_budget_ns: 1,
            })
        );
    }

    #[test]
    fn price_matrix_requires_zero_diagonal_and_exact_region_shape() {
        let scenario =
            CloudScenario::built_in(CloudProfileKind::AwsLike, 0).expect("cloud scenario");
        let mut matrix = vec![vec![1; scenario.regions.len()]; scenario.regions.len()];
        assert_eq!(
            CloudcastEgressPrices::new(&scenario, matrix.clone()),
            Err(CloudcastPolicyError::PriceMatrix)
        );
        for (region, row) in matrix.iter_mut().enumerate() {
            row[region] = 0;
        }
        matrix.pop();
        assert_eq!(
            CloudcastEgressPrices::new(&scenario, matrix),
            Err(CloudcastPolicyError::PriceMatrix)
        );
    }
}
