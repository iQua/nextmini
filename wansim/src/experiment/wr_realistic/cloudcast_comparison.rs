use std::collections::BTreeMap;
use std::path::Path;

use serde::Serialize;
use thiserror::Error;

use super::persistence;
use super::{
    CLOUDCAST_COMPLETION_BUDGET_NS, CLOUDCAST_STRIPE_COUNT, EVIDENCE_CLASS, RawTrial, Slice, Task,
    WR_SYMBOL_PAYLOAD_BYTES, mean_u64, percentile_u64, to_csv,
};
use crate::scenario::{
    CloudProfileKind, CloudScenario, CloudcastPolicyError, CloudcastPolicyRequest,
    ReceiverAdmissionPolicy, WrProtocol, cloudcast_policy_budget_frontier, plan_cloudcast_policy,
    representative_egress_prices,
};
use crate::{SCENARIO_SCHEMA_VERSION, SIMULATOR_VERSION};

const EXPERIMENT: &str = "cloudcast-comparison-v2-throughput";
const CELL_DIRECTORY: &str = "comparison-v2-cells";
const REQUIRED_SEEDS: u64 = 16;
const PROFILE: CloudProfileKind = CloudProfileKind::DigitaloceanLike;
const PLACEMENT: usize = 1;
const UTILIZATIONS: [u8; 2] = [30, 70];
const EXECUTION_VARIANTS: [WrProtocol; 5] = [
    WrProtocol::CloudcastPolicy,
    WrProtocol::Carousel,
    WrProtocol::PerStripeFec,
    WrProtocol::BestSingleTree0,
    WrProtocol::BestSingleTree1,
];

const CLOUDCAST_LABEL: &str = WrProtocol::CloudcastPolicy.evidence_name();
const CAROUSEL_LABEL: &str = "pooled-FEC carousel on wansim transport";
const PER_STRIPE_LABEL: &str = "per-stripe FEC on wansim transport";
const BEST_SINGLE_LABEL: &str = "best-single tree on wansim transport";

#[derive(Clone, Debug)]
pub struct CloudcastComparisonArtifacts {
    pub trials_csv: String,
    pub summaries_csv: String,
    pub policy_frontier_csv: String,
    pub sharing_csv: String,
}

#[derive(Clone, Debug)]
pub struct CloudcastComparisonRun {
    pub artifacts: Option<CloudcastComparisonArtifacts>,
    pub failures_csv: String,
    pub total_cells: usize,
    pub successful_cells: usize,
    pub failed_cells: usize,
    pub skipped_cells: usize,
}

#[derive(Debug, Error)]
pub enum CloudcastComparisonError {
    #[error("Cloudcast comparison requires exactly 16 seeds")]
    SeedCount,
    #[error("Cloudcast comparison worker count must be nonzero")]
    ZeroWorkers,
    #[error("Cloudcast comparison persistence failed: {0}")]
    Persistence(String),
    #[error(
        "Cloudcast comparison is missing matched arm {arm} at utilization {utilization}, seed {seed}"
    )]
    MissingArm {
        utilization: u8,
        seed: u64,
        arm: &'static str,
    },
    #[error("Cloudcast throughput frontier is empty at utilization {utilization}")]
    MissingFrontier { utilization: u8 },
    #[error(
        "Cloudcast throughput frontier at utilization {utilization} remains feasible below its claimed minimum budget {minimum_budget_ns}"
    )]
    NonMinimalFrontier {
        utilization: u8,
        minimum_budget_ns: u64,
    },
    #[error(transparent)]
    CloudcastPolicy(#[from] CloudcastPolicyError),
    #[error(transparent)]
    Csv(#[from] csv::Error),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum ReportedArm {
    CloudcastPolicy,
    Carousel,
    PerStripeFec,
    BestSingle,
}

impl ReportedArm {
    const fn label(self) -> &'static str {
        match self {
            Self::CloudcastPolicy => CLOUDCAST_LABEL,
            Self::Carousel => CAROUSEL_LABEL,
            Self::PerStripeFec => PER_STRIPE_LABEL,
            Self::BestSingle => BEST_SINGLE_LABEL,
        }
    }
}

#[derive(Clone, Debug)]
struct ReportedTrial<'a> {
    arm: ReportedArm,
    execution_variant: &'static str,
    trial: &'a RawTrial,
}

#[derive(Clone, Debug, Serialize)]
struct TrialRow {
    schema_version: u32,
    simulator_version: &'static str,
    evidence_class: &'static str,
    profile: &'static str,
    placement: String,
    utilization_percent: u8,
    jitter: bool,
    source_symbols: usize,
    seed: u64,
    arm: &'static str,
    execution_variant: &'static str,
    barrier_completion_ns: u64,
    sender_completion_ns: u64,
    modeled_egress_nano_usd: u64,
    foreground_egress_wire_bytes: u64,
    total_emissions: usize,
    application_drops: usize,
    link_drops: usize,
    maximum_mailbox_high_water: usize,
}

#[derive(Clone, Debug, Serialize)]
struct SummaryRow {
    schema_version: u32,
    simulator_version: &'static str,
    evidence_class: &'static str,
    profile: &'static str,
    placement: String,
    utilization_percent: u8,
    jitter: bool,
    source_symbols: usize,
    seeds: usize,
    arm: &'static str,
    barrier_mean_ns: u64,
    barrier_p5_ns: u64,
    barrier_p95_ns: u64,
    sender_mean_ns: u64,
    sender_p5_ns: u64,
    sender_p95_ns: u64,
    modeled_egress_mean_nano_usd: u64,
    modeled_egress_p5_nano_usd: u64,
    modeled_egress_p95_nano_usd: u64,
    foreground_wire_mean_bytes: u64,
    emissions_mean: u64,
    application_drops_total: usize,
    link_drops_total: usize,
}

#[derive(Clone, Debug, Serialize)]
struct PolicyPlanRow {
    schema_version: u32,
    simulator_version: &'static str,
    evidence_class: &'static str,
    arm: &'static str,
    profile: &'static str,
    placement: String,
    utilization_percent: u8,
    source_symbols: usize,
    frontier_index: usize,
    selected_for_comparison: bool,
    stripe_count: usize,
    completion_budget_ns: u64,
    estimated_completion_ns: u64,
    estimated_payload_egress_nano_usd: u64,
    candidate_tree_count: usize,
    evaluated_assignment_count: usize,
    tree0_relay_a: String,
    tree0_relay_b: String,
    tree1_relay_a: String,
    tree1_relay_b: String,
    tree0_stripes: usize,
    tree1_stripes: usize,
    tree0_source_symbols: usize,
    tree1_source_symbols: usize,
    stripe_tree_ids: String,
}

#[derive(Clone, Debug, Serialize, PartialEq, Eq, PartialOrd, Ord)]
struct SharingRow {
    schema_version: u32,
    simulator_version: &'static str,
    evidence_class: &'static str,
    utilization_percent: u8,
    execution_variant: &'static str,
    resource: String,
    total_flow_directions: usize,
    foreground_flow_directions: usize,
    background_flow_directions: usize,
}

pub fn run_cloudcast_comparison_persistent(
    seeds: u64,
    workers: usize,
    output: &Path,
    resume: bool,
) -> Result<CloudcastComparisonRun, CloudcastComparisonError> {
    if seeds != REQUIRED_SEEDS {
        return Err(CloudcastComparisonError::SeedCount);
    }
    if workers == 0 {
        return Err(CloudcastComparisonError::ZeroWorkers);
    }
    let persisted = persistence::run_cells(
        EXPERIMENT,
        CELL_DIRECTORY,
        seeds,
        workers,
        output,
        resume,
        build_tasks(seeds),
    )
    .map_err(|error| CloudcastComparisonError::Persistence(error.to_string()))?;
    let artifacts = if persisted.failed_cells == 0 {
        Some(artifacts(&persisted.successful)?)
    } else {
        None
    };
    Ok(CloudcastComparisonRun {
        artifacts,
        failures_csv: persisted.failures_csv,
        total_cells: persisted.total_cells,
        successful_cells: persisted.successful.len(),
        failed_cells: persisted.failed_cells,
        skipped_cells: persisted.skipped_cells,
    })
}

fn build_tasks(seeds: u64) -> Vec<Task> {
    let mut tasks = Vec::with_capacity(
        UTILIZATIONS.len() * EXECUTION_VARIANTS.len() * usize::try_from(seeds).unwrap_or(0),
    );
    for utilization in UTILIZATIONS {
        for protocol in EXECUTION_VARIANTS {
            for seed in 0..seeds {
                tasks.push(Task {
                    slice: Slice::Main,
                    profile: PROFILE,
                    placement: PLACEMENT,
                    utilization,
                    jitter: true,
                    protocol,
                    k: 8_192,
                    seed,
                    cadence: 2,
                    admission: ReceiverAdmissionPolicy::HybridDrop,
                    slow_receiver: None,
                    sessions: 1,
                    price_egress: true,
                    anchor_background: true,
                    flow_count_match_single_tree: false,
                    cloudcast_shared_topology: true,
                });
            }
        }
    }
    tasks
}

fn artifacts(
    trials: &[RawTrial],
) -> Result<CloudcastComparisonArtifacts, CloudcastComparisonError> {
    let reported = reported_trials(trials)?;
    let trial_rows = reported
        .iter()
        .map(|reported| TrialRow {
            schema_version: SCENARIO_SCHEMA_VERSION,
            simulator_version: SIMULATOR_VERSION,
            evidence_class: EVIDENCE_CLASS,
            profile: reported.trial.profile_name,
            placement: reported.trial.placement_id.clone(),
            utilization_percent: reported.trial.task.utilization,
            jitter: reported.trial.task.jitter,
            source_symbols: reported.trial.task.k,
            seed: reported.trial.task.seed,
            arm: reported.arm.label(),
            execution_variant: reported.execution_variant,
            barrier_completion_ns: reported.trial.barrier_ns,
            sender_completion_ns: reported.trial.sender_ns,
            modeled_egress_nano_usd: reported.trial.modeled_egress_nano_usd,
            foreground_egress_wire_bytes: reported.trial.foreground_egress_wire_bytes,
            total_emissions: reported.trial.emissions,
            application_drops: reported.trial.drops,
            link_drops: reported.trial.link_drops,
            maximum_mailbox_high_water: reported.trial.maximum_mailbox_high_water,
        })
        .collect::<Vec<_>>();
    Ok(CloudcastComparisonArtifacts {
        trials_csv: to_csv(&trial_rows)?,
        summaries_csv: to_csv(&summary_rows(&reported))?,
        policy_frontier_csv: to_csv(&policy_frontier_rows()?)?,
        sharing_csv: to_csv(&sharing_rows(trials))?,
    })
}

fn reported_trials<'a>(
    trials: &'a [RawTrial],
) -> Result<Vec<ReportedTrial<'a>>, CloudcastComparisonError> {
    let map = trials
        .iter()
        .map(|trial| {
            (
                (trial.task.utilization, trial.task.seed, trial.task.protocol),
                trial,
            )
        })
        .collect::<BTreeMap<_, _>>();
    let mut reported = Vec::with_capacity(UTILIZATIONS.len() * REQUIRED_SEEDS as usize * 4);
    for utilization in UTILIZATIONS {
        for seed in 0..REQUIRED_SEEDS {
            for (arm, protocol) in [
                (ReportedArm::CloudcastPolicy, WrProtocol::CloudcastPolicy),
                (ReportedArm::Carousel, WrProtocol::Carousel),
                (ReportedArm::PerStripeFec, WrProtocol::PerStripeFec),
            ] {
                let trial = find_trial(&map, utilization, seed, protocol, arm.label())?;
                reported.push(ReportedTrial {
                    arm,
                    execution_variant: protocol.evidence_name(),
                    trial,
                });
            }
            let tree0 = find_trial(
                &map,
                utilization,
                seed,
                WrProtocol::BestSingleTree0,
                BEST_SINGLE_LABEL,
            )?;
            let tree1 = find_trial(
                &map,
                utilization,
                seed,
                WrProtocol::BestSingleTree1,
                BEST_SINGLE_LABEL,
            )?;
            let selected = if (tree0.barrier_ns, tree0.sender_ns, tree0.task.protocol)
                <= (tree1.barrier_ns, tree1.sender_ns, tree1.task.protocol)
            {
                tree0
            } else {
                tree1
            };
            reported.push(ReportedTrial {
                arm: ReportedArm::BestSingle,
                execution_variant: selected.task.protocol.name(),
                trial: selected,
            });
        }
    }
    Ok(reported)
}

fn find_trial<'a>(
    map: &BTreeMap<(u8, u64, WrProtocol), &'a RawTrial>,
    utilization: u8,
    seed: u64,
    protocol: WrProtocol,
    arm: &'static str,
) -> Result<&'a RawTrial, CloudcastComparisonError> {
    map.get(&(utilization, seed, protocol))
        .copied()
        .ok_or(CloudcastComparisonError::MissingArm {
            utilization,
            seed,
            arm,
        })
}

fn summary_rows(reported: &[ReportedTrial<'_>]) -> Vec<SummaryRow> {
    let mut groups: BTreeMap<(u8, ReportedArm), Vec<&ReportedTrial<'_>>> = BTreeMap::new();
    for trial in reported {
        groups
            .entry((trial.trial.task.utilization, trial.arm))
            .or_default()
            .push(trial);
    }
    groups
        .into_iter()
        .map(|((utilization, arm), group)| {
            let barriers = values(&group, |trial| trial.barrier_ns);
            let senders = values(&group, |trial| trial.sender_ns);
            let costs = values(&group, |trial| trial.modeled_egress_nano_usd);
            let wire = values(&group, |trial| trial.foreground_egress_wire_bytes);
            let emissions = values(&group, |trial| {
                u64::try_from(trial.emissions).unwrap_or(u64::MAX)
            });
            SummaryRow {
                schema_version: SCENARIO_SCHEMA_VERSION,
                simulator_version: SIMULATOR_VERSION,
                evidence_class: EVIDENCE_CLASS,
                profile: PROFILE.name(),
                placement: "west-origin".to_owned(),
                utilization_percent: utilization,
                jitter: true,
                source_symbols: 8_192,
                seeds: group.len(),
                arm: arm.label(),
                barrier_mean_ns: mean_u64(&barriers),
                barrier_p5_ns: percentile_u64(&barriers, 5),
                barrier_p95_ns: percentile_u64(&barriers, 95),
                sender_mean_ns: mean_u64(&senders),
                sender_p5_ns: percentile_u64(&senders, 5),
                sender_p95_ns: percentile_u64(&senders, 95),
                modeled_egress_mean_nano_usd: mean_u64(&costs),
                modeled_egress_p5_nano_usd: percentile_u64(&costs, 5),
                modeled_egress_p95_nano_usd: percentile_u64(&costs, 95),
                foreground_wire_mean_bytes: mean_u64(&wire),
                emissions_mean: mean_u64(&emissions),
                application_drops_total: group.iter().map(|trial| trial.trial.drops).sum(),
                link_drops_total: group.iter().map(|trial| trial.trial.link_drops).sum(),
            }
        })
        .collect()
}

fn values(group: &[&ReportedTrial<'_>], project: impl Fn(&RawTrial) -> u64) -> Vec<u64> {
    group.iter().map(|trial| project(trial.trial)).collect()
}

fn policy_frontier_rows() -> Result<Vec<PolicyPlanRow>, CloudcastComparisonError> {
    let scenario =
        CloudScenario::built_in(PROFILE, PLACEMENT).map_err(CloudcastPolicyError::Scenario)?;
    let prices = representative_egress_prices(&scenario)?;
    let mut rows = Vec::new();
    for utilization in UTILIZATIONS {
        let request = CloudcastPolicyRequest {
            scenario: &scenario,
            egress_prices: &prices,
            source_symbols: 8_192,
            symbol_payload_bytes: WR_SYMBOL_PAYLOAD_BYTES,
            stripe_count: CLOUDCAST_STRIPE_COUNT,
            completion_budget_ns: CLOUDCAST_COMPLETION_BUDGET_NS,
            background_utilization_percent: utilization,
        };
        let frontier = cloudcast_policy_budget_frontier(request)?;
        for (frontier_index, plan) in frontier.into_iter().enumerate() {
            let relays = plan.relay_regions();
            let stripes = plan.tree_stripe_counts();
            let quotas = plan.tree_symbol_quotas();
            rows.push(PolicyPlanRow {
                schema_version: SCENARIO_SCHEMA_VERSION,
                simulator_version: SIMULATOR_VERSION,
                evidence_class: EVIDENCE_CLASS,
                arm: CLOUDCAST_LABEL,
                profile: PROFILE.name(),
                placement: scenario.placement.id.clone(),
                utilization_percent: utilization,
                source_symbols: 8_192,
                frontier_index,
                selected_for_comparison: frontier_index == 0,
                stripe_count: plan.stripe_count(),
                completion_budget_ns: plan.completion_budget_ns(),
                estimated_completion_ns: plan.estimated_completion_ns(),
                estimated_payload_egress_nano_usd: plan.estimated_egress_nano_usd(),
                candidate_tree_count: plan.candidate_tree_count(),
                evaluated_assignment_count: plan.evaluated_assignment_count(),
                tree0_relay_a: relays[0][0].clone(),
                tree0_relay_b: relays[0][1].clone(),
                tree1_relay_a: relays[1][0].clone(),
                tree1_relay_b: relays[1][1].clone(),
                tree0_stripes: stripes[0],
                tree1_stripes: stripes[1],
                tree0_source_symbols: quotas[0],
                tree1_source_symbols: quotas[1],
                stripe_tree_ids: plan
                    .stripe_tree_ids()
                    .iter()
                    .map(u8::to_string)
                    .collect::<Vec<_>>()
                    .join(";"),
            });
        }
        let selected = rows
            .iter()
            .find(|row| row.utilization_percent == utilization && row.selected_for_comparison)
            .ok_or(CloudcastComparisonError::MissingFrontier { utilization })?;
        let tighter = selected.completion_budget_ns - 1;
        if !matches!(
            plan_cloudcast_policy(CloudcastPolicyRequest {
                completion_budget_ns: tighter,
                ..request
            }),
            Err(CloudcastPolicyError::NoFeasiblePlan {
                completion_budget_ns
            }) if completion_budget_ns == tighter
        ) {
            return Err(CloudcastComparisonError::NonMinimalFrontier {
                utilization,
                minimum_budget_ns: selected.completion_budget_ns,
            });
        }
    }
    Ok(rows)
}

fn sharing_rows(trials: &[RawTrial]) -> Vec<SharingRow> {
    let mut rows = trials
        .iter()
        .filter(|trial| trial.task.seed == 0)
        .flat_map(|trial| {
            trial.sharing.iter().map(move |sharing| SharingRow {
                schema_version: SCENARIO_SCHEMA_VERSION,
                simulator_version: SIMULATOR_VERSION,
                evidence_class: EVIDENCE_CLASS,
                utilization_percent: trial.task.utilization,
                execution_variant: trial.task.protocol.evidence_name(),
                resource: sharing.resource.clone(),
                total_flow_directions: sharing.total_flow_directions,
                foreground_flow_directions: sharing.foreground_flow_directions,
                background_flow_directions: sharing.background_flow_directions,
            })
        })
        .collect::<Vec<_>>();
    rows.sort();
    rows.dedup();
    rows
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn comparison_matrix_is_exactly_the_requested_tier_one_scope() {
        let tasks = build_tasks(REQUIRED_SEEDS);
        assert_eq!(tasks.len(), 2 * 5 * 16);
        assert!(tasks.iter().all(|task| {
            task.profile == CloudProfileKind::DigitaloceanLike
                && task.placement == 1
                && [30, 70].contains(&task.utilization)
                && task.jitter
                && task.k == 8_192
                && task.price_egress
                && task.anchor_background
                && !task.flow_count_match_single_tree
                && task.cloudcast_shared_topology
                && task.slow_receiver.is_none()
                && task.sessions == 1
        }));
    }

    #[test]
    fn exact_policy_frontier_selects_two_trees_at_both_requested_loads() {
        let plans = policy_frontier_rows().expect("two feasible Cloudcast policy frontiers");
        assert!(plans.len() >= 2);
        assert!(plans.iter().all(|plan| {
            plan.arm == CLOUDCAST_LABEL
                && plan.stripe_count == 8
                && plan.tree0_source_symbols + plan.tree1_source_symbols == 8_192
                && plan.estimated_completion_ns <= plan.completion_budget_ns
        }));
        for utilization in UTILIZATIONS {
            let selected = plans
                .iter()
                .find(|plan| {
                    plan.utilization_percent == utilization && plan.selected_for_comparison
                })
                .expect("selected fastest point");
            assert!(selected.tree0_stripes > 0);
            assert!(selected.tree1_stripes > 0);
            assert_eq!(
                selected.completion_budget_ns,
                selected.estimated_completion_ns
            );
        }
    }
}
