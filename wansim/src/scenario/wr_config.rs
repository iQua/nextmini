use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::SCENARIO_SCHEMA_VERSION;

const EXPECTED_REGION_MIN: usize = 5;
const EXPECTED_REGION_MAX: usize = 6;
const PLACEMENT_RECEIVERS: usize = 3;
const TREE_COUNT: usize = 2;
const RELAYS_PER_TREE: usize = 2;

/// A representative public-cloud family, not the identity of a measured provider network.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(rename_all = "kebab-case")]
pub enum CloudProfileKind {
    AwsLike,
    GcpLike,
    DigitaloceanLike,
}

impl CloudProfileKind {
    pub const ALL: [Self; 3] = [Self::AwsLike, Self::GcpLike, Self::DigitaloceanLike];

    pub const fn name(self) -> &'static str {
        match self {
            Self::AwsLike => "aws-like",
            Self::GcpLike => "gcp-like",
            Self::DigitaloceanLike => "digitalocean-like",
        }
    }
}

/// A geographic region attached to one modeled provider transit hub.
///
/// `access_one_way_ns` is a representative public-cloud access magnitude. It is deliberately
/// synthesized from the WR envelope rather than copied from a probe or provider SLA.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct CloudRegion {
    pub id: String,
    pub hub: String,
    pub access_one_way_ns: u64,
}

/// One directed shared provider-backbone resource.
///
/// Capacity, propagation, and queue values are representative scenario inputs, never claims about
/// the named provider's private topology.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct CloudTrunk {
    pub id: String,
    pub from_hub: String,
    pub to_hub: String,
    pub capacity_bps: u64,
    pub propagation_ns: u64,
    pub queue_bytes: usize,
}

/// Placement of the fixed W1 receiver-covering overlay shape onto regions.
///
/// Relay pairs are selected by the deterministic placement generator. Tree 1 uses relay regions
/// disjoint from tree 0 whenever the region set makes that possible.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct CloudPlacement {
    pub id: String,
    pub sender_region: String,
    pub receiver_regions: [String; PLACEMENT_RECEIVERS],
    pub relay_regions: [[String; RELAYS_PER_TREE]; TREE_COUNT],
}

/// A committed WR scenario. Every numeric field is a representative public value, not a
/// measurement. The generated TOML repeats that qualification in comments.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct CloudScenario {
    pub schema_version: u32,
    pub profile: CloudProfileKind,
    pub representative_only: bool,
    pub vm_nic_cap_bps: u64,
    pub vm_nic_queue_bytes: usize,
    pub intra_region_rtt_ns: u64,
    pub jitter_max_ppm: u32,
    pub jitter_epoch_ns: u64,
    pub background_reference_rate_bps: u64,
    pub regions: Vec<CloudRegion>,
    pub trunks: Vec<CloudTrunk>,
    pub placement: CloudPlacement,
    pub rtt_matrix_ns: Vec<Vec<u64>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct HubRoute {
    pub trunk_indexes: Vec<usize>,
    pub propagation_ns: u64,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum CloudScenarioError {
    #[error("WR scenario schema {0} is unsupported")]
    Schema(u32),
    #[error("WR scenarios must be explicitly marked representative-only")]
    MeasurementClaim,
    #[error("WR profile requires 5-6 regions, got {0}")]
    RegionCount(usize),
    #[error("WR scenario contains an empty or duplicate identifier: {0}")]
    Identifier(String),
    #[error("WR scenario contains zero geometry: {0}")]
    ZeroGeometry(&'static str),
    #[error("WR placement references unknown region {0}")]
    UnknownRegion(String),
    #[error("WR placement must use three distinct receiver regions away from the sender")]
    ReceiverPlacement,
    #[error("WR relay placement must use two distinct relay regions per tree")]
    RelayPlacement,
    #[error("WR backbone has no directed route from {from} to {to}")]
    NoRoute { from: String, to: String },
    #[error("WR RTT matrix shape or derived value is invalid")]
    RttMatrix,
    #[error("WR RTT matrix has no directional asymmetry")]
    MissingAsymmetry,
    #[error("WR placement generator could not select disjoint relay pairs")]
    NoRelayPair,
    #[error("failed to serialize WR scenario TOML: {0}")]
    TomlSerialize(String),
    #[error("failed to parse WR scenario TOML: {0}")]
    TomlParse(String),
}

impl CloudScenario {
    pub fn built_in(
        profile: CloudProfileKind,
        placement_index: usize,
    ) -> Result<Self, CloudScenarioError> {
        let mut scenario = base_profile(profile);
        let template = placement_template(profile, placement_index).ok_or_else(|| {
            CloudScenarioError::Identifier(format!(
                "{} placement index {placement_index}",
                profile.name()
            ))
        })?;
        scenario.placement = select_relay_regions(&scenario, template)?;
        scenario.rtt_matrix_ns = scenario.derived_rtt_matrix()?;
        scenario.validate()?;
        Ok(scenario)
    }

    pub fn all_built_in() -> Result<Vec<Self>, CloudScenarioError> {
        let mut scenarios = Vec::with_capacity(CloudProfileKind::ALL.len() * 2);
        for profile in CloudProfileKind::ALL {
            for placement in 0..2 {
                scenarios.push(Self::built_in(profile, placement)?);
            }
        }
        Ok(scenarios)
    }

    pub fn scenario_id(&self) -> String {
        format!("{}-{}", self.profile.name(), self.placement.id)
    }

    pub fn file_name(&self) -> String {
        format!("{}.toml", self.scenario_id())
    }

    pub fn validate(&self) -> Result<(), CloudScenarioError> {
        if self.schema_version != SCENARIO_SCHEMA_VERSION {
            return Err(CloudScenarioError::Schema(self.schema_version));
        }
        if !self.representative_only {
            return Err(CloudScenarioError::MeasurementClaim);
        }
        if !(EXPECTED_REGION_MIN..=EXPECTED_REGION_MAX).contains(&self.regions.len()) {
            return Err(CloudScenarioError::RegionCount(self.regions.len()));
        }
        for (name, value) in [
            ("vm_nic_cap_bps", self.vm_nic_cap_bps),
            ("intra_region_rtt_ns", self.intra_region_rtt_ns),
            ("jitter_epoch_ns", self.jitter_epoch_ns),
            (
                "background_reference_rate_bps",
                self.background_reference_rate_bps,
            ),
        ] {
            if value == 0 {
                return Err(CloudScenarioError::ZeroGeometry(name));
            }
        }
        if self.vm_nic_queue_bytes == 0 || self.jitter_max_ppm == 0 {
            return Err(CloudScenarioError::ZeroGeometry(
                "vm_nic_queue_bytes or jitter_max_ppm",
            ));
        }
        let region_ids = unique_nonempty(self.regions.iter().map(|region| region.id.as_str()))?;
        let hubs = self
            .regions
            .iter()
            .map(|region| region.hub.as_str())
            .collect::<BTreeSet<_>>();
        if hubs.contains("") {
            return Err(CloudScenarioError::Identifier(String::new()));
        }
        unique_nonempty(self.trunks.iter().map(|trunk| trunk.id.as_str()))?;
        if self
            .regions
            .iter()
            .any(|region| region.access_one_way_ns == 0)
        {
            return Err(CloudScenarioError::ZeroGeometry("region access delay"));
        }
        for trunk in &self.trunks {
            if !hubs.contains(trunk.from_hub.as_str())
                || !hubs.contains(trunk.to_hub.as_str())
                || trunk.from_hub == trunk.to_hub
            {
                return Err(CloudScenarioError::Identifier(trunk.id.clone()));
            }
            if trunk.capacity_bps == 0 || trunk.propagation_ns == 0 || trunk.queue_bytes == 0 {
                return Err(CloudScenarioError::ZeroGeometry("backbone trunk"));
            }
        }
        let region_known = |region: &str| region_ids.contains(region);
        if !region_known(&self.placement.sender_region) {
            return Err(CloudScenarioError::UnknownRegion(
                self.placement.sender_region.clone(),
            ));
        }
        if self
            .placement
            .receiver_regions
            .iter()
            .any(|region| !region_known(region))
        {
            let region = self
                .placement
                .receiver_regions
                .iter()
                .find(|region| !region_known(region))
                .expect("an unknown receiver was found");
            return Err(CloudScenarioError::UnknownRegion(region.clone()));
        }
        let receiver_set: BTreeSet<_> = self.placement.receiver_regions.iter().collect();
        if receiver_set.len() != PLACEMENT_RECEIVERS
            || receiver_set.contains(&self.placement.sender_region)
        {
            return Err(CloudScenarioError::ReceiverPlacement);
        }
        for relays in &self.placement.relay_regions {
            if relays[0] == relays[1] || relays.iter().any(|region| !region_known(region)) {
                return Err(CloudScenarioError::RelayPlacement);
            }
        }
        for from in &self.regions {
            for to in &self.regions {
                if from.id != to.id {
                    self.hub_route(&from.hub, &to.hub)?;
                }
            }
        }
        let expected = self.derived_rtt_matrix()?;
        if expected != self.rtt_matrix_ns {
            return Err(CloudScenarioError::RttMatrix);
        }
        let mut asymmetric = false;
        for left in 0..self.regions.len() {
            for right in left + 1..self.regions.len() {
                let forward = self.directed_region_delay_ns(left, right)?;
                let reverse = self.directed_region_delay_ns(right, left)?;
                asymmetric |= forward != reverse;
            }
        }
        if !asymmetric {
            return Err(CloudScenarioError::MissingAsymmetry);
        }
        Ok(())
    }

    pub fn region_index(&self, id: &str) -> Result<usize, CloudScenarioError> {
        self.regions
            .iter()
            .position(|region| region.id == id)
            .ok_or_else(|| CloudScenarioError::UnknownRegion(id.to_owned()))
    }

    pub(crate) fn hub_route(&self, from: &str, to: &str) -> Result<HubRoute, CloudScenarioError> {
        if from == to {
            return Ok(HubRoute {
                trunk_indexes: Vec::new(),
                propagation_ns: 0,
            });
        }
        let mut hubs = self
            .regions
            .iter()
            .map(|region| region.hub.clone())
            .collect::<Vec<_>>();
        hubs.sort();
        hubs.dedup();
        let hub_indexes: BTreeMap<_, _> = hubs
            .iter()
            .enumerate()
            .map(|(index, hub)| (hub.as_str(), index))
            .collect();
        let Some(&start) = hub_indexes.get(from) else {
            return Err(CloudScenarioError::NoRoute {
                from: from.to_owned(),
                to: to.to_owned(),
            });
        };
        let Some(&goal) = hub_indexes.get(to) else {
            return Err(CloudScenarioError::NoRoute {
                from: from.to_owned(),
                to: to.to_owned(),
            });
        };
        let mut distances = vec![u64::MAX; hubs.len()];
        let mut previous: Vec<Option<(usize, usize)>> = vec![None; hubs.len()];
        let mut visited = vec![false; hubs.len()];
        distances[start] = 0;
        while let Some(current) = (0..hubs.len())
            .filter(|index| !visited[*index])
            .min_by_key(|index| (distances[*index], *index))
        {
            if distances[current] == u64::MAX || current == goal {
                break;
            }
            visited[current] = true;
            for (trunk_index, trunk) in self.trunks.iter().enumerate() {
                if trunk.from_hub != hubs[current] {
                    continue;
                }
                let Some(&next) = hub_indexes.get(trunk.to_hub.as_str()) else {
                    continue;
                };
                let candidate = distances[current].saturating_add(trunk.propagation_ns);
                if candidate < distances[next]
                    || (candidate == distances[next]
                        && previous[next].is_none_or(|(_, prior)| trunk_index < prior))
                {
                    distances[next] = candidate;
                    previous[next] = Some((current, trunk_index));
                }
            }
        }
        if distances[goal] == u64::MAX {
            return Err(CloudScenarioError::NoRoute {
                from: from.to_owned(),
                to: to.to_owned(),
            });
        }
        let mut trunk_indexes = Vec::new();
        let mut cursor = goal;
        while cursor != start {
            let Some((prior, trunk)) = previous[cursor] else {
                return Err(CloudScenarioError::NoRoute {
                    from: from.to_owned(),
                    to: to.to_owned(),
                });
            };
            trunk_indexes.push(trunk);
            cursor = prior;
        }
        trunk_indexes.reverse();
        Ok(HubRoute {
            trunk_indexes,
            propagation_ns: distances[goal],
        })
    }

    pub fn directed_region_delay_ns(
        &self,
        from: usize,
        to: usize,
    ) -> Result<u64, CloudScenarioError> {
        if from == to {
            return Ok(self.intra_region_rtt_ns / 2);
        }
        let Some(source) = self.regions.get(from) else {
            return Err(CloudScenarioError::RttMatrix);
        };
        let Some(destination) = self.regions.get(to) else {
            return Err(CloudScenarioError::RttMatrix);
        };
        let core = self.hub_route(&source.hub, &destination.hub)?;
        Ok(source
            .access_one_way_ns
            .saturating_add(core.propagation_ns)
            .saturating_add(destination.access_one_way_ns))
    }

    pub fn derived_rtt_matrix(&self) -> Result<Vec<Vec<u64>>, CloudScenarioError> {
        let mut matrix = vec![vec![0; self.regions.len()]; self.regions.len()];
        for (from, row) in matrix.iter_mut().enumerate() {
            for (to, value) in row.iter_mut().enumerate() {
                *value = if from == to {
                    self.intra_region_rtt_ns
                } else {
                    self.directed_region_delay_ns(from, to)?
                        .saturating_add(self.directed_region_delay_ns(to, from)?)
                };
            }
        }
        Ok(matrix)
    }

    pub fn to_commented_toml(&self) -> Result<String, CloudScenarioError> {
        let body = toml::to_string_pretty(self)
            .map_err(|error| CloudScenarioError::TomlSerialize(error.to_string()))?;
        Ok(format!(
            "# Generated deterministic WR scenario. DO NOT interpret any value as a measurement.\n\
# Region names and provider-network facts follow public provider documentation; latency,\n\
# capacity, queue, jitter, and topology magnitudes are representative public-cloud envelope\n\
# inputs synthesized from plans/wansim-plan.md, not provider guarantees or reverse engineering.\n\
# vm_nic_cap_bps: representative VM ceiling; vm_nic_queue_bytes: modeled finite NIC queue.\n\
# intra_region_rtt_ns/access_one_way_ns/trunk propagation_ns: representative latency inputs.\n\
# jitter_max_ppm/jitter_epoch_ns: +/-5 percent piecewise-constant delay, updated every 100 ms.\n\
# background_reference_rate_bps: denominator for the 30/50/70 percent offered-load sweep.\n\
# trunk capacity_bps/queue_bytes: modeled shared transit service, not physical inventory.\n\
# relay_regions and rtt_matrix_ns are deterministic generator outputs.\n\
# Public identity/cap context only (not RTT/topology measurements): AWS regions and EC2 network\n\
# bandwidth docs; Google Cloud locations, Network Service Tiers, and VM bandwidth docs; and\n\
# DigitalOcean regional-availability and Droplet-network-limit docs. Exact URLs are recorded in\n\
# plans/wansim-wr-report.md.\n\
{body}"
        ))
    }

    pub fn from_toml(input: &str) -> Result<Self, CloudScenarioError> {
        let scenario: Self = toml::from_str(input)
            .map_err(|error| CloudScenarioError::TomlParse(error.to_string()))?;
        scenario.validate()?;
        Ok(scenario)
    }
}

#[derive(Clone, Copy)]
struct PlacementTemplate {
    id: &'static str,
    sender: usize,
    receivers: [usize; PLACEMENT_RECEIVERS],
}

fn placement_template(_profile: CloudProfileKind, index: usize) -> Option<PlacementTemplate> {
    match index {
        0 => Some(PlacementTemplate {
            id: "east-origin",
            sender: 0,
            receivers: [2, 3, 5],
        }),
        1 => Some(PlacementTemplate {
            id: "west-origin",
            sender: 2,
            receivers: [0, 4, 5],
        }),
        _ => None,
    }
}

fn select_relay_regions(
    scenario: &CloudScenario,
    template: PlacementTemplate,
) -> Result<CloudPlacement, CloudScenarioError> {
    let candidates = (0..scenario.regions.len())
        .filter(|region| *region != template.sender)
        .collect::<Vec<_>>();
    let mut pairs = Vec::new();
    for &relay_a in &candidates {
        for &relay_b in &candidates {
            if relay_a == relay_b {
                continue;
            }
            let score = overlay_score(scenario, template, relay_a, relay_b)?;
            pairs.push((score, relay_a, relay_b));
        }
    }
    pairs.sort();
    let Some(&(_, first_a, first_b)) = pairs.first() else {
        return Err(CloudScenarioError::NoRelayPair);
    };
    let second = pairs
        .iter()
        .copied()
        .find(|(_, relay_a, relay_b)| {
            ![first_a, first_b].contains(relay_a) && ![first_a, first_b].contains(relay_b)
        })
        .or_else(|| {
            pairs
                .iter()
                .copied()
                .find(|(_, relay_a, relay_b)| (*relay_a, *relay_b) != (first_a, first_b))
        })
        .ok_or(CloudScenarioError::NoRelayPair)?;
    Ok(CloudPlacement {
        id: template.id.to_owned(),
        sender_region: scenario.regions[template.sender].id.clone(),
        receiver_regions: template
            .receivers
            .map(|region| scenario.regions[region].id.clone()),
        relay_regions: [
            [
                scenario.regions[first_a].id.clone(),
                scenario.regions[first_b].id.clone(),
            ],
            [
                scenario.regions[second.1].id.clone(),
                scenario.regions[second.2].id.clone(),
            ],
        ],
    })
}

fn overlay_score(
    scenario: &CloudScenario,
    template: PlacementTemplate,
    relay_a: usize,
    relay_b: usize,
) -> Result<u64, CloudScenarioError> {
    let edges = [
        (template.sender, relay_a),
        (relay_a, template.receivers[0]),
        (relay_a, relay_b),
        (relay_b, template.receivers[1]),
        (relay_b, template.receivers[2]),
    ];
    edges.into_iter().try_fold(0_u64, |score, (from, to)| {
        Ok(score.saturating_add(scenario.directed_region_delay_ns(from, to)?))
    })
}

fn unique_nonempty<'a>(
    values: impl Iterator<Item = &'a str>,
) -> Result<BTreeSet<&'a str>, CloudScenarioError> {
    let mut set = BTreeSet::new();
    for value in values {
        if value.is_empty() || !set.insert(value) {
            return Err(CloudScenarioError::Identifier(value.to_owned()));
        }
    }
    Ok(set)
}

fn base_profile(profile: CloudProfileKind) -> CloudScenario {
    let (regions, vm_nic_cap_bps, vm_nic_queue_bytes, core_rate, core_queue, delays) = match profile
    {
        CloudProfileKind::AwsLike => (
            region_specs([
                ("us-east-1", "na-east", 3_000_000),
                ("us-east-2", "na-east", 3_500_000),
                ("us-west-2", "na-west", 3_000_000),
                ("eu-west-1", "eu", 3_000_000),
                ("eu-central-1", "eu", 3_500_000),
                ("ap-northeast-1", "ap", 3_500_000),
            ]),
            2_000_000_000,
            4 * 1024 * 1024,
            1_200_000_000,
            8 * 1024 * 1024,
            [22, 24, 32, 34, 43, 45, 58, 61, 37, 39],
        ),
        CloudProfileKind::GcpLike => (
            region_specs([
                ("us-east4", "na-east", 2_500_000),
                ("us-central1", "na-east", 3_500_000),
                ("us-west1", "na-west", 2_500_000),
                ("europe-west1", "eu", 2_500_000),
                ("europe-west3", "eu", 3_000_000),
                ("asia-northeast1", "ap", 3_000_000),
            ]),
            3_000_000_000,
            6 * 1024 * 1024,
            1_600_000_000,
            10 * 1024 * 1024,
            [22, 23, 31, 32, 44, 46, 55, 57, 38, 39],
        ),
        CloudProfileKind::DigitaloceanLike => (
            region_specs([
                ("nyc3", "na-east", 3_000_000),
                ("tor1", "na-east", 4_000_000),
                ("sfo3", "na-west", 3_500_000),
                ("lon1", "eu", 3_000_000),
                ("fra1", "eu", 3_500_000),
                ("sgp1", "ap", 4_500_000),
            ]),
            2_000_000_000,
            3 * 1024 * 1024,
            800_000_000,
            6 * 1024 * 1024,
            [22, 24, 35, 37, 41, 44, 62, 65, 36, 39],
        ),
    };
    CloudScenario {
        schema_version: SCENARIO_SCHEMA_VERSION,
        profile,
        representative_only: true,
        vm_nic_cap_bps,
        vm_nic_queue_bytes,
        intra_region_rtt_ns: 1_500_000,
        jitter_max_ppm: 50_000,
        jitter_epoch_ns: 100_000_000,
        background_reference_rate_bps: core_rate,
        regions,
        trunks: trunk_specs(core_rate, core_queue, delays),
        placement: CloudPlacement {
            id: String::new(),
            sender_region: String::new(),
            receiver_regions: std::array::from_fn(|_| String::new()),
            relay_regions: std::array::from_fn(|_| std::array::from_fn(|_| String::new())),
        },
        rtt_matrix_ns: Vec::new(),
    }
}

fn region_specs<const N: usize>(values: [(&str, &str, u64); N]) -> Vec<CloudRegion> {
    values
        .into_iter()
        .map(|(id, hub, access_one_way_ns)| CloudRegion {
            id: id.to_owned(),
            hub: hub.to_owned(),
            access_one_way_ns,
        })
        .collect()
}

fn trunk_specs(capacity_bps: u64, queue_bytes: usize, delay_ms: [u64; 10]) -> Vec<CloudTrunk> {
    let pairs = [
        ("na-east", "na-west", delay_ms[0]),
        ("na-west", "na-east", delay_ms[1]),
        ("na-east", "eu", delay_ms[2]),
        ("eu", "na-east", delay_ms[3]),
        ("na-west", "ap", delay_ms[4]),
        ("ap", "na-west", delay_ms[5]),
        ("eu", "ap", delay_ms[6]),
        ("ap", "eu", delay_ms[7]),
        ("na-west", "eu", delay_ms[8]),
        ("eu", "na-west", delay_ms[9]),
    ];
    pairs
        .into_iter()
        .map(|(from, to, milliseconds)| CloudTrunk {
            id: format!("{from}-to-{to}"),
            from_hub: from.to_owned(),
            to_hub: to.to_owned(),
            capacity_bps,
            propagation_ns: milliseconds.saturating_mul(1_000_000),
            queue_bytes,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn built_in_profiles_validate_and_have_two_disjoint_relay_trees() {
        for scenario in CloudScenario::all_built_in().expect("built-in scenarios") {
            scenario.validate().expect("valid scenario");
            let first: BTreeSet<_> = scenario.placement.relay_regions[0].iter().collect();
            let second: BTreeSet<_> = scenario.placement.relay_regions[1].iter().collect();
            assert!(first.is_disjoint(&second));
        }
    }

    #[test]
    fn public_envelope_contains_expected_latency_regimes_and_asymmetry() {
        for scenario in CloudScenario::all_built_in().expect("built-in scenarios") {
            let matrix = &scenario.rtt_matrix_ns;
            assert!(
                matrix
                    .iter()
                    .flatten()
                    .any(|rtt| (1_000_000..=2_000_000).contains(rtt))
            );
            assert!(
                matrix
                    .iter()
                    .flatten()
                    .any(|rtt| (10_000_000..=60_000_000).contains(rtt))
            );
            assert!(
                matrix
                    .iter()
                    .flatten()
                    .any(|rtt| (70_000_000..=90_000_000).contains(rtt))
            );
            assert!(
                matrix
                    .iter()
                    .flatten()
                    .any(|rtt| (100_000_000..=150_000_000).contains(rtt))
            );
            let maximum = matrix.iter().flatten().copied().max().unwrap_or(0);
            assert!(
                maximum <= 150_000_000,
                "{} maximum RTT is {maximum}",
                scenario.scenario_id()
            );
            let mut directional_difference = false;
            for left in 0..scenario.regions.len() {
                for right in left + 1..scenario.regions.len() {
                    directional_difference |= scenario
                        .directed_region_delay_ns(left, right)
                        .expect("forward")
                        != scenario
                            .directed_region_delay_ns(right, left)
                            .expect("reverse");
                }
            }
            assert!(directional_difference);
        }
    }

    #[test]
    fn commented_toml_roundtrips_without_measurement_claims() {
        for scenario in CloudScenario::all_built_in().expect("built-in scenarios") {
            let toml = scenario.to_commented_toml().expect("serialize");
            assert!(toml.contains("DO NOT interpret any value as a measurement"));
            assert_eq!(CloudScenario::from_toml(&toml).expect("parse"), scenario);
        }
    }
}
