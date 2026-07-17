use thiserror::Error;

use crate::protocol::{CarouselTiming, ProtocolKind, equal_quotas, proportional_quotas};

use super::{FanoutAdmission, RegistrationOrder};

const HOMOGENEOUS_RATE_BPS: u64 = 80_000_000;
const FAST_RATE_BPS: u64 = 80_000_000;
const SLOW_RATE_BPS: u64 = 20_000_000;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum W1RateProfile {
    Homogeneous,
    CrossedHeterogeneous,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct W1CouplingConfig {
    pub overlap_percent: u8,
    pub aggregate_rate_bps: u64,
    pub aggregate_queue_bytes: usize,
    pub background_traffic: bool,
    /// The otherwise-unused lane receives a saturated matched flow in a best-single-tree run.
    pub flow_count_match_lane: Option<usize>,
}

impl W1RateProfile {
    pub const ALL: [Self; 2] = [Self::Homogeneous, Self::CrossedHeterogeneous];

    pub const fn name(self) -> &'static str {
        match self {
            Self::Homogeneous => "homogeneous",
            Self::CrossedHeterogeneous => "crossed_heterogeneous",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct W1Scenario {
    pub scenario_id: String,
    pub master_seed: u64,
    pub protocol: ProtocolKind,
    pub rate_profile: W1RateProfile,
    /// Test-only/experiment override for the two trees' five directed data-hop rates.
    ///
    /// Production experiment grids leave this as `None`; validation cases use it to isolate
    /// physical-resource independence without adding another named rate profile.
    pub data_rate_override_bps: Option<[[u64; 5]; 2]>,
    /// Test-only/experiment override for exact stripe ownership.
    pub quota_override: Option<[usize; 2]>,
    pub coupling: Option<W1CouplingConfig>,
    pub active_receivers: usize,
    pub source_symbols: usize,
    pub frame_payload_bytes: usize,
    pub tcp_mss_bytes: usize,
    pub socket_send_buffer_bytes: usize,
    pub socket_receive_buffer_bytes: usize,
    pub relay_application_buffer_bytes: usize,
    pub relay_child_queue_bytes: usize,
    pub runtime_command_capacity_frames: usize,
    pub receiver_data_inbox_capacity_frames: usize,
    pub runtime_command_service_ns: u64,
    pub decoder_sink_service_ns: u64,
    pub link_propagation_ns: u64,
    pub link_queue_bytes: usize,
    pub control_rate_bps: u64,
    pub control_propagation_ns: [u64; 3],
    pub simulation_end_ns: u64,
    pub timer_interval_ns: u64,
    pub initial_rto_ns: u64,
    pub persist_interval_ns: u64,
    pub fanout_admission: FanoutAdmission,
    pub registration_order: RegistrationOrder,
    pub carousel: CarouselTiming,
}

impl W1Scenario {
    pub fn experiment0(
        protocol: ProtocolKind,
        rate_profile: W1RateProfile,
        active_receivers: usize,
        seed: u64,
    ) -> Self {
        let source_symbols = 64;
        let maximum_frames = source_symbols * 8;
        Self {
            scenario_id: format!(
                "w1-e0-{}-{}-r{}-s{}",
                protocol.name(),
                rate_profile.name(),
                active_receivers,
                seed
            ),
            master_seed: seed,
            protocol,
            rate_profile,
            data_rate_override_bps: None,
            quota_override: None,
            coupling: None,
            active_receivers,
            source_symbols,
            frame_payload_bytes: 508,
            tcp_mss_bytes: 512,
            socket_send_buffer_bytes: 2_048,
            socket_receive_buffer_bytes: 2_048,
            relay_application_buffer_bytes: 8_192,
            relay_child_queue_bytes: 8_192,
            runtime_command_capacity_frames: maximum_frames * 2,
            receiver_data_inbox_capacity_frames: maximum_frames * 2,
            runtime_command_service_ns: 10_000,
            decoder_sink_service_ns: 10_000,
            link_propagation_ns: 1_000_000,
            link_queue_bytes: 65_536,
            control_rate_bps: 100_000_000,
            control_propagation_ns: [2_000_000, 3_000_000, 3_000_000],
            simulation_end_ns: 1_000_000_000,
            timer_interval_ns: 50_000,
            initial_rto_ns: 50_000_000,
            persist_interval_ns: 5_000_000,
            fanout_admission: FanoutAdmission::Sequential,
            registration_order: RegistrationOrder::Forward,
            carousel: CarouselTiming {
                ack_debounce_ns: 500_000,
                ack_heartbeat_ns: 5_000_000,
                ack_probe_interval_ns: 4_000_000,
                peer_silence_timeout_ns: 100_000_000,
                peer_stall_timeout_ns: 500_000_000,
                receiver_passive_window_ns: 750_000_000,
                session_complete_repeats: 3,
                session_complete_interval_ns: 100_000,
            },
        }
    }

    pub fn w3_coupling(
        protocol: ProtocolKind,
        overlap_percent: u8,
        best_single_tree: Option<usize>,
        seed: u64,
    ) -> Self {
        let mut scenario = Self::experiment0(protocol, W1RateProfile::Homogeneous, 3, seed);
        let best_label =
            best_single_tree.map_or("two-tree".to_owned(), |tree| format!("best-tree{tree}"));
        scenario.scenario_id = format!(
            "w3-{}-overlap{}-{}-s{}",
            protocol.name(),
            overlap_percent,
            best_label,
            seed
        );
        scenario.source_symbols = 512;
        scenario.quota_override = best_single_tree.map(|tree| {
            if tree == 0 {
                [scenario.source_symbols, 0]
            } else {
                [0, scenario.source_symbols]
            }
        });
        scenario.coupling = Some(W1CouplingConfig {
            overlap_percent,
            aggregate_rate_bps: 160_000_000,
            aggregate_queue_bytes: 131_072,
            background_traffic: true,
            flow_count_match_lane: best_single_tree.map(|tree| 1 - tree),
        });
        scenario.data_rate_override_bps = Some([[160_000_000; 5]; 2]);
        scenario.socket_send_buffer_bytes = 4_096;
        scenario.socket_receive_buffer_bytes = 4_096;
        scenario.relay_application_buffer_bytes = 65_536;
        scenario.relay_child_queue_bytes = 65_536;
        scenario.runtime_command_capacity_frames = 2_048;
        scenario.receiver_data_inbox_capacity_frames = 256;
        scenario.runtime_command_service_ns = 5_000;
        scenario.decoder_sink_service_ns = 10_000;
        scenario.link_queue_bytes = 131_072;
        scenario.simulation_end_ns = 5_000_000_000;
        scenario.timer_interval_ns = 100_000;
        scenario.carousel.peer_silence_timeout_ns = 500_000_000;
        scenario.carousel.peer_stall_timeout_ns = 4_000_000_000;
        scenario.carousel.receiver_passive_window_ns = 4_500_000_000;
        scenario
    }

    pub fn validate(&self) -> Result<(), W1ScenarioError> {
        if self.scenario_id.is_empty() {
            return Err(W1ScenarioError::EmptyScenarioId);
        }
        if !matches!(self.active_receivers, 1 | 3) {
            return Err(W1ScenarioError::ReceiverCount(self.active_receivers));
        }
        for (field, value) in [
            ("source_symbols", self.source_symbols),
            ("frame_payload_bytes", self.frame_payload_bytes),
            ("tcp_mss_bytes", self.tcp_mss_bytes),
            ("socket_send_buffer_bytes", self.socket_send_buffer_bytes),
            (
                "socket_receive_buffer_bytes",
                self.socket_receive_buffer_bytes,
            ),
            (
                "relay_application_buffer_bytes",
                self.relay_application_buffer_bytes,
            ),
            ("relay_child_queue_bytes", self.relay_child_queue_bytes),
            (
                "runtime_command_capacity_frames",
                self.runtime_command_capacity_frames,
            ),
            (
                "receiver_data_inbox_capacity_frames",
                self.receiver_data_inbox_capacity_frames,
            ),
            ("link_queue_bytes", self.link_queue_bytes),
        ] {
            if value == 0 {
                return Err(W1ScenarioError::ZeroValue(field));
            }
        }
        for (field, value) in [
            (
                "runtime_command_service_ns",
                self.runtime_command_service_ns,
            ),
            ("decoder_sink_service_ns", self.decoder_sink_service_ns),
            ("link_propagation_ns", self.link_propagation_ns),
            ("control_rate_bps", self.control_rate_bps),
            ("simulation_end_ns", self.simulation_end_ns),
            ("timer_interval_ns", self.timer_interval_ns),
            ("initial_rto_ns", self.initial_rto_ns),
            ("persist_interval_ns", self.persist_interval_ns),
        ] {
            if value == 0 {
                return Err(W1ScenarioError::ZeroU64Value(field));
            }
        }
        if self.control_propagation_ns.contains(&0) {
            return Err(W1ScenarioError::ZeroU64Value("control_propagation_ns"));
        }
        if self
            .data_rate_override_bps
            .is_some_and(|rates| rates.into_iter().flatten().any(|rate| rate == 0))
        {
            return Err(W1ScenarioError::ZeroU64Value("data_rate_override_bps"));
        }
        if let Some(coupling) = self.coupling {
            if !matches!(coupling.overlap_percent, 0 | 25 | 50 | 100) {
                return Err(W1ScenarioError::CouplingOverlap(coupling.overlap_percent));
            }
            if coupling.aggregate_rate_bps < 2 || coupling.aggregate_queue_bytes < 2 {
                return Err(W1ScenarioError::CouplingGeometry);
            }
            if coupling.flow_count_match_lane.is_some_and(|lane| lane > 1) {
                return Err(W1ScenarioError::CouplingMatchLane);
            }
        }
        let frame_wire_bytes = self.frame_wire_bytes()?;
        if self.tcp_mss_bytes < frame_wire_bytes {
            return Err(W1ScenarioError::MssBelowFrame);
        }
        if self.socket_send_buffer_bytes < frame_wire_bytes
            || self.socket_receive_buffer_bytes < frame_wire_bytes
            || self.relay_application_buffer_bytes < frame_wire_bytes
            || self.relay_child_queue_bytes < frame_wire_bytes
        {
            return Err(W1ScenarioError::BufferBelowFrame);
        }
        self.carousel.validate()?;
        let quotas = self.quotas()?;
        if quotas.iter().sum::<usize>() != self.source_symbols {
            return Err(W1ScenarioError::QuotaMismatch);
        }
        Ok(())
    }

    pub fn frame_wire_bytes(&self) -> Result<usize, W1ScenarioError> {
        self.frame_payload_bytes
            .checked_add(4)
            .ok_or(W1ScenarioError::GeometryOverflow)
    }

    pub fn maximum_frames_per_tree(&self) -> Result<usize, W1ScenarioError> {
        self.source_symbols
            .checked_mul(8)
            .ok_or(W1ScenarioError::GeometryOverflow)
    }

    pub fn data_rates_bps(&self) -> [[u64; 5]; 2] {
        if let Some(rates) = self.data_rate_override_bps {
            return rates;
        }
        match self.rate_profile {
            W1RateProfile::Homogeneous => [[HOMOGENEOUS_RATE_BPS; 5]; 2],
            W1RateProfile::CrossedHeterogeneous => [
                [
                    FAST_RATE_BPS,
                    FAST_RATE_BPS,
                    SLOW_RATE_BPS,
                    SLOW_RATE_BPS,
                    SLOW_RATE_BPS,
                ],
                [
                    FAST_RATE_BPS,
                    SLOW_RATE_BPS,
                    FAST_RATE_BPS,
                    FAST_RATE_BPS,
                    FAST_RATE_BPS,
                ],
            ],
        }
    }

    pub fn quotas(&self) -> Result<Vec<usize>, W1ScenarioError> {
        if let Some(quotas) = self.quota_override {
            return Ok(quotas.into());
        }
        match self.protocol {
            ProtocolKind::EqualSplitStriping
            | ProtocolKind::PooledRounds
            | ProtocolKind::PooledCarousel => {
                equal_quotas(self.source_symbols, 2).ok_or(W1ScenarioError::QuotaMismatch)
            }
            ProtocolKind::RateProportionalStriping | ProtocolKind::PerStripeFec => {
                let weights = match (self.rate_profile, self.active_receivers) {
                    (W1RateProfile::CrossedHeterogeneous, 1) => [FAST_RATE_BPS, SLOW_RATE_BPS],
                    _ => [1, 1],
                };
                proportional_quotas(self.source_symbols, &weights)
                    .ok_or(W1ScenarioError::QuotaMismatch)
            }
        }
    }
}

#[derive(Debug, Error)]
pub enum W1ScenarioError {
    #[error("W1 scenario_id must not be empty")]
    EmptyScenarioId,
    #[error("W1 supports exactly 1 or 3 active receivers, got {0}")]
    ReceiverCount(usize),
    #[error("{0} must be nonzero")]
    ZeroValue(&'static str),
    #[error("{0} must be nonzero")]
    ZeroU64Value(&'static str),
    #[error("W1 scenario geometry overflow")]
    GeometryOverflow,
    #[error("TCP MSS must contain one complete W1 logical frame")]
    MssBelowFrame,
    #[error("every modeled W1 buffer must contain at least one complete frame")]
    BufferBelowFrame,
    #[error("W1 stripe quotas do not sum exactly to K")]
    QuotaMismatch,
    #[error("W1 coupling overlap must be 0, 25, 50, or 100 percent, got {0}")]
    CouplingOverlap(u8),
    #[error("W1 coupling aggregate rate and queue geometry must support two lanes")]
    CouplingGeometry,
    #[error("W1 coupling flow-count match lane must be 0 or 1")]
    CouplingMatchLane,
    #[error(transparent)]
    Carousel(#[from] crate::protocol::CarouselConfigError),
}
