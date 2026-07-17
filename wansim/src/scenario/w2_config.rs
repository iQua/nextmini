use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::protocol::CarouselTiming;

use super::{FanoutAdmission, RegistrationOrder};

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum ReceiverAdmissionPolicy {
    HybridDrop,
    NaiveBlocking,
    IsolatedCredit,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum ReceiverServiceRate {
    One,
    Half,
    Tenth,
}

impl ReceiverServiceRate {
    pub const ALL: [Self; 3] = [Self::One, Self::Half, Self::Tenth];

    pub const fn name(self) -> &'static str {
        match self {
            Self::One => "1x",
            Self::Half => "1_2x",
            Self::Tenth => "1_10x",
        }
    }

    pub const fn service_multiplier(self) -> u64 {
        match self {
            Self::One => 1,
            Self::Half => 2,
            Self::Tenth => 10,
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum BufferBudget {
    QuarterBdp,
    OneBdp,
    FourBdp,
}

impl BufferBudget {
    pub const ALL: [Self; 3] = [Self::QuarterBdp, Self::OneBdp, Self::FourBdp];

    pub const fn name(self) -> &'static str {
        match self {
            Self::QuarterBdp => "0.25_bdp",
            Self::OneBdp => "1_bdp",
            Self::FourBdp => "4_bdp",
        }
    }

    const fn ratio(self) -> (usize, usize) {
        match self {
            Self::QuarterBdp => (1, 4),
            Self::OneBdp => (1, 1),
            Self::FourBdp => (4, 1),
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum ChildOrder {
    SlowFirst,
    SlowLast,
}

impl ChildOrder {
    pub const ALL: [Self; 2] = [Self::SlowFirst, Self::SlowLast];

    pub const fn name(self) -> &'static str {
        match self {
            Self::SlowFirst => "slow_first",
            Self::SlowLast => "slow_last",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BufferGeometry {
    pub per_hop_bdp_bytes: usize,
    pub scaled_per_hop_budget_bytes: usize,
    pub socket_send_bytes: usize,
    pub socket_receive_bytes: usize,
    pub link_queue_bytes: usize,
    pub relay_application_bytes: usize,
    pub relay_child_queue_bytes: usize,
    pub runtime_command_frames: usize,
    pub receiver_data_inbox_frames: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct W2ControlAsymmetry {
    pub reverse_rate_bps: u64,
    pub reverse_propagation_ns: u64,
    pub reverse_background_bursts: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct W2SharedLeafBottleneck {
    pub receivers: [usize; 2],
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct W2Scenario {
    pub scenario_id: String,
    pub master_seed: u64,
    pub admission_policy: ReceiverAdmissionPolicy,
    pub slow_service_rate: ReceiverServiceRate,
    pub buffer_budget: BufferBudget,
    pub receiver_count: usize,
    pub fanout_degree: usize,
    pub child_order: ChildOrder,
    pub slow_receiver_count: usize,
    pub source_symbols: usize,
    pub frame_payload_bytes: usize,
    pub tcp_mss_bytes: usize,
    pub data_rate_bps: u64,
    pub link_propagation_ns: u64,
    pub control_rate_bps: u64,
    pub runtime_command_service_ns: u64,
    pub healthy_decoder_sink_service_ns: u64,
    pub simulation_end_ns: u64,
    pub timer_interval_ns: u64,
    pub initial_rto_ns: u64,
    pub persist_interval_ns: u64,
    pub fanout_admission: FanoutAdmission,
    pub registration_order: RegistrationOrder,
    pub carousel: CarouselTiming,
    pub w3_control_asymmetry: Option<W2ControlAsymmetry>,
    pub w3_shared_leaf_bottleneck: Option<W2SharedLeafBottleneck>,
}

impl W2Scenario {
    pub fn screening(
        admission_policy: ReceiverAdmissionPolicy,
        slow_service_rate: ReceiverServiceRate,
        buffer_budget: BufferBudget,
        receiver_count: usize,
        fanout_degree: usize,
        child_order: ChildOrder,
        seed: u64,
    ) -> Self {
        Self {
            scenario_id: format!(
                "w2-{}-{}-{}-r{}-f{}-{}-s{}",
                admission_policy.name(),
                slow_service_rate.name(),
                buffer_budget.name(),
                receiver_count,
                fanout_degree,
                child_order.name(),
                seed
            ),
            master_seed: seed,
            admission_policy,
            slow_service_rate,
            buffer_budget,
            receiver_count,
            fanout_degree,
            child_order,
            slow_receiver_count: 1,
            source_symbols: 512,
            frame_payload_bytes: 508,
            tcp_mss_bytes: 512,
            data_rate_bps: 80_000_000,
            link_propagation_ns: 1_000_000,
            control_rate_bps: 100_000_000,
            runtime_command_service_ns: 5_000,
            healthy_decoder_sink_service_ns: 100_000,
            simulation_end_ns: 3_000_000_000,
            timer_interval_ns: 50_000,
            initial_rto_ns: 50_000_000,
            persist_interval_ns: 5_000_000,
            fanout_admission: FanoutAdmission::Sequential,
            registration_order: RegistrationOrder::Forward,
            carousel: CarouselTiming {
                ack_debounce_ns: 500_000,
                ack_heartbeat_ns: 5_000_000,
                ack_probe_interval_ns: 4_000_000,
                peer_silence_timeout_ns: 250_000_000,
                peer_stall_timeout_ns: 2_000_000_000,
                receiver_passive_window_ns: 2_500_000_000,
                session_complete_repeats: 3,
                session_complete_interval_ns: 100_000,
            },
            w3_control_asymmetry: None,
            w3_shared_leaf_bottleneck: None,
        }
    }

    pub fn validate(&self) -> Result<(), W2ScenarioError> {
        if self.scenario_id.is_empty() {
            return Err(W2ScenarioError::EmptyScenarioId);
        }
        if !matches!(self.receiver_count, 3 | 8) {
            return Err(W2ScenarioError::ReceiverCount(self.receiver_count));
        }
        if !matches!(self.fanout_degree, 2 | 4) {
            return Err(W2ScenarioError::FanoutDegree(self.fanout_degree));
        }
        if self.slow_receiver_count > 1 || self.slow_receiver_count > self.receiver_count {
            return Err(W2ScenarioError::SlowReceiverCount(self.slow_receiver_count));
        }
        if self.source_symbols == 0
            || self.frame_payload_bytes == 0
            || self.tcp_mss_bytes == 0
            || self.data_rate_bps == 0
            || self.link_propagation_ns == 0
            || self.control_rate_bps == 0
            || self.runtime_command_service_ns == 0
            || self.healthy_decoder_sink_service_ns == 0
            || self.simulation_end_ns == 0
            || self.timer_interval_ns == 0
            || self.initial_rto_ns == 0
            || self.persist_interval_ns == 0
        {
            return Err(W2ScenarioError::ZeroGeometry);
        }
        if self.tcp_mss_bytes < self.frame_wire_bytes()? {
            return Err(W2ScenarioError::MssBelowFrame);
        }
        self.carousel.validate()?;
        if let Some(control) = self.w3_control_asymmetry
            && (self.receiver_count != 8
                || control.reverse_rate_bps == 0
                || control.reverse_propagation_ns == 0)
        {
            return Err(W2ScenarioError::ControlAsymmetryGeometry);
        }
        if let Some(shared) = self.w3_shared_leaf_bottleneck {
            let [left, right] = shared.receivers;
            if left == right || left >= self.receiver_count || right >= self.receiver_count {
                return Err(W2ScenarioError::SharedLeafGeometry);
            }
        }
        let geometry = self.buffer_geometry()?;
        if [
            geometry.socket_send_bytes,
            geometry.socket_receive_bytes,
            geometry.link_queue_bytes,
            geometry.relay_application_bytes,
            geometry.relay_child_queue_bytes,
        ]
        .into_iter()
        .any(|bytes| bytes < self.frame_wire_bytes().unwrap_or(usize::MAX))
        {
            return Err(W2ScenarioError::BufferBelowFrame);
        }
        Ok(())
    }

    pub fn frame_wire_bytes(&self) -> Result<usize, W2ScenarioError> {
        self.frame_payload_bytes
            .checked_add(4)
            .ok_or(W2ScenarioError::GeometryOverflow)
    }

    pub fn maximum_frames_per_tree(&self) -> Result<usize, W2ScenarioError> {
        self.source_symbols
            .checked_mul(16)
            .ok_or(W2ScenarioError::GeometryOverflow)
    }

    pub fn buffer_geometry(&self) -> Result<BufferGeometry, W2ScenarioError> {
        let round_trip_ns = self
            .link_propagation_ns
            .checked_mul(2)
            .ok_or(W2ScenarioError::GeometryOverflow)?;
        let per_hop_bdp_bytes_u128 = u128::from(self.data_rate_bps)
            .checked_mul(u128::from(round_trip_ns))
            .ok_or(W2ScenarioError::GeometryOverflow)?
            / 8
            / 1_000_000_000;
        let per_hop_bdp_bytes = usize::try_from(per_hop_bdp_bytes_u128)
            .map_err(|_| W2ScenarioError::GeometryOverflow)?;
        let (numerator, denominator) = self.buffer_budget.ratio();
        let scaled = per_hop_bdp_bytes
            .checked_mul(numerator)
            .ok_or(W2ScenarioError::GeometryOverflow)?
            .div_ceil(denominator);
        let frame = self.frame_wire_bytes()?;
        let share = |percent: usize| scaled.saturating_mul(percent).div_ceil(100).max(frame);
        let frames = |percent: usize| share(percent).div_ceil(frame).max(1);
        Ok(BufferGeometry {
            per_hop_bdp_bytes,
            scaled_per_hop_budget_bytes: scaled,
            socket_send_bytes: share(25),
            socket_receive_bytes: share(20),
            link_queue_bytes: share(25),
            relay_application_bytes: share(15),
            relay_child_queue_bytes: share(15),
            runtime_command_frames: frames(15),
            receiver_data_inbox_frames: frames(15),
        })
    }

    pub fn receiver_service_ns(&self, receiver: usize) -> u64 {
        if receiver < self.slow_receiver_count {
            self.healthy_decoder_sink_service_ns
                .saturating_mul(self.slow_service_rate.service_multiplier())
        } else {
            self.healthy_decoder_sink_service_ns
        }
    }

    pub fn ordered_receivers(&self) -> Vec<usize> {
        let mut receivers: Vec<_> = (0..self.receiver_count).collect();
        if self.slow_receiver_count == 1 && self.child_order == ChildOrder::SlowLast {
            receivers.rotate_left(1);
        }
        receivers
    }
}

#[derive(Debug, Error)]
pub enum W2ScenarioError {
    #[error("W2 scenario id must not be empty")]
    EmptyScenarioId,
    #[error("W2 receiver count must be 3 or 8, got {0}")]
    ReceiverCount(usize),
    #[error("W2 fan-out degree must be 2 or 4, got {0}")]
    FanoutDegree(usize),
    #[error("W2 slow receiver count must be zero or one, got {0}")]
    SlowReceiverCount(usize),
    #[error("W2 geometry values must be nonzero")]
    ZeroGeometry,
    #[error("W2 geometry arithmetic overflow")]
    GeometryOverflow,
    #[error("TCP MSS is below one framed W2 symbol")]
    MssBelowFrame,
    #[error("a modeled W2 buffer is below one framed symbol")]
    BufferBelowFrame,
    #[error("W3 control asymmetry requires eight receivers and nonzero reverse geometry")]
    ControlAsymmetryGeometry,
    #[error("W3 shared-leaf receivers must be distinct in-range receiver indexes")]
    SharedLeafGeometry,
    #[error(transparent)]
    Carousel(#[from] crate::protocol::CarouselConfigError),
}

impl ReceiverAdmissionPolicy {
    pub const ALL: [Self; 3] = [Self::HybridDrop, Self::NaiveBlocking, Self::IsolatedCredit];

    pub const fn name(self) -> &'static str {
        match self {
            Self::HybridDrop => "hybrid_drop",
            Self::NaiveBlocking => "naive_blocking",
            Self::IsolatedCredit => "isolated_credit",
        }
    }
}
