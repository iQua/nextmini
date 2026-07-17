use std::collections::BTreeSet;
use std::path::Path;

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::SCENARIO_SCHEMA_VERSION;

use super::RegistrationOrder;

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum FanoutAdmission {
    Sequential,
    Concurrent,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum TreeEndpoint {
    Receiver1,
    RelayB,
    Receiver2,
    Receiver3,
}

impl TreeEndpoint {
    pub(crate) fn component(self) -> &'static str {
        match self {
            Self::Receiver1 => "receiver1",
            Self::RelayB => "relay_b",
            Self::Receiver2 => "receiver2",
            Self::Receiver3 => "receiver3",
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ReceiverTiming {
    pub transport_resume_at_ns: u64,
    pub service_start_at_ns: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct TreeScenario {
    pub schema_version: u32,
    pub scenario_kind: String,
    pub scenario_id: String,
    pub master_seed: u64,
    pub source_symbols_k: usize,
    pub frame_count: usize,
    pub frame_payload_bytes: usize,
    pub tcp_mss_bytes: usize,
    pub socket_send_buffer_bytes: usize,
    pub socket_receive_buffer_bytes: usize,
    pub relay_application_buffer_bytes: usize,
    pub relay_child_queue_bytes: usize,
    pub runtime_command_capacity_frames: usize,
    pub receiver_data_inbox_capacity_frames: usize,
    pub receiver_control_inbox_capacity_frames: usize,
    pub runtime_command_service_ns: u64,
    pub decoder_sink_service_ns: u64,
    pub link_rate_bps: u64,
    pub link_propagation_ns: u64,
    pub link_queue_bytes: usize,
    pub simulation_end_ns: u64,
    pub plateau_probe_at_ns: u64,
    pub timer_interval_ns: u64,
    pub initial_rto_ns: u64,
    pub persist_interval_ns: u64,
    pub fanout_admission: FanoutAdmission,
    pub relay_a_children: Vec<TreeEndpoint>,
    pub relay_b_children: Vec<TreeEndpoint>,
    pub receiver1: ReceiverTiming,
    pub receiver2: ReceiverTiming,
    pub receiver3: ReceiverTiming,
    #[serde(default)]
    pub registration_order: RegistrationOrder,
}

impl TreeScenario {
    pub fn from_path(path: &Path) -> Result<Self, TreeScenarioError> {
        let source = std::fs::read_to_string(path).map_err(TreeScenarioError::Read)?;
        let scenario: Self = toml::from_str(&source).map_err(TreeScenarioError::Parse)?;
        scenario.validate()?;
        Ok(scenario)
    }

    pub fn validate(&self) -> Result<(), TreeScenarioError> {
        if self.schema_version != SCENARIO_SCHEMA_VERSION {
            return Err(TreeScenarioError::SchemaVersion {
                expected: SCENARIO_SCHEMA_VERSION,
                actual: self.schema_version,
            });
        }
        if self.scenario_kind != "tree" {
            return Err(TreeScenarioError::ScenarioKind(self.scenario_kind.clone()));
        }
        if self.scenario_id.is_empty() {
            return Err(TreeScenarioError::EmptyScenarioId);
        }
        for (name, value) in [
            ("source_symbols_k", self.source_symbols_k),
            ("frame_count", self.frame_count),
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
            (
                "receiver_control_inbox_capacity_frames",
                self.receiver_control_inbox_capacity_frames,
            ),
            ("link_queue_bytes", self.link_queue_bytes),
        ] {
            if value == 0 {
                return Err(TreeScenarioError::ZeroValue(name));
            }
        }
        for (name, value) in [
            (
                "runtime_command_service_ns",
                self.runtime_command_service_ns,
            ),
            ("decoder_sink_service_ns", self.decoder_sink_service_ns),
            ("link_rate_bps", self.link_rate_bps),
            ("link_propagation_ns", self.link_propagation_ns),
            ("simulation_end_ns", self.simulation_end_ns),
            ("timer_interval_ns", self.timer_interval_ns),
            ("initial_rto_ns", self.initial_rto_ns),
            ("persist_interval_ns", self.persist_interval_ns),
        ] {
            if value == 0 {
                return Err(TreeScenarioError::ZeroU64Value(name));
            }
        }
        if self.source_symbols_k != self.frame_count {
            return Err(TreeScenarioError::SourceCountMismatch {
                source_symbols_k: self.source_symbols_k,
                frame_count: self.frame_count,
            });
        }
        let frame_wire_bytes = self.frame_wire_bytes()?;
        if self.relay_application_buffer_bytes < frame_wire_bytes {
            return Err(TreeScenarioError::RelayBufferBelowFrame {
                relay_buffer: self.relay_application_buffer_bytes,
                frame_wire_bytes,
            });
        }
        if self.relay_child_queue_bytes < frame_wire_bytes {
            return Err(TreeScenarioError::ChildQueueBelowFrame {
                child_queue: self.relay_child_queue_bytes,
                frame_wire_bytes,
            });
        }
        for capacity in [
            self.runtime_command_capacity_frames,
            self.receiver_data_inbox_capacity_frames,
            self.receiver_control_inbox_capacity_frames,
        ] {
            let _ = capacity
                .checked_mul(frame_wire_bytes)
                .ok_or(TreeScenarioError::GeometryOverflow)?;
        }
        validate_children(
            "relay_a_children",
            &self.relay_a_children,
            [TreeEndpoint::Receiver1, TreeEndpoint::RelayB],
        )?;
        validate_children(
            "relay_b_children",
            &self.relay_b_children,
            [TreeEndpoint::Receiver2, TreeEndpoint::Receiver3],
        )?;
        for (name, timing) in [
            ("receiver1", self.receiver1),
            ("receiver2", self.receiver2),
            ("receiver3", self.receiver3),
        ] {
            if timing.transport_resume_at_ns >= self.simulation_end_ns {
                return Err(TreeScenarioError::ReceiverTimeAfterEnd {
                    receiver: name,
                    field: "transport_resume_at_ns",
                });
            }
            if timing.service_start_at_ns >= self.simulation_end_ns {
                return Err(TreeScenarioError::ReceiverTimeAfterEnd {
                    receiver: name,
                    field: "service_start_at_ns",
                });
            }
        }
        if self.plateau_probe_at_ns >= self.simulation_end_ns {
            return Err(TreeScenarioError::ProbeAfterEnd);
        }
        let _ = self.stream_bytes()?;
        Ok(())
    }

    pub fn frame_wire_bytes(&self) -> Result<usize, TreeScenarioError> {
        self.frame_payload_bytes
            .checked_add(4)
            .ok_or(TreeScenarioError::GeometryOverflow)
    }

    pub fn stream_bytes(&self) -> Result<usize, TreeScenarioError> {
        self.frame_count
            .checked_mul(self.frame_wire_bytes()?)
            .ok_or(TreeScenarioError::GeometryOverflow)
    }
}

fn validate_children(
    field: &'static str,
    actual: &[TreeEndpoint],
    expected: [TreeEndpoint; 2],
) -> Result<(), TreeScenarioError> {
    let actual_set: BTreeSet<_> = actual.iter().copied().collect();
    let expected_set: BTreeSet<_> = expected.into_iter().collect();
    if actual.len() != 2 || actual_set != expected_set {
        return Err(TreeScenarioError::InvalidChildren { field });
    }
    Ok(())
}

#[derive(Debug, Error)]
pub enum TreeScenarioError {
    #[error("failed to read tree scenario: {0}")]
    Read(std::io::Error),
    #[error("failed to parse tree scenario: {0}")]
    Parse(toml::de::Error),
    #[error("scenario schema version {actual} does not match {expected}")]
    SchemaVersion { expected: u32, actual: u32 },
    #[error("scenario_kind must be `tree`, got `{0}`")]
    ScenarioKind(String),
    #[error("scenario_id must not be empty")]
    EmptyScenarioId,
    #[error("{0} must be nonzero")]
    ZeroValue(&'static str),
    #[error("{0} must be nonzero")]
    ZeroU64Value(&'static str),
    #[error("tree scenario geometry overflows usize")]
    GeometryOverflow,
    #[error(
        "source_symbols_k {source_symbols_k} must equal fixed frame_count {frame_count} in W0b"
    )]
    SourceCountMismatch {
        source_symbols_k: usize,
        frame_count: usize,
    },
    #[error(
        "relay application buffer {relay_buffer} is smaller than one framed message ({frame_wire_bytes})"
    )]
    RelayBufferBelowFrame {
        relay_buffer: usize,
        frame_wire_bytes: usize,
    },
    #[error(
        "relay child queue {child_queue} is smaller than one framed message ({frame_wire_bytes})"
    )]
    ChildQueueBelowFrame {
        child_queue: usize,
        frame_wire_bytes: usize,
    },
    #[error("{field} must contain exactly the two topology-defined children")]
    InvalidChildren { field: &'static str },
    #[error("{receiver}.{field} must precede simulation_end_ns")]
    ReceiverTimeAfterEnd {
        receiver: &'static str,
        field: &'static str,
    },
    #[error("plateau_probe_at_ns must precede simulation_end_ns")]
    ProbeAfterEnd,
}
