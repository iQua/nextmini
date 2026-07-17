use std::collections::BTreeSet;
use std::path::Path;

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::SCENARIO_SCHEMA_VERSION;

#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RegistrationOrder {
    #[default]
    Forward,
    Reverse,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ChainScenario {
    pub schema_version: u32,
    pub scenario_id: String,
    pub master_seed: u64,
    pub frame_count: usize,
    pub frame_payload_bytes: usize,
    pub tcp_mss_bytes: usize,
    pub socket_send_buffer_bytes: usize,
    pub socket_receive_buffer_bytes: usize,
    pub relay_application_buffer_bytes: usize,
    pub link_rate_bps: u64,
    pub link_propagation_ns: u64,
    pub link_queue_bytes: usize,
    pub receiver_resume_at_ns: u64,
    pub simulation_end_ns: u64,
    pub timer_interval_ns: u64,
    pub initial_rto_ns: u64,
    pub persist_interval_ns: u64,
    #[serde(default)]
    pub hop1_drop_attempts: BTreeSet<u64>,
    #[serde(default)]
    pub hop2_drop_attempts: BTreeSet<u64>,
    #[serde(default)]
    pub registration_order: RegistrationOrder,
}

impl ChainScenario {
    pub fn from_path(path: &Path) -> Result<Self, ScenarioError> {
        let source = std::fs::read_to_string(path).map_err(ScenarioError::Read)?;
        let scenario: Self = toml::from_str(&source).map_err(ScenarioError::Parse)?;
        scenario.validate()?;
        Ok(scenario)
    }

    pub fn validate(&self) -> Result<(), ScenarioError> {
        if self.schema_version != SCENARIO_SCHEMA_VERSION {
            return Err(ScenarioError::SchemaVersion {
                expected: SCENARIO_SCHEMA_VERSION,
                actual: self.schema_version,
            });
        }
        if self.scenario_id.is_empty() {
            return Err(ScenarioError::EmptyScenarioId);
        }
        for (name, value) in [
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
            ("link_queue_bytes", self.link_queue_bytes),
        ] {
            if value == 0 {
                return Err(ScenarioError::ZeroValue(name));
            }
        }
        for (name, value) in [
            ("link_rate_bps", self.link_rate_bps),
            ("link_propagation_ns", self.link_propagation_ns),
            ("simulation_end_ns", self.simulation_end_ns),
            ("timer_interval_ns", self.timer_interval_ns),
            ("initial_rto_ns", self.initial_rto_ns),
            ("persist_interval_ns", self.persist_interval_ns),
        ] {
            if value == 0 {
                return Err(ScenarioError::ZeroU64Value(name));
            }
        }
        let frame_wire_bytes = self
            .frame_payload_bytes
            .checked_add(4)
            .ok_or(ScenarioError::GeometryOverflow)?;
        if self.relay_application_buffer_bytes < frame_wire_bytes {
            return Err(ScenarioError::RelayBufferBelowFrame {
                relay_buffer: self.relay_application_buffer_bytes,
                frame_wire_bytes,
            });
        }
        if self.receiver_resume_at_ns >= self.simulation_end_ns {
            return Err(ScenarioError::ResumeAfterEnd);
        }
        let _ = self
            .frame_count
            .checked_mul(frame_wire_bytes)
            .ok_or(ScenarioError::GeometryOverflow)?;
        Ok(())
    }

    pub fn frame_wire_bytes(&self) -> Result<usize, ScenarioError> {
        self.frame_payload_bytes
            .checked_add(4)
            .ok_or(ScenarioError::GeometryOverflow)
    }

    pub fn stream_bytes(&self) -> Result<usize, ScenarioError> {
        self.frame_count
            .checked_mul(self.frame_wire_bytes()?)
            .ok_or(ScenarioError::GeometryOverflow)
    }
}

#[derive(Debug, Error)]
pub enum ScenarioError {
    #[error("failed to read scenario: {0}")]
    Read(std::io::Error),
    #[error("failed to parse scenario: {0}")]
    Parse(toml::de::Error),
    #[error("scenario schema version {actual} does not match {expected}")]
    SchemaVersion { expected: u32, actual: u32 },
    #[error("scenario_id must not be empty")]
    EmptyScenarioId,
    #[error("{0} must be nonzero")]
    ZeroValue(&'static str),
    #[error("{0} must be nonzero")]
    ZeroU64Value(&'static str),
    #[error("scenario geometry overflows usize")]
    GeometryOverflow,
    #[error(
        "relay application buffer {relay_buffer} is smaller than one framed message ({frame_wire_bytes})"
    )]
    RelayBufferBelowFrame {
        relay_buffer: usize,
        frame_wire_bytes: usize,
    },
    #[error("receiver resume time must precede simulation end")]
    ResumeAfterEnd,
}
