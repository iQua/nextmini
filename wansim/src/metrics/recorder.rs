use std::sync::Arc;

use parking_lot::Mutex;
use serde::Serialize;

use crate::{SCENARIO_SCHEMA_VERSION, SIMULATOR_VERSION};

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize)]
pub struct Record {
    pub schema_version: u32,
    pub simulator_version: &'static str,
    pub scenario: String,
    pub seed: u64,
    pub time_ns: u64,
    pub component: &'static str,
    pub event: &'static str,
    pub flow_id: usize,
    pub sequence: usize,
    pub bytes: usize,
    pub value: usize,
}

#[derive(Clone, Debug)]
pub struct Recorder {
    scenario: Arc<str>,
    seed: u64,
    records: Arc<Mutex<Vec<Record>>>,
    failure: Arc<Mutex<Option<String>>>,
    compact_w2: bool,
}

impl Recorder {
    pub fn new(scenario: impl Into<Arc<str>>, seed: u64) -> Self {
        Self {
            scenario: scenario.into(),
            seed,
            records: Arc::default(),
            failure: Arc::default(),
            compact_w2: false,
        }
    }

    pub(crate) fn new_compact_w2(scenario: impl Into<Arc<str>>, seed: u64) -> Self {
        Self {
            scenario: scenario.into(),
            seed,
            records: Arc::default(),
            failure: Arc::default(),
            compact_w2: true,
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn record(
        &self,
        time_ns: u64,
        component: &'static str,
        event: &'static str,
        flow_id: usize,
        sequence: usize,
        bytes: usize,
        value: usize,
    ) {
        if self.compact_w2 && !w2_metric_event(event) {
            return;
        }
        self.records.lock().push(Record {
            schema_version: SCENARIO_SCHEMA_VERSION,
            simulator_version: SIMULATOR_VERSION,
            scenario: self.scenario.to_string(),
            seed: self.seed,
            time_ns,
            component,
            event,
            flow_id,
            sequence,
            bytes,
            value,
        });
    }

    pub fn fail(&self, error: impl std::fmt::Display) {
        let mut failure = self.failure.lock();
        if failure.is_none() {
            *failure = Some(error.to_string());
        }
    }

    pub fn failure(&self) -> Option<String> {
        self.failure.lock().clone()
    }

    pub fn records(&self) -> Vec<Record> {
        let mut records = self.records.lock().clone();
        records.sort();
        records
    }

    pub(crate) fn event_count(&self, event: &str) -> usize {
        self.records
            .lock()
            .iter()
            .filter(|record| record.event == event)
            .count()
    }

    pub fn to_csv(&self) -> Result<String, csv::Error> {
        let mut writer = csv::WriterBuilder::new()
            .terminator(csv::Terminator::Any(b'\n'))
            .from_writer(Vec::new());
        for record in self.records() {
            writer.serialize(record)?;
        }
        writer.flush()?;
        let bytes = writer
            .into_inner()
            .map_err(|error| csv::Error::from(error.into_error()))?;
        Ok(String::from_utf8_lossy(&bytes).into_owned())
    }
}

fn w2_metric_event(event: &str) -> bool {
    matches!(
        event,
        "single_worker"
            | "w2_configured_fanout_degree"
            | "budget_socket_send"
            | "budget_socket_receive"
            | "budget_link_queue"
            | "budget_relay_application"
            | "budget_relay_child_queue"
            | "budget_runtime_command"
            | "budget_receiver_inbox"
            | "configured_path_hops"
            | "fanout_child_configured"
            | "fanout_parent_configured"
            | "runtime_command_enqueue_data"
            | "runtime_command_dispatch"
            | "data_inbox_enqueue"
            | "data_inbox_drop_after_tcp_ack"
            | "data_inbox_blocking_wait"
            | "isolated_credit_wait"
            | "decoder_sink_complete"
            | "protocol_local_complete"
            | "protocol_sender_complete"
            | "data_frame_emitted"
            | "child_admission_blocked"
            | "isolated_credit_deferred"
            | "isolated_credit_replay"
            | "block_ack_received"
            | "ack_probe_submitted"
            | "queue_drop"
            | "segment_drop"
            | "mailbox_high_water"
    )
}
