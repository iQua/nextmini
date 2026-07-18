use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

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
    local_completion_count: Arc<AtomicUsize>,
    sender_completion_count: Arc<AtomicUsize>,
    compact_w2: bool,
    compact_w3: bool,
    compact_wr: bool,
    wr_triage: Option<WrTriageFilter>,
    event_class_counts: Option<Arc<Mutex<BTreeMap<&'static str, u64>>>>,
}

#[derive(Clone, Copy, Debug)]
struct WrTriageFilter {
    source_component: &'static str,
    receiver_component: &'static str,
}

impl Recorder {
    pub fn new(scenario: impl Into<Arc<str>>, seed: u64) -> Self {
        Self {
            scenario: scenario.into(),
            seed,
            records: Arc::default(),
            failure: Arc::default(),
            local_completion_count: Arc::default(),
            sender_completion_count: Arc::default(),
            compact_w2: false,
            compact_w3: false,
            compact_wr: false,
            wr_triage: None,
            event_class_counts: None,
        }
    }

    pub(crate) fn new_compact_w2(scenario: impl Into<Arc<str>>, seed: u64) -> Self {
        Self {
            scenario: scenario.into(),
            seed,
            records: Arc::default(),
            failure: Arc::default(),
            local_completion_count: Arc::default(),
            sender_completion_count: Arc::default(),
            compact_w2: true,
            compact_w3: false,
            compact_wr: false,
            wr_triage: None,
            event_class_counts: None,
        }
    }

    pub(crate) fn new_compact_w3(scenario: impl Into<Arc<str>>, seed: u64) -> Self {
        Self {
            scenario: scenario.into(),
            seed,
            records: Arc::default(),
            failure: Arc::default(),
            local_completion_count: Arc::default(),
            sender_completion_count: Arc::default(),
            compact_w2: false,
            compact_w3: true,
            compact_wr: false,
            wr_triage: None,
            event_class_counts: None,
        }
    }

    pub(crate) fn new_compact_wr(scenario: impl Into<Arc<str>>, seed: u64) -> Self {
        Self {
            scenario: scenario.into(),
            seed,
            records: Arc::default(),
            failure: Arc::default(),
            local_completion_count: Arc::default(),
            sender_completion_count: Arc::default(),
            compact_w2: false,
            compact_w3: false,
            compact_wr: true,
            wr_triage: None,
            event_class_counts: None,
        }
    }

    pub(crate) fn new_wr_triage(
        scenario: impl Into<Arc<str>>,
        seed: u64,
        source_component: &'static str,
        receiver_component: &'static str,
    ) -> Self {
        Self {
            scenario: scenario.into(),
            seed,
            records: Arc::default(),
            failure: Arc::default(),
            local_completion_count: Arc::default(),
            sender_completion_count: Arc::default(),
            compact_w2: false,
            compact_w3: false,
            compact_wr: false,
            wr_triage: Some(WrTriageFilter {
                source_component,
                receiver_component,
            }),
            event_class_counts: Some(Arc::default()),
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
        self.count_event_class(event);
        if self.compact_w2 && !w2_metric_event(event) {
            return;
        }
        if self.compact_w3 && !w3_metric_event(event) {
            return;
        }
        if self.compact_wr && !wr_metric_event(event) {
            return;
        }
        if self
            .wr_triage
            .is_some_and(|filter| !filter.retains(component, event))
        {
            return;
        }
        match event {
            "protocol_local_complete" => {
                self.local_completion_count.fetch_add(1, Ordering::Relaxed);
            }
            "protocol_sender_complete" => {
                self.sender_completion_count.fetch_add(1, Ordering::Relaxed);
            }
            _ => {}
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

    #[track_caller]
    pub fn fail(&self, error: impl std::fmt::Display) {
        let mut failure = self.failure.lock();
        if failure.is_none() {
            let caller = std::panic::Location::caller();
            *failure = Some(format!(
                "{error} (reported at {}:{})",
                caller.file(),
                caller.line()
            ));
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

    pub(crate) fn record_count(&self) -> usize {
        self.records.lock().len()
    }

    pub(crate) fn event_count(&self, event: &str) -> usize {
        match event {
            "protocol_local_complete" => self.local_completion_count.load(Ordering::Relaxed),
            "protocol_sender_complete" => self.sender_completion_count.load(Ordering::Relaxed),
            _ => self
                .records
                .lock()
                .iter()
                .filter(|record| record.event == event)
                .count(),
        }
    }

    pub(crate) fn count_event_class(&self, event_class: &'static str) {
        let Some(counts) = &self.event_class_counts else {
            return;
        };
        let mut counts = counts.lock();
        let count = counts.entry(event_class).or_default();
        *count = count.saturating_add(1);
    }

    pub(crate) fn event_class_counts(&self) -> BTreeMap<&'static str, u64> {
        self.event_class_counts
            .as_ref()
            .map_or_else(BTreeMap::new, |counts| counts.lock().clone())
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

impl WrTriageFilter {
    fn retains(self, component: &str, event: &str) -> bool {
        matches!(
            event,
            "single_worker"
                | "wr_concurrent_sessions"
                | "queue_drop"
                | "segment_drop"
                | "mailbox_high_water"
                | "protocol_local_complete"
                | "protocol_sender_complete"
                | "carousel_liveness_silent_abort"
                | "carousel_liveness_stall_abort"
                | "carousel_liveness_last_ack_seen"
                | "carousel_liveness_last_ack_progress"
        ) || (component == self.source_component
            && matches!(
                event,
                "data_frame_emitted"
                    | "block_ack_received"
                    | "carousel_ack_join_progress"
                    | "carousel_ack_join_noop"
                    | "ack_probe_submitted"
                    | "session_complete_submitted"
            ))
            || (component == self.receiver_component
                && matches!(
                    event,
                    "runtime_command_enqueue_data"
                        | "data_inbox_enqueue"
                        | "data_inbox_drop_after_tcp_ack"
                        | "decoder_sink_complete"
                        | "carousel_progress_observed"
                        | "block_ack_submitted"
                        | "ack_probe_runtime_dispatch"
                        | "protocol_local_complete"
                ))
    }
}

fn wr_metric_event(event: &str) -> bool {
    matches!(
        event,
        "single_worker"
            | "wr_concurrent_sessions"
            | "data_frame_emitted"
            | "flow_count_match_frame_emitted"
            | "protocol_local_complete"
            | "protocol_sender_complete"
            | "data_inbox_drop_after_tcp_ack"
            | "data_inbox_blocking_wait"
            | "child_admission_blocked"
            | "block_ack_received"
            | "ack_probe_submitted"
            | "round_deficit_received"
            | "round_need_generated"
            | "queue_drop"
            | "segment_drop"
            | "wr_resource_sample"
            | "wr_tree_rate_sample"
            | "wr_resource_queue_high_water"
            | "mailbox_high_water"
    )
}

fn w3_metric_event(event: &str) -> bool {
    matches!(
        event,
        "single_worker"
            | "data_frame_emitted"
            | "flow_count_match_frame_emitted"
            | "protocol_local_complete"
            | "protocol_sender_complete"
            | "runtime_command_enqueue_data"
            | "data_inbox_enqueue"
            | "data_inbox_drop_after_tcp_ack"
            | "decoder_sink_complete"
            | "coupled_queue_admit"
            | "coupled_serialization_start"
            | "coupled_serialization_end"
            | "coupled_path_exit"
            | "background_bytes_delivered"
            | "block_ack_received"
            | "ack_probe_submitted"
            | "round_deficit_received"
            | "round_need_generated"
            | "queue_drop"
            | "segment_drop"
            | "mailbox_high_water"
    )
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
            | "coupled_path_exit"
            | "background_bytes_delivered"
            | "mailbox_high_water"
    )
}
