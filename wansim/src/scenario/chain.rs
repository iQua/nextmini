use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use nexosim::simulation::{Mailbox, SimInit};
use nexosim::time::MonotonicTime;
use thiserror::Error;

use crate::days_bridge::{PhysicalLink, PhysicalLinkConfig};
use crate::determinism::CounterPrf;
use crate::metrics::{MAILBOX_CAPACITY, MailboxTracker, Record, Recorder};
use crate::overlay::{FramedStream, ReceiverEndpoint, RelayEndpoint, SourceEndpoint};
use crate::scenario::{ChainScenario, RegistrationOrder, ScenarioError};
use crate::transport::SocketPairConfig;

#[derive(Clone, Debug)]
pub struct ChainOutcome {
    pub csv: String,
    pub records: Vec<Record>,
    pub mailbox_high_water: BTreeMap<&'static str, usize>,
    pub stream_bytes: usize,
    pub source_bytes_admitted_before_resume: usize,
    pub expected_backpressure_plateau_bytes: usize,
    pub completed: bool,
}

#[derive(Debug, Error)]
pub enum ChainRunError {
    #[error(transparent)]
    Scenario(#[from] ScenarioError),
    #[error("failed to construct W0a model: {0}")]
    Construction(String),
    #[error("nexosim execution failed: {0}")]
    Simulation(String),
    #[error("model failed: {0}")]
    Model(String),
    #[error("failed to serialize deterministic CSV: {0}")]
    Csv(#[from] csv::Error),
}

pub fn run_chain(scenario: &ChainScenario) -> Result<ChainOutcome, ChainRunError> {
    scenario.validate()?;
    let recorder = Recorder::new(scenario.scenario_id.as_str(), scenario.master_seed);
    let mailbox_tracker = MailboxTracker::default();
    let prf = CounterPrf::new(scenario.master_seed, &scenario.scenario_id);
    let stream = FramedStream::new(scenario.frame_count, scenario.frame_payload_bytes, prf)
        .map_err(|error| ChainRunError::Construction(error.to_string()))?;
    let stream_bytes = stream.total_bytes();
    let frame_wire_bytes = stream.frame_wire_bytes();
    let socket = SocketPairConfig::new(
        scenario.tcp_mss_bytes,
        scenario.socket_send_buffer_bytes,
        scenario.socket_receive_buffer_bytes,
        scenario.initial_rto_ns,
        scenario.persist_interval_ns,
    )
    .map_err(|error| ChainRunError::Construction(error.to_string()))?;

    let mut source = SourceEndpoint::new(
        socket,
        stream_bytes,
        scenario.timer_interval_ns,
        recorder.clone(),
        mailbox_tracker.clone(),
    )
    .map_err(|error| ChainRunError::Construction(error.to_string()))?;
    let mut relay = RelayEndpoint::new(
        socket,
        socket,
        stream.clone(),
        scenario.frame_payload_bytes,
        scenario.relay_application_buffer_bytes,
        scenario.timer_interval_ns,
        recorder.clone(),
        mailbox_tracker.clone(),
    )
    .map_err(|error| ChainRunError::Construction(error.to_string()))?;
    let mut receiver = ReceiverEndpoint::new(
        socket,
        stream,
        scenario.frame_payload_bytes,
        scenario.receiver_resume_at_ns,
        recorder.clone(),
        mailbox_tracker.clone(),
    )
    .map_err(|error| ChainRunError::Construction(error.to_string()))?;

    let mut hop1_forward = physical_link(
        "hop1_forward",
        "relay",
        scenario,
        scenario.hop1_drop_attempts.clone(),
        recorder.clone(),
        mailbox_tracker.clone(),
    );
    let mut hop1_reverse = physical_link(
        "hop1_reverse",
        "source",
        scenario,
        BTreeSet::new(),
        recorder.clone(),
        mailbox_tracker.clone(),
    );
    let mut hop2_forward = physical_link(
        "hop2_forward",
        "receiver",
        scenario,
        scenario.hop2_drop_attempts.clone(),
        recorder.clone(),
        mailbox_tracker.clone(),
    );
    let mut hop2_reverse = physical_link(
        "hop2_reverse",
        "relay",
        scenario,
        BTreeSet::new(),
        recorder.clone(),
        mailbox_tracker.clone(),
    );

    let source_mailbox = Mailbox::with_capacity(MAILBOX_CAPACITY);
    let relay_mailbox = Mailbox::with_capacity(MAILBOX_CAPACITY);
    let receiver_mailbox = Mailbox::with_capacity(MAILBOX_CAPACITY);
    let hop1_forward_mailbox = Mailbox::with_capacity(MAILBOX_CAPACITY);
    let hop1_reverse_mailbox = Mailbox::with_capacity(MAILBOX_CAPACITY);
    let hop2_forward_mailbox = Mailbox::with_capacity(MAILBOX_CAPACITY);
    let hop2_reverse_mailbox = Mailbox::with_capacity(MAILBOX_CAPACITY);

    source
        .data_output
        .connect(PhysicalLink::receive, &hop1_forward_mailbox);
    hop1_forward
        .output
        .connect(RelayEndpoint::upstream_segment, &relay_mailbox);
    relay
        .upstream_ack_output
        .connect(PhysicalLink::receive, &hop1_reverse_mailbox);
    hop1_reverse
        .output
        .connect(SourceEndpoint::acknowledgment, &source_mailbox);
    relay
        .downstream_data_output
        .connect(PhysicalLink::receive, &hop2_forward_mailbox);
    hop2_forward
        .output
        .connect(ReceiverEndpoint::segment, &receiver_mailbox);
    receiver
        .ack_output
        .connect(PhysicalLink::receive, &hop2_reverse_mailbox);
    hop2_reverse
        .output
        .connect(RelayEndpoint::downstream_acknowledgment, &relay_mailbox);

    recorder.record(0, "simulation", "single_worker", 0, 0, 0, 1);
    recorder.record(
        0,
        "simulation",
        "logical_length_prefix_bytes",
        0,
        0,
        4,
        frame_wire_bytes,
    );

    let bench = SimInit::with_num_threads(1);
    let bench = match scenario.registration_order {
        RegistrationOrder::Forward => bench
            .add_model(source, source_mailbox, "source")
            .add_model(hop1_forward, hop1_forward_mailbox, "hop1_forward")
            .add_model(relay, relay_mailbox, "relay")
            .add_model(hop2_forward, hop2_forward_mailbox, "hop2_forward")
            .add_model(receiver, receiver_mailbox, "receiver")
            .add_model(hop2_reverse, hop2_reverse_mailbox, "hop2_reverse")
            .add_model(hop1_reverse, hop1_reverse_mailbox, "hop1_reverse"),
        RegistrationOrder::Reverse => bench
            .add_model(hop1_reverse, hop1_reverse_mailbox, "hop1_reverse")
            .add_model(hop2_reverse, hop2_reverse_mailbox, "hop2_reverse")
            .add_model(receiver, receiver_mailbox, "receiver")
            .add_model(hop2_forward, hop2_forward_mailbox, "hop2_forward")
            .add_model(relay, relay_mailbox, "relay")
            .add_model(hop1_forward, hop1_forward_mailbox, "hop1_forward")
            .add_model(source, source_mailbox, "source"),
    };
    let mut simulation = bench
        .init(MonotonicTime::EPOCH)
        .map_err(|error| ChainRunError::Simulation(error.to_string()))?;
    simulation
        .step_until(Duration::from_nanos(scenario.simulation_end_ns))
        .map_err(|error| ChainRunError::Simulation(error.to_string()))?;

    if let Some(failure) = recorder.failure() {
        return Err(ChainRunError::Model(failure));
    }
    let mailbox_high_water = mailbox_tracker.high_water_marks();
    for (&mailbox, &high_water) in &mailbox_high_water {
        recorder.record(
            scenario.simulation_end_ns,
            mailbox,
            "mailbox_high_water",
            0,
            0,
            high_water,
            MAILBOX_CAPACITY,
        );
    }
    let records = recorder.records();
    let completed = records
        .iter()
        .any(|record| record.component == "receiver" && record.event == "stream_complete");
    let source_bytes_admitted_before_resume = records
        .iter()
        .filter(|record| {
            record.component == "source"
                && record.event == "application_write"
                && record.time_ns <= scenario.receiver_resume_at_ns
        })
        .map(|record| record.value)
        .max()
        .unwrap_or(0);
    let expected_backpressure_plateau_bytes = scenario
        .socket_send_buffer_bytes
        .saturating_mul(2)
        .saturating_add(scenario.socket_receive_buffer_bytes.saturating_mul(2))
        .saturating_add(scenario.relay_application_buffer_bytes)
        .min(stream_bytes);

    Ok(ChainOutcome {
        csv: recorder.to_csv()?,
        records,
        mailbox_high_water,
        stream_bytes,
        source_bytes_admitted_before_resume,
        expected_backpressure_plateau_bytes,
        completed,
    })
}

fn physical_link(
    mailbox: &'static str,
    downstream_mailbox: &'static str,
    scenario: &ChainScenario,
    drop_attempts: BTreeSet<u64>,
    recorder: Recorder,
    mailbox_tracker: MailboxTracker,
) -> PhysicalLink {
    PhysicalLink::new(
        PhysicalLinkConfig {
            component: mailbox,
            mailbox,
            downstream_mailbox,
            rate_bps: scenario.link_rate_bps,
            propagation_ns: scenario.link_propagation_ns,
            queue_bytes: scenario.link_queue_bytes,
            drop_attempts,
        },
        recorder,
        mailbox_tracker,
    )
}
