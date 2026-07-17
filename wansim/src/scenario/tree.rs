use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use nexosim::simulation::{Mailbox, SimInit};
use nexosim::time::MonotonicTime;
use thiserror::Error;

use crate::days_bridge::{PhysicalLink, PhysicalLinkConfig};
use crate::determinism::CounterPrf;
use crate::metrics::{MAILBOX_CAPACITY, MailboxTracker, OwnershipLedger, Record, Recorder};
use crate::overlay::{
    FanoutRelayEndpoint, FramedStream, RelayChildSpec, TreeReceiverEndpoint, TreeSourceEndpoint,
};
use crate::scenario::{RegistrationOrder, TreeEndpoint, TreeScenario, TreeScenarioError};
use crate::transport::{
    FLOW_RELAY_A_RECEIVER_1, FLOW_RELAY_A_RELAY_B, FLOW_RELAY_B_RECEIVER_2,
    FLOW_RELAY_B_RECEIVER_3, FLOW_SOURCE_RELAY_A, SocketPairConfig,
};

const RELAY_A_MAILBOX: &str = "relay_a";
const RELAY_B_MAILBOX: &str = "relay_b";
const RECEIVER_1_MAILBOX: &str = "receiver1";
const RECEIVER_2_MAILBOX: &str = "receiver2";
const RECEIVER_3_MAILBOX: &str = "receiver3";

const SOURCE_RELAY_A_FORWARD: &str = "source_relay_a_forward";
const SOURCE_RELAY_A_REVERSE: &str = "source_relay_a_reverse";
const RELAY_A_RECEIVER_1_FORWARD: &str = "relay_a_receiver1_forward";
const RELAY_A_RECEIVER_1_REVERSE: &str = "relay_a_receiver1_reverse";
const RELAY_A_RELAY_B_FORWARD: &str = "relay_a_relay_b_forward";
const RELAY_A_RELAY_B_REVERSE: &str = "relay_a_relay_b_reverse";
const RELAY_B_RECEIVER_2_FORWARD: &str = "relay_b_receiver2_forward";
const RELAY_B_RECEIVER_2_REVERSE: &str = "relay_b_receiver2_reverse";
const RELAY_B_RECEIVER_3_FORWARD: &str = "relay_b_receiver3_forward";
const RELAY_B_RECEIVER_3_REVERSE: &str = "relay_b_receiver3_reverse";

#[derive(Clone, Debug)]
pub struct TreeOutcome {
    pub csv: String,
    pub records: Vec<Record>,
    pub mailbox_high_water: BTreeMap<&'static str, usize>,
    pub ownership_at_probe: BTreeMap<&'static str, usize>,
    pub ownership_total_at_probe: usize,
    pub source_bytes_admitted_at_probe: usize,
    pub expected_source_plateau_bytes: usize,
    pub expected_resident_copy_plateau_bytes: usize,
    pub completion_times_ns: BTreeMap<&'static str, u64>,
}

#[derive(Debug, Error)]
pub enum TreeRunError {
    #[error(transparent)]
    Scenario(#[from] TreeScenarioError),
    #[error("failed to construct W0b model: {0}")]
    Construction(String),
    #[error("nexosim execution failed: {0}")]
    Simulation(String),
    #[error("model failed: {0}")]
    Model(String),
    #[error("failed to serialize deterministic CSV: {0}")]
    Csv(#[from] csv::Error),
}

pub fn run_tree(scenario: &TreeScenario) -> Result<TreeOutcome, TreeRunError> {
    scenario.validate()?;
    let recorder = Recorder::new(scenario.scenario_id.as_str(), scenario.master_seed);
    let mailbox_tracker = MailboxTracker::default();
    let ownership = OwnershipLedger::default();
    let prf = CounterPrf::new(scenario.master_seed, &scenario.scenario_id);
    let stream = FramedStream::new(scenario.frame_count, scenario.frame_payload_bytes, prf)
        .map_err(|error| TreeRunError::Construction(error.to_string()))?;
    let stream_bytes = stream.total_bytes();
    let frame_wire_bytes = stream.frame_wire_bytes();
    let socket = SocketPairConfig::new(
        scenario.tcp_mss_bytes,
        scenario.socket_send_buffer_bytes,
        scenario.socket_receive_buffer_bytes,
        scenario.initial_rto_ns,
        scenario.persist_interval_ns,
    )
    .map_err(|error| TreeRunError::Construction(error.to_string()))?;

    let mut source = TreeSourceEndpoint::new(
        socket,
        stream_bytes,
        scenario.timer_interval_ns,
        recorder.clone(),
        mailbox_tracker.clone(),
        ownership.clone(),
    )
    .map_err(|error| TreeRunError::Construction(error.to_string()))?;
    let relay_a_specs = [
        relay_a_child_spec(scenario.relay_a_children[0]),
        relay_a_child_spec(scenario.relay_a_children[1]),
    ];
    let relay_b_specs = [
        relay_b_child_spec(scenario.relay_b_children[0]),
        relay_b_child_spec(scenario.relay_b_children[1]),
    ];
    let mut relay_a = FanoutRelayEndpoint::new(
        "relay_a",
        RELAY_A_MAILBOX,
        FLOW_SOURCE_RELAY_A,
        SOURCE_RELAY_A_REVERSE,
        "relay_a.upstream_rcv",
        "relay_a.application",
        socket,
        socket,
        relay_a_specs,
        stream.clone(),
        scenario.frame_payload_bytes,
        scenario.relay_application_buffer_bytes,
        scenario.relay_child_queue_bytes,
        scenario.fanout_admission,
        scenario.timer_interval_ns,
        recorder.clone(),
        mailbox_tracker.clone(),
        ownership.clone(),
    )
    .map_err(|error| TreeRunError::Construction(error.to_string()))?;
    let mut relay_b = FanoutRelayEndpoint::new(
        "relay_b",
        RELAY_B_MAILBOX,
        FLOW_RELAY_A_RELAY_B,
        RELAY_A_RELAY_B_REVERSE,
        "relay_b.upstream_rcv",
        "relay_b.application",
        socket,
        socket,
        relay_b_specs,
        stream.clone(),
        scenario.frame_payload_bytes,
        scenario.relay_application_buffer_bytes,
        scenario.relay_child_queue_bytes,
        scenario.fanout_admission,
        scenario.timer_interval_ns,
        recorder.clone(),
        mailbox_tracker.clone(),
        ownership.clone(),
    )
    .map_err(|error| TreeRunError::Construction(error.to_string()))?;

    let mut receiver1 = tree_receiver(
        "receiver1",
        RECEIVER_1_MAILBOX,
        RELAY_A_RECEIVER_1_REVERSE,
        "receiver1.tcp_rcv",
        "receiver1.runtime_command",
        "receiver1.data_inbox",
        "receiver1.decoder_sink",
        FLOW_RELAY_A_RECEIVER_1,
        scenario.receiver1,
        socket,
        stream.clone(),
        scenario,
        recorder.clone(),
        mailbox_tracker.clone(),
        ownership.clone(),
    )?;
    let mut receiver2 = tree_receiver(
        "receiver2",
        RECEIVER_2_MAILBOX,
        RELAY_B_RECEIVER_2_REVERSE,
        "receiver2.tcp_rcv",
        "receiver2.runtime_command",
        "receiver2.data_inbox",
        "receiver2.decoder_sink",
        FLOW_RELAY_B_RECEIVER_2,
        scenario.receiver2,
        socket,
        stream.clone(),
        scenario,
        recorder.clone(),
        mailbox_tracker.clone(),
        ownership.clone(),
    )?;
    let mut receiver3 = tree_receiver(
        "receiver3",
        RECEIVER_3_MAILBOX,
        RELAY_B_RECEIVER_3_REVERSE,
        "receiver3.tcp_rcv",
        "receiver3.runtime_command",
        "receiver3.data_inbox",
        "receiver3.decoder_sink",
        FLOW_RELAY_B_RECEIVER_3,
        scenario.receiver3,
        socket,
        stream,
        scenario,
        recorder.clone(),
        mailbox_tracker.clone(),
        ownership.clone(),
    )?;

    let mut source_relay_a_forward = physical_link(
        SOURCE_RELAY_A_FORWARD,
        RELAY_A_MAILBOX,
        scenario,
        recorder.clone(),
        mailbox_tracker.clone(),
    );
    let mut source_relay_a_reverse = physical_link(
        SOURCE_RELAY_A_REVERSE,
        TreeSourceEndpoint::MAILBOX,
        scenario,
        recorder.clone(),
        mailbox_tracker.clone(),
    );
    let mut relay_a_receiver1_forward = physical_link(
        RELAY_A_RECEIVER_1_FORWARD,
        RECEIVER_1_MAILBOX,
        scenario,
        recorder.clone(),
        mailbox_tracker.clone(),
    );
    let mut relay_a_receiver1_reverse = physical_link(
        RELAY_A_RECEIVER_1_REVERSE,
        RELAY_A_MAILBOX,
        scenario,
        recorder.clone(),
        mailbox_tracker.clone(),
    );
    let mut relay_a_relay_b_forward = physical_link(
        RELAY_A_RELAY_B_FORWARD,
        RELAY_B_MAILBOX,
        scenario,
        recorder.clone(),
        mailbox_tracker.clone(),
    );
    let mut relay_a_relay_b_reverse = physical_link(
        RELAY_A_RELAY_B_REVERSE,
        RELAY_A_MAILBOX,
        scenario,
        recorder.clone(),
        mailbox_tracker.clone(),
    );
    let mut relay_b_receiver2_forward = physical_link(
        RELAY_B_RECEIVER_2_FORWARD,
        RECEIVER_2_MAILBOX,
        scenario,
        recorder.clone(),
        mailbox_tracker.clone(),
    );
    let mut relay_b_receiver2_reverse = physical_link(
        RELAY_B_RECEIVER_2_REVERSE,
        RELAY_B_MAILBOX,
        scenario,
        recorder.clone(),
        mailbox_tracker.clone(),
    );
    let mut relay_b_receiver3_forward = physical_link(
        RELAY_B_RECEIVER_3_FORWARD,
        RECEIVER_3_MAILBOX,
        scenario,
        recorder.clone(),
        mailbox_tracker.clone(),
    );
    let mut relay_b_receiver3_reverse = physical_link(
        RELAY_B_RECEIVER_3_REVERSE,
        RELAY_B_MAILBOX,
        scenario,
        recorder.clone(),
        mailbox_tracker.clone(),
    );

    let source_mailbox = Mailbox::with_capacity(MAILBOX_CAPACITY);
    let relay_a_mailbox = Mailbox::with_capacity(MAILBOX_CAPACITY);
    let relay_b_mailbox = Mailbox::with_capacity(MAILBOX_CAPACITY);
    let receiver1_mailbox = Mailbox::with_capacity(MAILBOX_CAPACITY);
    let receiver2_mailbox = Mailbox::with_capacity(MAILBOX_CAPACITY);
    let receiver3_mailbox = Mailbox::with_capacity(MAILBOX_CAPACITY);
    let source_relay_a_forward_mailbox = Mailbox::with_capacity(MAILBOX_CAPACITY);
    let source_relay_a_reverse_mailbox = Mailbox::with_capacity(MAILBOX_CAPACITY);
    let relay_a_receiver1_forward_mailbox = Mailbox::with_capacity(MAILBOX_CAPACITY);
    let relay_a_receiver1_reverse_mailbox = Mailbox::with_capacity(MAILBOX_CAPACITY);
    let relay_a_relay_b_forward_mailbox = Mailbox::with_capacity(MAILBOX_CAPACITY);
    let relay_a_relay_b_reverse_mailbox = Mailbox::with_capacity(MAILBOX_CAPACITY);
    let relay_b_receiver2_forward_mailbox = Mailbox::with_capacity(MAILBOX_CAPACITY);
    let relay_b_receiver2_reverse_mailbox = Mailbox::with_capacity(MAILBOX_CAPACITY);
    let relay_b_receiver3_forward_mailbox = Mailbox::with_capacity(MAILBOX_CAPACITY);
    let relay_b_receiver3_reverse_mailbox = Mailbox::with_capacity(MAILBOX_CAPACITY);

    source
        .data_output
        .connect(PhysicalLink::receive, &source_relay_a_forward_mailbox);
    source_relay_a_forward
        .output
        .connect(FanoutRelayEndpoint::upstream_segment, &relay_a_mailbox);
    relay_a
        .upstream_ack_output
        .connect(PhysicalLink::receive, &source_relay_a_reverse_mailbox);
    source_relay_a_reverse
        .output
        .connect(TreeSourceEndpoint::acknowledgment, &source_mailbox);

    connect_relay_a_children(
        &mut relay_a,
        &relay_a_mailbox,
        &relay_a_receiver1_forward_mailbox,
        &relay_a_relay_b_forward_mailbox,
        &mut relay_a_receiver1_reverse,
        &mut relay_a_relay_b_reverse,
        &scenario.relay_a_children,
    );
    connect_relay_b_children(
        &mut relay_b,
        &relay_b_mailbox,
        &relay_b_receiver2_forward_mailbox,
        &relay_b_receiver3_forward_mailbox,
        &mut relay_b_receiver2_reverse,
        &mut relay_b_receiver3_reverse,
        &scenario.relay_b_children,
    );

    relay_a_receiver1_forward
        .output
        .connect(TreeReceiverEndpoint::segment, &receiver1_mailbox);
    receiver1
        .ack_output
        .connect(PhysicalLink::receive, &relay_a_receiver1_reverse_mailbox);
    relay_a_relay_b_forward
        .output
        .connect(FanoutRelayEndpoint::upstream_segment, &relay_b_mailbox);
    relay_b
        .upstream_ack_output
        .connect(PhysicalLink::receive, &relay_a_relay_b_reverse_mailbox);
    relay_b_receiver2_forward
        .output
        .connect(TreeReceiverEndpoint::segment, &receiver2_mailbox);
    receiver2
        .ack_output
        .connect(PhysicalLink::receive, &relay_b_receiver2_reverse_mailbox);
    relay_b_receiver3_forward
        .output
        .connect(TreeReceiverEndpoint::segment, &receiver3_mailbox);
    receiver3
        .ack_output
        .connect(PhysicalLink::receive, &relay_b_receiver3_reverse_mailbox);

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

    let bench = match scenario.registration_order {
        RegistrationOrder::Forward => SimInit::with_num_threads(1)
            .add_model(source, source_mailbox, TreeSourceEndpoint::MAILBOX)
            .add_model(relay_a, relay_a_mailbox, RELAY_A_MAILBOX)
            .add_model(relay_b, relay_b_mailbox, RELAY_B_MAILBOX)
            .add_model(receiver1, receiver1_mailbox, RECEIVER_1_MAILBOX)
            .add_model(receiver2, receiver2_mailbox, RECEIVER_2_MAILBOX)
            .add_model(receiver3, receiver3_mailbox, RECEIVER_3_MAILBOX),
        RegistrationOrder::Reverse => SimInit::with_num_threads(1)
            .add_model(receiver3, receiver3_mailbox, RECEIVER_3_MAILBOX)
            .add_model(receiver2, receiver2_mailbox, RECEIVER_2_MAILBOX)
            .add_model(receiver1, receiver1_mailbox, RECEIVER_1_MAILBOX)
            .add_model(relay_b, relay_b_mailbox, RELAY_B_MAILBOX)
            .add_model(relay_a, relay_a_mailbox, RELAY_A_MAILBOX)
            .add_model(source, source_mailbox, TreeSourceEndpoint::MAILBOX),
    };
    let mut links = vec![
        (
            source_relay_a_forward,
            source_relay_a_forward_mailbox,
            SOURCE_RELAY_A_FORWARD,
        ),
        (
            source_relay_a_reverse,
            source_relay_a_reverse_mailbox,
            SOURCE_RELAY_A_REVERSE,
        ),
        (
            relay_a_receiver1_forward,
            relay_a_receiver1_forward_mailbox,
            RELAY_A_RECEIVER_1_FORWARD,
        ),
        (
            relay_a_receiver1_reverse,
            relay_a_receiver1_reverse_mailbox,
            RELAY_A_RECEIVER_1_REVERSE,
        ),
        (
            relay_a_relay_b_forward,
            relay_a_relay_b_forward_mailbox,
            RELAY_A_RELAY_B_FORWARD,
        ),
        (
            relay_a_relay_b_reverse,
            relay_a_relay_b_reverse_mailbox,
            RELAY_A_RELAY_B_REVERSE,
        ),
        (
            relay_b_receiver2_forward,
            relay_b_receiver2_forward_mailbox,
            RELAY_B_RECEIVER_2_FORWARD,
        ),
        (
            relay_b_receiver2_reverse,
            relay_b_receiver2_reverse_mailbox,
            RELAY_B_RECEIVER_2_REVERSE,
        ),
        (
            relay_b_receiver3_forward,
            relay_b_receiver3_forward_mailbox,
            RELAY_B_RECEIVER_3_FORWARD,
        ),
        (
            relay_b_receiver3_reverse,
            relay_b_receiver3_reverse_mailbox,
            RELAY_B_RECEIVER_3_REVERSE,
        ),
    ];
    if scenario.registration_order == RegistrationOrder::Reverse {
        links.reverse();
    }
    let mut bench = bench;
    for (link, mailbox, name) in links {
        bench = bench.add_model(link, mailbox, name);
    }
    let mut simulation = bench
        .init(MonotonicTime::EPOCH)
        .map_err(|error| TreeRunError::Simulation(error.to_string()))?;
    simulation
        .step_until(Duration::from_nanos(scenario.plateau_probe_at_ns))
        .map_err(|error| TreeRunError::Simulation(error.to_string()))?;
    let ownership_at_probe = ownership.snapshot();
    let ownership_total_at_probe = checked_owner_total(&ownership_at_probe)?;
    recorder.record(
        scenario.plateau_probe_at_ns,
        "simulation",
        "ownership_probe",
        0,
        ownership_at_probe.len(),
        ownership_total_at_probe,
        ownership_total_at_probe,
    );
    simulation
        .step_until(Duration::from_nanos(scenario.simulation_end_ns))
        .map_err(|error| TreeRunError::Simulation(error.to_string()))?;

    if let Some(failure) = recorder.failure() {
        return Err(TreeRunError::Model(failure));
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
    let source_bytes_admitted_at_probe = records
        .iter()
        .filter(|record| {
            record.component == "source"
                && record.event == "application_write"
                && record.time_ns <= scenario.plateau_probe_at_ns
        })
        .map(|record| record.value)
        .max()
        .unwrap_or(0);
    let completion_times_ns = ["receiver1", "receiver2", "receiver3"]
        .into_iter()
        .filter_map(|receiver| {
            records
                .iter()
                .find(|record| record.component == receiver && record.event == "stream_complete")
                .map(|record| (receiver, record.time_ns))
        })
        .collect();
    let upstream_prefix = checked_sum([
        scenario.socket_send_buffer_bytes,
        scenario.socket_receive_buffer_bytes,
        scenario.relay_application_buffer_bytes,
    ])?;
    let leaf_chain = checked_sum([
        scenario.relay_child_queue_bytes,
        scenario.socket_send_buffer_bytes,
        scenario.socket_receive_buffer_bytes,
    ])?;
    let expected_source_plateau_bytes = upstream_prefix
        .checked_add(leaf_chain)
        .ok_or_else(|| TreeRunError::Construction("plateau geometry overflow".to_owned()))?
        .min(stream_bytes);
    let expected_resident_copy_plateau_bytes =
        upstream_prefix
            .checked_add(leaf_chain.checked_mul(3).ok_or_else(|| {
                TreeRunError::Construction("plateau geometry overflow".to_owned())
            })?)
            .ok_or_else(|| TreeRunError::Construction("plateau geometry overflow".to_owned()))?;

    Ok(TreeOutcome {
        csv: recorder.to_csv()?,
        records,
        mailbox_high_water,
        ownership_at_probe,
        ownership_total_at_probe,
        source_bytes_admitted_at_probe,
        expected_source_plateau_bytes,
        expected_resident_copy_plateau_bytes,
        completion_times_ns,
    })
}

#[allow(clippy::too_many_arguments)]
fn tree_receiver(
    component: &'static str,
    mailbox: &'static str,
    reverse_link: &'static str,
    receive_owner: &'static str,
    runtime_owner: &'static str,
    data_inbox_owner: &'static str,
    service_owner: &'static str,
    flow_id: usize,
    timing: crate::scenario::ReceiverTiming,
    socket: SocketPairConfig,
    stream: FramedStream,
    scenario: &TreeScenario,
    recorder: Recorder,
    mailbox_tracker: MailboxTracker,
    ownership: OwnershipLedger,
) -> Result<TreeReceiverEndpoint, TreeRunError> {
    TreeReceiverEndpoint::new(
        component,
        mailbox,
        reverse_link,
        receive_owner,
        runtime_owner,
        data_inbox_owner,
        service_owner,
        flow_id,
        socket,
        stream,
        scenario.frame_payload_bytes,
        scenario.source_symbols_k,
        scenario.runtime_command_capacity_frames,
        scenario.receiver_data_inbox_capacity_frames,
        scenario.receiver_control_inbox_capacity_frames,
        timing,
        scenario.runtime_command_service_ns,
        scenario.decoder_sink_service_ns,
        recorder,
        mailbox_tracker,
        ownership,
    )
    .map_err(|error| TreeRunError::Construction(error.to_string()))
}

fn relay_a_child_spec(endpoint: TreeEndpoint) -> RelayChildSpec {
    match endpoint {
        TreeEndpoint::Receiver1 => RelayChildSpec {
            endpoint,
            flow_id: FLOW_RELAY_A_RECEIVER_1,
            forward_link_mailbox: RELAY_A_RECEIVER_1_FORWARD,
            queue_owner: "relay_a.receiver1.queue",
            send_owner: "relay_a.receiver1.sndbuf",
            downstream_receive_owner: "receiver1.tcp_rcv",
        },
        TreeEndpoint::RelayB => RelayChildSpec {
            endpoint,
            flow_id: FLOW_RELAY_A_RELAY_B,
            forward_link_mailbox: RELAY_A_RELAY_B_FORWARD,
            queue_owner: "relay_a.relay_b.queue",
            send_owner: "relay_a.relay_b.sndbuf",
            downstream_receive_owner: "relay_b.upstream_rcv",
        },
        TreeEndpoint::Receiver2 | TreeEndpoint::Receiver3 => {
            unreachable!("tree scenario validation restricts relay A children")
        }
    }
}

fn relay_b_child_spec(endpoint: TreeEndpoint) -> RelayChildSpec {
    match endpoint {
        TreeEndpoint::Receiver2 => RelayChildSpec {
            endpoint,
            flow_id: FLOW_RELAY_B_RECEIVER_2,
            forward_link_mailbox: RELAY_B_RECEIVER_2_FORWARD,
            queue_owner: "relay_b.receiver2.queue",
            send_owner: "relay_b.receiver2.sndbuf",
            downstream_receive_owner: "receiver2.tcp_rcv",
        },
        TreeEndpoint::Receiver3 => RelayChildSpec {
            endpoint,
            flow_id: FLOW_RELAY_B_RECEIVER_3,
            forward_link_mailbox: RELAY_B_RECEIVER_3_FORWARD,
            queue_owner: "relay_b.receiver3.queue",
            send_owner: "relay_b.receiver3.sndbuf",
            downstream_receive_owner: "receiver3.tcp_rcv",
        },
        TreeEndpoint::Receiver1 | TreeEndpoint::RelayB => {
            unreachable!("tree scenario validation restricts relay B children")
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn connect_relay_a_children(
    relay: &mut FanoutRelayEndpoint,
    relay_mailbox: &Mailbox<FanoutRelayEndpoint>,
    receiver1_forward: &Mailbox<PhysicalLink>,
    relay_b_forward: &Mailbox<PhysicalLink>,
    receiver1_reverse: &mut PhysicalLink,
    relay_b_reverse: &mut PhysicalLink,
    children: &[TreeEndpoint],
) {
    for (index, endpoint) in children.iter().copied().enumerate() {
        let forward = match endpoint {
            TreeEndpoint::Receiver1 => receiver1_forward,
            TreeEndpoint::RelayB => relay_b_forward,
            TreeEndpoint::Receiver2 | TreeEndpoint::Receiver3 => unreachable!("validated child"),
        };
        relay.child_data_outputs[index].connect(PhysicalLink::receive, forward);
        let reverse = match endpoint {
            TreeEndpoint::Receiver1 => &mut *receiver1_reverse,
            TreeEndpoint::RelayB => &mut *relay_b_reverse,
            TreeEndpoint::Receiver2 | TreeEndpoint::Receiver3 => unreachable!("validated child"),
        };
        if index == 0 {
            reverse
                .output
                .connect(FanoutRelayEndpoint::child0_acknowledgment, relay_mailbox);
        } else {
            reverse
                .output
                .connect(FanoutRelayEndpoint::child1_acknowledgment, relay_mailbox);
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn connect_relay_b_children(
    relay: &mut FanoutRelayEndpoint,
    relay_mailbox: &Mailbox<FanoutRelayEndpoint>,
    receiver2_forward: &Mailbox<PhysicalLink>,
    receiver3_forward: &Mailbox<PhysicalLink>,
    receiver2_reverse: &mut PhysicalLink,
    receiver3_reverse: &mut PhysicalLink,
    children: &[TreeEndpoint],
) {
    for (index, endpoint) in children.iter().copied().enumerate() {
        let forward = match endpoint {
            TreeEndpoint::Receiver2 => receiver2_forward,
            TreeEndpoint::Receiver3 => receiver3_forward,
            TreeEndpoint::Receiver1 | TreeEndpoint::RelayB => unreachable!("validated child"),
        };
        relay.child_data_outputs[index].connect(PhysicalLink::receive, forward);
        let reverse = match endpoint {
            TreeEndpoint::Receiver2 => &mut *receiver2_reverse,
            TreeEndpoint::Receiver3 => &mut *receiver3_reverse,
            TreeEndpoint::Receiver1 | TreeEndpoint::RelayB => unreachable!("validated child"),
        };
        if index == 0 {
            reverse
                .output
                .connect(FanoutRelayEndpoint::child0_acknowledgment, relay_mailbox);
        } else {
            reverse
                .output
                .connect(FanoutRelayEndpoint::child1_acknowledgment, relay_mailbox);
        }
    }
}

fn physical_link(
    mailbox: &'static str,
    downstream_mailbox: &'static str,
    scenario: &TreeScenario,
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
            drop_attempts: BTreeSet::new(),
        },
        recorder,
        mailbox_tracker,
    )
}

fn checked_owner_total(owners: &BTreeMap<&'static str, usize>) -> Result<usize, TreeRunError> {
    checked_sum(owners.values().copied())
}

fn checked_sum(values: impl IntoIterator<Item = usize>) -> Result<usize, TreeRunError> {
    values.into_iter().try_fold(0_usize, |sum, value| {
        sum.checked_add(value)
            .ok_or_else(|| TreeRunError::Construction("ownership geometry overflow".to_owned()))
    })
}
