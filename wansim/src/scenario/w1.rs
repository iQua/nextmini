use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use nexosim::simulation::{Mailbox, SimInit};
use nexosim::time::MonotonicTime;
use thiserror::Error;

use crate::days_bridge::{PhysicalLink, PhysicalLinkConfig};
use crate::determinism::CounterPrf;
use crate::metrics::{MAILBOX_CAPACITY, MailboxTracker, OwnershipLedger, Record, Recorder};
use crate::overlay::{
    ControlStream, FanoutRelayEndpoint, FramedStream, ReceiverControlGeometry, RelayChildSpec,
    W1ReceiverEndpoint, W1ReceiverProtocol, W1SourceEndpoint, W1SourceProtocol,
};
use crate::scenario::{FanoutAdmission, RegistrationOrder, W1Scenario, W1ScenarioError};
use crate::transport::SocketPairConfig;

const TREE_COUNT: usize = 2;
const HOPS_PER_TREE: usize = 5;
const RECEIVER_COUNT: usize = 3;

const SOURCE_COMPONENT: &str = "w1_source";
const SOURCE_MAILBOX: &str = "w1_source";
const RELAY_COMPONENTS: [[&str; 2]; TREE_COUNT] = [
    ["w1_tree0_relay_a", "w1_tree0_relay_b"],
    ["w1_tree1_relay_a", "w1_tree1_relay_b"],
];
const RELAY_MAILBOXES: [[&str; 2]; TREE_COUNT] = [
    ["w1_tree0_relay_a", "w1_tree0_relay_b"],
    ["w1_tree1_relay_a", "w1_tree1_relay_b"],
];
const RECEIVER_COMPONENTS: [&str; RECEIVER_COUNT] =
    ["w1_receiver1", "w1_receiver2", "w1_receiver3"];
const RECEIVER_MAILBOXES: [&str; RECEIVER_COUNT] = ["w1_receiver1", "w1_receiver2", "w1_receiver3"];
const DATA_FLOW_IDS: [[usize; HOPS_PER_TREE]; TREE_COUNT] = [
    [30_001, 30_002, 30_003, 30_004, 30_005],
    [31_001, 31_002, 31_003, 31_004, 31_005],
];
const DATA_FORWARD_LINKS: [[&str; HOPS_PER_TREE]; TREE_COUNT] = [
    [
        "w1_t0_source_relay_a_forward",
        "w1_t0_relay_a_receiver1_forward",
        "w1_t0_relay_a_relay_b_forward",
        "w1_t0_relay_b_receiver2_forward",
        "w1_t0_relay_b_receiver3_forward",
    ],
    [
        "w1_t1_source_relay_a_forward",
        "w1_t1_relay_a_receiver1_forward",
        "w1_t1_relay_a_relay_b_forward",
        "w1_t1_relay_b_receiver2_forward",
        "w1_t1_relay_b_receiver3_forward",
    ],
];
const DATA_REVERSE_LINKS: [[&str; HOPS_PER_TREE]; TREE_COUNT] = [
    [
        "w1_t0_source_relay_a_reverse",
        "w1_t0_relay_a_receiver1_reverse",
        "w1_t0_relay_a_relay_b_reverse",
        "w1_t0_relay_b_receiver2_reverse",
        "w1_t0_relay_b_receiver3_reverse",
    ],
    [
        "w1_t1_source_relay_a_reverse",
        "w1_t1_relay_a_receiver1_reverse",
        "w1_t1_relay_a_relay_b_reverse",
        "w1_t1_relay_b_receiver2_reverse",
        "w1_t1_relay_b_receiver3_reverse",
    ],
];
const CONTROL_FORWARD_LINKS: [&str; RECEIVER_COUNT] = [
    "w1_control_receiver1_forward",
    "w1_control_receiver2_forward",
    "w1_control_receiver3_forward",
];
const CONTROL_REVERSE_LINKS: [&str; RECEIVER_COUNT] = [
    "w1_control_receiver1_reverse",
    "w1_control_receiver2_reverse",
    "w1_control_receiver3_reverse",
];
const CONTROL_FLOW_IDS: [(usize, usize); RECEIVER_COUNT] =
    [(40_001, 40_002), (40_003, 40_004), (40_005, 40_006)];

#[derive(Clone, Debug)]
pub struct W1Outcome {
    pub csv: String,
    pub records: Vec<Record>,
    pub completion_times_ns: Vec<u64>,
    pub barrier_completion_ns: u64,
    pub sender_completion_ns: Option<u64>,
    pub total_emissions: usize,
    pub per_tree_emissions: [usize; TREE_COUNT],
    pub post_barrier_tail_emissions: usize,
    pub ack_flight_tail_emissions: usize,
    pub positive_round_deficits: usize,
    pub round_deficit_sum: usize,
    pub maximum_round_deficit: usize,
    pub application_drops: usize,
    pub link_drops: usize,
    pub mailbox_high_water: BTreeMap<&'static str, usize>,
}

#[derive(Debug, Error)]
pub enum W1RunError {
    #[error(transparent)]
    Scenario(#[from] W1ScenarioError),
    #[error("failed to construct W1 model: {0}")]
    Construction(String),
    #[error("nexosim W1 execution failed: {0}")]
    Simulation(String),
    #[error("W1 model failed: {0}")]
    Model(String),
    #[error("W1 active receiver {0} did not complete")]
    IncompleteReceiver(usize),
    #[error("failed to serialize deterministic W1 CSV: {0}")]
    Csv(#[from] csv::Error),
}

struct LinkSlot {
    model: PhysicalLink,
    mailbox: Mailbox<PhysicalLink>,
    name: &'static str,
}

pub fn run_w1(scenario: &W1Scenario) -> Result<W1Outcome, W1RunError> {
    scenario.validate()?;
    let recorder = Recorder::new(scenario.scenario_id.as_str(), scenario.master_seed);
    let mailbox_tracker = MailboxTracker::default();
    let ownership = OwnershipLedger::default();
    let data_socket = SocketPairConfig::new(
        scenario.tcp_mss_bytes,
        scenario.socket_send_buffer_bytes,
        scenario.socket_receive_buffer_bytes,
        scenario.initial_rto_ns,
        scenario.persist_interval_ns,
    )
    .map_err(|error| W1RunError::Construction(error.to_string()))?;
    let control_socket = data_socket;
    let maximum_frames = scenario.maximum_frames_per_tree()?;
    let frame_wire_bytes = scenario.frame_wire_bytes()?;
    let streams = [
        FramedStream::new(
            maximum_frames,
            scenario.frame_payload_bytes,
            CounterPrf::new(
                scenario.master_seed,
                &format!("{}-tree0", scenario.scenario_id),
            ),
        ),
        FramedStream::new(
            maximum_frames,
            scenario.frame_payload_bytes,
            CounterPrf::new(
                scenario.master_seed,
                &format!("{}-tree1", scenario.scenario_id),
            ),
        ),
    ];
    let [stream0, stream1] =
        streams.map(|stream| stream.map_err(|error| W1RunError::Construction(error.to_string())));
    let streams = [stream0?, stream1?];
    let peer_ids: Vec<u64> = (1..=scenario.active_receivers)
        .map(|peer| peer as u64)
        .collect();
    let quotas = scenario.quotas()?;
    let source_protocol = W1SourceProtocol::new(
        scenario.protocol,
        scenario.source_symbols,
        quotas.clone(),
        &peer_ids,
        0,
        scenario.carousel,
    )
    .map_err(|error| W1RunError::Construction(error.to_string()))?;
    let downlink_streams: Vec<_> = peer_ids.iter().map(|_| ControlStream::default()).collect();
    let uplink_streams: Vec<_> = peer_ids.iter().map(|_| ControlStream::default()).collect();
    let active_control_flows = CONTROL_FLOW_IDS[..scenario.active_receivers].to_vec();
    let active_control_forward_links = CONTROL_FORWARD_LINKS[..scenario.active_receivers].to_vec();
    let mut source = W1SourceEndpoint::new(
        SOURCE_COMPONENT,
        SOURCE_MAILBOX,
        [DATA_FLOW_IDS[0][0], DATA_FLOW_IDS[1][0]],
        [DATA_FORWARD_LINKS[0][0], DATA_FORWARD_LINKS[1][0]],
        data_socket,
        frame_wire_bytes,
        maximum_frames,
        source_protocol,
        &peer_ids,
        &active_control_flows,
        active_control_forward_links,
        control_socket,
        &downlink_streams,
        &uplink_streams,
        scenario.timer_interval_ns,
        recorder.clone(),
        mailbox_tracker.clone(),
    )
    .map_err(|error| W1RunError::Construction(error.to_string()))?;

    let mut relays = Vec::with_capacity(TREE_COUNT * 2);
    for (tree, stream) in streams.iter().enumerate() {
        relays.push(build_relay(
            tree,
            false,
            scenario,
            data_socket,
            stream.clone(),
            recorder.clone(),
            mailbox_tracker.clone(),
            ownership.clone(),
        )?);
        relays.push(build_relay(
            tree,
            true,
            scenario,
            data_socket,
            stream.clone(),
            recorder.clone(),
            mailbox_tracker.clone(),
            ownership.clone(),
        )?);
    }

    let mut receivers = Vec::with_capacity(RECEIVER_COUNT);
    for receiver in 0..RECEIVER_COUNT {
        let active = receiver < scenario.active_receivers;
        let protocol = W1ReceiverProtocol::new(
            scenario.protocol,
            (receiver + 1) as u64,
            scenario.source_symbols,
            quotas.clone(),
            0,
            scenario.carousel,
            active,
        )
        .map_err(|error| W1RunError::Construction(error.to_string()))?;
        let hop = receiver_hop(receiver);
        let control_geometry = active.then(|| ReceiverControlGeometry {
            downlink_flow_id: CONTROL_FLOW_IDS[receiver].0,
            uplink_flow_id: CONTROL_FLOW_IDS[receiver].1,
            reverse_link_mailbox: CONTROL_REVERSE_LINKS[receiver],
            downlink_stream: downlink_streams[receiver].clone(),
            uplink_stream: uplink_streams[receiver].clone(),
        });
        receivers.push(
            W1ReceiverEndpoint::new(
                RECEIVER_COMPONENTS[receiver],
                RECEIVER_MAILBOXES[receiver],
                [DATA_FLOW_IDS[0][hop], DATA_FLOW_IDS[1][hop]],
                [DATA_REVERSE_LINKS[0][hop], DATA_REVERSE_LINKS[1][hop]],
                data_socket,
                [streams[0].clone(), streams[1].clone()],
                scenario.frame_payload_bytes,
                scenario.runtime_command_capacity_frames,
                scenario.receiver_data_inbox_capacity_frames,
                crate::scenario::ReceiverAdmissionPolicy::HybridDrop,
                scenario.runtime_command_capacity_frames,
                scenario.runtime_command_service_ns,
                scenario.decoder_sink_service_ns,
                protocol,
                control_geometry,
                control_socket,
                scenario.timer_interval_ns,
                recorder.clone(),
                mailbox_tracker.clone(),
            )
            .map_err(|error| W1RunError::Construction(error.to_string()))?,
        );
    }

    let mut links = build_links(scenario, &recorder, &mailbox_tracker);
    let source_mailbox = Mailbox::with_capacity(MAILBOX_CAPACITY);
    let relay_mailboxes: Vec<_> = (0..relays.len())
        .map(|_| Mailbox::with_capacity(MAILBOX_CAPACITY))
        .collect();
    let receiver_mailboxes: Vec<_> = (0..RECEIVER_COUNT)
        .map(|_| Mailbox::with_capacity(MAILBOX_CAPACITY))
        .collect();

    wire_data_plane(
        &mut source,
        &source_mailbox,
        &mut relays,
        &relay_mailboxes,
        &mut receivers,
        &receiver_mailboxes,
        &mut links,
    );
    wire_control_plane(
        scenario.active_receivers,
        &mut source,
        &source_mailbox,
        &mut receivers,
        &receiver_mailboxes,
        &mut links,
    );

    recorder.record(0, "simulation", "single_worker", 0, 0, 0, 1);
    recorder.record(
        0,
        "simulation",
        "w1_sequential_admission_default",
        0,
        0,
        0,
        usize::from(scenario.fanout_admission == FanoutAdmission::Sequential),
    );
    recorder.record(
        0,
        "simulation",
        "control_scope_connections",
        0,
        0,
        scenario.active_receivers * 2,
        scenario.active_receivers,
    );

    let mut bench = match scenario.registration_order {
        RegistrationOrder::Forward => {
            let mut bench =
                SimInit::with_num_threads(1).add_model(source, source_mailbox, SOURCE_MAILBOX);
            for (index, (relay, mailbox)) in relays.into_iter().zip(relay_mailboxes).enumerate() {
                bench = bench.add_model(relay, mailbox, RELAY_MAILBOXES[index / 2][index % 2]);
            }
            for (index, (receiver, mailbox)) in
                receivers.into_iter().zip(receiver_mailboxes).enumerate()
            {
                bench = bench.add_model(receiver, mailbox, RECEIVER_MAILBOXES[index]);
            }
            bench
        }
        RegistrationOrder::Reverse => {
            let mut bench = SimInit::with_num_threads(1);
            for (index, (receiver, mailbox)) in receivers
                .into_iter()
                .zip(receiver_mailboxes)
                .enumerate()
                .rev()
            {
                bench = bench.add_model(receiver, mailbox, RECEIVER_MAILBOXES[index]);
            }
            for (index, (relay, mailbox)) in
                relays.into_iter().zip(relay_mailboxes).enumerate().rev()
            {
                bench = bench.add_model(relay, mailbox, RELAY_MAILBOXES[index / 2][index % 2]);
            }
            bench.add_model(source, source_mailbox, SOURCE_MAILBOX)
        }
    };
    if scenario.registration_order == RegistrationOrder::Reverse {
        links.reverse();
    }
    for slot in links {
        bench = bench.add_model(slot.model, slot.mailbox, slot.name);
    }
    let mut simulation = bench
        .init(MonotonicTime::EPOCH)
        .map_err(|error| W1RunError::Simulation(error.to_string()))?;
    simulation
        .step_until(Duration::from_nanos(scenario.simulation_end_ns))
        .map_err(|error| W1RunError::Simulation(error.to_string()))?;
    if let Some(failure) = recorder.failure() {
        return Err(W1RunError::Model(failure));
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
    summarize_outcome(scenario, recorder.to_csv()?, records, mailbox_high_water)
}

#[allow(clippy::too_many_arguments)]
fn build_relay(
    tree: usize,
    relay_b: bool,
    scenario: &W1Scenario,
    socket: SocketPairConfig,
    stream: FramedStream,
    recorder: Recorder,
    mailbox_tracker: MailboxTracker,
    ownership: OwnershipLedger,
) -> Result<FanoutRelayEndpoint, W1RunError> {
    let relay = usize::from(relay_b);
    let upstream_hop = if relay_b { 2 } else { 0 };
    let (upstream_receive_owner, application_owner) = relay_owners(tree, relay_b);
    let child_specs = if relay_b {
        [child_spec(tree, 3), child_spec(tree, 4)]
    } else {
        [child_spec(tree, 1), child_spec(tree, 2)]
    };
    FanoutRelayEndpoint::new(
        RELAY_COMPONENTS[tree][relay],
        RELAY_MAILBOXES[tree][relay],
        DATA_FLOW_IDS[tree][upstream_hop],
        DATA_REVERSE_LINKS[tree][upstream_hop],
        upstream_receive_owner,
        application_owner,
        socket,
        socket,
        child_specs.into(),
        stream,
        scenario.frame_payload_bytes,
        scenario.relay_application_buffer_bytes,
        scenario.relay_child_queue_bytes,
        scenario.fanout_admission,
        scenario.timer_interval_ns,
        recorder,
        mailbox_tracker,
        ownership,
    )
    .map_err(|error| W1RunError::Construction(error.to_string()))
}

fn child_spec(tree: usize, hop: usize) -> RelayChildSpec {
    let endpoint_component = match hop {
        1 => RECEIVER_COMPONENTS[0],
        2 => RELAY_COMPONENTS[tree][1],
        3 => RECEIVER_COMPONENTS[1],
        4 => RECEIVER_COMPONENTS[2],
        _ => unreachable!("W1 relay child hop"),
    };
    let (queue_owner, send_owner, receive_owner) = child_owners(tree, hop);
    RelayChildSpec {
        endpoint_component,
        flow_id: DATA_FLOW_IDS[tree][hop],
        forward_link_mailbox: DATA_FORWARD_LINKS[tree][hop],
        queue_owner,
        send_owner,
        downstream_receive_owner: receive_owner,
    }
}

fn relay_owners(tree: usize, relay_b: bool) -> (&'static str, &'static str) {
    match (tree, relay_b) {
        (0, false) => ("w1.t0.ra.rcv", "w1.t0.ra.app"),
        (0, true) => ("w1.t0.rb.rcv", "w1.t0.rb.app"),
        (1, false) => ("w1.t1.ra.rcv", "w1.t1.ra.app"),
        (1, true) => ("w1.t1.rb.rcv", "w1.t1.rb.app"),
        _ => unreachable!("two W1 trees"),
    }
}

fn child_owners(tree: usize, hop: usize) -> (&'static str, &'static str, &'static str) {
    match (tree, hop) {
        (0, 1) => ("w1.t0.ra.r1.q", "w1.t0.ra.r1.snd", "w1.t0.r1.rcv"),
        (0, 2) => ("w1.t0.ra.rb.q", "w1.t0.ra.rb.snd", "w1.t0.rb.rcv"),
        (0, 3) => ("w1.t0.rb.r2.q", "w1.t0.rb.r2.snd", "w1.t0.r2.rcv"),
        (0, 4) => ("w1.t0.rb.r3.q", "w1.t0.rb.r3.snd", "w1.t0.r3.rcv"),
        (1, 1) => ("w1.t1.ra.r1.q", "w1.t1.ra.r1.snd", "w1.t1.r1.rcv"),
        (1, 2) => ("w1.t1.ra.rb.q", "w1.t1.ra.rb.snd", "w1.t1.rb.rcv"),
        (1, 3) => ("w1.t1.rb.r2.q", "w1.t1.rb.r2.snd", "w1.t1.r2.rcv"),
        (1, 4) => ("w1.t1.rb.r3.q", "w1.t1.rb.r3.snd", "w1.t1.r3.rcv"),
        _ => unreachable!("W1 child owner"),
    }
}

fn build_links(
    scenario: &W1Scenario,
    recorder: &Recorder,
    mailbox_tracker: &MailboxTracker,
) -> Vec<LinkSlot> {
    let rates = scenario.data_rates_bps();
    let mut links = Vec::with_capacity(TREE_COUNT * HOPS_PER_TREE * 2 + RECEIVER_COUNT * 2);
    for tree in 0..TREE_COUNT {
        for hop in 0..HOPS_PER_TREE {
            links.push(link_slot(
                DATA_FORWARD_LINKS[tree][hop],
                forward_downstream(tree, hop),
                rates[tree][hop],
                scenario.link_propagation_ns,
                scenario.link_queue_bytes,
                recorder,
                mailbox_tracker,
            ));
            links.push(link_slot(
                DATA_REVERSE_LINKS[tree][hop],
                reverse_downstream(tree, hop),
                rates[tree][hop],
                scenario.link_propagation_ns,
                scenario.link_queue_bytes,
                recorder,
                mailbox_tracker,
            ));
        }
    }
    for receiver in 0..scenario.active_receivers {
        links.push(link_slot(
            CONTROL_FORWARD_LINKS[receiver],
            RECEIVER_MAILBOXES[receiver],
            scenario.control_rate_bps,
            scenario.control_propagation_ns[receiver],
            scenario.link_queue_bytes,
            recorder,
            mailbox_tracker,
        ));
        links.push(link_slot(
            CONTROL_REVERSE_LINKS[receiver],
            SOURCE_MAILBOX,
            scenario.control_rate_bps,
            scenario.control_propagation_ns[receiver],
            scenario.link_queue_bytes,
            recorder,
            mailbox_tracker,
        ));
    }
    links
}

#[allow(clippy::too_many_arguments)]
fn link_slot(
    name: &'static str,
    downstream: &'static str,
    rate_bps: u64,
    propagation_ns: u64,
    queue_bytes: usize,
    recorder: &Recorder,
    mailbox_tracker: &MailboxTracker,
) -> LinkSlot {
    LinkSlot {
        model: PhysicalLink::new(
            PhysicalLinkConfig {
                component: name,
                mailbox: name,
                downstream_mailbox: downstream,
                rate_bps,
                propagation_ns,
                queue_bytes,
                drop_attempts: BTreeSet::new(),
            },
            recorder.clone(),
            mailbox_tracker.clone(),
        ),
        mailbox: Mailbox::with_capacity(MAILBOX_CAPACITY),
        name,
    }
}

fn forward_downstream(tree: usize, hop: usize) -> &'static str {
    match hop {
        0 => RELAY_MAILBOXES[tree][0],
        1 => RECEIVER_MAILBOXES[0],
        2 => RELAY_MAILBOXES[tree][1],
        3 => RECEIVER_MAILBOXES[1],
        4 => RECEIVER_MAILBOXES[2],
        _ => unreachable!("W1 forward hop"),
    }
}

fn reverse_downstream(tree: usize, hop: usize) -> &'static str {
    match hop {
        0 => SOURCE_MAILBOX,
        1 | 2 => RELAY_MAILBOXES[tree][0],
        3 | 4 => RELAY_MAILBOXES[tree][1],
        _ => unreachable!("W1 reverse hop"),
    }
}

fn data_link_index(tree: usize, hop: usize, reverse: bool) -> usize {
    (tree * HOPS_PER_TREE * 2) + (hop * 2) + usize::from(reverse)
}

fn control_link_index(receiver: usize, reverse: bool) -> usize {
    (TREE_COUNT * HOPS_PER_TREE * 2) + (receiver * 2) + usize::from(reverse)
}

#[allow(clippy::too_many_arguments)]
fn wire_data_plane(
    source: &mut W1SourceEndpoint,
    source_mailbox: &Mailbox<W1SourceEndpoint>,
    relays: &mut [FanoutRelayEndpoint],
    relay_mailboxes: &[Mailbox<FanoutRelayEndpoint>],
    receivers: &mut [W1ReceiverEndpoint],
    receiver_mailboxes: &[Mailbox<W1ReceiverEndpoint>],
    links: &mut [LinkSlot],
) {
    for tree in 0..TREE_COUNT {
        let relay_a = tree * 2;
        let relay_b = relay_a + 1;

        source.data_outputs[tree].connect(
            PhysicalLink::receive,
            &links[data_link_index(tree, 0, false)].mailbox,
        );
        links[data_link_index(tree, 0, false)].model.output.connect(
            FanoutRelayEndpoint::upstream_segment,
            &relay_mailboxes[relay_a],
        );
        relays[relay_a].upstream_ack_output.connect(
            PhysicalLink::receive,
            &links[data_link_index(tree, 0, true)].mailbox,
        );
        if tree == 0 {
            links[data_link_index(tree, 0, true)]
                .model
                .output
                .connect(W1SourceEndpoint::data0_ack, source_mailbox);
        } else {
            links[data_link_index(tree, 0, true)]
                .model
                .output
                .connect(W1SourceEndpoint::data1_ack, source_mailbox);
        }

        relays[relay_a].child_data_outputs[0].connect(
            PhysicalLink::receive,
            &links[data_link_index(tree, 1, false)].mailbox,
        );
        connect_receiver_forward(tree, 0, 1, receivers, receiver_mailboxes, links);
        receivers[0].data_ack_outputs[tree].connect(
            PhysicalLink::receive,
            &links[data_link_index(tree, 1, true)].mailbox,
        );
        links[data_link_index(tree, 1, true)].model.output.connect(
            FanoutRelayEndpoint::child0_acknowledgment,
            &relay_mailboxes[relay_a],
        );

        relays[relay_a].child_data_outputs[1].connect(
            PhysicalLink::receive,
            &links[data_link_index(tree, 2, false)].mailbox,
        );
        links[data_link_index(tree, 2, false)].model.output.connect(
            FanoutRelayEndpoint::upstream_segment,
            &relay_mailboxes[relay_b],
        );
        relays[relay_b].upstream_ack_output.connect(
            PhysicalLink::receive,
            &links[data_link_index(tree, 2, true)].mailbox,
        );
        links[data_link_index(tree, 2, true)].model.output.connect(
            FanoutRelayEndpoint::child1_acknowledgment,
            &relay_mailboxes[relay_a],
        );

        relays[relay_b].child_data_outputs[0].connect(
            PhysicalLink::receive,
            &links[data_link_index(tree, 3, false)].mailbox,
        );
        connect_receiver_forward(tree, 1, 3, receivers, receiver_mailboxes, links);
        receivers[1].data_ack_outputs[tree].connect(
            PhysicalLink::receive,
            &links[data_link_index(tree, 3, true)].mailbox,
        );
        links[data_link_index(tree, 3, true)].model.output.connect(
            FanoutRelayEndpoint::child0_acknowledgment,
            &relay_mailboxes[relay_b],
        );

        relays[relay_b].child_data_outputs[1].connect(
            PhysicalLink::receive,
            &links[data_link_index(tree, 4, false)].mailbox,
        );
        connect_receiver_forward(tree, 2, 4, receivers, receiver_mailboxes, links);
        receivers[2].data_ack_outputs[tree].connect(
            PhysicalLink::receive,
            &links[data_link_index(tree, 4, true)].mailbox,
        );
        links[data_link_index(tree, 4, true)].model.output.connect(
            FanoutRelayEndpoint::child1_acknowledgment,
            &relay_mailboxes[relay_b],
        );
    }
}

fn connect_receiver_forward(
    tree: usize,
    receiver: usize,
    hop: usize,
    _receivers: &mut [W1ReceiverEndpoint],
    receiver_mailboxes: &[Mailbox<W1ReceiverEndpoint>],
    links: &mut [LinkSlot],
) {
    if tree == 0 {
        links[data_link_index(tree, hop, false)]
            .model
            .output
            .connect(
                W1ReceiverEndpoint::data0_segment,
                &receiver_mailboxes[receiver],
            );
    } else {
        links[data_link_index(tree, hop, false)]
            .model
            .output
            .connect(
                W1ReceiverEndpoint::data1_segment,
                &receiver_mailboxes[receiver],
            );
    }
}

fn wire_control_plane(
    active_receivers: usize,
    source: &mut W1SourceEndpoint,
    source_mailbox: &Mailbox<W1SourceEndpoint>,
    receivers: &mut [W1ReceiverEndpoint],
    receiver_mailboxes: &[Mailbox<W1ReceiverEndpoint>],
    links: &mut [LinkSlot],
) {
    for receiver in 0..active_receivers {
        let forward = control_link_index(receiver, false);
        let reverse = control_link_index(receiver, true);
        source.control_forward_outputs[receiver]
            .connect(PhysicalLink::receive, &links[forward].mailbox);
        links[forward].model.output.connect(
            W1ReceiverEndpoint::control_packet,
            &receiver_mailboxes[receiver],
        );
        receivers[receiver]
            .control_reverse_output
            .connect(PhysicalLink::receive, &links[reverse].mailbox);
        match receiver {
            0 => links[reverse]
                .model
                .output
                .connect(W1SourceEndpoint::control_peer0_packet, source_mailbox),
            1 => links[reverse]
                .model
                .output
                .connect(W1SourceEndpoint::control_peer1_packet, source_mailbox),
            2 => links[reverse]
                .model
                .output
                .connect(W1SourceEndpoint::control_peer2_packet, source_mailbox),
            _ => unreachable!("three W1 receivers"),
        }
    }
}

fn receiver_hop(receiver: usize) -> usize {
    match receiver {
        0 => 1,
        1 => 3,
        2 => 4,
        _ => unreachable!("three W1 receivers"),
    }
}

fn summarize_outcome(
    scenario: &W1Scenario,
    csv: String,
    records: Vec<Record>,
    mailbox_high_water: BTreeMap<&'static str, usize>,
) -> Result<W1Outcome, W1RunError> {
    let mut completion_times_ns = Vec::with_capacity(scenario.active_receivers);
    for (receiver, component) in RECEIVER_COMPONENTS
        .iter()
        .enumerate()
        .take(scenario.active_receivers)
    {
        let completion = records
            .iter()
            .find(|record| {
                record.component == *component && record.event == "protocol_local_complete"
            })
            .map(|record| record.time_ns)
            .ok_or(W1RunError::IncompleteReceiver(receiver + 1))?;
        completion_times_ns.push(completion);
    }
    let barrier_completion_ns = completion_times_ns.iter().copied().max().unwrap_or(0);
    let sender_completion_ns = records
        .iter()
        .find(|record| {
            record.component == SOURCE_COMPONENT && record.event == "protocol_sender_complete"
        })
        .map(|record| record.time_ns);
    let total_emissions = records
        .iter()
        .filter(|record| {
            record.component == SOURCE_COMPONENT && record.event == "data_frame_emitted"
        })
        .count();
    let per_tree_emissions = [
        records
            .iter()
            .filter(|record| {
                record.component == SOURCE_COMPONENT
                    && record.event == "data_frame_emitted"
                    && record.flow_id == DATA_FLOW_IDS[0][0]
            })
            .count(),
        records
            .iter()
            .filter(|record| {
                record.component == SOURCE_COMPONENT
                    && record.event == "data_frame_emitted"
                    && record.flow_id == DATA_FLOW_IDS[1][0]
            })
            .count(),
    ];
    let post_barrier_tail_emissions = records
        .iter()
        .filter(|record| {
            record.component == SOURCE_COMPONENT
                && record.event == "data_frame_emitted"
                && record.time_ns > barrier_completion_ns
        })
        .count();
    let ack_flight_tail_emissions =
        if scenario.protocol == crate::protocol::ProtocolKind::PooledCarousel {
            total_emissions.saturating_sub(scenario.source_symbols)
        } else {
            0
        };
    let positive_round_deficits = records
        .iter()
        .filter(|record| record.event == "round_deficit_received" && record.value > 0)
        .count();
    let round_deficit_sum = records
        .iter()
        .filter(|record| record.event == "round_deficit_received" && record.value > 0)
        .map(|record| record.value)
        .sum();
    let maximum_round_deficit = records
        .iter()
        .filter(|record| record.event == "round_deficit_received")
        .map(|record| record.value)
        .max()
        .unwrap_or(0);
    let application_drops = records
        .iter()
        .filter(|record| record.event == "data_inbox_drop_after_tcp_ack")
        .count();
    let link_drops = records
        .iter()
        .filter(|record| matches!(record.event, "queue_drop" | "segment_drop"))
        .count();
    Ok(W1Outcome {
        csv,
        records,
        completion_times_ns,
        barrier_completion_ns,
        sender_completion_ns,
        total_emissions,
        per_tree_emissions,
        post_barrier_tail_emissions,
        ack_flight_tail_emissions,
        positive_round_deficits,
        round_deficit_sum,
        maximum_round_deficit,
        application_drops,
        link_drops,
        mailbox_high_water,
    })
}
