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
use crate::protocol::ProtocolKind;
use crate::scenario::{
    FanoutAdmission, ReceiverAdmissionPolicy, RegistrationOrder, W2Scenario, W2ScenarioError,
};
use crate::transport::SocketPairConfig;

const TREE_COUNT: usize = 2;
const MAX_RECEIVERS: usize = 8;
const MAX_RELAYS_PER_TREE: usize = 7;
const MAX_EDGES_PER_TREE: usize = 15;
const CONTROL_RUNTIME_RESERVE: usize = 32;

const SOURCE_COMPONENT: &str = "w2_source";
const SOURCE_MAILBOX: &str = "w2_source";
const RECEIVER_COMPONENTS: [&str; MAX_RECEIVERS] = [
    "w2_receiver1",
    "w2_receiver2",
    "w2_receiver3",
    "w2_receiver4",
    "w2_receiver5",
    "w2_receiver6",
    "w2_receiver7",
    "w2_receiver8",
];
const RELAY_COMPONENTS: [[&str; MAX_RELAYS_PER_TREE]; TREE_COUNT] = [
    [
        "w2_t0_relay0",
        "w2_t0_relay1",
        "w2_t0_relay2",
        "w2_t0_relay3",
        "w2_t0_relay4",
        "w2_t0_relay5",
        "w2_t0_relay6",
    ],
    [
        "w2_t1_relay0",
        "w2_t1_relay1",
        "w2_t1_relay2",
        "w2_t1_relay3",
        "w2_t1_relay4",
        "w2_t1_relay5",
        "w2_t1_relay6",
    ],
];
const RELAY_APPLICATION_OWNERS: [[&str; MAX_RELAYS_PER_TREE]; TREE_COUNT] = [
    [
        "w2.t0.r0.app",
        "w2.t0.r1.app",
        "w2.t0.r2.app",
        "w2.t0.r3.app",
        "w2.t0.r4.app",
        "w2.t0.r5.app",
        "w2.t0.r6.app",
    ],
    [
        "w2.t1.r0.app",
        "w2.t1.r1.app",
        "w2.t1.r2.app",
        "w2.t1.r3.app",
        "w2.t1.r4.app",
        "w2.t1.r5.app",
        "w2.t1.r6.app",
    ],
];
const DATA_FORWARD_LINKS: [[&str; MAX_EDGES_PER_TREE]; TREE_COUNT] = [
    [
        "w2_t0_f00",
        "w2_t0_f01",
        "w2_t0_f02",
        "w2_t0_f03",
        "w2_t0_f04",
        "w2_t0_f05",
        "w2_t0_f06",
        "w2_t0_f07",
        "w2_t0_f08",
        "w2_t0_f09",
        "w2_t0_f10",
        "w2_t0_f11",
        "w2_t0_f12",
        "w2_t0_f13",
        "w2_t0_f14",
    ],
    [
        "w2_t1_f00",
        "w2_t1_f01",
        "w2_t1_f02",
        "w2_t1_f03",
        "w2_t1_f04",
        "w2_t1_f05",
        "w2_t1_f06",
        "w2_t1_f07",
        "w2_t1_f08",
        "w2_t1_f09",
        "w2_t1_f10",
        "w2_t1_f11",
        "w2_t1_f12",
        "w2_t1_f13",
        "w2_t1_f14",
    ],
];
const DATA_REVERSE_LINKS: [[&str; MAX_EDGES_PER_TREE]; TREE_COUNT] = [
    [
        "w2_t0_r00",
        "w2_t0_r01",
        "w2_t0_r02",
        "w2_t0_r03",
        "w2_t0_r04",
        "w2_t0_r05",
        "w2_t0_r06",
        "w2_t0_r07",
        "w2_t0_r08",
        "w2_t0_r09",
        "w2_t0_r10",
        "w2_t0_r11",
        "w2_t0_r12",
        "w2_t0_r13",
        "w2_t0_r14",
    ],
    [
        "w2_t1_r00",
        "w2_t1_r01",
        "w2_t1_r02",
        "w2_t1_r03",
        "w2_t1_r04",
        "w2_t1_r05",
        "w2_t1_r06",
        "w2_t1_r07",
        "w2_t1_r08",
        "w2_t1_r09",
        "w2_t1_r10",
        "w2_t1_r11",
        "w2_t1_r12",
        "w2_t1_r13",
        "w2_t1_r14",
    ],
];
const CONTROL_FORWARD_LINKS: [&str; MAX_RECEIVERS] = [
    "w2_c1_forward",
    "w2_c2_forward",
    "w2_c3_forward",
    "w2_c4_forward",
    "w2_c5_forward",
    "w2_c6_forward",
    "w2_c7_forward",
    "w2_c8_forward",
];
const CONTROL_REVERSE_LINKS: [&str; MAX_RECEIVERS] = [
    "w2_c1_reverse",
    "w2_c2_reverse",
    "w2_c3_reverse",
    "w2_c4_reverse",
    "w2_c5_reverse",
    "w2_c6_reverse",
    "w2_c7_reverse",
    "w2_c8_reverse",
];

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CriticalPathAttribution {
    pub receiver: usize,
    pub final_tree: usize,
    pub final_frame_id: usize,
    pub source_to_runtime_ns: u64,
    pub runtime_wait_ns: u64,
    pub decoder_queue_ns: u64,
    pub decoder_service_ns: u64,
}

#[derive(Clone, Debug)]
pub struct W2Outcome {
    pub csv: String,
    pub records: Vec<Record>,
    pub completion_times_ns: Vec<u64>,
    pub barrier_completion_ns: u64,
    pub sender_completion_ns: Option<u64>,
    pub total_emissions: usize,
    pub per_tree_emissions: [usize; TREE_COUNT],
    pub post_completion_tail_emissions: usize,
    pub application_drops: usize,
    pub blocking_wait_events: usize,
    pub isolated_credit_deferrals: usize,
    pub isolated_credit_replays: usize,
    pub link_drops: usize,
    pub liveness_pressure_permille: u64,
    pub receiver_hops: Vec<usize>,
    pub critical_paths: Vec<CriticalPathAttribution>,
    pub mailbox_high_water: BTreeMap<&'static str, usize>,
}

#[derive(Debug, Error)]
pub enum W2RunError {
    #[error(transparent)]
    Scenario(#[from] W2ScenarioError),
    #[error("failed to construct W2 model: {0}")]
    Construction(String),
    #[error("nexosim W2 execution failed: {0}")]
    Simulation(String),
    #[error("W2 model failed: {0}")]
    Model(String),
    #[error("W2 receiver {0} did not complete")]
    IncompleteReceiver(usize),
    #[error("failed to serialize deterministic W2 CSV: {0}")]
    Csv(#[from] csv::Error),
}

#[derive(Clone, Copy, Debug)]
enum Endpoint {
    Relay(usize),
    Receiver(usize),
}

#[derive(Clone, Debug, Default)]
struct RelayNode {
    children: Vec<Endpoint>,
}

#[derive(Clone, Copy, Debug)]
struct Edge {
    parent: Option<usize>,
    child: Endpoint,
    child_slot: usize,
}

#[derive(Clone, Debug)]
struct Topology {
    relays: Vec<RelayNode>,
    edges: Vec<Edge>,
    relay_upstream_edges: Vec<usize>,
    relay_child_edges: Vec<Vec<usize>>,
    receiver_edges: Vec<usize>,
    receiver_hops: Vec<usize>,
}

struct LinkSlot {
    model: PhysicalLink,
    mailbox: Mailbox<PhysicalLink>,
    name: &'static str,
}

pub fn run_w2(scenario: &W2Scenario) -> Result<W2Outcome, W2RunError> {
    scenario.validate()?;
    let topology = build_topology(&scenario.ordered_receivers(), scenario.fanout_degree);
    let geometry = scenario.buffer_geometry()?;
    let recorder = Recorder::new(scenario.scenario_id.as_str(), scenario.master_seed);
    let mailbox_tracker = MailboxTracker::default();
    let ownership = OwnershipLedger::default();
    let socket = SocketPairConfig::new(
        scenario.tcp_mss_bytes,
        geometry.socket_send_bytes,
        geometry.socket_receive_bytes,
        scenario.initial_rto_ns,
        scenario.persist_interval_ns,
    )
    .map_err(|error| W2RunError::Construction(error.to_string()))?;
    let frame_wire_bytes = scenario.frame_wire_bytes()?;
    let maximum_frames = scenario.maximum_frames_per_tree()?;
    let streams = [0, 1].map(|tree| {
        FramedStream::new(
            maximum_frames,
            scenario.frame_payload_bytes,
            CounterPrf::new(
                scenario.master_seed,
                &format!("{}-tree{tree}", scenario.scenario_id),
            ),
        )
        .map_err(|error| W2RunError::Construction(error.to_string()))
    });
    let [stream0, stream1] = streams;
    let streams = [stream0?, stream1?];
    let peer_ids: Vec<u64> = (1..=scenario.receiver_count)
        .map(|peer| peer as u64)
        .collect();
    let source_protocol = W1SourceProtocol::new(
        ProtocolKind::PooledCarousel,
        scenario.source_symbols,
        vec![
            scenario.source_symbols.div_ceil(2),
            scenario.source_symbols / 2,
        ],
        &peer_ids,
        0,
        scenario.carousel,
    )
    .map_err(|error| W2RunError::Construction(error.to_string()))?;
    let downlink_streams: Vec<_> = peer_ids.iter().map(|_| ControlStream::default()).collect();
    let uplink_streams: Vec<_> = peer_ids.iter().map(|_| ControlStream::default()).collect();
    let active_control_flow_ids: Vec<_> =
        (0..scenario.receiver_count).map(control_flow_ids).collect();
    let mut source = W1SourceEndpoint::new(
        SOURCE_COMPONENT,
        SOURCE_MAILBOX,
        [data_flow_id(0, 0), data_flow_id(1, 0)],
        [DATA_FORWARD_LINKS[0][0], DATA_FORWARD_LINKS[1][0]],
        socket,
        frame_wire_bytes,
        maximum_frames,
        source_protocol,
        &peer_ids,
        &active_control_flow_ids,
        CONTROL_FORWARD_LINKS[..scenario.receiver_count].to_vec(),
        socket,
        &downlink_streams,
        &uplink_streams,
        scenario.timer_interval_ns,
        recorder.clone(),
        mailbox_tracker.clone(),
    )
    .map_err(|error| W2RunError::Construction(error.to_string()))?;

    let relay_admission = if scenario.admission_policy == ReceiverAdmissionPolicy::IsolatedCredit {
        FanoutAdmission::IsolatedCredit
    } else {
        scenario.fanout_admission
    };
    let mut relays = Vec::with_capacity(TREE_COUNT * topology.relays.len());
    for tree in 0..TREE_COUNT {
        for relay in 0..topology.relays.len() {
            let upstream_edge = topology.relay_upstream_edges[relay];
            let child_specs = topology.relay_child_edges[relay]
                .iter()
                .map(|edge_index| {
                    let edge = topology.edges[*edge_index];
                    RelayChildSpec {
                        endpoint_component: endpoint_component(tree, edge.child),
                        flow_id: data_flow_id(tree, *edge_index),
                        forward_link_mailbox: DATA_FORWARD_LINKS[tree][*edge_index],
                        queue_owner: DATA_FORWARD_LINKS[tree][*edge_index],
                        send_owner: DATA_REVERSE_LINKS[tree][*edge_index],
                        downstream_receive_owner: endpoint_component(tree, edge.child),
                    }
                })
                .collect();
            relays.push(
                FanoutRelayEndpoint::new(
                    RELAY_COMPONENTS[tree][relay],
                    RELAY_COMPONENTS[tree][relay],
                    data_flow_id(tree, upstream_edge),
                    DATA_REVERSE_LINKS[tree][upstream_edge],
                    RELAY_COMPONENTS[tree][relay],
                    RELAY_APPLICATION_OWNERS[tree][relay],
                    socket,
                    socket,
                    child_specs,
                    streams[tree].clone(),
                    scenario.frame_payload_bytes,
                    geometry.relay_application_bytes,
                    geometry.relay_child_queue_bytes,
                    relay_admission,
                    scenario.timer_interval_ns,
                    recorder.clone(),
                    mailbox_tracker.clone(),
                    ownership.clone(),
                )
                .map_err(|error| W2RunError::Construction(error.to_string()))?,
            );
        }
    }

    let mut receivers = Vec::with_capacity(scenario.receiver_count);
    for receiver in 0..scenario.receiver_count {
        let edge = topology.receiver_edges[receiver];
        let protocol = W1ReceiverProtocol::new(
            ProtocolKind::PooledCarousel,
            (receiver + 1) as u64,
            scenario.source_symbols,
            vec![
                scenario.source_symbols.div_ceil(2),
                scenario.source_symbols / 2,
            ],
            0,
            scenario.carousel,
            true,
        )
        .map_err(|error| W2RunError::Construction(error.to_string()))?;
        let (downlink_flow_id, uplink_flow_id) = control_flow_ids(receiver);
        receivers.push(
            W1ReceiverEndpoint::new(
                RECEIVER_COMPONENTS[receiver],
                RECEIVER_COMPONENTS[receiver],
                [data_flow_id(0, edge), data_flow_id(1, edge)],
                [DATA_REVERSE_LINKS[0][edge], DATA_REVERSE_LINKS[1][edge]],
                socket,
                [streams[0].clone(), streams[1].clone()],
                scenario.frame_payload_bytes,
                geometry
                    .runtime_command_frames
                    .saturating_add(CONTROL_RUNTIME_RESERVE),
                geometry.receiver_data_inbox_frames,
                scenario.admission_policy,
                geometry.runtime_command_frames.div_ceil(TREE_COUNT).max(1),
                scenario.runtime_command_service_ns,
                scenario.receiver_service_ns(receiver),
                protocol,
                Some(ReceiverControlGeometry {
                    downlink_flow_id,
                    uplink_flow_id,
                    reverse_link_mailbox: CONTROL_REVERSE_LINKS[receiver],
                    downlink_stream: downlink_streams[receiver].clone(),
                    uplink_stream: uplink_streams[receiver].clone(),
                }),
                socket,
                scenario.timer_interval_ns,
                recorder.clone(),
                mailbox_tracker.clone(),
            )
            .map_err(|error| W2RunError::Construction(error.to_string()))?,
        );
    }

    let mut links = build_links(scenario, &topology, &recorder, &mailbox_tracker);
    let source_mailbox = Mailbox::with_capacity(MAILBOX_CAPACITY);
    let relay_mailboxes: Vec<_> = (0..relays.len())
        .map(|_| Mailbox::with_capacity(MAILBOX_CAPACITY))
        .collect();
    let receiver_mailboxes: Vec<_> = (0..receivers.len())
        .map(|_| Mailbox::with_capacity(MAILBOX_CAPACITY))
        .collect();
    wire_data_plane(
        &topology,
        &mut source,
        &source_mailbox,
        &mut relays,
        &relay_mailboxes,
        &mut receivers,
        &receiver_mailboxes,
        &mut links,
    );
    wire_control_plane(
        scenario.receiver_count,
        topology.edges.len(),
        &mut source,
        &source_mailbox,
        &mut receivers,
        &receiver_mailboxes,
        &mut links,
    );

    record_geometry(scenario, &topology, &recorder)?;
    let mut bench = match scenario.registration_order {
        RegistrationOrder::Forward => {
            let mut bench =
                SimInit::with_num_threads(1).add_model(source, source_mailbox, SOURCE_MAILBOX);
            for (index, (relay, mailbox)) in relays.into_iter().zip(relay_mailboxes).enumerate() {
                let tree = index / topology.relays.len();
                let relay_index = index % topology.relays.len();
                bench = bench.add_model(relay, mailbox, RELAY_COMPONENTS[tree][relay_index]);
            }
            for (index, (receiver, mailbox)) in
                receivers.into_iter().zip(receiver_mailboxes).enumerate()
            {
                bench = bench.add_model(receiver, mailbox, RECEIVER_COMPONENTS[index]);
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
                bench = bench.add_model(receiver, mailbox, RECEIVER_COMPONENTS[index]);
            }
            for (index, (relay, mailbox)) in
                relays.into_iter().zip(relay_mailboxes).enumerate().rev()
            {
                let tree = index / topology.relays.len();
                let relay_index = index % topology.relays.len();
                bench = bench.add_model(relay, mailbox, RELAY_COMPONENTS[tree][relay_index]);
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
        .map_err(|error| W2RunError::Simulation(error.to_string()))?;
    simulation
        .step_until(Duration::from_nanos(scenario.simulation_end_ns))
        .map_err(|error| W2RunError::Simulation(error.to_string()))?;
    if let Some(failure) = recorder.failure() {
        return Err(W2RunError::Model(failure));
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
    summarize_outcome(
        scenario,
        &topology,
        recorder.to_csv()?,
        records,
        mailbox_high_water,
    )
}

fn build_topology(ordered_receivers: &[usize], degree: usize) -> Topology {
    fn add_relay(receivers: &[usize], degree: usize, relays: &mut Vec<RelayNode>) -> usize {
        let id = relays.len();
        relays.push(RelayNode::default());
        let chunk_size = receivers.len().div_ceil(degree);
        let mut children = Vec::new();
        for chunk in receivers.chunks(chunk_size) {
            if chunk.len() == 1 {
                children.push(Endpoint::Receiver(chunk[0]));
            } else {
                children.push(Endpoint::Relay(add_relay(chunk, degree, relays)));
            }
        }
        relays[id].children = children;
        id
    }

    let mut relays = Vec::new();
    let root = add_relay(ordered_receivers, degree, &mut relays);
    debug_assert_eq!(root, 0);
    let mut edges = vec![Edge {
        parent: None,
        child: Endpoint::Relay(root),
        child_slot: 0,
    }];
    let mut relay_upstream_edges = vec![usize::MAX; relays.len()];
    relay_upstream_edges[root] = 0;
    let mut relay_child_edges = vec![Vec::new(); relays.len()];
    let mut receiver_edges = vec![usize::MAX; ordered_receivers.len()];
    let mut relay_hops = vec![0; relays.len()];
    relay_hops[root] = 1;
    let mut receiver_hops = vec![0; ordered_receivers.len()];
    for relay in 0..relays.len() {
        for (slot, child) in relays[relay].children.iter().copied().enumerate() {
            let edge_index = edges.len();
            edges.push(Edge {
                parent: Some(relay),
                child,
                child_slot: slot,
            });
            relay_child_edges[relay].push(edge_index);
            match child {
                Endpoint::Relay(child_relay) => {
                    relay_upstream_edges[child_relay] = edge_index;
                    relay_hops[child_relay] = relay_hops[relay] + 1;
                }
                Endpoint::Receiver(receiver) => {
                    receiver_edges[receiver] = edge_index;
                    receiver_hops[receiver] = relay_hops[relay] + 1;
                }
            }
        }
    }
    debug_assert!(relays.len() <= MAX_RELAYS_PER_TREE);
    debug_assert!(edges.len() <= MAX_EDGES_PER_TREE);
    Topology {
        relays,
        edges,
        relay_upstream_edges,
        relay_child_edges,
        receiver_edges,
        receiver_hops,
    }
}

fn build_links(
    scenario: &W2Scenario,
    topology: &Topology,
    recorder: &Recorder,
    mailbox_tracker: &MailboxTracker,
) -> Vec<LinkSlot> {
    let geometry = scenario.buffer_geometry().expect("validated geometry");
    let mut links =
        Vec::with_capacity(TREE_COUNT * topology.edges.len() * 2 + scenario.receiver_count * 2);
    for tree in 0..TREE_COUNT {
        for (edge_index, edge) in topology.edges.iter().enumerate() {
            links.push(link_slot(
                DATA_FORWARD_LINKS[tree][edge_index],
                endpoint_component(tree, edge.child),
                scenario.data_rate_bps,
                scenario.link_propagation_ns,
                geometry.link_queue_bytes,
                recorder,
                mailbox_tracker,
            ));
            links.push(link_slot(
                DATA_REVERSE_LINKS[tree][edge_index],
                edge.parent
                    .map_or(SOURCE_MAILBOX, |relay| RELAY_COMPONENTS[tree][relay]),
                scenario.data_rate_bps,
                scenario.link_propagation_ns,
                geometry.link_queue_bytes,
                recorder,
                mailbox_tracker,
            ));
        }
    }
    for receiver in 0..scenario.receiver_count {
        let propagation = scenario
            .link_propagation_ns
            .saturating_mul(topology.receiver_hops[receiver] as u64);
        links.push(link_slot(
            CONTROL_FORWARD_LINKS[receiver],
            RECEIVER_COMPONENTS[receiver],
            scenario.control_rate_bps,
            propagation,
            geometry.link_queue_bytes,
            recorder,
            mailbox_tracker,
        ));
        links.push(link_slot(
            CONTROL_REVERSE_LINKS[receiver],
            SOURCE_MAILBOX,
            scenario.control_rate_bps,
            propagation,
            geometry.link_queue_bytes,
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

#[allow(clippy::too_many_arguments)]
fn wire_data_plane(
    topology: &Topology,
    source: &mut W1SourceEndpoint,
    source_mailbox: &Mailbox<W1SourceEndpoint>,
    relays: &mut [FanoutRelayEndpoint],
    relay_mailboxes: &[Mailbox<FanoutRelayEndpoint>],
    receivers: &mut [W1ReceiverEndpoint],
    receiver_mailboxes: &[Mailbox<W1ReceiverEndpoint>],
    links: &mut [LinkSlot],
) {
    let relay_count = topology.relays.len();
    for tree in 0..TREE_COUNT {
        for (edge_index, edge) in topology.edges.iter().copied().enumerate() {
            let forward = data_link_index(topology.edges.len(), tree, edge_index, false);
            let reverse = data_link_index(topology.edges.len(), tree, edge_index, true);
            match edge.parent {
                None => source.data_outputs[tree]
                    .connect(PhysicalLink::receive, &links[forward].mailbox),
                Some(parent) => relays[tree * relay_count + parent].child_data_outputs
                    [edge.child_slot]
                    .connect(PhysicalLink::receive, &links[forward].mailbox),
            }
            match edge.child {
                Endpoint::Relay(child) => {
                    links[forward].model.output.connect(
                        FanoutRelayEndpoint::upstream_segment,
                        &relay_mailboxes[tree * relay_count + child],
                    );
                    relays[tree * relay_count + child]
                        .upstream_ack_output
                        .connect(PhysicalLink::receive, &links[reverse].mailbox);
                }
                Endpoint::Receiver(receiver) => {
                    if tree == 0 {
                        links[forward].model.output.connect(
                            W1ReceiverEndpoint::data0_segment,
                            &receiver_mailboxes[receiver],
                        );
                    } else {
                        links[forward].model.output.connect(
                            W1ReceiverEndpoint::data1_segment,
                            &receiver_mailboxes[receiver],
                        );
                    }
                    receivers[receiver].data_ack_outputs[tree]
                        .connect(PhysicalLink::receive, &links[reverse].mailbox);
                }
            }
            match edge.parent {
                None if tree == 0 => links[reverse]
                    .model
                    .output
                    .connect(W1SourceEndpoint::data0_ack, source_mailbox),
                None => links[reverse]
                    .model
                    .output
                    .connect(W1SourceEndpoint::data1_ack, source_mailbox),
                Some(parent) => connect_relay_ack(
                    &mut links[reverse].model,
                    edge.child_slot,
                    &relay_mailboxes[tree * relay_count + parent],
                ),
            }
        }
    }
}

fn connect_relay_ack(
    link: &mut PhysicalLink,
    child_slot: usize,
    relay_mailbox: &Mailbox<FanoutRelayEndpoint>,
) {
    match child_slot {
        0 => link
            .output
            .connect(FanoutRelayEndpoint::child0_acknowledgment, relay_mailbox),
        1 => link
            .output
            .connect(FanoutRelayEndpoint::child1_acknowledgment, relay_mailbox),
        2 => link
            .output
            .connect(FanoutRelayEndpoint::child2_acknowledgment, relay_mailbox),
        3 => link
            .output
            .connect(FanoutRelayEndpoint::child3_acknowledgment, relay_mailbox),
        _ => unreachable!("validated W2 fan-out degree"),
    }
}

#[allow(clippy::too_many_arguments)]
fn wire_control_plane(
    receiver_count: usize,
    edge_count: usize,
    source: &mut W1SourceEndpoint,
    source_mailbox: &Mailbox<W1SourceEndpoint>,
    receivers: &mut [W1ReceiverEndpoint],
    receiver_mailboxes: &[Mailbox<W1ReceiverEndpoint>],
    links: &mut [LinkSlot],
) {
    for receiver in 0..receiver_count {
        let forward = control_link_index(edge_count, receiver, false);
        let reverse = control_link_index(edge_count, receiver, true);
        source.control_forward_outputs[receiver]
            .connect(PhysicalLink::receive, &links[forward].mailbox);
        links[forward].model.output.connect(
            W1ReceiverEndpoint::control_packet,
            &receiver_mailboxes[receiver],
        );
        receivers[receiver]
            .control_reverse_output
            .connect(PhysicalLink::receive, &links[reverse].mailbox);
        connect_source_control(&mut links[reverse].model, receiver, source_mailbox);
    }
}

fn connect_source_control(
    link: &mut PhysicalLink,
    receiver: usize,
    source_mailbox: &Mailbox<W1SourceEndpoint>,
) {
    match receiver {
        0 => link
            .output
            .connect(W1SourceEndpoint::control_peer0_packet, source_mailbox),
        1 => link
            .output
            .connect(W1SourceEndpoint::control_peer1_packet, source_mailbox),
        2 => link
            .output
            .connect(W1SourceEndpoint::control_peer2_packet, source_mailbox),
        3 => link
            .output
            .connect(W1SourceEndpoint::control_peer3_packet, source_mailbox),
        4 => link
            .output
            .connect(W1SourceEndpoint::control_peer4_packet, source_mailbox),
        5 => link
            .output
            .connect(W1SourceEndpoint::control_peer5_packet, source_mailbox),
        6 => link
            .output
            .connect(W1SourceEndpoint::control_peer6_packet, source_mailbox),
        7 => link
            .output
            .connect(W1SourceEndpoint::control_peer7_packet, source_mailbox),
        _ => unreachable!("validated W2 receiver count"),
    }
}

fn record_geometry(
    scenario: &W2Scenario,
    topology: &Topology,
    recorder: &Recorder,
) -> Result<(), W2RunError> {
    let geometry = scenario.buffer_geometry()?;
    recorder.record(0, "simulation", "single_worker", 0, 0, 0, 1);
    recorder.record(
        0,
        "simulation",
        "w2_configured_fanout_degree",
        0,
        0,
        topology.relays.len(),
        scenario.fanout_degree,
    );
    for (event, bytes) in [
        ("budget_socket_send", geometry.socket_send_bytes),
        ("budget_socket_receive", geometry.socket_receive_bytes),
        ("budget_link_queue", geometry.link_queue_bytes),
        ("budget_relay_application", geometry.relay_application_bytes),
        ("budget_relay_child_queue", geometry.relay_child_queue_bytes),
        (
            "budget_runtime_command",
            geometry.runtime_command_frames * scenario.frame_wire_bytes()?,
        ),
        (
            "budget_receiver_inbox",
            geometry.receiver_data_inbox_frames * scenario.frame_wire_bytes()?,
        ),
    ] {
        recorder.record(0, "simulation", event, 0, 0, bytes, bytes);
    }
    for (receiver, component) in RECEIVER_COMPONENTS
        .iter()
        .copied()
        .enumerate()
        .take(scenario.receiver_count)
    {
        recorder.record(
            0,
            component,
            "configured_path_hops",
            0,
            receiver,
            0,
            topology.receiver_hops[receiver],
        );
    }
    Ok(())
}

fn summarize_outcome(
    scenario: &W2Scenario,
    topology: &Topology,
    csv: String,
    records: Vec<Record>,
    mailbox_high_water: BTreeMap<&'static str, usize>,
) -> Result<W2Outcome, W2RunError> {
    let mut completion_times_ns = Vec::with_capacity(scenario.receiver_count);
    let mut critical_paths = Vec::with_capacity(scenario.receiver_count);
    for (receiver, component) in RECEIVER_COMPONENTS
        .iter()
        .copied()
        .enumerate()
        .take(scenario.receiver_count)
    {
        let completion = records
            .iter()
            .find(|record| {
                record.component == component && record.event == "protocol_local_complete"
            })
            .ok_or(W2RunError::IncompleteReceiver(receiver + 1))?;
        completion_times_ns.push(completion.time_ns);
        critical_paths.push(critical_path(scenario, receiver, completion, &records));
    }
    let barrier_completion_ns = completion_times_ns.iter().copied().max().unwrap_or(0);
    let sender_completion_ns = records
        .iter()
        .find(|record| {
            record.component == SOURCE_COMPONENT && record.event == "protocol_sender_complete"
        })
        .map(|record| record.time_ns);
    let emission_records: Vec<_> = records
        .iter()
        .filter(|record| {
            record.component == SOURCE_COMPONENT && record.event == "data_frame_emitted"
        })
        .collect();
    let total_emissions = emission_records.len();
    let per_tree_emissions = [
        emission_records
            .iter()
            .filter(|record| record.flow_id == data_flow_id(0, 0))
            .count(),
        emission_records
            .iter()
            .filter(|record| record.flow_id == data_flow_id(1, 0))
            .count(),
    ];
    let post_completion_tail_emissions = emission_records
        .iter()
        .filter(|record| record.time_ns > barrier_completion_ns)
        .count();
    let count = |event: &str| {
        records
            .iter()
            .filter(|record| record.event == event)
            .count()
    };
    Ok(W2Outcome {
        csv,
        records: records.clone(),
        completion_times_ns,
        barrier_completion_ns,
        sender_completion_ns,
        total_emissions,
        per_tree_emissions,
        post_completion_tail_emissions,
        application_drops: count("data_inbox_drop_after_tcp_ack"),
        blocking_wait_events: count("data_inbox_blocking_wait"),
        isolated_credit_deferrals: count("isolated_credit_deferred"),
        isolated_credit_replays: count("isolated_credit_replay"),
        link_drops: records
            .iter()
            .filter(|record| matches!(record.event, "queue_drop" | "segment_drop"))
            .count(),
        liveness_pressure_permille: barrier_completion_ns.saturating_mul(1_000)
            / scenario.carousel.peer_stall_timeout_ns,
        receiver_hops: topology.receiver_hops.clone(),
        critical_paths,
        mailbox_high_water,
    })
}

fn critical_path(
    scenario: &W2Scenario,
    receiver: usize,
    completion: &Record,
    records: &[Record],
) -> CriticalPathAttribution {
    let tree = completion.value;
    let frame_id = completion.sequence;
    let runtime = records.iter().find(|record| {
        record.component == RECEIVER_COMPONENTS[receiver]
            && record.event == "runtime_command_enqueue_data"
            && record.flow_id == completion.flow_id
            && record.sequence == frame_id
    });
    let inbox = records.iter().find(|record| {
        record.component == RECEIVER_COMPONENTS[receiver]
            && record.event == "data_inbox_enqueue"
            && record.flow_id == completion.flow_id
            && record.sequence == frame_id
    });
    let source = records.iter().find(|record| {
        record.component == SOURCE_COMPONENT
            && record.event == "data_frame_emitted"
            && record.flow_id == data_flow_id(tree, 0)
            && record.value == frame_id
    });
    let runtime_ns = runtime.map_or(completion.time_ns, |record| record.time_ns);
    let inbox_ns = inbox.map_or(runtime_ns, |record| record.time_ns);
    let source_ns = source.map_or(0, |record| record.time_ns);
    let service = scenario.receiver_service_ns(receiver);
    CriticalPathAttribution {
        receiver,
        final_tree: tree,
        final_frame_id: frame_id,
        source_to_runtime_ns: runtime_ns.saturating_sub(source_ns),
        runtime_wait_ns: inbox_ns.saturating_sub(runtime_ns),
        decoder_queue_ns: completion
            .time_ns
            .saturating_sub(inbox_ns)
            .saturating_sub(service),
        decoder_service_ns: service,
    }
}

const fn data_flow_id(tree: usize, edge: usize) -> usize {
    50_000 + tree * 100 + edge
}

const fn control_flow_ids(receiver: usize) -> (usize, usize) {
    (60_000 + receiver * 2, 60_001 + receiver * 2)
}

fn endpoint_component(tree: usize, endpoint: Endpoint) -> &'static str {
    match endpoint {
        Endpoint::Relay(relay) => RELAY_COMPONENTS[tree][relay],
        Endpoint::Receiver(receiver) => RECEIVER_COMPONENTS[receiver],
    }
}

const fn data_link_index(edge_count: usize, tree: usize, edge: usize, reverse: bool) -> usize {
    tree * edge_count * 2 + edge * 2 + reverse as usize
}

const fn control_link_index(edge_count: usize, receiver: usize, reverse: bool) -> usize {
    TREE_COUNT * edge_count * 2 + receiver * 2 + reverse as usize
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn balanced_topologies_preserve_order_and_requested_maximum_degree() {
        for (receivers, degree, relay_count, edge_count) in
            [(3, 2, 2, 5), (3, 4, 1, 4), (8, 2, 7, 15), (8, 4, 5, 13)]
        {
            let order: Vec<_> = (0..receivers).collect();
            let topology = build_topology(&order, degree);
            assert_eq!(topology.relays.len(), relay_count);
            assert_eq!(topology.edges.len(), edge_count);
            assert!(
                topology
                    .relays
                    .iter()
                    .all(|relay| relay.children.len() <= degree)
            );
            assert!(
                topology
                    .receiver_edges
                    .iter()
                    .all(|edge| *edge != usize::MAX)
            );
        }
    }
}
