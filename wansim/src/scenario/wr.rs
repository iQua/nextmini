use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use nexosim::simulation::{Mailbox, SimInit};
use nexosim::time::MonotonicTime;
use thiserror::Error;

use crate::days_bridge::{
    BackboneResourceConfig, BackboneRouteConfig, BackboneRouteHop, BackgroundFlow,
    BackgroundFlowConfig, BackgroundTrafficKind, RegionalBackbone, RegionalBackboneConfig,
};
use crate::determinism::CounterPrf;
use crate::metrics::{MAILBOX_CAPACITY, MailboxTracker, OwnershipLedger, Record, Recorder};
use crate::overlay::{
    ControlStream, FanoutRelayEndpoint, FramedStream, ReceiverControlGeometry, RelayChildSpec,
    W1ReceiverEndpoint, W1ReceiverProtocol, W1SourceEndpoint, W1SourceProtocol,
};
use crate::protocol::{CarouselTiming, ProtocolKind, equal_quotas, proportional_quotas};
use crate::scenario::{
    CloudScenario, CloudScenarioError, FanoutAdmission, ReceiverAdmissionPolicy, RegistrationOrder,
};
use crate::transport::{SocketPairConfig, TcpCongestionControl};

const TREE_COUNT: usize = 2;
const HOPS_PER_TREE: usize = 5;
const RECEIVER_COUNT: usize = 3;
const MAX_SESSIONS: usize = 2;
const BACKGROUND_COUNT: usize = 8;
const BACKBONE_COMPONENT: &str = "wr_regional_backbone";
const BACKBONE_MAILBOX: &str = "wr_regional_backbone";
const BACKBONE_MAILBOX_CAPACITY: usize = 1_048_576;
pub const WR_FOREGROUND_START_NS: u64 = 1_000_000_000;
const PRODUCTION_DEFAULT_BLOCK_BYTES: usize = 8_500;
const FRAME_PAYLOAD_BYTES: usize = 508;
const SOURCE_COMPONENTS: [&str; MAX_SESSIONS] = ["wr_s0_source", "wr_s1_source"];
const SOURCE_MAILBOXES: [&str; MAX_SESSIONS] = ["wr_s0_source", "wr_s1_source"];
const RELAY_COMPONENTS: [[[&str; 2]; TREE_COUNT]; MAX_SESSIONS] = [
    [
        ["wr_s0_t0_relay_a", "wr_s0_t0_relay_b"],
        ["wr_s0_t1_relay_a", "wr_s0_t1_relay_b"],
    ],
    [
        ["wr_s1_t0_relay_a", "wr_s1_t0_relay_b"],
        ["wr_s1_t1_relay_a", "wr_s1_t1_relay_b"],
    ],
];
const RECEIVER_COMPONENTS: [[&str; RECEIVER_COUNT]; MAX_SESSIONS] = [
    ["wr_s0_receiver1", "wr_s0_receiver2", "wr_s0_receiver3"],
    ["wr_s1_receiver1", "wr_s1_receiver2", "wr_s1_receiver3"],
];
const BACKGROUND_COMPONENTS: [&str; BACKGROUND_COUNT] = [
    "wr_bg_tree0_forward_bulk",
    "wr_bg_tree0_forward_onoff",
    "wr_bg_tree0_reverse_bulk",
    "wr_bg_tree0_reverse_onoff",
    "wr_bg_tree1_forward_bulk",
    "wr_bg_tree1_forward_onoff",
    "wr_bg_tree1_reverse_bulk",
    "wr_bg_tree1_reverse_onoff",
];
const OWNER_UPSTREAM_RECEIVE: [[&str; 2]; TREE_COUNT] = [
    ["wr.t0.ra.rcv", "wr.t0.rb.rcv"],
    ["wr.t1.ra.rcv", "wr.t1.rb.rcv"],
];
const OWNER_APPLICATION: [[&str; 2]; TREE_COUNT] = [
    ["wr.t0.ra.app", "wr.t0.rb.app"],
    ["wr.t1.ra.app", "wr.t1.rb.app"],
];
const OWNER_QUEUE: [[[&str; 2]; 2]; TREE_COUNT] = [
    [
        ["wr.t0.ra.child0.q", "wr.t0.ra.child1.q"],
        ["wr.t0.rb.child0.q", "wr.t0.rb.child1.q"],
    ],
    [
        ["wr.t1.ra.child0.q", "wr.t1.ra.child1.q"],
        ["wr.t1.rb.child0.q", "wr.t1.rb.child1.q"],
    ],
];
const OWNER_SEND: [[[&str; 2]; 2]; TREE_COUNT] = [
    [
        ["wr.t0.ra.child0.snd", "wr.t0.ra.child1.snd"],
        ["wr.t0.rb.child0.snd", "wr.t0.rb.child1.snd"],
    ],
    [
        ["wr.t1.ra.child0.snd", "wr.t1.ra.child1.snd"],
        ["wr.t1.rb.child0.snd", "wr.t1.rb.child1.snd"],
    ],
];
const OWNER_CHILD_RECEIVE: [[[&str; 2]; 2]; TREE_COUNT] = [
    [
        ["wr.t0.r1.rcv", "wr.t0.rb.rcv.child"],
        ["wr.t0.r2.rcv", "wr.t0.r3.rcv"],
    ],
    [
        ["wr.t1.r1.rcv", "wr.t1.rb.rcv.child"],
        ["wr.t1.r2.rcv", "wr.t1.r3.rcv"],
    ],
];

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum WrProtocol {
    Carousel,
    Rounds,
    PerStripeFec,
    BestSingleTree0,
    BestSingleTree1,
}

impl WrProtocol {
    pub const fn name(self) -> &'static str {
        match self {
            Self::Carousel => "carousel",
            Self::Rounds => "rounds",
            Self::PerStripeFec => "per-stripe-fec",
            Self::BestSingleTree0 => "best-single-tree0",
            Self::BestSingleTree1 => "best-single-tree1",
        }
    }

    fn endpoint_kind(self) -> ProtocolKind {
        match self {
            Self::Carousel => ProtocolKind::PooledCarousel,
            Self::Rounds => ProtocolKind::PooledRounds,
            Self::PerStripeFec | Self::BestSingleTree0 | Self::BestSingleTree1 => {
                ProtocolKind::PerStripeFec
            }
        }
    }
}

#[derive(Clone, Debug)]
pub struct WrRunConfig {
    pub cloud: CloudScenario,
    pub protocol: WrProtocol,
    pub source_symbols: usize,
    pub background_utilization_percent: u8,
    pub jitter_enabled: bool,
    pub seed: u64,
    pub ack_cadence_multiplier: u8,
    pub receiver_admission: ReceiverAdmissionPolicy,
    pub slow_receiver: Option<usize>,
    pub concurrent_sessions: usize,
    pub registration_order: RegistrationOrder,
}

impl WrRunConfig {
    pub fn scenario_id(&self) -> String {
        format!(
            "wr-{}-{}-u{}-j{}-k{}-c{}-s{}",
            self.cloud.scenario_id(),
            self.protocol.name(),
            self.background_utilization_percent,
            usize::from(self.jitter_enabled),
            self.source_symbols,
            self.ack_cadence_multiplier,
            self.seed
        )
    }

    fn validate(&self) -> Result<(), WrRunError> {
        self.cloud.validate()?;
        if self.source_symbols == 0
            || !matches!(self.background_utilization_percent, 30 | 50 | 70)
            || !matches!(self.ack_cadence_multiplier, 1 | 2 | 4)
            || !(1..=MAX_SESSIONS).contains(&self.concurrent_sessions)
            || self
                .slow_receiver
                .is_some_and(|receiver| receiver >= RECEIVER_COUNT)
        {
            return Err(WrRunError::Geometry);
        }
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct WrSharingRow {
    pub resource: String,
    pub total_flow_directions: usize,
    pub foreground_flow_directions: usize,
    pub background_flow_directions: usize,
}

#[derive(Clone, Debug)]
pub struct WrSessionOutcome {
    pub completion_times_ns: [u64; RECEIVER_COUNT],
    pub barrier_completion_ns: u64,
    pub sender_completion_ns: u64,
    pub total_emissions: usize,
    pub per_tree_emissions: [usize; TREE_COUNT],
    pub application_drops: usize,
    pub blocking_waits: usize,
    pub positive_round_deficits: usize,
    pub ack_probes: usize,
    pub maximum_ack_gap_ns: u64,
    pub stall_budget_consumption_ppm: u64,
}

#[derive(Clone, Debug)]
pub struct WrOutcome {
    pub csv: String,
    pub records: Vec<Record>,
    pub sessions: Vec<WrSessionOutcome>,
    pub sharing: Vec<WrSharingRow>,
    pub mailbox_high_water: BTreeMap<&'static str, usize>,
    pub mailbox_capacities: BTreeMap<&'static str, usize>,
    pub link_drops: usize,
    pub maximum_background_trunk_utilization_ppm: u64,
}

#[derive(Clone, Debug)]
pub struct WrEventClassCount {
    pub event_class: String,
    pub count: u64,
}

#[derive(Clone, Debug)]
pub struct WrTriageOutcome {
    pub records: Vec<Record>,
    pub event_class_counts: Vec<WrEventClassCount>,
    pub failure: Option<String>,
    pub stopped_at_ns: u64,
    pub acknowledgement_progress_units: u64,
    pub configured_emission_ceiling: usize,
}

#[derive(Debug, Error)]
pub enum WrRunError {
    #[error(transparent)]
    Cloud(#[from] CloudScenarioError),
    #[error("WR scenario has invalid experiment geometry")]
    Geometry,
    #[error("failed to construct WR model: {0}")]
    Construction(String),
    #[error("nexosim WR execution failed: {0}")]
    Simulation(String),
    #[error("WR model failed: {0}")]
    Model(String),
    #[error("WR session {session} receiver {receiver} did not complete")]
    IncompleteReceiver { session: usize, receiver: usize },
    #[error("WR session {0} sender did not complete")]
    IncompleteSender(usize),
    #[error("failed to serialize deterministic WR CSV: {0}")]
    Csv(#[from] csv::Error),
}

struct SessionSlot {
    source: W1SourceEndpoint,
    source_mailbox: Mailbox<W1SourceEndpoint>,
    relays: Vec<FanoutRelayEndpoint>,
    relay_mailboxes: Vec<Mailbox<FanoutRelayEndpoint>>,
    receivers: Vec<W1ReceiverEndpoint>,
    receiver_mailboxes: Vec<Mailbox<W1ReceiverEndpoint>>,
}

struct BackgroundSlot {
    model: BackgroundFlow,
    mailbox: Mailbox<BackgroundFlow>,
    component: &'static str,
    flow_id: usize,
}

struct BackboneGeometry {
    config: RegionalBackboneConfig,
    sharing: Vec<WrSharingRow>,
}

struct WrExecution {
    records: Vec<Record>,
    sharing: Vec<WrSharingRow>,
    mailbox_high_water: BTreeMap<&'static str, usize>,
    mailbox_capacities: BTreeMap<&'static str, usize>,
    failure: Option<String>,
    elapsed_ns: u64,
    event_class_counts: Vec<WrEventClassCount>,
}

pub fn run_wr(config: &WrRunConfig) -> Result<WrOutcome, WrRunError> {
    let recorder = Recorder::new_compact_wr(config.scenario_id(), config.seed);
    let execution = execute_wr(config, recorder)?;
    if let Some(failure) = execution.failure {
        return Err(WrRunError::Model(failure));
    }
    let timing = carousel_timing(config.ack_cadence_multiplier);
    let sessions = summarize_sessions(config, &execution.records, timing.peer_stall_timeout_ns)?;
    let maximum_background_trunk_utilization_ppm = maximum_background_trunk_utilization_ppm(
        &config.cloud,
        &execution.records,
        sessions
            .iter()
            .map(|session| session.barrier_completion_ns)
            .max()
            .unwrap_or(0),
    );
    let link_drops = execution
        .records
        .iter()
        .filter(|record| matches!(record.event, "queue_drop" | "segment_drop"))
        .count();
    Ok(WrOutcome {
        csv: records_to_csv(&execution.records)?,
        records: execution.records,
        sessions,
        sharing: execution.sharing,
        mailbox_high_water: execution.mailbox_high_water,
        mailbox_capacities: execution.mailbox_capacities,
        link_drops,
        maximum_background_trunk_utilization_ppm,
    })
}

pub fn run_wr_triage(config: &WrRunConfig) -> Result<WrTriageOutcome, WrRunError> {
    let receiver = config.slow_receiver.unwrap_or(0);
    let recorder = Recorder::new_wr_triage(
        config.scenario_id(),
        config.seed,
        SOURCE_COMPONENTS[0],
        RECEIVER_COMPONENTS[0][receiver],
    );
    let execution = execute_wr(config, recorder)?;
    Ok(WrTriageOutcome {
        records: execution.records,
        event_class_counts: execution.event_class_counts,
        failure: execution.failure,
        stopped_at_ns: execution.elapsed_ns,
        acknowledgement_progress_units: ack_progress_units(config.source_symbols)?,
        configured_emission_ceiling: maximum_frames_per_tree(config.source_symbols)?
            .checked_mul(TREE_COUNT)
            .ok_or(WrRunError::Geometry)?,
    })
}

fn execute_wr(config: &WrRunConfig, recorder: Recorder) -> Result<WrExecution, WrRunError> {
    config.validate()?;
    let tracker = MailboxTracker::default();
    let data_socket = socket_config()?;
    let control_socket = data_socket;
    let background_socket = background_socket_config()?;
    let timing = carousel_timing(config.ack_cadence_multiplier);
    let mut sessions = Vec::with_capacity(config.concurrent_sessions);
    for session in 0..config.concurrent_sessions {
        sessions.push(build_session(
            session,
            config,
            timing,
            data_socket,
            control_socket,
            &recorder,
            &tracker,
        )?);
    }
    let mut background = build_background(config, background_socket, &recorder, &tracker)?;
    let geometry = build_backbone_geometry(config)?;
    let sharing = geometry.sharing;
    let mut backbone = RegionalBackbone::new(geometry.config, recorder.clone(), tracker.clone())
        .map_err(|error| WrRunError::Construction(error.to_string()))?;
    let backbone_mailbox = Mailbox::with_capacity(BACKBONE_MAILBOX_CAPACITY);

    for (session, slot) in sessions.iter_mut().enumerate() {
        wire_session(session, slot, &mut backbone, &backbone_mailbox);
    }
    wire_background(&mut background, &mut backbone, &backbone_mailbox);

    recorder.record(0, "simulation", "single_worker", 0, 0, 0, 1);
    recorder.record(
        0,
        "simulation",
        "wr_concurrent_sessions",
        0,
        0,
        config.concurrent_sessions,
        config.concurrent_sessions,
    );

    let mut bench = SimInit::with_num_threads(1);
    match config.registration_order {
        RegistrationOrder::Forward => {
            for (session, slot) in sessions.into_iter().enumerate() {
                bench = add_session(bench, session, slot, false);
            }
            for slot in background {
                bench = bench.add_model(slot.model, slot.mailbox, slot.component);
            }
            bench = bench.add_model(backbone, backbone_mailbox, BACKBONE_MAILBOX);
        }
        RegistrationOrder::Reverse => {
            bench = bench.add_model(backbone, backbone_mailbox, BACKBONE_MAILBOX);
            background.reverse();
            for slot in background {
                bench = bench.add_model(slot.model, slot.mailbox, slot.component);
            }
            sessions.reverse();
            for (reverse_index, slot) in sessions.into_iter().enumerate() {
                let session = config.concurrent_sessions - 1 - reverse_index;
                bench = add_session(bench, session, slot, true);
            }
        }
    }
    let mut simulation = bench
        .init(MonotonicTime::EPOCH)
        .map_err(|error| WrRunError::Simulation(error.to_string()))?;
    let simulation_end_ns = simulation_end_ns(config);
    let report_progress = std::env::var_os("WANSIM_WR_PROGRESS").is_some();
    // No foreground payload can complete during warm-up. Advance it in one host call so the
    // single-worker engine does not pay controller synchronization overhead every few
    // milliseconds while the background TCP flows reach their operating point.
    simulation
        .step_until(Duration::from_nanos(WR_FOREGROUND_START_NS))
        .map_err(|error| WrRunError::Simulation(error.to_string()))?;
    let mut elapsed_ns = WR_FOREGROUND_START_NS;
    let quantum_ns = 100_000_000_u64;
    while elapsed_ns < simulation_end_ns {
        let step = quantum_ns.min(simulation_end_ns - elapsed_ns);
        simulation
            .step_until(Duration::from_nanos(step))
            .map_err(|error| WrRunError::Simulation(error.to_string()))?;
        elapsed_ns = elapsed_ns.saturating_add(step);
        if report_progress {
            eprintln!(
                "wr_progress elapsed_ns={elapsed_ns} records={} local_complete={} sender_complete={}",
                recorder.record_count(),
                recorder.event_count("protocol_local_complete"),
                recorder.event_count("protocol_sender_complete"),
            );
        }
        if recorder.event_count("protocol_local_complete")
            >= RECEIVER_COUNT * config.concurrent_sessions
            && recorder.event_count("protocol_sender_complete") >= config.concurrent_sessions
        {
            break;
        }
        if recorder.failure().is_some() {
            break;
        }
    }
    let failure = recorder.failure();
    let mailbox_high_water = tracker.high_water_marks();
    let mailbox_capacities = mailbox_high_water
        .keys()
        .map(|&mailbox| {
            (
                mailbox,
                if mailbox == BACKBONE_MAILBOX {
                    BACKBONE_MAILBOX_CAPACITY
                } else {
                    MAILBOX_CAPACITY
                },
            )
        })
        .collect::<BTreeMap<_, _>>();
    for (&mailbox, &high_water) in &mailbox_high_water {
        recorder.record(
            elapsed_ns,
            mailbox,
            "mailbox_high_water",
            0,
            0,
            high_water,
            mailbox_capacities[mailbox],
        );
    }
    let records = recorder.records();
    let event_class_counts = recorder
        .event_class_counts()
        .into_iter()
        .map(|(event_class, count)| WrEventClassCount {
            event_class: event_class.to_owned(),
            count,
        })
        .collect();
    Ok(WrExecution {
        records,
        sharing,
        mailbox_high_water,
        mailbox_capacities,
        failure,
        elapsed_ns,
        event_class_counts,
    })
}

fn records_to_csv(records: &[Record]) -> Result<String, csv::Error> {
    let mut writer = csv::WriterBuilder::new()
        .terminator(csv::Terminator::Any(b'\n'))
        .from_writer(Vec::new());
    for record in records {
        writer.serialize(record)?;
    }
    writer.flush()?;
    let bytes = writer
        .into_inner()
        .map_err(|error| csv::Error::from(error.into_error()))?;
    Ok(String::from_utf8_lossy(&bytes).into_owned())
}

fn socket_config() -> Result<SocketPairConfig, WrRunError> {
    SocketPairConfig::new(
        512,
        4 * 1024 * 1024,
        4 * 1024 * 1024,
        1_000_000_000,
        20_000_000,
    )
    .map(|socket| socket.with_congestion_control(TcpCongestionControl::WindowScaledReno))
    .map_err(|error| WrRunError::Construction(error.to_string()))
}

fn background_socket_config() -> Result<SocketPairConfig, WrRunError> {
    SocketPairConfig::new(
        256 * 1024,
        1024 * 1024,
        32 * 1024 * 1024,
        1_000_000_000,
        20_000_000,
    )
    .map(|socket| socket.with_congestion_control(TcpCongestionControl::WindowScaledReno))
    .map_err(|error| WrRunError::Construction(error.to_string()))
}

fn carousel_timing(multiplier: u8) -> CarouselTiming {
    // Encoded as {1,2,4} = {0.5x,1x,2x}. The 1x values mirror the production
    // defaults in dataplane/src/node/config.rs; liveness clocks do not scale.
    let scale = u64::from(multiplier);
    CarouselTiming {
        ack_debounce_ns: 4_000_000 * scale,
        ack_heartbeat_ns: 150_000_000 * scale,
        ack_probe_interval_ns: 250_000_000,
        peer_silence_timeout_ns: 3_000_000_000,
        peer_stall_timeout_ns: 15_000_000_000,
        receiver_passive_window_ns: 17_000_000_000,
        session_complete_repeats: 3,
        session_complete_interval_ns: 20_000_000,
    }
}

fn simulation_end_ns(config: &WrRunConfig) -> u64 {
    if config.slow_receiver.is_some() {
        WR_FOREGROUND_START_NS.saturating_add(60_000_000_000)
    } else if config.source_symbols >= 65_536 {
        // This is an observation horizon, not a protocol timeout. The slowest
        // representative single-tree path can legitimately need more than 60 s
        // at K=65,536; run_wr still stops as soon as all endpoints complete.
        WR_FOREGROUND_START_NS.saturating_add(180_000_000_000)
    } else {
        WR_FOREGROUND_START_NS.saturating_add(60_000_000_000)
    }
}

fn build_session(
    session: usize,
    config: &WrRunConfig,
    timing: CarouselTiming,
    data_socket: SocketPairConfig,
    control_socket: SocketPairConfig,
    recorder: &Recorder,
    tracker: &MailboxTracker,
) -> Result<SessionSlot, WrRunError> {
    let maximum_frames = maximum_frames_per_tree(config.source_symbols)?;
    let streams = [0, 1].map(|tree| {
        FramedStream::new(
            maximum_frames,
            508,
            CounterPrf::new(config.seed, &format!("wr-session{session}-tree{tree}")),
        )
        .map_err(|error| WrRunError::Construction(error.to_string()))
    });
    let [stream0, stream1] = streams;
    let streams = [stream0?, stream1?];
    let peer_ids = [1_u64, 2, 3];
    let quotas = quotas(config)?;
    let protocol = W1SourceProtocol::new_with_ack_units(
        config.protocol.endpoint_kind(),
        config.source_symbols,
        quotas.clone(),
        &peer_ids,
        WR_FOREGROUND_START_NS,
        timing,
        ack_progress_units(config.source_symbols)?,
    )
    .map_err(|error| WrRunError::Construction(error.to_string()))?;
    let downlinks: Vec<_> = peer_ids.iter().map(|_| ControlStream::default()).collect();
    let uplinks: Vec<_> = peer_ids.iter().map(|_| ControlStream::default()).collect();
    let control_flows: Vec<_> = (0..RECEIVER_COUNT)
        .map(|receiver| control_flow_ids(session, receiver))
        .collect();
    let mut source = W1SourceEndpoint::new(
        SOURCE_COMPONENTS[session],
        SOURCE_MAILBOXES[session],
        [data_flow_id(session, 0, 0), data_flow_id(session, 1, 0)],
        [BACKBONE_MAILBOX; TREE_COUNT],
        data_socket,
        512,
        maximum_frames,
        protocol,
        &peer_ids,
        &control_flows,
        vec![BACKBONE_MAILBOX; RECEIVER_COUNT],
        control_socket,
        &downlinks,
        &uplinks,
        100_000,
        recorder.clone(),
        tracker.clone(),
    )
    .map_err(|error| WrRunError::Construction(error.to_string()))?;
    if let Some(tree) = single_tree(config.protocol) {
        source.enable_flow_count_match(1 - tree);
    }
    source.set_start_delay_ns(WR_FOREGROUND_START_NS);
    source.set_data_not_before_ns(WR_FOREGROUND_START_NS);

    let ownership = OwnershipLedger::default();
    let mut relays = Vec::with_capacity(TREE_COUNT * 2);
    for tree in 0..TREE_COUNT {
        for relay in 0..2 {
            let upstream_hop = if relay == 0 { 0 } else { 2 };
            let child_hops = if relay == 0 { [1, 2] } else { [3, 4] };
            let specs = child_hops
                .into_iter()
                .enumerate()
                .map(|(child, hop)| RelayChildSpec {
                    endpoint_component: forward_component(session, tree, hop),
                    flow_id: data_flow_id(session, tree, hop),
                    forward_link_mailbox: BACKBONE_MAILBOX,
                    queue_owner: OWNER_QUEUE[tree][relay][child],
                    send_owner: OWNER_SEND[tree][relay][child],
                    downstream_receive_owner: OWNER_CHILD_RECEIVE[tree][relay][child],
                })
                .collect();
            let mut endpoint = FanoutRelayEndpoint::new(
                RELAY_COMPONENTS[session][tree][relay],
                RELAY_COMPONENTS[session][tree][relay],
                data_flow_id(session, tree, upstream_hop),
                BACKBONE_MAILBOX,
                OWNER_UPSTREAM_RECEIVE[tree][relay],
                OWNER_APPLICATION[tree][relay],
                data_socket,
                data_socket,
                specs,
                streams[tree].clone(),
                508,
                4 * 1024 * 1024,
                4 * 1024 * 1024,
                FanoutAdmission::Sequential,
                100_000,
                recorder.clone(),
                tracker.clone(),
                ownership.clone(),
            )
            .map_err(|error| WrRunError::Construction(error.to_string()))?;
            endpoint.set_start_delay_ns(WR_FOREGROUND_START_NS);
            relays.push(endpoint);
        }
    }

    let runtime_capacity = maximum_frames.checked_mul(2).ok_or(WrRunError::Geometry)?;
    let inbox_capacity = if config.slow_receiver.is_some() {
        128
    } else {
        runtime_capacity
    };
    let mut receivers = Vec::with_capacity(RECEIVER_COUNT);
    for receiver in 0..RECEIVER_COUNT {
        let protocol = W1ReceiverProtocol::new_with_ack_units(
            config.protocol.endpoint_kind(),
            peer_ids[receiver],
            config.source_symbols,
            quotas.clone(),
            WR_FOREGROUND_START_NS,
            timing,
            true,
            ack_progress_units(config.source_symbols)?,
        )
        .map_err(|error| WrRunError::Construction(error.to_string()))?;
        let hop = receiver_hop(receiver);
        let (downlink_flow_id, uplink_flow_id) = control_flow_ids(session, receiver);
        let mut endpoint = W1ReceiverEndpoint::new(
            RECEIVER_COMPONENTS[session][receiver],
            RECEIVER_COMPONENTS[session][receiver],
            [data_flow_id(session, 0, hop), data_flow_id(session, 1, hop)],
            [BACKBONE_MAILBOX; TREE_COUNT],
            data_socket,
            [streams[0].clone(), streams[1].clone()],
            508,
            runtime_capacity,
            inbox_capacity,
            config.receiver_admission,
            runtime_capacity,
            5_000,
            if config.slow_receiver == Some(receiver) {
                1_000_000
            } else {
                8_000
            },
            protocol,
            Some(ReceiverControlGeometry {
                downlink_flow_id,
                uplink_flow_id,
                reverse_link_mailbox: BACKBONE_MAILBOX,
                downlink_stream: downlinks[receiver].clone(),
                uplink_stream: uplinks[receiver].clone(),
            }),
            control_socket,
            100_000,
            recorder.clone(),
            tracker.clone(),
        )
        .map_err(|error| WrRunError::Construction(error.to_string()))?;
        endpoint.set_start_delay_ns(WR_FOREGROUND_START_NS);
        receivers.push(endpoint);
    }
    Ok(SessionSlot {
        source,
        source_mailbox: Mailbox::with_capacity(MAILBOX_CAPACITY),
        relays,
        relay_mailboxes: (0..TREE_COUNT * 2)
            .map(|_| Mailbox::with_capacity(MAILBOX_CAPACITY))
            .collect(),
        receivers,
        receiver_mailboxes: (0..RECEIVER_COUNT)
            .map(|_| Mailbox::with_capacity(MAILBOX_CAPACITY))
            .collect(),
    })
}

fn maximum_frames_per_tree(source_symbols: usize) -> Result<usize, WrRunError> {
    source_symbols.checked_mul(8).ok_or(WrRunError::Geometry)
}

fn ack_progress_units(source_symbols: usize) -> Result<u64, WrRunError> {
    let symbols_per_default_block = PRODUCTION_DEFAULT_BLOCK_BYTES.div_ceil(FRAME_PAYLOAD_BYTES);
    u64::try_from(source_symbols.div_ceil(symbols_per_default_block))
        .map_err(|_| WrRunError::Geometry)
}

fn quotas(config: &WrRunConfig) -> Result<Vec<usize>, WrRunError> {
    if let Some(tree) = single_tree(config.protocol) {
        let mut quotas = vec![0, 0];
        quotas[tree] = config.source_symbols;
        return Ok(quotas);
    }
    if matches!(config.protocol, WrProtocol::Carousel | WrProtocol::Rounds) {
        return equal_quotas(config.source_symbols, TREE_COUNT).ok_or(WrRunError::Geometry);
    }
    let weights = tree_capacity_weights(&config.cloud)?;
    proportional_quotas(config.source_symbols, &weights).ok_or(WrRunError::Geometry)
}

fn tree_capacity_weights(cloud: &CloudScenario) -> Result<[u64; TREE_COUNT], WrRunError> {
    let node_regions = overlay_node_regions(cloud)?;
    let mut weights = [0_u64; TREE_COUNT];
    for (tree, weight) in weights.iter_mut().enumerate() {
        let mut bottleneck_bps = u64::MAX;
        let mut edge_delays = [0_u64; HOPS_PER_TREE];
        for (edge, (from, to)) in overlay_edges(tree).into_iter().enumerate() {
            let route = region_route(cloud, node_regions[from], node_regions[to])?;
            edge_delays[edge] =
                cloud.directed_region_delay_ns(node_regions[from], node_regions[to])?;
            for resource in route.resource_indexes {
                let capacity = if resource < cloud.regions.len() {
                    cloud.vm_nic_cap_bps
                } else {
                    cloud.trunks[resource - cloud.regions.len()].capacity_bps
                };
                bottleneck_bps = bottleneck_bps.min(capacity);
            }
        }
        let slowest_path_ns = [
            edge_delays[0].saturating_add(edge_delays[1]),
            edge_delays[0]
                .saturating_add(edge_delays[2])
                .saturating_add(edge_delays[3]),
            edge_delays[0]
                .saturating_add(edge_delays[2])
                .saturating_add(edge_delays[4]),
        ]
        .into_iter()
        .max()
        .ok_or(WrRunError::Geometry)?;
        *weight = u64::try_from(
            u128::from(bottleneck_bps).saturating_mul(1_000_000_000)
                / u128::from(slowest_path_ns.max(1)),
        )
        .unwrap_or(u64::MAX)
        .max(1);
    }
    Ok(weights)
}

fn single_tree(protocol: WrProtocol) -> Option<usize> {
    match protocol {
        WrProtocol::BestSingleTree0 => Some(0),
        WrProtocol::BestSingleTree1 => Some(1),
        _ => None,
    }
}

fn build_background(
    config: &WrRunConfig,
    socket: SocketPairConfig,
    recorder: &Recorder,
    tracker: &MailboxTracker,
) -> Result<Vec<BackgroundSlot>, WrRunError> {
    let utilization = u64::from(config.background_utilization_percent);
    let reference = config.cloud.background_reference_rate_bps;
    // Each tree-root pair gets one requested 30/50/70 load bundle, split evenly between its two
    // directions and between bulk and mean on/off contribution. Placement can make both bundles
    // converge on one resource; that emergent saturation is reported as realized utilization.
    let bulk_rate = reference.saturating_mul(utilization) / 400;
    // The bounded heavy-tail generator has mean on/off ratio 2:3, so 1.25x target load
    // contributes the other half in expectation.
    let onoff_rate = reference.saturating_mul(utilization).saturating_mul(5) / 800;
    let mut slots = Vec::with_capacity(BACKGROUND_COUNT);
    for (index, &component) in BACKGROUND_COMPONENTS.iter().enumerate() {
        let kind = if index % 2 == 0 {
            BackgroundTrafficKind::Bulk
        } else {
            BackgroundTrafficKind::HeavyTailedOnOff
        };
        slots.push(BackgroundSlot {
            model: BackgroundFlow::new(
                BackgroundFlowConfig {
                    component,
                    mailbox: component,
                    flow_id: background_flow_id(index),
                    data_path_mailbox: BACKBONE_MAILBOX,
                    ack_path_mailbox: BACKBONE_MAILBOX,
                    kind,
                    timer_interval_ns: 10_000_000,
                    on_base_ns: 100_000_000,
                    off_base_ns: 150_000_000,
                    application_rate_bps: Some(if kind == BackgroundTrafficKind::Bulk {
                        bulk_rate
                    } else {
                        onoff_rate
                    }),
                    prf: CounterPrf::new(config.seed, &format!("wr-background-{index}")),
                },
                socket,
                recorder.clone(),
                tracker.clone(),
            )
            .map_err(|error| WrRunError::Construction(error.to_string()))?,
            mailbox: Mailbox::with_capacity(MAILBOX_CAPACITY),
            component,
            flow_id: background_flow_id(index),
        });
    }
    Ok(slots)
}

fn add_session(
    mut bench: SimInit,
    session: usize,
    mut slot: SessionSlot,
    reverse: bool,
) -> SimInit {
    if reverse {
        slot.receivers.reverse();
        slot.receiver_mailboxes.reverse();
        for (index, (receiver, mailbox)) in slot
            .receivers
            .into_iter()
            .zip(slot.receiver_mailboxes)
            .enumerate()
        {
            let original = RECEIVER_COUNT - 1 - index;
            bench = bench.add_model(receiver, mailbox, RECEIVER_COMPONENTS[session][original]);
        }
        slot.relays.reverse();
        slot.relay_mailboxes.reverse();
        for (index, (relay, mailbox)) in slot
            .relays
            .into_iter()
            .zip(slot.relay_mailboxes)
            .enumerate()
        {
            let original = TREE_COUNT * 2 - 1 - index;
            bench = bench.add_model(
                relay,
                mailbox,
                RELAY_COMPONENTS[session][original / 2][original % 2],
            );
        }
        bench.add_model(slot.source, slot.source_mailbox, SOURCE_MAILBOXES[session])
    } else {
        bench = bench.add_model(slot.source, slot.source_mailbox, SOURCE_MAILBOXES[session]);
        for (index, (relay, mailbox)) in slot
            .relays
            .into_iter()
            .zip(slot.relay_mailboxes)
            .enumerate()
        {
            bench = bench.add_model(
                relay,
                mailbox,
                RELAY_COMPONENTS[session][index / 2][index % 2],
            );
        }
        for (index, (receiver, mailbox)) in slot
            .receivers
            .into_iter()
            .zip(slot.receiver_mailboxes)
            .enumerate()
        {
            bench = bench.add_model(receiver, mailbox, RECEIVER_COMPONENTS[session][index]);
        }
        bench
    }
}

#[derive(Clone, Debug)]
struct RegionRoute {
    resource_indexes: Vec<usize>,
    hops: Vec<BackboneRouteHop>,
}

fn build_backbone_geometry(config: &WrRunConfig) -> Result<BackboneGeometry, WrRunError> {
    let cloud = &config.cloud;
    let mut resources = cloud
        .regions
        .iter()
        .map(|region| BackboneResourceConfig {
            name: format!("nic:{}", region.id),
            rate_bps: cloud.vm_nic_cap_bps,
            queue_bytes: cloud.vm_nic_queue_bytes,
        })
        .collect::<Vec<_>>();
    resources.extend(cloud.trunks.iter().map(|trunk| BackboneResourceConfig {
        name: format!("trunk:{}", trunk.id),
        rate_bps: trunk.capacity_bps,
        queue_bytes: trunk.queue_bytes,
    }));
    let mut routes = BTreeMap::new();
    let mut foreground_counts = vec![0_usize; resources.len()];
    let mut background_counts = vec![0_usize; resources.len()];
    let nodes = overlay_node_regions(cloud)?;
    let tree_probe_flow_ids = BTreeSet::from([background_flow_id(0), background_flow_id(4)]);
    for session in 0..config.concurrent_sessions {
        for tree in 0..TREE_COUNT {
            for (hop, (from_node, to_node)) in overlay_edges(tree).into_iter().enumerate() {
                insert_tcp_route(
                    cloud,
                    &mut routes,
                    &mut foreground_counts,
                    data_flow_id(session, tree, hop),
                    nodes[from_node],
                    nodes[to_node],
                    forward_component(session, tree, hop),
                    reverse_component(session, tree, hop),
                )?;
            }
        }
        let sender_region = nodes[0];
        for receiver in 0..RECEIVER_COUNT {
            let receiver_region = nodes[receiver_node(receiver)];
            let (downlink, uplink) = control_flow_ids(session, receiver);
            insert_tcp_route(
                cloud,
                &mut routes,
                &mut foreground_counts,
                downlink,
                sender_region,
                receiver_region,
                RECEIVER_COMPONENTS[session][receiver],
                SOURCE_MAILBOXES[session],
            )?;
            insert_tcp_route(
                cloud,
                &mut routes,
                &mut foreground_counts,
                uplink,
                receiver_region,
                sender_region,
                SOURCE_MAILBOXES[session],
                RECEIVER_COMPONENTS[session][receiver],
            )?;
        }
    }
    let background_pairs = background_region_pairs(cloud)?;
    for (index, &(from, to)) in background_pairs.iter().enumerate() {
        insert_tcp_route(
            cloud,
            &mut routes,
            &mut background_counts,
            background_flow_id(index),
            from,
            to,
            BACKGROUND_COMPONENTS[index],
            BACKGROUND_COMPONENTS[index],
        )?;
    }
    let background_flow_ids = (0..BACKGROUND_COUNT)
        .map(background_flow_id)
        .collect::<BTreeSet<_>>();
    let sharing = resources
        .iter()
        .enumerate()
        .map(|(index, resource)| WrSharingRow {
            resource: resource.name.clone(),
            total_flow_directions: foreground_counts[index]
                .saturating_add(background_counts[index]),
            foreground_flow_directions: foreground_counts[index],
            background_flow_directions: background_counts[index],
        })
        .collect();
    Ok(BackboneGeometry {
        config: RegionalBackboneConfig {
            component: BACKBONE_COMPONENT,
            mailbox: BACKBONE_MAILBOX,
            resources,
            routes,
            background_flow_ids,
            tree_probe_flow_ids,
            jitter_enabled: config.jitter_enabled,
            jitter_max_ppm: cloud.jitter_max_ppm,
            jitter_epoch_ns: cloud.jitter_epoch_ns,
            sample_interval_ns: 10_000_000,
            simulation_end_ns: simulation_end_ns(config),
            prf: CounterPrf::new(config.seed, "wr-regional-backbone"),
        },
        sharing,
    })
}

#[allow(clippy::too_many_arguments)]
fn insert_tcp_route(
    cloud: &CloudScenario,
    routes: &mut BTreeMap<(usize, bool), BackboneRouteConfig>,
    counts: &mut [usize],
    flow_id: usize,
    from: usize,
    to: usize,
    forward_mailbox: &'static str,
    reverse_mailbox: &'static str,
) -> Result<(), WrRunError> {
    let forward = region_route(cloud, from, to)?;
    let reverse = region_route(cloud, to, from)?;
    for resource in &forward.resource_indexes {
        counts[*resource] = counts[*resource].saturating_add(1);
    }
    for resource in &reverse.resource_indexes {
        counts[*resource] = counts[*resource].saturating_add(1);
    }
    if routes
        .insert(
            (flow_id, false),
            BackboneRouteConfig {
                hops: forward.hops,
                downstream_mailbox: forward_mailbox,
            },
        )
        .is_some()
        || routes
            .insert(
                (flow_id, true),
                BackboneRouteConfig {
                    hops: reverse.hops,
                    downstream_mailbox: reverse_mailbox,
                },
            )
            .is_some()
    {
        return Err(WrRunError::Geometry);
    }
    Ok(())
}

fn region_route(cloud: &CloudScenario, from: usize, to: usize) -> Result<RegionRoute, WrRunError> {
    let source = cloud.regions.get(from).ok_or(WrRunError::Geometry)?;
    let destination = cloud.regions.get(to).ok_or(WrRunError::Geometry)?;
    if from == to {
        return Ok(RegionRoute {
            resource_indexes: vec![from],
            hops: vec![BackboneRouteHop {
                resource: from,
                propagation_ns: cloud.intra_region_rtt_ns / 2,
            }],
        });
    }
    let core = cloud.hub_route(&source.hub, &destination.hub)?;
    let mut resource_indexes = Vec::with_capacity(core.trunk_indexes.len() + 2);
    let mut hops = Vec::with_capacity(core.trunk_indexes.len() + 2);
    resource_indexes.push(from);
    hops.push(BackboneRouteHop {
        resource: from,
        propagation_ns: source.access_one_way_ns,
    });
    for trunk in core.trunk_indexes {
        let resource = cloud.regions.len() + trunk;
        resource_indexes.push(resource);
        hops.push(BackboneRouteHop {
            resource,
            propagation_ns: cloud.trunks[trunk].propagation_ns,
        });
    }
    resource_indexes.push(to);
    hops.push(BackboneRouteHop {
        resource: to,
        propagation_ns: destination.access_one_way_ns,
    });
    Ok(RegionRoute {
        resource_indexes,
        hops,
    })
}

fn overlay_node_regions(cloud: &CloudScenario) -> Result<[usize; 8], WrRunError> {
    Ok([
        cloud.region_index(&cloud.placement.sender_region)?,
        cloud.region_index(&cloud.placement.relay_regions[0][0])?,
        cloud.region_index(&cloud.placement.receiver_regions[0])?,
        cloud.region_index(&cloud.placement.relay_regions[0][1])?,
        cloud.region_index(&cloud.placement.receiver_regions[1])?,
        cloud.region_index(&cloud.placement.receiver_regions[2])?,
        cloud.region_index(&cloud.placement.relay_regions[1][0])?,
        cloud.region_index(&cloud.placement.relay_regions[1][1])?,
    ])
}

fn overlay_edges(tree: usize) -> [(usize, usize); HOPS_PER_TREE] {
    let relay_a = if tree == 0 { 1 } else { 6 };
    let relay_b = if tree == 0 { 3 } else { 7 };
    [
        (0, relay_a),
        (relay_a, 2),
        (relay_a, relay_b),
        (relay_b, 4),
        (relay_b, 5),
    ]
}

fn background_region_pairs(
    cloud: &CloudScenario,
) -> Result<[(usize, usize); BACKGROUND_COUNT], WrRunError> {
    let nodes = overlay_node_regions(cloud)?;
    let sender = nodes[0];
    let tree0_relay = nodes[1];
    let tree1_relay = nodes[6];
    Ok([
        (sender, tree0_relay),
        (sender, tree0_relay),
        (tree0_relay, sender),
        (tree0_relay, sender),
        (sender, tree1_relay),
        (sender, tree1_relay),
        (tree1_relay, sender),
        (tree1_relay, sender),
    ])
}

fn wire_session(
    session: usize,
    slot: &mut SessionSlot,
    backbone: &mut RegionalBackbone,
    backbone_mailbox: &Mailbox<RegionalBackbone>,
) {
    for tree in 0..TREE_COUNT {
        let relay_a = tree * 2;
        let relay_b = relay_a + 1;
        slot.source.data_outputs[tree].connect(RegionalBackbone::receive, backbone_mailbox);
        connect_backbone(
            backbone,
            data_flow_id(session, tree, 0),
            false,
            FanoutRelayEndpoint::upstream_segment,
            &slot.relay_mailboxes[relay_a],
        );
        slot.relays[relay_a]
            .upstream_ack_output
            .connect(RegionalBackbone::receive, backbone_mailbox);
        if tree == 0 {
            connect_backbone(
                backbone,
                data_flow_id(session, tree, 0),
                true,
                W1SourceEndpoint::data0_ack,
                &slot.source_mailbox,
            );
        } else {
            connect_backbone(
                backbone,
                data_flow_id(session, tree, 0),
                true,
                W1SourceEndpoint::data1_ack,
                &slot.source_mailbox,
            );
        }

        slot.relays[relay_a].child_data_outputs[0]
            .connect(RegionalBackbone::receive, backbone_mailbox);
        connect_receiver_data(
            backbone,
            session,
            tree,
            0,
            1,
            false,
            &slot.receiver_mailboxes[0],
        );
        slot.receivers[0].data_ack_outputs[tree]
            .connect(RegionalBackbone::receive, backbone_mailbox);
        connect_backbone(
            backbone,
            data_flow_id(session, tree, 1),
            true,
            FanoutRelayEndpoint::child0_acknowledgment,
            &slot.relay_mailboxes[relay_a],
        );

        slot.relays[relay_a].child_data_outputs[1]
            .connect(RegionalBackbone::receive, backbone_mailbox);
        connect_backbone(
            backbone,
            data_flow_id(session, tree, 2),
            false,
            FanoutRelayEndpoint::upstream_segment,
            &slot.relay_mailboxes[relay_b],
        );
        slot.relays[relay_b]
            .upstream_ack_output
            .connect(RegionalBackbone::receive, backbone_mailbox);
        connect_backbone(
            backbone,
            data_flow_id(session, tree, 2),
            true,
            FanoutRelayEndpoint::child1_acknowledgment,
            &slot.relay_mailboxes[relay_a],
        );

        for (receiver, hop, child) in [(1, 3, 0), (2, 4, 1)] {
            slot.relays[relay_b].child_data_outputs[child]
                .connect(RegionalBackbone::receive, backbone_mailbox);
            connect_receiver_data(
                backbone,
                session,
                tree,
                receiver,
                hop,
                false,
                &slot.receiver_mailboxes[receiver],
            );
            slot.receivers[receiver].data_ack_outputs[tree]
                .connect(RegionalBackbone::receive, backbone_mailbox);
            if child == 0 {
                connect_backbone(
                    backbone,
                    data_flow_id(session, tree, hop),
                    true,
                    FanoutRelayEndpoint::child0_acknowledgment,
                    &slot.relay_mailboxes[relay_b],
                );
            } else {
                connect_backbone(
                    backbone,
                    data_flow_id(session, tree, hop),
                    true,
                    FanoutRelayEndpoint::child1_acknowledgment,
                    &slot.relay_mailboxes[relay_b],
                );
            }
        }
    }
    for receiver in 0..RECEIVER_COUNT {
        slot.source.control_forward_outputs[receiver]
            .connect(RegionalBackbone::receive, backbone_mailbox);
        slot.receivers[receiver]
            .control_reverse_output
            .connect(RegionalBackbone::receive, backbone_mailbox);
        let (downlink, uplink) = control_flow_ids(session, receiver);
        connect_backbone_flow(
            backbone,
            downlink,
            false,
            W1ReceiverEndpoint::control_packet,
            &slot.receiver_mailboxes[receiver],
        );
        connect_backbone_flow(
            backbone,
            uplink,
            true,
            W1ReceiverEndpoint::control_packet,
            &slot.receiver_mailboxes[receiver],
        );
        match receiver {
            0 => {
                connect_backbone_flow(
                    backbone,
                    downlink,
                    true,
                    W1SourceEndpoint::control_peer0_packet,
                    &slot.source_mailbox,
                );
                connect_backbone_flow(
                    backbone,
                    uplink,
                    false,
                    W1SourceEndpoint::control_peer0_packet,
                    &slot.source_mailbox,
                );
            }
            1 => {
                connect_backbone_flow(
                    backbone,
                    downlink,
                    true,
                    W1SourceEndpoint::control_peer1_packet,
                    &slot.source_mailbox,
                );
                connect_backbone_flow(
                    backbone,
                    uplink,
                    false,
                    W1SourceEndpoint::control_peer1_packet,
                    &slot.source_mailbox,
                );
            }
            2 => {
                connect_backbone_flow(
                    backbone,
                    downlink,
                    true,
                    W1SourceEndpoint::control_peer2_packet,
                    &slot.source_mailbox,
                );
                connect_backbone_flow(
                    backbone,
                    uplink,
                    false,
                    W1SourceEndpoint::control_peer2_packet,
                    &slot.source_mailbox,
                );
            }
            _ => unreachable!("three receivers"),
        }
    }
}

fn connect_receiver_data(
    backbone: &mut RegionalBackbone,
    session: usize,
    tree: usize,
    _receiver: usize,
    hop: usize,
    is_ack: bool,
    mailbox: &Mailbox<W1ReceiverEndpoint>,
) {
    if tree == 0 {
        connect_backbone(
            backbone,
            data_flow_id(session, tree, hop),
            is_ack,
            W1ReceiverEndpoint::data0_segment,
            mailbox,
        );
    } else {
        connect_backbone(
            backbone,
            data_flow_id(session, tree, hop),
            is_ack,
            W1ReceiverEndpoint::data1_segment,
            mailbox,
        );
    }
}

fn connect_backbone<M, F, S>(
    backbone: &mut RegionalBackbone,
    flow_id: usize,
    is_ack: bool,
    handler: F,
    mailbox: &Mailbox<M>,
) where
    M: nexosim::model::Model,
    F: for<'a> nexosim::ports::InputFn<'a, M, crate::metrics::TrackedPacket, S> + Clone,
    S: Send + 'static,
{
    connect_backbone_flow(backbone, flow_id, is_ack, handler, mailbox);
}

fn connect_backbone_flow<M, F, S>(
    backbone: &mut RegionalBackbone,
    flow_id: usize,
    is_ack: bool,
    handler: F,
    mailbox: &Mailbox<M>,
) where
    M: nexosim::model::Model,
    F: for<'a> nexosim::ports::InputFn<'a, M, crate::metrics::TrackedPacket, S> + Clone,
    S: Send + 'static,
{
    backbone.output.filter_map_connect(
        move |tracked: &crate::metrics::TrackedPacket| {
            (tracked.packet.flow_id == flow_id && tracked.packet.ack.is_some() == is_ack)
                .then(|| tracked.clone())
        },
        handler,
        mailbox,
    );
}

fn wire_background(
    background: &mut [BackgroundSlot],
    backbone: &mut RegionalBackbone,
    backbone_mailbox: &Mailbox<RegionalBackbone>,
) {
    for slot in background {
        slot.model
            .data_output
            .connect(RegionalBackbone::receive, backbone_mailbox);
        slot.model
            .ack_output
            .connect(RegionalBackbone::receive, backbone_mailbox);
        let flow_id = slot.flow_id;
        backbone.output.filter_map_connect(
            move |tracked: &crate::metrics::TrackedPacket| {
                (tracked.packet.flow_id == flow_id).then(|| tracked.clone())
            },
            BackgroundFlow::network_packet,
            &slot.mailbox,
        );
    }
}

fn summarize_sessions(
    config: &WrRunConfig,
    records: &[Record],
    stall_budget_ns: u64,
) -> Result<Vec<WrSessionOutcome>, WrRunError> {
    let mut outcomes = Vec::with_capacity(config.concurrent_sessions);
    for session in 0..config.concurrent_sessions {
        let mut completion_times_ns = [0_u64; RECEIVER_COUNT];
        for (receiver, completion) in completion_times_ns.iter_mut().enumerate() {
            *completion = records
                .iter()
                .find(|record| {
                    record.component == RECEIVER_COMPONENTS[session][receiver]
                        && record.event == "protocol_local_complete"
                })
                .map(|record| record.time_ns.saturating_sub(WR_FOREGROUND_START_NS))
                .ok_or(WrRunError::IncompleteReceiver { session, receiver })?;
        }
        let barrier_completion_ns = completion_times_ns.iter().copied().max().unwrap_or(0);
        let sender_completion_at_ns = records
            .iter()
            .find(|record| {
                record.component == SOURCE_COMPONENTS[session]
                    && record.event == "protocol_sender_complete"
            })
            .map(|record| record.time_ns)
            .ok_or(WrRunError::IncompleteSender(session))?;
        let sender_completion_ns = sender_completion_at_ns.saturating_sub(WR_FOREGROUND_START_NS);
        let source_records = records
            .iter()
            .filter(|record| record.component == SOURCE_COMPONENTS[session])
            .collect::<Vec<_>>();
        let total_emissions = source_records
            .iter()
            .filter(|record| record.event == "data_frame_emitted")
            .count();
        let per_tree_emissions = [0, 1].map(|tree| {
            source_records
                .iter()
                .filter(|record| {
                    record.event == "data_frame_emitted"
                        && record.flow_id == data_flow_id(session, tree, 0)
                })
                .count()
        });
        let application_drops = records
            .iter()
            .filter(|record| {
                RECEIVER_COMPONENTS[session].contains(&record.component)
                    && record.event == "data_inbox_drop_after_tcp_ack"
            })
            .count();
        let blocking_waits = records
            .iter()
            .filter(|record| {
                RECEIVER_COMPONENTS[session].contains(&record.component)
                    && record.event == "data_inbox_blocking_wait"
            })
            .count();
        let positive_round_deficits = source_records
            .iter()
            .filter(|record| record.event == "round_deficit_received" && record.value > 0)
            .count();
        let ack_probes = source_records
            .iter()
            .filter(|record| record.event == "ack_probe_submitted")
            .count();
        let mut ack_times = source_records
            .iter()
            .filter(|record| record.event == "block_ack_received")
            .map(|record| record.time_ns)
            .collect::<Vec<_>>();
        ack_times.sort_unstable();
        let maximum_ack_gap_ns = ack_times
            .iter()
            .copied()
            .chain(std::iter::once(sender_completion_at_ns))
            .scan(WR_FOREGROUND_START_NS, |prior, now| {
                let gap = now.saturating_sub(*prior);
                *prior = now;
                Some(gap)
            })
            .max()
            .unwrap_or(sender_completion_ns);
        let stall_budget_consumption_ppm =
            maximum_ack_gap_ns.saturating_mul(1_000_000) / stall_budget_ns.max(1);
        outcomes.push(WrSessionOutcome {
            completion_times_ns,
            barrier_completion_ns,
            sender_completion_ns,
            total_emissions,
            per_tree_emissions,
            application_drops,
            blocking_waits,
            positive_round_deficits,
            ack_probes,
            maximum_ack_gap_ns,
            stall_budget_consumption_ppm,
        });
    }
    Ok(outcomes)
}

fn maximum_background_trunk_utilization_ppm(
    cloud: &CloudScenario,
    records: &[Record],
    foreground_duration_ns: u64,
) -> u64 {
    if foreground_duration_ns == 0 {
        return 0;
    }
    let first_trunk = cloud.regions.len();
    let end_ns = WR_FOREGROUND_START_NS.saturating_add(foreground_duration_ns);
    let mut background_bytes = vec![0_u128; cloud.trunks.len()];
    for record in records.iter().filter(|record| {
        record.event == "wr_resource_sample"
            && record.time_ns > WR_FOREGROUND_START_NS
            && record.time_ns <= end_ns
            && record.flow_id >= first_trunk
    }) {
        let Some(slot) = record.flow_id.checked_sub(first_trunk) else {
            continue;
        };
        if let Some(bytes) = background_bytes.get_mut(slot) {
            *bytes = bytes.saturating_add(record.value as u128);
        }
    }
    background_bytes
        .into_iter()
        .zip(&cloud.trunks)
        .map(|(bytes, trunk)| {
            let numerator = bytes.saturating_mul(8_000_000_000_000_000);
            let denominator = u128::from(foreground_duration_ns)
                .saturating_mul(u128::from(trunk.capacity_bps))
                .max(1);
            u64::try_from(numerator / denominator).unwrap_or(u64::MAX)
        })
        .max()
        .unwrap_or(0)
}

fn data_flow_id(session: usize, tree: usize, hop: usize) -> usize {
    100_000 + session * 10_000 + tree * 100 + hop
}

fn control_flow_ids(session: usize, receiver: usize) -> (usize, usize) {
    let base = 105_000 + session * 10_000 + receiver * 2;
    (base, base + 1)
}

fn background_flow_id(index: usize) -> usize {
    200_000 + index
}

fn receiver_hop(receiver: usize) -> usize {
    match receiver {
        0 => 1,
        1 => 3,
        2 => 4,
        _ => unreachable!("three receivers"),
    }
}

fn receiver_node(receiver: usize) -> usize {
    match receiver {
        0 => 2,
        1 => 4,
        2 => 5,
        _ => unreachable!("three receivers"),
    }
}

fn forward_component(session: usize, tree: usize, hop: usize) -> &'static str {
    match hop {
        0 => RELAY_COMPONENTS[session][tree][0],
        1 => RECEIVER_COMPONENTS[session][0],
        2 => RELAY_COMPONENTS[session][tree][1],
        3 => RECEIVER_COMPONENTS[session][1],
        4 => RECEIVER_COMPONENTS[session][2],
        _ => unreachable!("five hops"),
    }
}

fn reverse_component(session: usize, tree: usize, hop: usize) -> &'static str {
    match hop {
        0 => SOURCE_MAILBOXES[session],
        1 | 2 => RELAY_COMPONENTS[session][tree][0],
        3 | 4 => RELAY_COMPONENTS[session][tree][1],
        _ => unreachable!("five hops"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scenario::CloudProfileKind;

    fn config() -> WrRunConfig {
        WrRunConfig {
            cloud: CloudScenario::built_in(CloudProfileKind::AwsLike, 0).expect("cloud scenario"),
            protocol: WrProtocol::Carousel,
            source_symbols: 64,
            background_utilization_percent: 30,
            jitter_enabled: true,
            seed: 7,
            ack_cadence_multiplier: 2,
            receiver_admission: ReceiverAdmissionPolicy::HybridDrop,
            slow_receiver: None,
            concurrent_sessions: 1,
            registration_order: RegistrationOrder::Forward,
        }
    }

    #[test]
    fn placement_derives_shared_routes_without_an_overlap_knob() {
        let config = config();
        let geometry = build_backbone_geometry(&config).expect("geometry");
        assert!(
            geometry
                .sharing
                .iter()
                .any(|row| row.foreground_flow_directions > 2)
        );
        assert!(
            geometry
                .sharing
                .iter()
                .any(|row| row.background_flow_directions > 0)
        );
        assert_eq!(geometry.config.routes.len(), 48);
    }

    #[test]
    fn wr_progress_geometry_and_one_x_timing_mirror_production_defaults() {
        assert_eq!(ack_progress_units(8_192).expect("units"), 482);
        assert_eq!(ack_progress_units(65_536).expect("units"), 3_856);
        assert_eq!(maximum_frames_per_tree(8_192).expect("frames"), 65_536);
        assert_eq!(
            carousel_timing(2),
            CarouselTiming {
                ack_debounce_ns: 8_000_000,
                ack_heartbeat_ns: 300_000_000,
                ack_probe_interval_ns: 250_000_000,
                peer_silence_timeout_ns: 3_000_000_000,
                peer_stall_timeout_ns: 15_000_000_000,
                receiver_passive_window_ns: 17_000_000_000,
                session_complete_repeats: 3,
                session_complete_interval_ns: 20_000_000,
            }
        );
        let mut scaling = config();
        scaling.source_symbols = 65_536;
        assert_eq!(simulation_end_ns(&scaling), 181_000_000_000);
    }

    #[test]
    fn cloud_carousel_run_is_deterministic_across_registration_order() {
        let forward = run_wr(&config()).expect("forward");
        assert!(forward.records.iter().any(|record| {
            record.event == "data_frame_emitted" && record.time_ns >= WR_FOREGROUND_START_NS
        }));
        let mut reverse_config = config();
        reverse_config.registration_order = RegistrationOrder::Reverse;
        let reverse = run_wr(&reverse_config).expect("reverse");
        assert_eq!(
            forward.sessions[0].barrier_completion_ns,
            reverse.sessions[0].barrier_completion_ns
        );
        assert_eq!(
            forward.sessions[0].total_emissions,
            reverse.sessions[0].total_emissions
        );
        assert_eq!(forward.link_drops, reverse.link_drops);
    }

    #[test]
    fn repeated_cloud_run_is_byte_identical() {
        let first = run_wr(&config()).expect("first");
        let second = run_wr(&config()).expect("second");
        assert_eq!(first.csv, second.csv);
    }

    #[test]
    fn all_protocol_and_multi_session_paths_complete() {
        for protocol in [
            WrProtocol::Carousel,
            WrProtocol::Rounds,
            WrProtocol::PerStripeFec,
            WrProtocol::BestSingleTree0,
            WrProtocol::BestSingleTree1,
        ] {
            let mut cell = config();
            cell.protocol = protocol;
            cell.source_symbols = 128;
            let outcome = run_wr(&cell).expect("protocol cell");
            assert_eq!(outcome.sessions.len(), 1);
            let maximum = outcome
                .mailbox_high_water
                .iter()
                .max_by_key(|(mailbox, level)| {
                    level.saturating_mul(1_000_000) / outcome.mailbox_capacities[**mailbox]
                })
                .expect("mailbox sample");
            assert!(
                *maximum.1 < outcome.mailbox_capacities[maximum.0],
                "{} mailbox reached {}/{}",
                maximum.0,
                maximum.1,
                outcome.mailbox_capacities[maximum.0]
            );
        }
        let mut concurrent = config();
        concurrent.source_symbols = 64;
        concurrent.concurrent_sessions = 2;
        let outcome = run_wr(&concurrent).expect("concurrent sessions");
        assert_eq!(outcome.sessions.len(), 2);
    }

    #[test]
    fn blocking_straggler_stalls_without_application_drop() {
        let mut cell = config();
        cell.source_symbols = 512;
        cell.receiver_admission = ReceiverAdmissionPolicy::NaiveBlocking;
        cell.slow_receiver = Some(0);
        let outcome = run_wr(&cell).expect("blocking straggler");
        assert_eq!(outcome.sessions[0].application_drops, 0);
        assert!(outcome.sessions[0].blocking_waits > 0);
    }

    #[test]
    #[ignore = "manual production-geometry runtime probe"]
    fn production_k8192_cell_completes() {
        let mut config = config();
        config.source_symbols = 8_192;
        let outcome = run_wr(&config).expect("production cell");
        assert!(outcome.sessions[0].barrier_completion_ns > 0);
        assert!(outcome.sessions[0].sender_completion_ns > 0);
    }
}
