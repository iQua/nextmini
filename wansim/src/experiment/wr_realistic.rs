use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;

use serde::Serialize;
use thiserror::Error;

use crate::metrics::{MAILBOX_CAPACITY, Record};
use crate::scenario::{
    CloudProfileKind, CloudScenario, ReceiverAdmissionPolicy, RegistrationOrder, WrOutcome,
    WrProtocol, WrRunConfig, WrRunError, run_wr,
};
use crate::{SCENARIO_SCHEMA_VERSION, SIMULATOR_VERSION};

mod persistence;

const EVIDENCE_CLASS: &str = "model-level realistic-envelope evidence; not a WAN measurement";
const MAIN_PROTOCOLS: [WrProtocol; 5] = [
    WrProtocol::Carousel,
    WrProtocol::Rounds,
    WrProtocol::PerStripeFec,
    WrProtocol::BestSingleTree0,
    WrProtocol::BestSingleTree1,
];

#[derive(Clone, Debug)]
pub struct WrArtifacts {
    pub trials_csv: String,
    pub summaries_csv: String,
    pub advantage_csv: String,
    pub rounds_csv: String,
    pub a4_csv: String,
    pub straggler_csv: String,
    pub cadence_csv: String,
    pub concurrent_csv: String,
    pub scaling_csv: String,
    pub sharing_csv: String,
}

#[derive(Clone, Debug)]
pub struct WrPersistentRun {
    pub artifacts: Option<WrArtifacts>,
    pub failures_csv: String,
    pub total_cells: usize,
    pub successful_cells: usize,
    pub failed_cells: usize,
    pub skipped_cells: usize,
}

#[derive(Debug, Error)]
pub enum WrExperimentError {
    #[error("WR requires at least 16 stochastic seeds per main cell")]
    TooFewSeeds,
    #[error("WR worker count must be nonzero")]
    ZeroWorkers,
    #[error("WR worker failed: {0}")]
    Worker(String),
    #[error("WR task {0} did not produce a result")]
    MissingTask(usize),
    #[error("WR pairing is missing: {0}")]
    MissingPair(String),
    #[error("WR nexosim mailbox became binding: {0}/{MAILBOX_CAPACITY}")]
    BindingMailbox(usize),
    #[error("WR persistence failed: {0}")]
    Persistence(String),
    #[error(transparent)]
    Run(#[from] WrRunError),
    #[error(transparent)]
    Csv(#[from] csv::Error),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "kebab-case")]
enum Slice {
    Main,
    Straggler,
    Cadence,
    Concurrent,
    Scaling,
}

impl Slice {
    const fn name(self) -> &'static str {
        match self {
            Self::Main => "main",
            Self::Straggler => "straggler",
            Self::Cadence => "cadence",
            Self::Concurrent => "concurrent",
            Self::Scaling => "scaling",
        }
    }
}

#[derive(Clone, Debug)]
struct Task {
    slice: Slice,
    profile: CloudProfileKind,
    placement: usize,
    utilization: u8,
    jitter: bool,
    protocol: WrProtocol,
    k: usize,
    seed: u64,
    cadence: u8,
    admission: ReceiverAdmissionPolicy,
    slow_receiver: Option<usize>,
    sessions: usize,
}

#[derive(Clone, Debug)]
struct RawTrial {
    task: Task,
    profile_name: &'static str,
    placement_id: String,
    barrier_ns: u64,
    sender_ns: u64,
    receiver_ns: [u64; 3],
    emissions: usize,
    useful_emissions: usize,
    per_tree_emissions: [usize; 2],
    drops: usize,
    blocking_waits: usize,
    positive_round_deficits: usize,
    ack_probes: usize,
    stall_ppm: u64,
    link_drops: usize,
    background_trunk_utilization_ppm: u64,
    a4_trace_correlation_ppm: i64,
    tree_bytes: [u64; 2],
    maximum_mailbox_high_water: usize,
    sharing: Vec<SharingRow>,
}

#[derive(Clone, Debug, Serialize, PartialEq, Eq, PartialOrd, Ord)]
struct SharingRow {
    profile: String,
    placement: String,
    resource: String,
    total_flow_directions: usize,
    foreground_flow_directions: usize,
    background_flow_directions: usize,
}

#[derive(Clone, Debug, Serialize)]
struct TrialRow {
    schema_version: u32,
    simulator_version: &'static str,
    evidence_class: &'static str,
    slice: &'static str,
    profile: &'static str,
    placement: String,
    utilization_percent: u8,
    realized_background_trunk_utilization_ppm: u64,
    jitter: bool,
    protocol: &'static str,
    source_symbols: usize,
    seed: u64,
    cadence: &'static str,
    admission: &'static str,
    slow_receiver: i8,
    sessions: usize,
    barrier_completion_ns: u64,
    sender_completion_ns: u64,
    receiver0_completion_ns: u64,
    receiver1_completion_ns: u64,
    receiver2_completion_ns: u64,
    total_emissions: usize,
    useful_emissions: usize,
    tree0_emissions: usize,
    tree1_emissions: usize,
    application_drops: usize,
    blocking_waits: usize,
    positive_round_deficits: usize,
    ack_probes: usize,
    stall_budget_consumption_ppm: u64,
    link_drops: usize,
    a4_trace_correlation_ppm: i64,
    tree0_delivered_bytes: u64,
    tree1_delivered_bytes: u64,
    maximum_mailbox_high_water: usize,
}

#[derive(Clone, Debug, Serialize)]
struct SummaryRow {
    schema_version: u32,
    simulator_version: &'static str,
    evidence_class: &'static str,
    slice: &'static str,
    profile: &'static str,
    placement: String,
    utilization_percent: u8,
    jitter: bool,
    protocol: &'static str,
    source_symbols: usize,
    cadence: &'static str,
    admission: &'static str,
    sessions: usize,
    seeds: usize,
    barrier_mean_ns: u64,
    barrier_p5_ns: u64,
    barrier_p95_ns: u64,
    sender_mean_ns: u64,
    sender_p5_ns: u64,
    sender_p95_ns: u64,
    emissions_mean: u64,
    emissions_p5: u64,
    emissions_p95: u64,
    application_drops_mean: u64,
    background_trunk_utilization_mean_ppm: u64,
    background_trunk_utilization_p5_ppm: u64,
    background_trunk_utilization_p95_ppm: u64,
    stall_consumption_p95_ppm: u64,
    link_drops_total: usize,
}

#[derive(Clone, Debug, Serialize)]
struct AdvantageRow {
    schema_version: u32,
    simulator_version: &'static str,
    evidence_class: &'static str,
    profile: &'static str,
    placement: String,
    utilization_percent: u8,
    jitter: bool,
    seeds: usize,
    carousel_mean_ns: u64,
    per_stripe_mean_ns: u64,
    best_single_mean_ns: u64,
    pooling_advantage_mean_ns: i64,
    pooling_advantage_p5_ns: i64,
    pooling_advantage_p95_ns: i64,
    path_diversity_advantage_mean_ns: i64,
    path_diversity_advantage_p5_ns: i64,
    path_diversity_advantage_p95_ns: i64,
    total_advantage_mean_ns: i64,
    additive_identity_holds: bool,
}

#[derive(Clone, Debug, Serialize)]
struct RoundsRow {
    schema_version: u32,
    simulator_version: &'static str,
    evidence_class: &'static str,
    profile: &'static str,
    placement: String,
    utilization_percent: u8,
    jitter: bool,
    seeds: usize,
    receiver_barrier_rounds_minus_carousel_mean_ns: i64,
    receiver_barrier_gap_p5_ns: i64,
    receiver_barrier_gap_p95_ns: i64,
    sender_rounds_minus_carousel_mean_ns: i64,
    sender_gap_p5_ns: i64,
    sender_gap_p95_ns: i64,
    carousel_tail_emissions_mean: u64,
    rounds_positive_deficit_total: usize,
}

#[derive(Clone, Debug, Serialize)]
struct A4Row {
    schema_version: u32,
    simulator_version: &'static str,
    evidence_class: &'static str,
    profile: &'static str,
    placement: String,
    utilization_percent: u8,
    jitter: bool,
    seeds: usize,
    cross_seed_tree_rate_correlation_ppm: i64,
    trace_correlation_mean_ppm: i64,
    trace_correlation_p5_ppm: i64,
    trace_correlation_p95_ppm: i64,
}

#[derive(Clone, Debug, Serialize)]
struct PairRow {
    schema_version: u32,
    simulator_version: &'static str,
    evidence_class: &'static str,
    question: &'static str,
    variant: String,
    seeds: usize,
    barrier_mean_ns: u64,
    barrier_p5_ns: u64,
    barrier_p95_ns: u64,
    sender_mean_ns: u64,
    healthy_receiver_mean_ns: u64,
    emissions_mean: u64,
    drops_mean: u64,
    blocking_waits_mean: u64,
    background_trunk_utilization_mean_ppm: u64,
    stall_consumption_p95_ppm: u64,
}

pub fn run_wr_experiment(seeds: u64, workers: usize) -> Result<WrArtifacts, WrExperimentError> {
    if seeds < 16 {
        return Err(WrExperimentError::TooFewSeeds);
    }
    if workers == 0 {
        return Err(WrExperimentError::ZeroWorkers);
    }
    let tasks = build_tasks(seeds);
    let trials = run_tasks(tasks, workers)?;
    artifacts_from_trials(&trials)
}

pub fn run_wr_experiment_persistent(
    seeds: u64,
    workers: usize,
    output: &std::path::Path,
    resume: bool,
) -> Result<WrPersistentRun, WrExperimentError> {
    if seeds < 16 {
        return Err(WrExperimentError::TooFewSeeds);
    }
    if workers == 0 {
        return Err(WrExperimentError::ZeroWorkers);
    }
    persistence::run(seeds, workers, output, resume)
        .map_err(|error| WrExperimentError::Persistence(error.to_string()))
}

fn artifacts_from_trials(trials: &[RawTrial]) -> Result<WrArtifacts, WrExperimentError> {
    let trial_rows = trials.iter().map(trial_row).collect::<Vec<_>>();
    let summaries = summarize(trials);
    let advantages = advantages(trials)?;
    let rounds = rounds_gaps(trials)?;
    let a4 = a4_rows(trials);
    let straggler = pair_rows(trials, Slice::Straggler, "hybrid-straggler");
    let cadence = pair_rows(trials, Slice::Cadence, "blockack-cadence");
    let concurrent = pair_rows(trials, Slice::Concurrent, "concurrent-sessions");
    let scaling = pair_rows(trials, Slice::Scaling, "k-scaling-spot");
    let mut sharing = trials
        .iter()
        .filter(|trial| {
            trial.task.slice == Slice::Main && trial.task.protocol == WrProtocol::Carousel
        })
        .flat_map(|trial| trial.sharing.clone())
        .collect::<Vec<_>>();
    sharing.sort();
    sharing.dedup();
    Ok(WrArtifacts {
        trials_csv: to_csv(&trial_rows)?,
        summaries_csv: to_csv(&summaries)?,
        advantage_csv: to_csv(&advantages)?,
        rounds_csv: to_csv(&rounds)?,
        a4_csv: to_csv(&a4)?,
        straggler_csv: to_csv(&straggler)?,
        cadence_csv: to_csv(&cadence)?,
        concurrent_csv: to_csv(&concurrent)?,
        scaling_csv: to_csv(&scaling)?,
        sharing_csv: to_csv(&sharing)?,
    })
}

fn build_tasks(seeds: u64) -> Vec<Task> {
    let mut tasks = Vec::new();
    let profile = CloudProfileKind::DigitaloceanLike;
    let placement = 1;
    for utilization in [30, 70] {
        for protocol in MAIN_PROTOCOLS {
            for seed in 0..seeds {
                tasks.push(Task {
                    slice: Slice::Main,
                    profile,
                    placement,
                    utilization,
                    jitter: true,
                    protocol,
                    k: 8_192,
                    seed,
                    cadence: 2,
                    admission: ReceiverAdmissionPolicy::HybridDrop,
                    slow_receiver: None,
                    sessions: 1,
                });
            }
        }
    }
    let harsh = (profile, placement, 70, true);
    for seed in 0..seeds {
        tasks.push(Task {
            slice: Slice::Straggler,
            profile: harsh.0,
            placement: harsh.1,
            utilization: harsh.2,
            jitter: harsh.3,
            protocol: WrProtocol::Carousel,
            k: 8_192,
            seed,
            cadence: 2,
            admission: ReceiverAdmissionPolicy::HybridDrop,
            slow_receiver: Some(0),
            sessions: 1,
        });
    }
    for cadence in [1, 2, 4] {
        for seed in 0..seeds {
            tasks.push(Task {
                slice: Slice::Cadence,
                profile: harsh.0,
                placement: harsh.1,
                utilization: harsh.2,
                jitter: harsh.3,
                protocol: WrProtocol::Carousel,
                k: 8_192,
                seed,
                cadence,
                admission: ReceiverAdmissionPolicy::HybridDrop,
                slow_receiver: None,
                sessions: 1,
            });
        }
    }
    tasks
}

fn run_tasks(tasks: Vec<Task>, workers: usize) -> Result<Vec<RawTrial>, WrExperimentError> {
    let tasks = Arc::new(tasks);
    let next = Arc::new(AtomicUsize::new(0));
    let results = Arc::new(Mutex::new(vec![None; tasks.len()]));
    let error = Arc::new(Mutex::new(None::<String>));
    thread::scope(|scope| {
        for _ in 0..workers {
            let tasks = Arc::clone(&tasks);
            let next = Arc::clone(&next);
            let results = Arc::clone(&results);
            let error = Arc::clone(&error);
            scope.spawn(move || {
                loop {
                    if error.lock().expect("WR error lock").is_some() {
                        return;
                    }
                    let index = next.fetch_add(1, Ordering::Relaxed);
                    let Some(task) = tasks.get(index).cloned() else {
                        return;
                    };
                    let task_context = format!("task {index}: {task:?}");
                    match run_task(task) {
                        Ok(trial) => results.lock().expect("WR results lock")[index] = Some(trial),
                        Err(problem) => {
                            *error.lock().expect("WR error lock") =
                                Some(format!("{task_context}: {problem}"));
                            return;
                        }
                    }
                }
            });
        }
    });
    if let Some(error) = error.lock().expect("WR error lock").take() {
        return Err(WrExperimentError::Worker(error));
    }
    Arc::try_unwrap(results)
        .expect("all WR worker references dropped")
        .into_inner()
        .expect("WR results lock")
        .into_iter()
        .enumerate()
        .map(|(index, trial)| trial.ok_or(WrExperimentError::MissingTask(index)))
        .collect()
}

fn run_task(task: Task) -> Result<RawTrial, WrExperimentError> {
    let cloud = CloudScenario::built_in(task.profile, task.placement).map_err(WrRunError::from)?;
    let profile_name = task.profile.name();
    let placement_id = cloud.placement.id.clone();
    let config = WrRunConfig {
        cloud,
        protocol: task.protocol,
        source_symbols: task.k,
        background_utilization_percent: task.utilization,
        jitter_enabled: task.jitter,
        seed: task.seed,
        ack_cadence_multiplier: task.cadence,
        receiver_admission: task.admission,
        slow_receiver: task.slow_receiver,
        concurrent_sessions: task.sessions,
        registration_order: RegistrationOrder::Forward,
    };
    let outcome = run_wr(&config)?;
    for (&mailbox, &high_water) in &outcome.mailbox_high_water {
        let capacity = outcome
            .mailbox_capacities
            .get(mailbox)
            .copied()
            .unwrap_or(MAILBOX_CAPACITY);
        if high_water >= capacity {
            return Err(WrExperimentError::BindingMailbox(high_water));
        }
    }
    raw_trial(task, profile_name, placement_id, outcome)
}

fn raw_trial(
    task: Task,
    profile_name: &'static str,
    placement_id: String,
    outcome: WrOutcome,
) -> Result<RawTrial, WrExperimentError> {
    let session = &outcome.sessions[0];
    let matched = outcome
        .records
        .iter()
        .filter(|record| record.event == "flow_count_match_frame_emitted")
        .count();
    let tree_bytes = tree_sample_totals(&outcome.records);
    let sharing = outcome
        .sharing
        .iter()
        .map(|row| SharingRow {
            profile: profile_name.to_owned(),
            placement: placement_id.clone(),
            resource: row.resource.clone(),
            total_flow_directions: row.total_flow_directions,
            foreground_flow_directions: row.foreground_flow_directions,
            background_flow_directions: row.background_flow_directions,
        })
        .collect();
    Ok(RawTrial {
        task,
        profile_name,
        placement_id,
        barrier_ns: session.barrier_completion_ns,
        sender_ns: session.sender_completion_ns,
        receiver_ns: session.completion_times_ns,
        emissions: session.total_emissions,
        useful_emissions: session.total_emissions.saturating_sub(matched),
        per_tree_emissions: session.per_tree_emissions,
        drops: session.application_drops,
        blocking_waits: session.blocking_waits,
        positive_round_deficits: session.positive_round_deficits,
        ack_probes: session.ack_probes,
        stall_ppm: session.stall_budget_consumption_ppm,
        link_drops: outcome.link_drops,
        background_trunk_utilization_ppm: outcome.maximum_background_trunk_utilization_ppm,
        a4_trace_correlation_ppm: trace_correlation(&outcome.records),
        tree_bytes,
        maximum_mailbox_high_water: outcome
            .mailbox_high_water
            .values()
            .copied()
            .max()
            .unwrap_or(0),
        sharing,
    })
}

fn trial_row(trial: &RawTrial) -> TrialRow {
    TrialRow {
        schema_version: SCENARIO_SCHEMA_VERSION,
        simulator_version: SIMULATOR_VERSION,
        evidence_class: EVIDENCE_CLASS,
        slice: trial.task.slice.name(),
        profile: trial.profile_name,
        placement: trial.placement_id.clone(),
        utilization_percent: trial.task.utilization,
        realized_background_trunk_utilization_ppm: trial.background_trunk_utilization_ppm,
        jitter: trial.task.jitter,
        protocol: trial.task.protocol.name(),
        source_symbols: trial.task.k,
        seed: trial.task.seed,
        cadence: cadence_name(trial.task.cadence),
        admission: admission_name(trial.task.admission),
        slow_receiver: trial.task.slow_receiver.map_or(-1, |value| value as i8),
        sessions: trial.task.sessions,
        barrier_completion_ns: trial.barrier_ns,
        sender_completion_ns: trial.sender_ns,
        receiver0_completion_ns: trial.receiver_ns[0],
        receiver1_completion_ns: trial.receiver_ns[1],
        receiver2_completion_ns: trial.receiver_ns[2],
        total_emissions: trial.emissions,
        useful_emissions: trial.useful_emissions,
        tree0_emissions: trial.per_tree_emissions[0],
        tree1_emissions: trial.per_tree_emissions[1],
        application_drops: trial.drops,
        blocking_waits: trial.blocking_waits,
        positive_round_deficits: trial.positive_round_deficits,
        ack_probes: trial.ack_probes,
        stall_budget_consumption_ppm: trial.stall_ppm,
        link_drops: trial.link_drops,
        a4_trace_correlation_ppm: trial.a4_trace_correlation_ppm,
        tree0_delivered_bytes: trial.tree_bytes[0],
        tree1_delivered_bytes: trial.tree_bytes[1],
        maximum_mailbox_high_water: trial.maximum_mailbox_high_water,
    }
}

type SummaryKey = (
    Slice,
    &'static str,
    String,
    u8,
    bool,
    WrProtocol,
    usize,
    u8,
    &'static str,
    usize,
);

fn summarize(trials: &[RawTrial]) -> Vec<SummaryRow> {
    let mut groups: BTreeMap<SummaryKey, Vec<&RawTrial>> = BTreeMap::new();
    for trial in trials {
        groups
            .entry((
                trial.task.slice,
                trial.profile_name,
                trial.placement_id.clone(),
                trial.task.utilization,
                trial.task.jitter,
                trial.task.protocol,
                trial.task.k,
                trial.task.cadence,
                admission_name(trial.task.admission),
                trial.task.sessions,
            ))
            .or_default()
            .push(trial);
    }
    groups
        .into_iter()
        .map(
            |(
                (
                    slice,
                    profile,
                    placement,
                    utilization,
                    jitter,
                    protocol,
                    k,
                    cadence,
                    admission,
                    sessions,
                ),
                group,
            )| {
                let barrier = values(&group, |trial| trial.barrier_ns);
                let sender = values(&group, |trial| trial.sender_ns);
                let emissions = values(&group, |trial| trial.emissions as u64);
                let drops = values(&group, |trial| trial.drops as u64);
                let background = values(&group, |trial| trial.background_trunk_utilization_ppm);
                let stall = values(&group, |trial| trial.stall_ppm);
                SummaryRow {
                    schema_version: SCENARIO_SCHEMA_VERSION,
                    simulator_version: SIMULATOR_VERSION,
                    evidence_class: EVIDENCE_CLASS,
                    slice: slice.name(),
                    profile,
                    placement,
                    utilization_percent: utilization,
                    jitter,
                    protocol: protocol.name(),
                    source_symbols: k,
                    cadence: cadence_name(cadence),
                    admission,
                    sessions,
                    seeds: group.len(),
                    barrier_mean_ns: mean_u64(&barrier),
                    barrier_p5_ns: percentile_u64(&barrier, 5),
                    barrier_p95_ns: percentile_u64(&barrier, 95),
                    sender_mean_ns: mean_u64(&sender),
                    sender_p5_ns: percentile_u64(&sender, 5),
                    sender_p95_ns: percentile_u64(&sender, 95),
                    emissions_mean: mean_u64(&emissions),
                    emissions_p5: percentile_u64(&emissions, 5),
                    emissions_p95: percentile_u64(&emissions, 95),
                    application_drops_mean: mean_u64(&drops),
                    background_trunk_utilization_mean_ppm: mean_u64(&background),
                    background_trunk_utilization_p5_ppm: percentile_u64(&background, 5),
                    background_trunk_utilization_p95_ppm: percentile_u64(&background, 95),
                    stall_consumption_p95_ppm: percentile_u64(&stall, 95),
                    link_drops_total: group.iter().map(|trial| trial.link_drops).sum(),
                }
            },
        )
        .collect()
}

type MainKey = (&'static str, String, u8, bool, u64);
type EnvelopeKey = (&'static str, String, u8, bool);
type AdvantageCell = (u64, u64, u64);
type RoundsCell = (i64, i64, u64, usize);

fn main_map(trials: &[RawTrial]) -> BTreeMap<(MainKey, WrProtocol), &RawTrial> {
    trials
        .iter()
        .filter(|trial| trial.task.slice == Slice::Main)
        .map(|trial| {
            (
                (
                    (
                        trial.profile_name,
                        trial.placement_id.clone(),
                        trial.task.utilization,
                        trial.task.jitter,
                        trial.task.seed,
                    ),
                    trial.task.protocol,
                ),
                trial,
            )
        })
        .collect()
}

fn advantages(trials: &[RawTrial]) -> Result<Vec<AdvantageRow>, WrExperimentError> {
    let map = main_map(trials);
    let mut groups: BTreeMap<EnvelopeKey, Vec<AdvantageCell>> = BTreeMap::new();
    for ((key, protocol), carousel) in &map {
        if *protocol != WrProtocol::Carousel {
            continue;
        }
        let stripe = paired(&map, key, WrProtocol::PerStripeFec)?;
        let single0 = paired(&map, key, WrProtocol::BestSingleTree0)?;
        let single1 = paired(&map, key, WrProtocol::BestSingleTree1)?;
        groups
            .entry((key.0, key.1.clone(), key.2, key.3))
            .or_default()
            .push((
                carousel.barrier_ns,
                stripe.barrier_ns,
                single0.barrier_ns.min(single1.barrier_ns),
            ));
    }
    Ok(groups
        .into_iter()
        .map(|((profile, placement, utilization, jitter), cells)| {
            let carousel = cells.iter().map(|cell| cell.0).collect::<Vec<_>>();
            let stripe = cells.iter().map(|cell| cell.1).collect::<Vec<_>>();
            let single = cells.iter().map(|cell| cell.2).collect::<Vec<_>>();
            let pooling = cells
                .iter()
                .map(|cell| difference(cell.1, cell.0))
                .collect::<Vec<_>>();
            let diversity = cells
                .iter()
                .map(|cell| difference(cell.2, cell.1))
                .collect::<Vec<_>>();
            let pooling_mean = mean_i64(&pooling);
            let diversity_mean = mean_i64(&diversity);
            let total_mean = pooling_mean.saturating_add(diversity_mean);
            AdvantageRow {
                schema_version: SCENARIO_SCHEMA_VERSION,
                simulator_version: SIMULATOR_VERSION,
                evidence_class: EVIDENCE_CLASS,
                profile,
                placement,
                utilization_percent: utilization,
                jitter,
                seeds: cells.len(),
                carousel_mean_ns: mean_u64(&carousel),
                per_stripe_mean_ns: mean_u64(&stripe),
                best_single_mean_ns: mean_u64(&single),
                pooling_advantage_mean_ns: pooling_mean,
                pooling_advantage_p5_ns: percentile_i64(&pooling, 5),
                pooling_advantage_p95_ns: percentile_i64(&pooling, 95),
                path_diversity_advantage_mean_ns: diversity_mean,
                path_diversity_advantage_p5_ns: percentile_i64(&diversity, 5),
                path_diversity_advantage_p95_ns: percentile_i64(&diversity, 95),
                total_advantage_mean_ns: total_mean,
                additive_identity_holds: cells.iter().all(|cell| {
                    difference(cell.1, cell.0).saturating_add(difference(cell.2, cell.1))
                        == difference(cell.2, cell.0)
                }),
            }
        })
        .collect())
}

fn rounds_gaps(trials: &[RawTrial]) -> Result<Vec<RoundsRow>, WrExperimentError> {
    let map = main_map(trials);
    let mut groups: BTreeMap<EnvelopeKey, Vec<RoundsCell>> = BTreeMap::new();
    for ((key, protocol), carousel) in &map {
        if *protocol != WrProtocol::Carousel {
            continue;
        }
        let rounds = paired(&map, key, WrProtocol::Rounds)?;
        groups
            .entry((key.0, key.1.clone(), key.2, key.3))
            .or_default()
            .push((
                difference(rounds.barrier_ns, carousel.barrier_ns),
                difference(rounds.sender_ns, carousel.sender_ns),
                carousel.emissions.saturating_sub(carousel.task.k) as u64,
                rounds.positive_round_deficits,
            ));
    }
    Ok(groups
        .into_iter()
        .map(|((profile, placement, utilization, jitter), cells)| {
            let barrier = cells.iter().map(|cell| cell.0).collect::<Vec<_>>();
            let sender = cells.iter().map(|cell| cell.1).collect::<Vec<_>>();
            RoundsRow {
                schema_version: SCENARIO_SCHEMA_VERSION,
                simulator_version: SIMULATOR_VERSION,
                evidence_class: EVIDENCE_CLASS,
                profile,
                placement,
                utilization_percent: utilization,
                jitter,
                seeds: cells.len(),
                receiver_barrier_rounds_minus_carousel_mean_ns: mean_i64(&barrier),
                receiver_barrier_gap_p5_ns: percentile_i64(&barrier, 5),
                receiver_barrier_gap_p95_ns: percentile_i64(&barrier, 95),
                sender_rounds_minus_carousel_mean_ns: mean_i64(&sender),
                sender_gap_p5_ns: percentile_i64(&sender, 5),
                sender_gap_p95_ns: percentile_i64(&sender, 95),
                carousel_tail_emissions_mean: mean_u64(
                    &cells.iter().map(|cell| cell.2).collect::<Vec<_>>(),
                ),
                rounds_positive_deficit_total: cells.iter().map(|cell| cell.3).sum(),
            }
        })
        .collect())
}

fn paired<'a>(
    map: &'a BTreeMap<(MainKey, WrProtocol), &'a RawTrial>,
    key: &MainKey,
    protocol: WrProtocol,
) -> Result<&'a RawTrial, WrExperimentError> {
    map.get(&(key.clone(), protocol)).copied().ok_or_else(|| {
        WrExperimentError::MissingPair(format!(
            "{} {} u{} j{} seed{} {}",
            key.0,
            key.1,
            key.2,
            key.3,
            key.4,
            protocol.name()
        ))
    })
}

fn a4_rows(trials: &[RawTrial]) -> Vec<A4Row> {
    let mut groups: BTreeMap<(&'static str, String, u8, bool), Vec<&RawTrial>> = BTreeMap::new();
    for trial in trials.iter().filter(|trial| {
        trial.task.slice == Slice::Main && trial.task.protocol == WrProtocol::Carousel
    }) {
        groups
            .entry((
                trial.profile_name,
                trial.placement_id.clone(),
                trial.task.utilization,
                trial.task.jitter,
            ))
            .or_default()
            .push(trial);
    }
    groups
        .into_iter()
        .map(|((profile, placement, utilization, jitter), group)| {
            let trace = group
                .iter()
                .map(|trial| trial.a4_trace_correlation_ppm)
                .collect::<Vec<_>>();
            let totals = group
                .iter()
                .map(|trial| (trial.tree_bytes[0], trial.tree_bytes[1]))
                .collect::<Vec<_>>();
            A4Row {
                schema_version: SCENARIO_SCHEMA_VERSION,
                simulator_version: SIMULATOR_VERSION,
                evidence_class: EVIDENCE_CLASS,
                profile,
                placement,
                utilization_percent: utilization,
                jitter,
                seeds: group.len(),
                cross_seed_tree_rate_correlation_ppm: correlation_ppm(&totals),
                trace_correlation_mean_ppm: mean_i64(&trace),
                trace_correlation_p5_ppm: percentile_i64(&trace, 5),
                trace_correlation_p95_ppm: percentile_i64(&trace, 95),
            }
        })
        .collect()
}

fn pair_rows(trials: &[RawTrial], slice: Slice, question: &'static str) -> Vec<PairRow> {
    let mut groups: BTreeMap<String, Vec<&RawTrial>> = BTreeMap::new();
    for trial in trials.iter().filter(|trial| trial.task.slice == slice) {
        let variant = match slice {
            Slice::Straggler => admission_name(trial.task.admission).to_owned(),
            Slice::Cadence => cadence_name(trial.task.cadence).to_owned(),
            Slice::Concurrent => format!("{}-session", trial.task.sessions),
            Slice::Scaling => format!("{}-{}", trial.profile_name, trial.task.protocol.name()),
            Slice::Main => unreachable!("main uses dedicated summaries"),
        };
        groups.entry(variant).or_default().push(trial);
    }
    groups
        .into_iter()
        .map(|(variant, group)| {
            let barrier = values(&group, |trial| trial.barrier_ns);
            let sender = values(&group, |trial| trial.sender_ns);
            let healthy = values(&group, |trial| {
                trial.receiver_ns[1].max(trial.receiver_ns[2])
            });
            let emissions = values(&group, |trial| trial.emissions as u64);
            let drops = values(&group, |trial| trial.drops as u64);
            let blocking = values(&group, |trial| trial.blocking_waits as u64);
            let background = values(&group, |trial| trial.background_trunk_utilization_ppm);
            let stall = values(&group, |trial| trial.stall_ppm);
            PairRow {
                schema_version: SCENARIO_SCHEMA_VERSION,
                simulator_version: SIMULATOR_VERSION,
                evidence_class: EVIDENCE_CLASS,
                question,
                variant,
                seeds: group.len(),
                barrier_mean_ns: mean_u64(&barrier),
                barrier_p5_ns: percentile_u64(&barrier, 5),
                barrier_p95_ns: percentile_u64(&barrier, 95),
                sender_mean_ns: mean_u64(&sender),
                healthy_receiver_mean_ns: mean_u64(&healthy),
                emissions_mean: mean_u64(&emissions),
                drops_mean: mean_u64(&drops),
                blocking_waits_mean: mean_u64(&blocking),
                background_trunk_utilization_mean_ppm: mean_u64(&background),
                stall_consumption_p95_ppm: percentile_u64(&stall, 95),
            }
        })
        .collect()
}

fn tree_sample_totals(records: &[Record]) -> [u64; 2] {
    [200_000, 200_004].map(|flow_id| {
        records
            .iter()
            .filter(|record| {
                record.event == "wr_tree_rate_sample"
                    && record.flow_id == flow_id
                    && record.time_ns >= crate::scenario::WR_FOREGROUND_START_NS
            })
            .map(|record| record.bytes as u64)
            .sum()
    })
}

fn trace_correlation(records: &[Record]) -> i64 {
    let mut samples: BTreeMap<usize, [u64; 2]> = BTreeMap::new();
    for record in records.iter().filter(|record| {
        record.event == "wr_tree_rate_sample"
            && record.time_ns >= crate::scenario::WR_FOREGROUND_START_NS
    }) {
        let tree = match record.flow_id {
            200_000 => 0,
            200_004 => 1,
            _ => continue,
        };
        samples.entry(record.sequence).or_default()[tree] = record.bytes as u64;
    }
    correlation_ppm(
        &samples
            .into_values()
            .map(|value| (value[0], value[1]))
            .collect::<Vec<_>>(),
    )
}

fn correlation_ppm(values: &[(u64, u64)]) -> i64 {
    if values.len() < 2 {
        return 0;
    }
    let n = values.len() as i128;
    let sum_x: i128 = values.iter().map(|value| i128::from(value.0)).sum();
    let sum_y: i128 = values.iter().map(|value| i128::from(value.1)).sum();
    let sum_x2: i128 = values
        .iter()
        .map(|value| i128::from(value.0).saturating_mul(i128::from(value.0)))
        .sum();
    let sum_y2: i128 = values
        .iter()
        .map(|value| i128::from(value.1).saturating_mul(i128::from(value.1)))
        .sum();
    let sum_xy: i128 = values
        .iter()
        .map(|value| i128::from(value.0).saturating_mul(i128::from(value.1)))
        .sum();
    let covariance = n
        .saturating_mul(sum_xy)
        .saturating_sub(sum_x.saturating_mul(sum_y));
    let variance_x = n
        .saturating_mul(sum_x2)
        .saturating_sub(sum_x.saturating_mul(sum_x));
    let variance_y = n
        .saturating_mul(sum_y2)
        .saturating_sub(sum_y.saturating_mul(sum_y));
    if variance_x <= 0 || variance_y <= 0 {
        return 0;
    }
    let product = (variance_x as u128).saturating_mul(variance_y as u128);
    let denominator = integer_sqrt(product).max(1);
    let magnitude = covariance.unsigned_abs().saturating_mul(1_000_000) / denominator;
    let signed = i64::try_from(magnitude.min(1_000_000)).unwrap_or(1_000_000);
    if covariance < 0 { -signed } else { signed }
}

fn integer_sqrt(value: u128) -> u128 {
    if value < 2 {
        return value;
    }
    let mut low = 1_u128;
    let mut high = value.min(u128::from(u64::MAX));
    while low <= high {
        let middle = low + (high - low) / 2;
        if middle <= value / middle {
            low = middle.saturating_add(1);
        } else {
            high = middle.saturating_sub(1);
        }
    }
    high
}

fn cadence_name(cadence: u8) -> &'static str {
    match cadence {
        1 => "0.5x",
        2 => "1x",
        4 => "2x",
        _ => "invalid",
    }
}

fn admission_name(admission: ReceiverAdmissionPolicy) -> &'static str {
    match admission {
        ReceiverAdmissionPolicy::HybridDrop => "hybrid-drop",
        ReceiverAdmissionPolicy::NaiveBlocking => "naive-blocking",
        ReceiverAdmissionPolicy::IsolatedCredit => "isolated-credit",
    }
}

fn values<T>(group: &[&T], project: impl Fn(&T) -> u64) -> Vec<u64> {
    group.iter().map(|item| project(item)).collect()
}

fn mean_u64(values: &[u64]) -> u64 {
    if values.is_empty() {
        return 0;
    }
    let sum: u128 = values.iter().map(|value| u128::from(*value)).sum();
    u64::try_from(sum / values.len() as u128).unwrap_or(u64::MAX)
}

fn mean_i64(values: &[i64]) -> i64 {
    if values.is_empty() {
        return 0;
    }
    let sum: i128 = values.iter().map(|value| i128::from(*value)).sum();
    i64::try_from(sum / values.len() as i128).unwrap_or(if sum < 0 { i64::MIN } else { i64::MAX })
}

fn percentile_u64(values: &[u64], percentile: usize) -> u64 {
    let mut sorted = values.to_vec();
    sorted.sort_unstable();
    sorted
        .get(percentile_index(sorted.len(), percentile))
        .copied()
        .unwrap_or(0)
}

fn percentile_i64(values: &[i64], percentile: usize) -> i64 {
    let mut sorted = values.to_vec();
    sorted.sort_unstable();
    sorted
        .get(percentile_index(sorted.len(), percentile))
        .copied()
        .unwrap_or(0)
}

fn percentile_index(len: usize, percentile: usize) -> usize {
    if len == 0 {
        return 0;
    }
    (len.saturating_mul(percentile).saturating_add(99) / 100)
        .saturating_sub(1)
        .min(len - 1)
}

fn difference(left: u64, right: u64) -> i64 {
    i64::try_from(i128::from(left) - i128::from(right)).unwrap_or(if left < right {
        i64::MIN
    } else {
        i64::MAX
    })
}

fn to_csv<T: Serialize>(rows: &[T]) -> Result<String, csv::Error> {
    let mut writer = csv::WriterBuilder::new()
        .terminator(csv::Terminator::Any(b'\n'))
        .from_writer(Vec::new());
    for row in rows {
        writer.serialize(row)?;
    }
    writer.flush()?;
    let bytes = writer
        .into_inner()
        .map_err(|error| csv::Error::from(error.into_error()))?;
    Ok(String::from_utf8_lossy(&bytes).into_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deterministic_integer_correlation_pins_extremes() {
        assert_eq!(correlation_ppm(&[(1, 2), (2, 4), (3, 6)]), 1_000_000);
        assert_eq!(correlation_ppm(&[(1, 6), (2, 4), (3, 2)]), -1_000_000);
        assert_eq!(correlation_ppm(&[(1, 2)]), 0);
    }

    #[test]
    fn tier_one_matrix_is_the_closed_decision_subset() {
        let tasks = build_tasks(16);
        assert_eq!(tasks.len(), 224);
        let main = tasks
            .iter()
            .filter(|task| task.slice == Slice::Main)
            .collect::<Vec<_>>();
        assert_eq!(main.len(), 2 * 5 * 16);
        assert!(main.iter().all(|task| {
            task.profile == CloudProfileKind::DigitaloceanLike
                && task.placement == 1
                && [30, 70].contains(&task.utilization)
                && task.jitter
                && task.k == 8_192
        }));
        let straggler = tasks
            .iter()
            .filter(|task| task.slice == Slice::Straggler)
            .collect::<Vec<_>>();
        assert_eq!(straggler.len(), 16);
        assert!(straggler.iter().all(|task| {
            task.admission == ReceiverAdmissionPolicy::HybridDrop && task.slow_receiver == Some(0)
        }));
        assert_eq!(
            tasks
                .iter()
                .filter(|task| task.slice == Slice::Cadence)
                .count(),
            3 * 16
        );
        assert!(tasks.iter().all(|task| task.sessions == 1));
        assert!(
            tasks
                .iter()
                .all(|task| { !matches!(task.slice, Slice::Concurrent | Slice::Scaling) })
        );
    }

    #[test]
    fn percentile_uses_exact_nearest_rank_counts() {
        let values = (1..=20).collect::<Vec<_>>();
        assert_eq!(percentile_u64(&values, 5), 1);
        assert_eq!(percentile_u64(&values, 95), 19);
    }
}
