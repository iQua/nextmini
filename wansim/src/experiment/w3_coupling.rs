use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;

use serde::Serialize;
use thiserror::Error;

use crate::metrics::{MAILBOX_CAPACITY, Record};
use crate::protocol::ProtocolKind;
use crate::scenario::{
    BufferBudget, ChildOrder, ReceiverAdmissionPolicy, ReceiverServiceRate, W2ControlAsymmetry,
    W2RunError, W2Scenario, W2SharedLeafBottleneck, run_w2,
};
use crate::scenario::{W1Outcome, W1RunError, W1Scenario, run_w1};
use crate::{SCENARIO_SCHEMA_VERSION, SIMULATOR_VERSION};

const EVIDENCE_CLASS: &str = "model-level causal evidence; not a WAN measurement";
const OVERLAPS: [u8; 4] = [0, 25, 50, 100];
const RATE_WINDOW_NS: u64 = 5_000_000;
const TREE_FLOW_IDS: [usize; 2] = [30_001, 31_001];
const RATE_PROBE_FLOW_IDS: [usize; 2] = [70_000, 70_004];

#[derive(Clone, Debug)]
pub struct W3Artifacts {
    pub trials_csv: String,
    pub summaries_csv: String,
    pub advantage_decomposition_csv: String,
    pub a4_correlation_csv: String,
    pub critical_paths_csv: String,
    pub flow_counts_csv: String,
    pub isolated_credit_trials_csv: String,
    pub isolated_credit_summary_csv: String,
    pub control_asymmetry_trials_csv: String,
    pub control_asymmetry_summary_csv: String,
}

#[derive(Debug, Error)]
pub enum W3ExperimentError {
    #[error("W3 requires at least one screening seed")]
    ZeroScreeningSeeds,
    #[error("W3 decisive seed count {decisive} is below screening count {screening}")]
    DecisiveBelowScreening { screening: u64, decisive: u64 },
    #[error("W3 worker count must be nonzero")]
    ZeroWorkers,
    #[error("W3 worker failed: {0}")]
    Worker(String),
    #[error("W3 task {0} did not produce a result")]
    MissingTask(usize),
    #[error("W3 sender did not complete in {0}")]
    SenderIncomplete(String),
    #[error("W3 mailbox plumbing bound reached in {scenario}: {high_water}/{capacity}")]
    BindingMailbox {
        scenario: String,
        high_water: usize,
        capacity: usize,
    },
    #[error("W3 pairing is missing: {0}")]
    MissingPair(String),
    #[error(transparent)]
    Run(#[from] W1RunError),
    #[error(transparent)]
    W2Run(#[from] W2RunError),
    #[error(transparent)]
    Csv(#[from] csv::Error),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum TaskProtocol {
    Carousel,
    PerStripeFec,
    BestTree0,
    BestTree1,
    Rounds,
}

impl TaskProtocol {
    const MAIN: [Self; 4] = [
        Self::Carousel,
        Self::PerStripeFec,
        Self::BestTree0,
        Self::BestTree1,
    ];

    const fn name(self) -> &'static str {
        match self {
            Self::Carousel => "carousel",
            Self::PerStripeFec => "per_stripe_fec",
            Self::BestTree0 => "best_tree0_candidate",
            Self::BestTree1 => "best_tree1_candidate",
            Self::Rounds => "pooled_rounds",
        }
    }

    const fn scenario(self) -> (ProtocolKind, Option<usize>) {
        match self {
            Self::Carousel => (ProtocolKind::PooledCarousel, None),
            Self::PerStripeFec => (ProtocolKind::PerStripeFec, None),
            Self::BestTree0 => (ProtocolKind::PerStripeFec, Some(0)),
            Self::BestTree1 => (ProtocolKind::PerStripeFec, Some(1)),
            Self::Rounds => (ProtocolKind::PooledRounds, None),
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct Task {
    overlap_percent: u8,
    protocol: TaskProtocol,
    seed: u64,
    decisive: bool,
}

#[derive(Clone, Debug)]
struct CriticalAttribution {
    receiver: usize,
    final_tree: usize,
    final_frame_id: usize,
    source_generation_ns: u64,
    source_to_runtime_ns: u64,
    runtime_wait_ns: u64,
    decoder_queue_ns: u64,
    decoder_service_ns: u64,
}

#[derive(Clone, Debug)]
struct RawTrial {
    task: Task,
    barrier_completion_ns: u64,
    sender_completion_ns: u64,
    total_emissions: usize,
    useful_emissions: usize,
    flow_count_match_emissions: usize,
    tree_emissions: [usize; 2],
    post_barrier_tail_emissions: usize,
    positive_round_deficits: usize,
    round_deficit_sum: usize,
    maximum_round_deficit: usize,
    application_drops: usize,
    link_drops: usize,
    background_delivered_bytes: usize,
    tree_path_bytes: [usize; 2],
    rate_probe_path_bytes: [usize; 2],
    tree_utilization_permille: [u64; 2],
    delivered_rate_correlation_ppm: i64,
    maximum_mailbox_high_water: usize,
    critical_paths: Vec<CriticalAttribution>,
}

#[derive(Clone, Debug, Serialize)]
struct TrialRow {
    schema_version: u32,
    simulator_version: &'static str,
    evidence_class: &'static str,
    sample_class: &'static str,
    seed: u64,
    overlap_percent: u8,
    protocol: &'static str,
    source_symbols: usize,
    barrier_completion_ns: u64,
    sender_completion_ns: u64,
    total_emissions: usize,
    useful_emissions: usize,
    flow_count_match_emissions: usize,
    tree0_emissions: usize,
    tree1_emissions: usize,
    post_barrier_tail_emissions: usize,
    positive_round_deficits: usize,
    round_deficit_sum: usize,
    maximum_round_deficit: usize,
    application_drops: usize,
    link_drops: usize,
    background_delivered_bytes: usize,
    tree0_path_bytes: usize,
    tree1_path_bytes: usize,
    tree0_utilization_permille: u64,
    tree1_utilization_permille: u64,
    delivered_rate_correlation_ppm: i64,
    maximum_mailbox_high_water: usize,
}

#[derive(Clone, Debug, Serialize)]
struct SummaryRow {
    schema_version: u32,
    simulator_version: &'static str,
    evidence_class: &'static str,
    sample_class: &'static str,
    overlap_percent: u8,
    protocol: &'static str,
    seeds: usize,
    barrier_mean_ns: u64,
    barrier_p95_ns: u64,
    sender_completion_mean_ns: u64,
    feedback_completion_lag_mean_ns: u64,
    total_emissions_mean: usize,
    useful_emissions_mean: usize,
    positive_round_deficits_sum: usize,
    round_deficit_sum: usize,
    maximum_round_deficit: usize,
    application_drops_sum: usize,
    link_drops_sum: usize,
    background_delivered_bytes_mean: usize,
    tree0_utilization_mean_permille: u64,
    tree1_utilization_mean_permille: u64,
    delivered_rate_correlation_mean_ppm: i64,
    delivered_rate_correlation_abs_mean_ppm: u64,
    maximum_mailbox_high_water: usize,
}

#[derive(Clone, Debug, Serialize)]
struct AdvantageRow {
    schema_version: u32,
    simulator_version: &'static str,
    evidence_class: &'static str,
    overlap_percent: u8,
    seeds: usize,
    carousel_barrier_mean_ns: u64,
    per_stripe_barrier_mean_ns: u64,
    best_single_barrier_mean_ns: u64,
    pooling_advantage_ns: i128,
    pooling_advantage_ppm: i128,
    path_diversity_advantage_ns: i128,
    path_diversity_advantage_ppm: i128,
    total_two_mechanism_advantage_ns: i128,
    additive_identity_holds: bool,
}

#[derive(Clone, Debug, Serialize)]
struct A4Row {
    schema_version: u32,
    simulator_version: &'static str,
    evidence_class: &'static str,
    overlap_percent: u8,
    seeds: usize,
    cross_seed_delivered_rate_correlation_ppm: i64,
    within_trace_delta_correlation_mean_ppm: i64,
    within_trace_delta_correlation_abs_mean_ppm: u64,
    interpretation: &'static str,
}

#[derive(Clone, Debug, Serialize)]
struct CriticalPathRow {
    schema_version: u32,
    simulator_version: &'static str,
    evidence_class: &'static str,
    sample_class: &'static str,
    overlap_percent: u8,
    protocol: &'static str,
    seed: u64,
    receiver: usize,
    is_barrier_receiver: bool,
    final_tree: usize,
    final_frame_id: usize,
    source_generation_ns: u64,
    source_to_runtime_ns: u64,
    runtime_wait_ns: u64,
    decoder_queue_ns: u64,
    decoder_service_ns: u64,
    total_ns: u64,
}

#[derive(Clone, Debug, Serialize)]
struct FlowCountRow {
    schema_version: u32,
    simulator_version: &'static str,
    evidence_class: &'static str,
    overlap_percent: u8,
    serial_bottleneck_stages: usize,
    shared_stages: usize,
    private_stages: usize,
    aggregate_capacity_per_stage_bps: u64,
    aggregate_capacity_all_serial_stages_bps: u64,
    foreground_tcp_connections: usize,
    explicit_background_tcp_flows: usize,
    directional_active_flow_count: usize,
    flows_per_shared_server: usize,
    flows_per_private_lane_server: usize,
    best_single_uses_existing_second_connection: bool,
}

#[derive(Clone, Copy, Debug)]
struct IsolatedTask {
    policy: ReceiverAdmissionPolicy,
    seed: u64,
}

#[derive(Clone, Debug, Serialize)]
struct IsolatedRow {
    schema_version: u32,
    simulator_version: &'static str,
    evidence_class: &'static str,
    seed: u64,
    admission_policy: &'static str,
    overlap_percent: u8,
    source_symbols: usize,
    barrier_completion_ns: u64,
    healthy_completion_max_ns: u64,
    total_emissions: usize,
    slow_branch_deliveries_through_completion: usize,
    slow_application_drop_deficits: usize,
    application_drops_total: usize,
    isolated_credit_deferrals: usize,
    isolated_credit_replays: usize,
    isolated_credit_outstanding_frames: usize,
    shared_leaf_wire_bytes: usize,
    liveness_pressure_permille: u64,
}

#[derive(Clone, Debug, Serialize)]
struct IsolatedSummaryRow {
    schema_version: u32,
    simulator_version: &'static str,
    evidence_class: &'static str,
    seeds: usize,
    overlap_percent: u8,
    hybrid_barrier_mean_ns: u64,
    isolated_barrier_mean_ns: u64,
    barrier_saved_by_isolated_ns: i128,
    hybrid_healthy_completion_mean_ns: u64,
    isolated_healthy_completion_mean_ns: u64,
    healthy_time_saved_by_isolated_ns: i128,
    hybrid_total_emissions_mean: usize,
    isolated_total_emissions_mean: usize,
    source_emissions_saved_by_isolated: i128,
    hybrid_slow_branch_deliveries_mean: usize,
    isolated_slow_branch_deliveries_mean: usize,
    slow_branch_deliveries_saved: i128,
    hybrid_shared_leaf_wire_bytes_mean: usize,
    isolated_shared_leaf_wire_bytes_mean: usize,
    shared_leaf_wire_bytes_saved: i128,
    reconsideration_condition_2_met: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum ControlCondition {
    IncastOnly,
    ReverseBursts,
}

impl ControlCondition {
    const ALL: [Self; 2] = [Self::IncastOnly, Self::ReverseBursts];

    const fn name(self) -> &'static str {
        match self {
            Self::IncastOnly => "ack_incast",
            Self::ReverseBursts => "ack_incast_plus_reverse_bursts",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum CadenceFactor {
    Half,
    One,
    Two,
}

impl CadenceFactor {
    const ALL: [Self; 3] = [Self::Half, Self::One, Self::Two];

    const fn name(self) -> &'static str {
        match self {
            Self::Half => "0.5x",
            Self::One => "1x",
            Self::Two => "2x",
        }
    }

    const fn scale(self, value: u64) -> u64 {
        match self {
            Self::Half => value / 2,
            Self::One => value,
            Self::Two => value.saturating_mul(2),
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct ControlTask {
    condition: ControlCondition,
    cadence: CadenceFactor,
    seed: u64,
    decisive: bool,
}

#[derive(Clone, Debug, Serialize)]
struct ControlRow {
    schema_version: u32,
    simulator_version: &'static str,
    evidence_class: &'static str,
    sample_class: &'static str,
    seed: u64,
    condition: &'static str,
    cadence_factor: &'static str,
    receiver_count: usize,
    source_symbols: usize,
    ack_debounce_ns: u64,
    ack_heartbeat_ns: u64,
    reverse_rate_bps: u64,
    reverse_propagation_ns: u64,
    barrier_completion_ns: u64,
    sender_completion_ns: u64,
    feedback_completion_lag_ns: u64,
    total_emissions: usize,
    post_completion_tail_emissions: usize,
    block_acks_received: usize,
    ack_probes: usize,
    reverse_path_wire_bytes: usize,
    reverse_background_delivered_bytes: usize,
    application_drops: usize,
    link_drops: usize,
    liveness_pressure_permille: u64,
    liveness_margin_permille: u64,
}

#[derive(Clone, Debug, Serialize)]
struct ControlSummaryRow {
    schema_version: u32,
    simulator_version: &'static str,
    evidence_class: &'static str,
    sample_class: &'static str,
    condition: &'static str,
    cadence_factor: &'static str,
    seeds: usize,
    barrier_mean_ns: u64,
    barrier_p95_ns: u64,
    sender_completion_mean_ns: u64,
    feedback_completion_lag_mean_ns: u64,
    total_emissions_mean: usize,
    post_completion_tail_mean: usize,
    ack_probes_sum: usize,
    reverse_path_wire_bytes_mean: usize,
    reverse_background_delivered_bytes_mean: usize,
    application_drops_sum: usize,
    link_drops_sum: usize,
    liveness_pressure_max_permille: u64,
    liveness_margin_min_permille: u64,
}

pub fn run_w3_experiment(
    screening_seeds: u64,
    decisive_seeds: u64,
    workers: usize,
) -> Result<W3Artifacts, W3ExperimentError> {
    if screening_seeds == 0 {
        return Err(W3ExperimentError::ZeroScreeningSeeds);
    }
    if decisive_seeds < screening_seeds {
        return Err(W3ExperimentError::DecisiveBelowScreening {
            screening: screening_seeds,
            decisive: decisive_seeds,
        });
    }
    if workers == 0 {
        return Err(W3ExperimentError::ZeroWorkers);
    }
    let tasks = build_tasks(screening_seeds, decisive_seeds);
    let raw = execute_tasks(&tasks, workers)?;
    let rows = trial_rows(&raw);
    let effective = effective_protocol_rows(&raw)?;
    let summaries = summarize(&effective);
    let advantages = advantage_rows(&effective)?;
    let correlations = a4_rows(&effective);
    let critical_paths = critical_path_rows(&raw);
    let flow_counts = flow_count_rows();
    let isolated = run_isolated_credit_slice(decisive_seeds, workers)?;
    let control = run_control_asymmetry_slice(screening_seeds, decisive_seeds, workers)?;
    Ok(W3Artifacts {
        trials_csv: to_csv(&rows)?,
        summaries_csv: to_csv(&summaries)?,
        advantage_decomposition_csv: to_csv(&advantages)?,
        a4_correlation_csv: to_csv(&correlations)?,
        critical_paths_csv: to_csv(&critical_paths)?,
        flow_counts_csv: to_csv(&flow_counts)?,
        isolated_credit_trials_csv: to_csv(&isolated.0)?,
        isolated_credit_summary_csv: to_csv(&isolated.1)?,
        control_asymmetry_trials_csv: to_csv(&control.0)?,
        control_asymmetry_summary_csv: to_csv(&control.1)?,
    })
}

fn build_tasks(screening_seeds: u64, decisive_seeds: u64) -> Vec<Task> {
    let mut tasks = Vec::new();
    for overlap_percent in OVERLAPS {
        let decisive = matches!(overlap_percent, 0 | 100);
        let seeds = if decisive {
            decisive_seeds
        } else {
            screening_seeds
        };
        for protocol in TaskProtocol::MAIN {
            for seed in 0..seeds {
                tasks.push(Task {
                    overlap_percent,
                    protocol,
                    seed,
                    decisive,
                });
            }
        }
    }
    for overlap_percent in [0, 100] {
        for seed in 0..screening_seeds {
            tasks.push(Task {
                overlap_percent,
                protocol: TaskProtocol::Rounds,
                seed,
                decisive: false,
            });
        }
    }
    tasks
}

fn execute_tasks(tasks: &[Task], workers: usize) -> Result<Vec<RawTrial>, W3ExperimentError> {
    let tasks = Arc::new(tasks.to_vec());
    let next = Arc::new(AtomicUsize::new(0));
    let results = Arc::new(Mutex::new(vec![None; tasks.len()]));
    let failure = Arc::new(Mutex::new(None));
    thread::scope(|scope| {
        for _ in 0..workers {
            let tasks = Arc::clone(&tasks);
            let next = Arc::clone(&next);
            let results = Arc::clone(&results);
            let failure = Arc::clone(&failure);
            scope.spawn(move || {
                loop {
                    if failure.lock().expect("W3 failure mutex").is_some() {
                        break;
                    }
                    let index = next.fetch_add(1, Ordering::Relaxed);
                    let Some(task) = tasks.get(index).copied() else {
                        break;
                    };
                    match execute_task(task) {
                        Ok(trial) => results.lock().expect("W3 result mutex")[index] = Some(trial),
                        Err(error) => {
                            *failure.lock().expect("W3 failure mutex") = Some(error.to_string());
                            break;
                        }
                    }
                }
            });
        }
    });
    if let Some(error) = failure.lock().expect("W3 failure mutex").take() {
        return Err(W3ExperimentError::Worker(error));
    }
    let mut locked = results.lock().expect("W3 result mutex");
    locked
        .iter_mut()
        .enumerate()
        .map(|(index, trial)| trial.take().ok_or(W3ExperimentError::MissingTask(index)))
        .collect()
}

fn execute_task(task: Task) -> Result<RawTrial, W3ExperimentError> {
    let (protocol, best_tree) = task.protocol.scenario();
    let scenario = W1Scenario::w3_coupling(protocol, task.overlap_percent, best_tree, task.seed);
    let outcome = run_w1(&scenario)?;
    let sender_completion_ns = outcome
        .sender_completion_ns
        .ok_or_else(|| W3ExperimentError::SenderIncomplete(scenario.scenario_id.clone()))?;
    let maximum_mailbox_high_water = outcome
        .mailbox_high_water
        .values()
        .copied()
        .max()
        .unwrap_or(0);
    if maximum_mailbox_high_water >= MAILBOX_CAPACITY {
        return Err(W3ExperimentError::BindingMailbox {
            scenario: scenario.scenario_id,
            high_water: maximum_mailbox_high_water,
            capacity: MAILBOX_CAPACITY,
        });
    }
    Ok(raw_trial(
        task,
        &scenario,
        &outcome,
        sender_completion_ns,
        maximum_mailbox_high_water,
    ))
}

fn raw_trial(
    task: Task,
    scenario: &W1Scenario,
    outcome: &W1Outcome,
    sender_completion_ns: u64,
    maximum_mailbox_high_water: usize,
) -> RawTrial {
    let match_emissions = count_event(&outcome.records, "flow_count_match_frame_emitted");
    let background_delivered_bytes = outcome
        .records
        .iter()
        .filter(|record| record.event == "background_bytes_delivered")
        .map(|record| record.bytes)
        .sum();
    let tree_path_bytes = TREE_FLOW_IDS.map(|flow_id| {
        outcome
            .records
            .iter()
            .filter(|record| {
                record.component == "w3_coupled_data_forward"
                    && record.event == "coupled_path_exit"
                    && record.flow_id == flow_id
            })
            .map(|record| record.bytes)
            .sum()
    });
    let rate_probe_path_bytes = RATE_PROBE_FLOW_IDS.map(|flow_id| {
        outcome
            .records
            .iter()
            .filter(|record| {
                record.component == "w3_coupled_data_forward"
                    && record.event == "coupled_path_exit"
                    && record.flow_id == flow_id
            })
            .map(|record| record.bytes)
            .sum()
    });
    let tree_utilization_permille = tree_path_bytes.map(|bytes| {
        u64::try_from(
            (bytes as u128).saturating_mul(8_000_000_000_000)
                / u128::from(80_000_000_u64)
                / u128::from(outcome.barrier_completion_ns.max(1)),
        )
        .unwrap_or(u64::MAX)
    });
    RawTrial {
        task,
        barrier_completion_ns: outcome.barrier_completion_ns,
        sender_completion_ns,
        total_emissions: outcome.total_emissions,
        useful_emissions: outcome.total_emissions.saturating_sub(match_emissions),
        flow_count_match_emissions: match_emissions,
        tree_emissions: outcome.per_tree_emissions,
        post_barrier_tail_emissions: outcome.post_barrier_tail_emissions,
        positive_round_deficits: outcome.positive_round_deficits,
        round_deficit_sum: outcome.round_deficit_sum,
        maximum_round_deficit: outcome.maximum_round_deficit,
        application_drops: outcome.application_drops,
        link_drops: outcome.link_drops,
        background_delivered_bytes,
        tree_path_bytes,
        rate_probe_path_bytes,
        tree_utilization_permille,
        delivered_rate_correlation_ppm: rate_correlation_ppm(
            &outcome.records,
            outcome.barrier_completion_ns,
        ),
        maximum_mailbox_high_water,
        critical_paths: critical_attributions(scenario, outcome),
    }
}

fn critical_attributions(scenario: &W1Scenario, outcome: &W1Outcome) -> Vec<CriticalAttribution> {
    (0..scenario.active_receivers)
        .filter_map(|receiver| {
            let component = match receiver {
                0 => "w1_receiver1",
                1 => "w1_receiver2",
                2 => "w1_receiver3",
                _ => return None,
            };
            let completion = outcome.records.iter().find(|record| {
                record.component == component && record.event == "protocol_local_complete"
            })?;
            let runtime = outcome.records.iter().find(|record| {
                record.component == component
                    && record.event == "runtime_command_enqueue_data"
                    && record.flow_id == completion.flow_id
                    && record.sequence == completion.sequence
            });
            let inbox = outcome.records.iter().find(|record| {
                record.component == component
                    && record.event == "data_inbox_enqueue"
                    && record.flow_id == completion.flow_id
                    && record.sequence == completion.sequence
            });
            let source = outcome.records.iter().find(|record| {
                record.component == "w1_source"
                    && record.event == "data_frame_emitted"
                    && record.flow_id == TREE_FLOW_IDS[completion.value]
                    && record.value == completion.sequence
            });
            let runtime_ns = runtime.map_or(completion.time_ns, |record| record.time_ns);
            let inbox_ns = inbox.map_or(runtime_ns, |record| record.time_ns);
            let source_ns = source.map_or(0, |record| record.time_ns);
            Some(CriticalAttribution {
                receiver,
                final_tree: completion.value,
                final_frame_id: completion.sequence,
                source_generation_ns: source_ns,
                source_to_runtime_ns: runtime_ns.saturating_sub(source_ns),
                runtime_wait_ns: inbox_ns.saturating_sub(runtime_ns),
                decoder_queue_ns: completion
                    .time_ns
                    .saturating_sub(inbox_ns)
                    .saturating_sub(scenario.decoder_sink_service_ns),
                decoder_service_ns: scenario.decoder_sink_service_ns,
            })
        })
        .collect()
}

fn rate_correlation_ppm(records: &[Record], end_ns: u64) -> i64 {
    let window_count = usize::try_from(end_ns.div_ceil(RATE_WINDOW_NS)).unwrap_or(usize::MAX);
    if !(2..=1_000_000).contains(&window_count) {
        return 0;
    }
    let mut windows = vec![[0_u64; 2]; window_count];
    for record in records {
        if record.component != "w3_coupled_data_forward"
            || record.event != "coupled_path_exit"
            || record.time_ns > end_ns
        {
            continue;
        }
        let Some(tree) = RATE_PROBE_FLOW_IDS
            .iter()
            .position(|flow| *flow == record.flow_id)
        else {
            continue;
        };
        let window = usize::try_from(record.time_ns / RATE_WINDOW_NS)
            .unwrap_or(usize::MAX)
            .min(window_count - 1);
        windows[window][tree] = windows[window][tree].saturating_add(record.bytes as u64);
    }
    // First differences remove the common TCP startup ramp and source clock. What remains is
    // per-tree service-rate co-movement caused by background bursts and shared resources.
    let samples = windows
        .windows(2)
        .skip(3)
        .map(|pair| {
            [
                i128::from(pair[1][0]) - i128::from(pair[0][0]),
                i128::from(pair[1][1]) - i128::from(pair[0][1]),
            ]
        })
        .collect::<Vec<_>>();
    if samples.len() < 2 {
        return 0;
    }
    let n = samples.len() as i128;
    let sx: i128 = samples.iter().map(|sample| sample[0]).sum();
    let sy: i128 = samples.iter().map(|sample| sample[1]).sum();
    let sxx: i128 = samples.iter().map(|sample| sample[0] * sample[0]).sum();
    let syy: i128 = samples.iter().map(|sample| sample[1] * sample[1]).sum();
    let sxy: i128 = samples.iter().map(|sample| sample[0] * sample[1]).sum();
    let covariance = n.saturating_mul(sxy).saturating_sub(sx.saturating_mul(sy));
    let variance_x = n.saturating_mul(sxx).saturating_sub(sx.saturating_mul(sx));
    let variance_y = n.saturating_mul(syy).saturating_sub(sy.saturating_mul(sy));
    if variance_x <= 0 || variance_y <= 0 {
        return 0;
    }
    let denominator = integer_sqrt(
        u128::try_from(variance_x)
            .unwrap_or(u128::MAX)
            .saturating_mul(u128::try_from(variance_y).unwrap_or(u128::MAX)),
    );
    if denominator == 0 {
        return 0;
    }
    i64::try_from(covariance.saturating_mul(1_000_000) / denominator as i128).unwrap_or_else(|_| {
        if covariance.is_negative() {
            i64::MIN
        } else {
            i64::MAX
        }
    })
}

fn integer_sqrt(value: u128) -> u128 {
    if value < 2 {
        return value;
    }
    let mut low = 1_u128;
    let mut high = value.min(u128::from(u64::MAX));
    while low <= high {
        let mid = low + (high - low) / 2;
        if mid <= value / mid {
            low = mid.saturating_add(1);
        } else {
            high = mid.saturating_sub(1);
        }
    }
    high
}

fn trial_rows(raw: &[RawTrial]) -> Vec<TrialRow> {
    raw.iter()
        .map(|trial| TrialRow {
            schema_version: SCENARIO_SCHEMA_VERSION,
            simulator_version: SIMULATOR_VERSION,
            evidence_class: EVIDENCE_CLASS,
            sample_class: task_sample_class(trial.task),
            seed: trial.task.seed,
            overlap_percent: trial.task.overlap_percent,
            protocol: trial.task.protocol.name(),
            source_symbols: 512,
            barrier_completion_ns: trial.barrier_completion_ns,
            sender_completion_ns: trial.sender_completion_ns,
            total_emissions: trial.total_emissions,
            useful_emissions: trial.useful_emissions,
            flow_count_match_emissions: trial.flow_count_match_emissions,
            tree0_emissions: trial.tree_emissions[0],
            tree1_emissions: trial.tree_emissions[1],
            post_barrier_tail_emissions: trial.post_barrier_tail_emissions,
            positive_round_deficits: trial.positive_round_deficits,
            round_deficit_sum: trial.round_deficit_sum,
            maximum_round_deficit: trial.maximum_round_deficit,
            application_drops: trial.application_drops,
            link_drops: trial.link_drops,
            background_delivered_bytes: trial.background_delivered_bytes,
            tree0_path_bytes: trial.tree_path_bytes[0],
            tree1_path_bytes: trial.tree_path_bytes[1],
            tree0_utilization_permille: trial.tree_utilization_permille[0],
            tree1_utilization_permille: trial.tree_utilization_permille[1],
            delivered_rate_correlation_ppm: trial.delivered_rate_correlation_ppm,
            maximum_mailbox_high_water: trial.maximum_mailbox_high_water,
        })
        .collect()
}

#[derive(Clone, Debug)]
struct EffectiveTrial {
    raw: RawTrial,
    protocol: &'static str,
}

fn effective_protocol_rows(raw: &[RawTrial]) -> Result<Vec<EffectiveTrial>, W3ExperimentError> {
    let mut effective = raw
        .iter()
        .filter(|trial| {
            !matches!(
                trial.task.protocol,
                TaskProtocol::BestTree0 | TaskProtocol::BestTree1
            )
        })
        .cloned()
        .map(|raw| EffectiveTrial {
            protocol: raw.task.protocol.name(),
            raw,
        })
        .collect::<Vec<_>>();
    let mut candidates: BTreeMap<(u8, u64), Vec<&RawTrial>> = BTreeMap::new();
    for trial in raw.iter().filter(|trial| {
        matches!(
            trial.task.protocol,
            TaskProtocol::BestTree0 | TaskProtocol::BestTree1
        )
    }) {
        candidates
            .entry((trial.task.overlap_percent, trial.task.seed))
            .or_default()
            .push(trial);
    }
    for ((overlap, seed), candidates) in candidates {
        if candidates.len() != 2 {
            return Err(W3ExperimentError::MissingPair(format!(
                "best-single candidates overlap={overlap} seed={seed}"
            )));
        }
        let best = candidates
            .into_iter()
            .min_by_key(|trial| (trial.barrier_completion_ns, trial.task.protocol))
            .expect("two candidates");
        effective.push(EffectiveTrial {
            raw: best.clone(),
            protocol: "best_single_tree",
        });
    }
    effective.sort_by_key(|trial| {
        (
            trial.raw.task.overlap_percent,
            trial.protocol,
            trial.raw.task.seed,
        )
    });
    Ok(effective)
}

fn summarize(rows: &[EffectiveTrial]) -> Vec<SummaryRow> {
    let mut groups: BTreeMap<(u8, &'static str), Vec<&EffectiveTrial>> = BTreeMap::new();
    for row in rows {
        groups
            .entry((row.raw.task.overlap_percent, row.protocol))
            .or_default()
            .push(row);
    }
    groups
        .into_iter()
        .map(|((overlap, protocol), group)| {
            let barriers = group
                .iter()
                .map(|trial| trial.raw.barrier_completion_ns)
                .collect::<Vec<_>>();
            let correlations = group
                .iter()
                .map(|trial| trial.raw.delivered_rate_correlation_ppm)
                .collect::<Vec<_>>();
            SummaryRow {
                schema_version: SCENARIO_SCHEMA_VERSION,
                simulator_version: SIMULATOR_VERSION,
                evidence_class: EVIDENCE_CLASS,
                sample_class: if protocol == "pooled_rounds" {
                    "reduced_rounds_slice"
                } else if matches!(overlap, 0 | 100) {
                    "decisive"
                } else {
                    "screening"
                },
                overlap_percent: overlap,
                protocol,
                seeds: group.len(),
                barrier_mean_ns: mean_u64(&barriers),
                barrier_p95_ns: percentile95(&barriers),
                sender_completion_mean_ns: mean_u64(
                    &group
                        .iter()
                        .map(|trial| trial.raw.sender_completion_ns)
                        .collect::<Vec<_>>(),
                ),
                feedback_completion_lag_mean_ns: mean_u64(
                    &group
                        .iter()
                        .map(|trial| {
                            trial
                                .raw
                                .sender_completion_ns
                                .saturating_sub(trial.raw.barrier_completion_ns)
                        })
                        .collect::<Vec<_>>(),
                ),
                total_emissions_mean: mean_usize(
                    &group
                        .iter()
                        .map(|trial| trial.raw.total_emissions)
                        .collect::<Vec<_>>(),
                ),
                useful_emissions_mean: mean_usize(
                    &group
                        .iter()
                        .map(|trial| trial.raw.useful_emissions)
                        .collect::<Vec<_>>(),
                ),
                positive_round_deficits_sum: group
                    .iter()
                    .map(|trial| trial.raw.positive_round_deficits)
                    .sum(),
                round_deficit_sum: group.iter().map(|trial| trial.raw.round_deficit_sum).sum(),
                maximum_round_deficit: group
                    .iter()
                    .map(|trial| trial.raw.maximum_round_deficit)
                    .max()
                    .unwrap_or(0),
                application_drops_sum: group.iter().map(|trial| trial.raw.application_drops).sum(),
                link_drops_sum: group.iter().map(|trial| trial.raw.link_drops).sum(),
                background_delivered_bytes_mean: mean_usize(
                    &group
                        .iter()
                        .map(|trial| trial.raw.background_delivered_bytes)
                        .collect::<Vec<_>>(),
                ),
                tree0_utilization_mean_permille: mean_u64(
                    &group
                        .iter()
                        .map(|trial| trial.raw.tree_utilization_permille[0])
                        .collect::<Vec<_>>(),
                ),
                tree1_utilization_mean_permille: mean_u64(
                    &group
                        .iter()
                        .map(|trial| trial.raw.tree_utilization_permille[1])
                        .collect::<Vec<_>>(),
                ),
                delivered_rate_correlation_mean_ppm: mean_i64(&correlations),
                delivered_rate_correlation_abs_mean_ppm: mean_u64(
                    &correlations
                        .iter()
                        .map(|value| value.unsigned_abs())
                        .collect::<Vec<_>>(),
                ),
                maximum_mailbox_high_water: group
                    .iter()
                    .map(|trial| trial.raw.maximum_mailbox_high_water)
                    .max()
                    .unwrap_or(0),
            }
        })
        .collect()
}

fn advantage_rows(rows: &[EffectiveTrial]) -> Result<Vec<AdvantageRow>, W3ExperimentError> {
    let mut result = Vec::new();
    for overlap in OVERLAPS {
        let carousel = protocol_barriers(rows, overlap, "carousel");
        let stripe = protocol_barriers(rows, overlap, "per_stripe_fec");
        let single = protocol_barriers(rows, overlap, "best_single_tree");
        if carousel.len() != stripe.len() || stripe.len() != single.len() || carousel.is_empty() {
            return Err(W3ExperimentError::MissingPair(format!(
                "advantage decomposition overlap={overlap}"
            )));
        }
        let carousel_mean = mean_u64(&carousel);
        let stripe_mean = mean_u64(&stripe);
        let single_mean = mean_u64(&single);
        let pooling = i128::from(stripe_mean) - i128::from(carousel_mean);
        let diversity = i128::from(single_mean) - i128::from(stripe_mean);
        let total = i128::from(single_mean) - i128::from(carousel_mean);
        result.push(AdvantageRow {
            schema_version: SCENARIO_SCHEMA_VERSION,
            simulator_version: SIMULATOR_VERSION,
            evidence_class: EVIDENCE_CLASS,
            overlap_percent: overlap,
            seeds: carousel.len(),
            carousel_barrier_mean_ns: carousel_mean,
            per_stripe_barrier_mean_ns: stripe_mean,
            best_single_barrier_mean_ns: single_mean,
            pooling_advantage_ns: pooling,
            pooling_advantage_ppm: ratio_ppm(pooling, stripe_mean),
            path_diversity_advantage_ns: diversity,
            path_diversity_advantage_ppm: ratio_ppm(diversity, single_mean),
            total_two_mechanism_advantage_ns: total,
            additive_identity_holds: pooling + diversity == total,
        });
    }
    Ok(result)
}

fn protocol_barriers(rows: &[EffectiveTrial], overlap: u8, protocol: &str) -> Vec<u64> {
    rows.iter()
        .filter(|trial| trial.raw.task.overlap_percent == overlap && trial.protocol == protocol)
        .map(|trial| trial.raw.barrier_completion_ns)
        .collect()
}

fn a4_rows(rows: &[EffectiveTrial]) -> Vec<A4Row> {
    OVERLAPS
        .into_iter()
        .map(|overlap| {
            let correlations = rows
                .iter()
                .filter(|trial| {
                    trial.raw.task.overlap_percent == overlap && trial.protocol == "carousel"
                })
                .map(|trial| trial.raw.delivered_rate_correlation_ppm)
                .collect::<Vec<_>>();
            let absolute = correlations
                .iter()
                .map(|value| value.unsigned_abs())
                .collect::<Vec<_>>();
            let probe_pairs = rows
                .iter()
                .filter(|trial| {
                    trial.raw.task.overlap_percent == overlap && trial.protocol == "carousel"
                })
                .map(|trial| {
                    [
                        trial.raw.rate_probe_path_bytes[0] as i128,
                        trial.raw.rate_probe_path_bytes[1] as i128,
                    ]
                })
                .collect::<Vec<_>>();
            A4Row {
                schema_version: SCENARIO_SCHEMA_VERSION,
                simulator_version: SIMULATOR_VERSION,
                evidence_class: EVIDENCE_CLASS,
                overlap_percent: overlap,
                seeds: correlations.len(),
                cross_seed_delivered_rate_correlation_ppm: sample_correlation_ppm(&probe_pairs),
                within_trace_delta_correlation_mean_ppm: mean_i64(&correlations),
                within_trace_delta_correlation_abs_mean_ppm: mean_u64(&absolute),
                interpretation: if overlap == 0 {
                    "edge-disjoint reference"
                } else {
                    "endogenous delivered-rate coupling; A4 violated"
                },
            }
        })
        .collect()
}

fn sample_correlation_ppm(samples: &[[i128; 2]]) -> i64 {
    if samples.len() < 2 {
        return 0;
    }
    let n = samples.len() as i128;
    let sx: i128 = samples.iter().map(|sample| sample[0]).sum();
    let sy: i128 = samples.iter().map(|sample| sample[1]).sum();
    let sxx: i128 = samples.iter().map(|sample| sample[0] * sample[0]).sum();
    let syy: i128 = samples.iter().map(|sample| sample[1] * sample[1]).sum();
    let sxy: i128 = samples.iter().map(|sample| sample[0] * sample[1]).sum();
    let covariance = n.saturating_mul(sxy).saturating_sub(sx.saturating_mul(sy));
    let variance_x = n.saturating_mul(sxx).saturating_sub(sx.saturating_mul(sx));
    let variance_y = n.saturating_mul(syy).saturating_sub(sy.saturating_mul(sy));
    if variance_x <= 0 || variance_y <= 0 {
        return 0;
    }
    let denominator = integer_sqrt(
        u128::try_from(variance_x)
            .unwrap_or(u128::MAX)
            .saturating_mul(u128::try_from(variance_y).unwrap_or(u128::MAX)),
    );
    if denominator == 0 {
        return 0;
    }
    i64::try_from(covariance.saturating_mul(1_000_000) / denominator as i128).unwrap_or_else(|_| {
        if covariance.is_negative() {
            i64::MIN
        } else {
            i64::MAX
        }
    })
}

fn critical_path_rows(raw: &[RawTrial]) -> Vec<CriticalPathRow> {
    let mut rows = Vec::new();
    for trial in raw {
        let barrier_receiver = trial
            .critical_paths
            .iter()
            .max_by_key(|path| attribution_total(path))
            .map_or(0, |path| path.receiver);
        for path in &trial.critical_paths {
            rows.push(CriticalPathRow {
                schema_version: SCENARIO_SCHEMA_VERSION,
                simulator_version: SIMULATOR_VERSION,
                evidence_class: EVIDENCE_CLASS,
                sample_class: task_sample_class(trial.task),
                overlap_percent: trial.task.overlap_percent,
                protocol: trial.task.protocol.name(),
                seed: trial.task.seed,
                receiver: path.receiver,
                is_barrier_receiver: path.receiver == barrier_receiver,
                final_tree: path.final_tree,
                final_frame_id: path.final_frame_id,
                source_generation_ns: path.source_generation_ns,
                source_to_runtime_ns: path.source_to_runtime_ns,
                runtime_wait_ns: path.runtime_wait_ns,
                decoder_queue_ns: path.decoder_queue_ns,
                decoder_service_ns: path.decoder_service_ns,
                total_ns: attribution_total(path),
            });
        }
    }
    rows
}

fn flow_count_rows() -> Vec<FlowCountRow> {
    OVERLAPS
        .into_iter()
        .map(|overlap| FlowCountRow {
            schema_version: SCENARIO_SCHEMA_VERSION,
            simulator_version: SIMULATOR_VERSION,
            evidence_class: EVIDENCE_CLASS,
            overlap_percent: overlap,
            serial_bottleneck_stages: 4,
            shared_stages: usize::from(overlap / 25),
            private_stages: 4 - usize::from(overlap / 25),
            aggregate_capacity_per_stage_bps: 160_000_000,
            aggregate_capacity_all_serial_stages_bps: 640_000_000,
            foreground_tcp_connections: 2,
            explicit_background_tcp_flows: 8,
            directional_active_flow_count: 10,
            flows_per_shared_server: 10,
            flows_per_private_lane_server: 5,
            best_single_uses_existing_second_connection: true,
        })
        .collect()
}

fn run_isolated_credit_slice(
    seeds: u64,
    workers: usize,
) -> Result<(Vec<IsolatedRow>, Vec<IsolatedSummaryRow>), W3ExperimentError> {
    let mut tasks = Vec::new();
    for policy in [
        ReceiverAdmissionPolicy::HybridDrop,
        ReceiverAdmissionPolicy::IsolatedCredit,
    ] {
        for seed in 0..seeds {
            tasks.push(IsolatedTask { policy, seed });
        }
    }
    let mut rows = execute_parallel(&tasks, workers, |task| {
        let mut scenario = W2Scenario::screening(
            task.policy,
            ReceiverServiceRate::Tenth,
            BufferBudget::QuarterBdp,
            8,
            2,
            ChildOrder::SlowFirst,
            task.seed,
        );
        scenario.scenario_id = format!(
            "w3-isolated-reconsider-{}-s{}",
            task.policy.name(),
            task.seed
        );
        scenario.w3_shared_leaf_bottleneck = Some(W2SharedLeafBottleneck { receivers: [0, 1] });
        let outcome = run_w2(&scenario).map_err(|error| error.to_string())?;
        let healthy_completion_max_ns = outcome
            .completion_times_ns
            .iter()
            .copied()
            .skip(1)
            .max()
            .unwrap_or(0);
        let shared_leaf_wire_bytes = outcome
            .records
            .iter()
            .filter(|record| {
                record.component.starts_with("w3_shared_leaf_")
                    && record.event == "coupled_path_exit"
            })
            .map(|record| record.bytes)
            .sum();
        Ok(IsolatedRow {
            schema_version: SCENARIO_SCHEMA_VERSION,
            simulator_version: SIMULATOR_VERSION,
            evidence_class: EVIDENCE_CLASS,
            seed: task.seed,
            admission_policy: task.policy.name(),
            overlap_percent: 100,
            source_symbols: scenario.source_symbols,
            barrier_completion_ns: outcome.barrier_completion_ns,
            healthy_completion_max_ns,
            total_emissions: outcome.total_emissions,
            slow_branch_deliveries_through_completion: outcome
                .receiver_transport_deliveries_through_completion[0],
            slow_application_drop_deficits: outcome
                .receiver_application_drop_deficits_through_completion[0],
            application_drops_total: outcome.application_drops,
            isolated_credit_deferrals: outcome.isolated_credit_deferrals,
            isolated_credit_replays: outcome.isolated_credit_replays,
            isolated_credit_outstanding_frames: outcome.isolated_credit_debt_outstanding_frames,
            shared_leaf_wire_bytes,
            liveness_pressure_permille: outcome.liveness_pressure_permille,
        })
    })?;
    rows.sort_by_key(|row| (row.admission_policy, row.seed));
    let hybrid = rows
        .iter()
        .filter(|row| row.admission_policy == ReceiverAdmissionPolicy::HybridDrop.name())
        .collect::<Vec<_>>();
    let isolated = rows
        .iter()
        .filter(|row| row.admission_policy == ReceiverAdmissionPolicy::IsolatedCredit.name())
        .collect::<Vec<_>>();
    if hybrid.len() != isolated.len() || hybrid.is_empty() {
        return Err(W3ExperimentError::MissingPair(
            "isolated-credit reconsideration policies".to_owned(),
        ));
    }
    let mean_field_u64 = |rows: &[&IsolatedRow], field: fn(&IsolatedRow) -> u64| {
        mean_u64(&rows.iter().map(|row| field(row)).collect::<Vec<_>>())
    };
    let mean_field_usize = |rows: &[&IsolatedRow], field: fn(&IsolatedRow) -> usize| {
        mean_usize(&rows.iter().map(|row| field(row)).collect::<Vec<_>>())
    };
    let hybrid_barrier = mean_field_u64(&hybrid, |row| row.barrier_completion_ns);
    let isolated_barrier = mean_field_u64(&isolated, |row| row.barrier_completion_ns);
    let hybrid_healthy = mean_field_u64(&hybrid, |row| row.healthy_completion_max_ns);
    let isolated_healthy = mean_field_u64(&isolated, |row| row.healthy_completion_max_ns);
    let hybrid_emissions = mean_field_usize(&hybrid, |row| row.total_emissions);
    let isolated_emissions = mean_field_usize(&isolated, |row| row.total_emissions);
    let hybrid_deliveries =
        mean_field_usize(&hybrid, |row| row.slow_branch_deliveries_through_completion);
    let isolated_deliveries = mean_field_usize(&isolated, |row| {
        row.slow_branch_deliveries_through_completion
    });
    let hybrid_wire = mean_field_usize(&hybrid, |row| row.shared_leaf_wire_bytes);
    let isolated_wire = mean_field_usize(&isolated, |row| row.shared_leaf_wire_bytes);
    let barrier_saved = i128::from(hybrid_barrier) - i128::from(isolated_barrier);
    let healthy_saved = i128::from(hybrid_healthy) - i128::from(isolated_healthy);
    let emission_saved = hybrid_emissions as i128 - isolated_emissions as i128;
    let delivery_saved = hybrid_deliveries as i128 - isolated_deliveries as i128;
    let wire_saved = hybrid_wire as i128 - isolated_wire as i128;
    let summary = IsolatedSummaryRow {
        schema_version: SCENARIO_SCHEMA_VERSION,
        simulator_version: SIMULATOR_VERSION,
        evidence_class: EVIDENCE_CLASS,
        seeds: hybrid.len(),
        overlap_percent: 100,
        hybrid_barrier_mean_ns: hybrid_barrier,
        isolated_barrier_mean_ns: isolated_barrier,
        barrier_saved_by_isolated_ns: barrier_saved,
        hybrid_healthy_completion_mean_ns: hybrid_healthy,
        isolated_healthy_completion_mean_ns: isolated_healthy,
        healthy_time_saved_by_isolated_ns: healthy_saved,
        hybrid_total_emissions_mean: hybrid_emissions,
        isolated_total_emissions_mean: isolated_emissions,
        source_emissions_saved_by_isolated: emission_saved,
        hybrid_slow_branch_deliveries_mean: hybrid_deliveries,
        isolated_slow_branch_deliveries_mean: isolated_deliveries,
        slow_branch_deliveries_saved: delivery_saved,
        hybrid_shared_leaf_wire_bytes_mean: hybrid_wire,
        isolated_shared_leaf_wire_bytes_mean: isolated_wire,
        shared_leaf_wire_bytes_saved: wire_saved,
        reconsideration_condition_2_met: healthy_saved > 0 || barrier_saved > 0,
    };
    Ok((rows, vec![summary]))
}

fn run_control_asymmetry_slice(
    screening_seeds: u64,
    decisive_seeds: u64,
    workers: usize,
) -> Result<(Vec<ControlRow>, Vec<ControlSummaryRow>), W3ExperimentError> {
    let mut tasks = Vec::new();
    for condition in ControlCondition::ALL {
        let decisive = condition == ControlCondition::ReverseBursts;
        let seeds = if decisive {
            decisive_seeds
        } else {
            screening_seeds
        };
        for cadence in CadenceFactor::ALL {
            for seed in 0..seeds {
                tasks.push(ControlTask {
                    condition,
                    cadence,
                    seed,
                    decisive,
                });
            }
        }
    }
    let mut rows = execute_parallel(&tasks, workers, |task| {
        let mut scenario = W2Scenario::screening(
            ReceiverAdmissionPolicy::HybridDrop,
            ReceiverServiceRate::One,
            BufferBudget::OneBdp,
            8,
            4,
            ChildOrder::SlowFirst,
            task.seed,
        );
        scenario.scenario_id = format!(
            "w3-control-{}-{}-s{}",
            task.condition.name(),
            task.cadence.name(),
            task.seed
        );
        scenario.slow_receiver_count = 0;
        scenario.carousel.ack_debounce_ns = task.cadence.scale(500_000);
        scenario.carousel.ack_heartbeat_ns = task.cadence.scale(5_000_000);
        scenario.w3_control_asymmetry = Some(W2ControlAsymmetry {
            reverse_rate_bps: 20_000_000,
            reverse_propagation_ns: 16_000_000,
            reverse_background_bursts: task.condition == ControlCondition::ReverseBursts,
        });
        let outcome = run_w2(&scenario).map_err(|error| error.to_string())?;
        let sender_completion_ns = outcome.sender_completion_ns.ok_or_else(|| {
            format!(
                "control-asymmetry sender incomplete: {}",
                scenario.scenario_id
            )
        })?;
        let reverse_path_wire_bytes = outcome
            .records
            .iter()
            .filter(|record| {
                record.component == "w3_control_reverse_shared"
                    && record.event == "coupled_path_exit"
            })
            .map(|record| record.bytes)
            .sum();
        let reverse_background_delivered_bytes = outcome
            .records
            .iter()
            .filter(|record| {
                record.component == "w3_control_reverse_onoff"
                    && record.event == "background_bytes_delivered"
            })
            .map(|record| record.bytes)
            .sum();
        Ok(ControlRow {
            schema_version: SCENARIO_SCHEMA_VERSION,
            simulator_version: SIMULATOR_VERSION,
            evidence_class: EVIDENCE_CLASS,
            sample_class: if task.decisive {
                "decisive"
            } else {
                "screening"
            },
            seed: task.seed,
            condition: task.condition.name(),
            cadence_factor: task.cadence.name(),
            receiver_count: scenario.receiver_count,
            source_symbols: scenario.source_symbols,
            ack_debounce_ns: scenario.carousel.ack_debounce_ns,
            ack_heartbeat_ns: scenario.carousel.ack_heartbeat_ns,
            reverse_rate_bps: 20_000_000,
            reverse_propagation_ns: 16_000_000,
            barrier_completion_ns: outcome.barrier_completion_ns,
            sender_completion_ns,
            feedback_completion_lag_ns: sender_completion_ns
                .saturating_sub(outcome.barrier_completion_ns),
            total_emissions: outcome.total_emissions,
            post_completion_tail_emissions: outcome.post_completion_tail_emissions,
            block_acks_received: outcome
                .records
                .iter()
                .filter(|record| record.event == "block_ack_received")
                .count(),
            ack_probes: outcome
                .records
                .iter()
                .filter(|record| record.event == "ack_probe_submitted")
                .count(),
            reverse_path_wire_bytes,
            reverse_background_delivered_bytes,
            application_drops: outcome.application_drops,
            link_drops: outcome.link_drops,
            liveness_pressure_permille: outcome.liveness_pressure_permille,
            liveness_margin_permille: 1_000_u64.saturating_sub(outcome.liveness_pressure_permille),
        })
    })?;
    rows.sort_by_key(|row| (row.condition, row.cadence_factor, row.seed));
    let mut groups: BTreeMap<(&'static str, &'static str), Vec<&ControlRow>> = BTreeMap::new();
    for row in &rows {
        groups
            .entry((row.condition, row.cadence_factor))
            .or_default()
            .push(row);
    }
    let summaries = groups
        .into_iter()
        .map(|((condition, cadence), group)| ControlSummaryRow {
            schema_version: SCENARIO_SCHEMA_VERSION,
            simulator_version: SIMULATOR_VERSION,
            evidence_class: EVIDENCE_CLASS,
            sample_class: if condition == ControlCondition::ReverseBursts.name() {
                "decisive"
            } else {
                "screening"
            },
            condition,
            cadence_factor: cadence,
            seeds: group.len(),
            barrier_mean_ns: mean_u64(
                &group
                    .iter()
                    .map(|row| row.barrier_completion_ns)
                    .collect::<Vec<_>>(),
            ),
            barrier_p95_ns: percentile95(
                &group
                    .iter()
                    .map(|row| row.barrier_completion_ns)
                    .collect::<Vec<_>>(),
            ),
            sender_completion_mean_ns: mean_u64(
                &group
                    .iter()
                    .map(|row| row.sender_completion_ns)
                    .collect::<Vec<_>>(),
            ),
            feedback_completion_lag_mean_ns: mean_u64(
                &group
                    .iter()
                    .map(|row| row.feedback_completion_lag_ns)
                    .collect::<Vec<_>>(),
            ),
            total_emissions_mean: mean_usize(
                &group
                    .iter()
                    .map(|row| row.total_emissions)
                    .collect::<Vec<_>>(),
            ),
            post_completion_tail_mean: mean_usize(
                &group
                    .iter()
                    .map(|row| row.post_completion_tail_emissions)
                    .collect::<Vec<_>>(),
            ),
            ack_probes_sum: group.iter().map(|row| row.ack_probes).sum(),
            reverse_path_wire_bytes_mean: mean_usize(
                &group
                    .iter()
                    .map(|row| row.reverse_path_wire_bytes)
                    .collect::<Vec<_>>(),
            ),
            reverse_background_delivered_bytes_mean: mean_usize(
                &group
                    .iter()
                    .map(|row| row.reverse_background_delivered_bytes)
                    .collect::<Vec<_>>(),
            ),
            application_drops_sum: group.iter().map(|row| row.application_drops).sum(),
            link_drops_sum: group.iter().map(|row| row.link_drops).sum(),
            liveness_pressure_max_permille: group
                .iter()
                .map(|row| row.liveness_pressure_permille)
                .max()
                .unwrap_or(0),
            liveness_margin_min_permille: group
                .iter()
                .map(|row| row.liveness_margin_permille)
                .min()
                .unwrap_or(0),
        })
        .collect();
    Ok((rows, summaries))
}

fn execute_parallel<T, R, F>(
    tasks: &[T],
    workers: usize,
    execute: F,
) -> Result<Vec<R>, W3ExperimentError>
where
    T: Copy + Send + Sync,
    R: Send,
    F: Fn(T) -> Result<R, String> + Send + Sync,
{
    let next = AtomicUsize::new(0);
    let results = Mutex::new((0..tasks.len()).map(|_| None).collect::<Vec<Option<R>>>());
    let failure = Mutex::new(None);
    thread::scope(|scope| {
        for _ in 0..workers {
            let next = &next;
            let results = &results;
            let failure = &failure;
            let execute = &execute;
            scope.spawn(move || {
                loop {
                    if failure.lock().expect("parallel failure mutex").is_some() {
                        break;
                    }
                    let index = next.fetch_add(1, Ordering::Relaxed);
                    let Some(task) = tasks.get(index).copied() else {
                        break;
                    };
                    match execute(task) {
                        Ok(result) => {
                            results.lock().expect("parallel result mutex")[index] = Some(result)
                        }
                        Err(error) => {
                            *failure.lock().expect("parallel failure mutex") = Some(error);
                            break;
                        }
                    }
                }
            });
        }
    });
    if let Some(error) = failure.lock().expect("parallel failure mutex").take() {
        return Err(W3ExperimentError::Worker(error));
    }
    let mut results = results.lock().expect("parallel result mutex");
    results
        .iter_mut()
        .enumerate()
        .map(|(index, result)| result.take().ok_or(W3ExperimentError::MissingTask(index)))
        .collect()
}

fn task_sample_class(task: Task) -> &'static str {
    if task.protocol == TaskProtocol::Rounds {
        "reduced_rounds_slice"
    } else if task.decisive {
        "decisive"
    } else {
        "screening"
    }
}

fn attribution_total(path: &CriticalAttribution) -> u64 {
    path.source_generation_ns
        .saturating_add(path.source_to_runtime_ns)
        .saturating_add(path.runtime_wait_ns)
        .saturating_add(path.decoder_queue_ns)
        .saturating_add(path.decoder_service_ns)
}

fn count_event(records: &[Record], event: &str) -> usize {
    records
        .iter()
        .filter(|record| record.event == event)
        .count()
}

fn ratio_ppm(numerator: i128, denominator: u64) -> i128 {
    if denominator == 0 {
        return 0;
    }
    numerator.saturating_mul(1_000_000) / i128::from(denominator)
}

fn mean_u64(values: &[u64]) -> u64 {
    if values.is_empty() {
        return 0;
    }
    u64::try_from(
        values.iter().map(|value| u128::from(*value)).sum::<u128>() / values.len() as u128,
    )
    .unwrap_or(u64::MAX)
}

fn mean_usize(values: &[usize]) -> usize {
    if values.is_empty() {
        return 0;
    }
    usize::try_from(values.iter().map(|value| *value as u128).sum::<u128>() / values.len() as u128)
        .unwrap_or(usize::MAX)
}

fn mean_i64(values: &[i64]) -> i64 {
    if values.is_empty() {
        return 0;
    }
    i64::try_from(
        values.iter().map(|value| i128::from(*value)).sum::<i128>() / values.len() as i128,
    )
    .unwrap_or(0)
}

fn percentile95<T: Copy + Ord + Default>(values: &[T]) -> T {
    if values.is_empty() {
        return T::default();
    }
    let mut ordered = values.to_vec();
    ordered.sort_unstable();
    let rank = 95usize.saturating_mul(ordered.len()).div_ceil(100);
    ordered[rank.saturating_sub(1).min(ordered.len() - 1)]
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
    fn main_and_reduced_task_budget_is_exact() {
        let tasks = build_tasks(32, 128);
        assert_eq!(tasks.len(), 1_344);
        assert_eq!(
            tasks
                .iter()
                .filter(|task| task.protocol == TaskProtocol::Rounds)
                .count(),
            64
        );
    }

    #[test]
    fn overlap_axis_holds_aggregate_capacity_and_active_flow_count_constant() {
        let rows = flow_count_rows();
        assert_eq!(rows.len(), 4);
        assert!(rows.iter().all(|row| {
            row.aggregate_capacity_per_stage_bps == 160_000_000
                && row.directional_active_flow_count == 10
                && row.foreground_tcp_connections == 2
                && row.shared_stages + row.private_stages == 4
        }));
    }

    #[test]
    fn integer_rate_correlation_pins_sign_and_scale() {
        let mut records = Vec::new();
        for (window, bytes) in [1, 2, 4, 7, 11, 16, 22].into_iter().enumerate() {
            records.push(record(window as u64 * RATE_WINDOW_NS, 70_000, bytes));
            records.push(record(window as u64 * RATE_WINDOW_NS, 70_004, bytes));
        }
        assert_eq!(
            rate_correlation_ppm(&records, RATE_WINDOW_NS * 7),
            1_000_000
        );
    }

    fn record(time_ns: u64, flow_id: usize, bytes: usize) -> Record {
        Record {
            schema_version: SCENARIO_SCHEMA_VERSION,
            simulator_version: SIMULATOR_VERSION,
            scenario: "test".to_owned(),
            seed: 0,
            time_ns,
            component: "w3_coupled_data_forward",
            event: "coupled_path_exit",
            flow_id,
            sequence: 0,
            bytes,
            value: 0,
        }
    }
}
