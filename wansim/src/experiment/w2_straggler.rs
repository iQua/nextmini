use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;

use serde::Serialize;
use thiserror::Error;

use crate::scenario::{
    BufferBudget, ChildOrder, ReceiverAdmissionPolicy, ReceiverServiceRate, W2Outcome, W2RunError,
    W2Scenario, run_w2,
};
use crate::{SCENARIO_SCHEMA_VERSION, SIMULATOR_VERSION};

const EVIDENCE_CLASS: &str = "model-level causal evidence; not a WAN measurement";
const RECEIVER_COUNTS: [usize; 2] = [3, 8];
const FANOUT_DEGREES: [usize; 2] = [2, 4];

#[derive(Clone, Debug)]
pub struct W2Artifacts {
    pub trials_csv: String,
    pub screening_summary_csv: String,
    pub decisive_summary_csv: String,
    pub decision_table_csv: String,
    pub critical_paths_csv: String,
    pub tail_fraction_csv: String,
    pub sanity_csv: String,
}

#[derive(Debug, Error)]
pub enum W2ExperimentError {
    #[error("W2 requires at least one screening seed")]
    ZeroScreeningSeeds,
    #[error("decisive seed count {decisive} is below screening count {screening}")]
    DecisiveBelowScreening { screening: u64, decisive: u64 },
    #[error("W2 worker count must be nonzero")]
    ZeroWorkers,
    #[error("W2 worker failed: {0}")]
    Worker(String),
    #[error("W2 task {0} did not produce a result")]
    MissingTask(usize),
    #[error("missing all-healthy baseline for {0}")]
    MissingBaseline(String),
    #[error("no-straggler sanity failed: {0}")]
    Sanity(String),
    #[error(transparent)]
    Run(#[from] W2RunError),
    #[error(transparent)]
    Csv(#[from] csv::Error),
}

#[derive(Clone, Copy, Debug)]
struct Task {
    policy: ReceiverAdmissionPolicy,
    service: ReceiverServiceRate,
    budget: BufferBudget,
    receivers: usize,
    fanout_degree: usize,
    order: ChildOrder,
    seed: u64,
    decisive: bool,
    supporting_baseline: bool,
}

#[derive(Clone, Debug)]
struct RawTrial {
    task: Task,
    barrier_completion_ns: u64,
    slow_completion_ns: u64,
    healthy_completion_sum_ns: u128,
    healthy_completion_max_ns: u64,
    healthy_receiver_count: usize,
    sender_completion_ns: u64,
    total_emissions: usize,
    tree0_emissions: usize,
    tree1_emissions: usize,
    emissions_through_completion: usize,
    post_completion_tail_emissions: usize,
    application_drops: usize,
    blocking_wait_events: usize,
    isolated_credit_deferrals: usize,
    isolated_credit_replays: usize,
    link_drops: usize,
    ack_probes: usize,
    liveness_pressure_permille: u64,
    maximum_mailbox_high_water: usize,
    critical_receiver: usize,
    critical_source_generation_ns: u64,
    critical_source_to_runtime_ns: u64,
    critical_runtime_wait_ns: u64,
    critical_decoder_queue_ns: u64,
    critical_decoder_service_ns: u64,
}

#[derive(Clone, Debug, Serialize)]
struct TrialRow {
    schema_version: u32,
    simulator_version: &'static str,
    evidence_class: &'static str,
    sample_class: &'static str,
    seed: u64,
    admission_policy: &'static str,
    slow_service_rate: &'static str,
    buffer_budget: &'static str,
    receiver_count: usize,
    fanout_degree: usize,
    child_order: &'static str,
    source_symbols: usize,
    barrier_completion_ns: u64,
    slow_completion_ns: u64,
    healthy_completion_mean_ns: u64,
    healthy_completion_max_ns: u64,
    healthy_externality_mean_ns: i128,
    healthy_externality_max_ns: i128,
    sender_completion_ns: u64,
    total_emissions: usize,
    tree0_emissions: usize,
    tree1_emissions: usize,
    emissions_through_completion: usize,
    sender_extra_emissions: usize,
    post_completion_tail_emissions: usize,
    application_drop_deficits: usize,
    blocking_wait_events: usize,
    isolated_credit_deferrals: usize,
    isolated_credit_replays: usize,
    link_drops: usize,
    ack_probes: usize,
    liveness_pressure_permille: u64,
    maximum_mailbox_high_water: usize,
    critical_receiver: usize,
    critical_source_generation_ns: u64,
    critical_source_to_runtime_ns: u64,
    critical_runtime_wait_ns: u64,
    critical_decoder_queue_ns: u64,
    critical_decoder_service_ns: u64,
}

#[derive(Clone, Debug, Serialize)]
struct SummaryRow {
    schema_version: u32,
    simulator_version: &'static str,
    evidence_class: &'static str,
    sample_class: &'static str,
    admission_policy: &'static str,
    slow_service_rate: &'static str,
    buffer_budget: &'static str,
    receiver_count: usize,
    fanout_degree: usize,
    child_order: &'static str,
    seeds: usize,
    barrier_mean_ns: u64,
    barrier_p95_ns: u64,
    healthy_externality_mean_ns: i128,
    healthy_externality_p95_ns: i128,
    healthy_externality_max_ns: i128,
    slow_completion_mean_ns: u64,
    total_emissions_mean: usize,
    sender_extra_emissions_mean: usize,
    post_completion_tail_mean: usize,
    application_drop_deficits_sum: usize,
    blocking_wait_events_sum: usize,
    isolated_credit_deferrals_sum: usize,
    link_drops_sum: usize,
    liveness_pressure_max_permille: u64,
    mailbox_high_water_max: usize,
}

#[derive(Clone, Debug, Serialize)]
struct DecisionRow {
    schema_version: u32,
    simulator_version: &'static str,
    evidence_class: &'static str,
    admission_policy: &'static str,
    scenario: String,
    seeds: usize,
    barrier_mean_ns: u64,
    healthy_externality_mean_ns: i128,
    healthy_externality_p95_ns: i128,
    total_emissions_mean: usize,
    application_drop_deficits_sum: usize,
    liveness_pressure_max_permille: u64,
}

#[derive(Clone, Debug, Serialize)]
struct CriticalPathRow {
    schema_version: u32,
    simulator_version: &'static str,
    evidence_class: &'static str,
    admission_policy: &'static str,
    receiver_count: usize,
    fanout_degree: usize,
    child_order: &'static str,
    seed: u64,
    critical_receiver: usize,
    source_generation_ns: u64,
    source_to_runtime_ns: u64,
    runtime_wait_ns: u64,
    decoder_queue_ns: u64,
    decoder_service_ns: u64,
    total_ns: u64,
}

#[derive(Clone, Debug, Serialize)]
struct TailRow {
    schema_version: u32,
    simulator_version: &'static str,
    evidence_class: &'static str,
    source_symbols: usize,
    total_emissions: usize,
    ack_flight_tail_emissions: usize,
    tail_fraction_ppm: usize,
}

#[derive(Clone, Debug, Serialize)]
struct SanityRow {
    schema_version: u32,
    simulator_version: &'static str,
    evidence_class: &'static str,
    admission_policy: &'static str,
    completion_times_ns: String,
    total_emissions: usize,
    application_drops: usize,
    blocking_wait_events: usize,
    isolated_credit_deferrals: usize,
    identical_to_hybrid: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct BaselineKey {
    policy: ReceiverAdmissionPolicy,
    budget: BufferBudget,
    receivers: usize,
    fanout_degree: usize,
    order: ChildOrder,
    seed: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct SummaryKey {
    policy: ReceiverAdmissionPolicy,
    service: ReceiverServiceRate,
    budget: BufferBudget,
    receivers: usize,
    fanout_degree: usize,
    order: ChildOrder,
}

pub fn run_w2_experiment(
    screening_seeds: u64,
    decisive_seeds: u64,
    workers: usize,
) -> Result<W2Artifacts, W2ExperimentError> {
    if screening_seeds == 0 {
        return Err(W2ExperimentError::ZeroScreeningSeeds);
    }
    if decisive_seeds < screening_seeds {
        return Err(W2ExperimentError::DecisiveBelowScreening {
            screening: screening_seeds,
            decisive: decisive_seeds,
        });
    }
    if workers == 0 {
        return Err(W2ExperimentError::ZeroWorkers);
    }
    let tasks = build_tasks(screening_seeds, decisive_seeds);
    let raw = execute_tasks(&tasks, workers)?;
    let trials = attach_externalities(&raw)?;
    let screening: Vec<_> = trials
        .iter()
        .filter(|row| row.seed < screening_seeds)
        .cloned()
        .collect();
    let decisive: Vec<_> = trials
        .iter()
        .filter(|row| is_decisive_row(row))
        .cloned()
        .collect();
    let screening_summary = summarize(&screening, "screening");
    let decisive_summary = summarize(&decisive, "decisive");
    let decision_table = decisive_summary
        .iter()
        .map(|row| DecisionRow {
            schema_version: row.schema_version,
            simulator_version: row.simulator_version,
            evidence_class: row.evidence_class,
            admission_policy: row.admission_policy,
            scenario: format!(
                "service={} budget={} receivers={} fanout={} order={}",
                row.slow_service_rate,
                row.buffer_budget,
                row.receiver_count,
                row.fanout_degree,
                row.child_order
            ),
            seeds: row.seeds,
            barrier_mean_ns: row.barrier_mean_ns,
            healthy_externality_mean_ns: row.healthy_externality_mean_ns,
            healthy_externality_p95_ns: row.healthy_externality_p95_ns,
            total_emissions_mean: row.total_emissions_mean,
            application_drop_deficits_sum: row.application_drop_deficits_sum,
            liveness_pressure_max_permille: row.liveness_pressure_max_permille,
        })
        .collect::<Vec<_>>();
    let critical_paths = trials
        .iter()
        .filter(|row| is_decisive_row(row))
        .map(|row| CriticalPathRow {
            schema_version: row.schema_version,
            simulator_version: row.simulator_version,
            evidence_class: row.evidence_class,
            admission_policy: row.admission_policy,
            receiver_count: row.receiver_count,
            fanout_degree: row.fanout_degree,
            child_order: row.child_order,
            seed: row.seed,
            critical_receiver: row.critical_receiver,
            source_generation_ns: row.critical_source_generation_ns,
            source_to_runtime_ns: row.critical_source_to_runtime_ns,
            runtime_wait_ns: row.critical_runtime_wait_ns,
            decoder_queue_ns: row.critical_decoder_queue_ns,
            decoder_service_ns: row.critical_decoder_service_ns,
            total_ns: row
                .critical_source_generation_ns
                .saturating_add(row.critical_source_to_runtime_ns)
                .saturating_add(row.critical_runtime_wait_ns)
                .saturating_add(row.critical_decoder_queue_ns)
                .saturating_add(row.critical_decoder_service_ns),
        })
        .collect::<Vec<_>>();
    let tail = tail_fraction_rows()?;
    let sanity = no_straggler_sanity()?;
    Ok(W2Artifacts {
        trials_csv: to_csv(&trials)?,
        screening_summary_csv: to_csv(&screening_summary)?,
        decisive_summary_csv: to_csv(&decisive_summary)?,
        decision_table_csv: to_csv(&decision_table)?,
        critical_paths_csv: to_csv(&critical_paths)?,
        tail_fraction_csv: to_csv(&tail)?,
        sanity_csv: to_csv(&sanity)?,
    })
}

fn build_tasks(screening_seeds: u64, decisive_seeds: u64) -> Vec<Task> {
    let mut tasks = Vec::new();
    for policy in ReceiverAdmissionPolicy::ALL {
        for service in ReceiverServiceRate::ALL {
            for budget in BufferBudget::ALL {
                for receivers in RECEIVER_COUNTS {
                    for fanout_degree in FANOUT_DEGREES {
                        for order in ChildOrder::ALL {
                            let decisive = service == ReceiverServiceRate::Tenth
                                && budget == BufferBudget::QuarterBdp
                                && receivers == 8;
                            let supporting_baseline = service == ReceiverServiceRate::One
                                && budget == BufferBudget::QuarterBdp
                                && receivers == 8;
                            let seeds = if decisive || supporting_baseline {
                                decisive_seeds
                            } else {
                                screening_seeds
                            };
                            for seed in 0..seeds {
                                tasks.push(Task {
                                    policy,
                                    service,
                                    budget,
                                    receivers,
                                    fanout_degree,
                                    order,
                                    seed,
                                    decisive,
                                    supporting_baseline,
                                });
                            }
                        }
                    }
                }
            }
        }
    }
    tasks
}

fn execute_tasks(tasks: &[Task], workers: usize) -> Result<Vec<RawTrial>, W2ExperimentError> {
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
                    if failure.lock().expect("worker failure mutex").is_some() {
                        break;
                    }
                    let index = next.fetch_add(1, Ordering::Relaxed);
                    let Some(task) = tasks.get(index).copied() else {
                        break;
                    };
                    match execute_task(task) {
                        Ok(trial) => {
                            results.lock().expect("worker result mutex")[index] = Some(trial)
                        }
                        Err(error) => {
                            *failure.lock().expect("worker failure mutex") =
                                Some(error.to_string());
                            break;
                        }
                    }
                }
            });
        }
    });
    if let Some(error) = failure.lock().expect("worker failure mutex").take() {
        return Err(W2ExperimentError::Worker(error));
    }
    let mut locked = results.lock().expect("worker result mutex");
    locked
        .iter_mut()
        .enumerate()
        .map(|(index, trial)| trial.take().ok_or(W2ExperimentError::MissingTask(index)))
        .collect()
}

fn execute_task(task: Task) -> Result<RawTrial, W2RunError> {
    let scenario = W2Scenario::screening(
        task.policy,
        task.service,
        task.budget,
        task.receivers,
        task.fanout_degree,
        task.order,
        task.seed,
    );
    let outcome = run_w2(&scenario)?;
    Ok(raw_trial(task, &scenario, &outcome))
}

fn raw_trial(task: Task, scenario: &W2Scenario, outcome: &W2Outcome) -> RawTrial {
    let healthy = &outcome.completion_times_ns[scenario.slow_receiver_count..];
    let critical_receiver = outcome
        .completion_times_ns
        .iter()
        .enumerate()
        .max_by_key(|(_, time)| *time)
        .map_or(0, |(receiver, _)| receiver);
    let critical = &outcome.critical_paths[critical_receiver];
    RawTrial {
        task,
        barrier_completion_ns: outcome.barrier_completion_ns,
        slow_completion_ns: outcome.completion_times_ns[0],
        healthy_completion_sum_ns: healthy.iter().map(|time| u128::from(*time)).sum(),
        healthy_completion_max_ns: healthy.iter().copied().max().unwrap_or(0),
        healthy_receiver_count: healthy.len(),
        sender_completion_ns: outcome.sender_completion_ns.unwrap_or(0),
        total_emissions: outcome.total_emissions,
        tree0_emissions: outcome.per_tree_emissions[0],
        tree1_emissions: outcome.per_tree_emissions[1],
        emissions_through_completion: outcome
            .records
            .iter()
            .filter(|record| {
                record.component == "w2_source"
                    && record.event == "data_frame_emitted"
                    && record.time_ns <= outcome.barrier_completion_ns
            })
            .count(),
        post_completion_tail_emissions: outcome.post_completion_tail_emissions,
        application_drops: outcome.application_drops,
        blocking_wait_events: outcome.blocking_wait_events,
        isolated_credit_deferrals: outcome.isolated_credit_deferrals,
        isolated_credit_replays: outcome.isolated_credit_replays,
        link_drops: outcome.link_drops,
        ack_probes: outcome
            .records
            .iter()
            .filter(|record| record.event == "ack_probe_submitted")
            .count(),
        liveness_pressure_permille: outcome.liveness_pressure_permille,
        maximum_mailbox_high_water: outcome
            .mailbox_high_water
            .values()
            .copied()
            .max()
            .unwrap_or(0),
        critical_receiver,
        critical_source_generation_ns: critical.source_generation_ns,
        critical_source_to_runtime_ns: critical.source_to_runtime_ns,
        critical_runtime_wait_ns: critical.runtime_wait_ns,
        critical_decoder_queue_ns: critical.decoder_queue_ns,
        critical_decoder_service_ns: critical.decoder_service_ns,
    }
}

fn attach_externalities(raw: &[RawTrial]) -> Result<Vec<TrialRow>, W2ExperimentError> {
    let baselines: BTreeMap<_, _> = raw
        .iter()
        .filter(|trial| trial.task.service == ReceiverServiceRate::One)
        .map(|trial| {
            (
                BaselineKey {
                    policy: trial.task.policy,
                    budget: trial.task.budget,
                    receivers: trial.task.receivers,
                    fanout_degree: trial.task.fanout_degree,
                    order: trial.task.order,
                    seed: trial.task.seed,
                },
                (
                    integer_mean_u64(
                        trial.healthy_completion_sum_ns,
                        trial.healthy_receiver_count,
                    ),
                    trial.healthy_completion_max_ns,
                ),
            )
        })
        .collect();
    raw.iter()
        .map(|trial| {
            let key = BaselineKey {
                policy: trial.task.policy,
                budget: trial.task.budget,
                receivers: trial.task.receivers,
                fanout_degree: trial.task.fanout_degree,
                order: trial.task.order,
                seed: trial.task.seed,
            };
            let (baseline_mean, baseline_max) = baselines.get(&key).copied().ok_or_else(|| {
                W2ExperimentError::MissingBaseline(format!(
                    "policy={} budget={} receivers={} fanout={} order={} seed={}",
                    key.policy.name(),
                    key.budget.name(),
                    key.receivers,
                    key.fanout_degree,
                    key.order.name(),
                    key.seed
                ))
            })?;
            let healthy_mean = integer_mean_u64(
                trial.healthy_completion_sum_ns,
                trial.healthy_receiver_count,
            );
            Ok(TrialRow {
                schema_version: SCENARIO_SCHEMA_VERSION,
                simulator_version: SIMULATOR_VERSION,
                evidence_class: EVIDENCE_CLASS,
                sample_class: if trial.task.decisive {
                    "decisive"
                } else if trial.task.supporting_baseline {
                    "decisive_baseline"
                } else {
                    "screening"
                },
                seed: trial.task.seed,
                admission_policy: trial.task.policy.name(),
                slow_service_rate: trial.task.service.name(),
                buffer_budget: trial.task.budget.name(),
                receiver_count: trial.task.receivers,
                fanout_degree: trial.task.fanout_degree,
                child_order: trial.task.order.name(),
                source_symbols: 512,
                barrier_completion_ns: trial.barrier_completion_ns,
                slow_completion_ns: trial.slow_completion_ns,
                healthy_completion_mean_ns: healthy_mean,
                healthy_completion_max_ns: trial.healthy_completion_max_ns,
                healthy_externality_mean_ns: i128::from(healthy_mean) - i128::from(baseline_mean),
                healthy_externality_max_ns: i128::from(trial.healthy_completion_max_ns)
                    - i128::from(baseline_max),
                sender_completion_ns: trial.sender_completion_ns,
                total_emissions: trial.total_emissions,
                tree0_emissions: trial.tree0_emissions,
                tree1_emissions: trial.tree1_emissions,
                emissions_through_completion: trial.emissions_through_completion,
                sender_extra_emissions: trial.total_emissions.saturating_sub(512),
                post_completion_tail_emissions: trial.post_completion_tail_emissions,
                application_drop_deficits: trial.application_drops,
                blocking_wait_events: trial.blocking_wait_events,
                isolated_credit_deferrals: trial.isolated_credit_deferrals,
                isolated_credit_replays: trial.isolated_credit_replays,
                link_drops: trial.link_drops,
                ack_probes: trial.ack_probes,
                liveness_pressure_permille: trial.liveness_pressure_permille,
                maximum_mailbox_high_water: trial.maximum_mailbox_high_water,
                critical_receiver: trial.critical_receiver,
                critical_source_generation_ns: trial.critical_source_generation_ns,
                critical_source_to_runtime_ns: trial.critical_source_to_runtime_ns,
                critical_runtime_wait_ns: trial.critical_runtime_wait_ns,
                critical_decoder_queue_ns: trial.critical_decoder_queue_ns,
                critical_decoder_service_ns: trial.critical_decoder_service_ns,
            })
        })
        .collect()
}

fn summarize(rows: &[TrialRow], sample_class: &'static str) -> Vec<SummaryRow> {
    let mut groups: BTreeMap<SummaryKey, Vec<&TrialRow>> = BTreeMap::new();
    for row in rows {
        let key = SummaryKey {
            policy: policy_from_name(row.admission_policy),
            service: service_from_name(row.slow_service_rate),
            budget: budget_from_name(row.buffer_budget),
            receivers: row.receiver_count,
            fanout_degree: row.fanout_degree,
            order: order_from_name(row.child_order),
        };
        groups.entry(key).or_default().push(row);
    }
    groups
        .into_iter()
        .map(|(key, group)| {
            let barriers: Vec<_> = group.iter().map(|row| row.barrier_completion_ns).collect();
            let externalities: Vec<_> = group
                .iter()
                .map(|row| row.healthy_externality_max_ns)
                .collect();
            SummaryRow {
                schema_version: SCENARIO_SCHEMA_VERSION,
                simulator_version: SIMULATOR_VERSION,
                evidence_class: EVIDENCE_CLASS,
                sample_class,
                admission_policy: key.policy.name(),
                slow_service_rate: key.service.name(),
                buffer_budget: key.budget.name(),
                receiver_count: key.receivers,
                fanout_degree: key.fanout_degree,
                child_order: key.order.name(),
                seeds: group.len(),
                barrier_mean_ns: mean_u64(&barriers),
                barrier_p95_ns: percentile95(&barriers),
                healthy_externality_mean_ns: mean_i128(&externalities),
                healthy_externality_p95_ns: percentile95(&externalities),
                healthy_externality_max_ns: externalities.iter().copied().max().unwrap_or(0),
                slow_completion_mean_ns: mean_u64(
                    &group
                        .iter()
                        .map(|row| row.slow_completion_ns)
                        .collect::<Vec<_>>(),
                ),
                total_emissions_mean: mean_usize(
                    &group
                        .iter()
                        .map(|row| row.total_emissions)
                        .collect::<Vec<_>>(),
                ),
                sender_extra_emissions_mean: mean_usize(
                    &group
                        .iter()
                        .map(|row| row.sender_extra_emissions)
                        .collect::<Vec<_>>(),
                ),
                post_completion_tail_mean: mean_usize(
                    &group
                        .iter()
                        .map(|row| row.post_completion_tail_emissions)
                        .collect::<Vec<_>>(),
                ),
                application_drop_deficits_sum: group
                    .iter()
                    .map(|row| row.application_drop_deficits)
                    .sum(),
                blocking_wait_events_sum: group.iter().map(|row| row.blocking_wait_events).sum(),
                isolated_credit_deferrals_sum: group
                    .iter()
                    .map(|row| row.isolated_credit_deferrals)
                    .sum(),
                link_drops_sum: group.iter().map(|row| row.link_drops).sum(),
                liveness_pressure_max_permille: group
                    .iter()
                    .map(|row| row.liveness_pressure_permille)
                    .max()
                    .unwrap_or(0),
                mailbox_high_water_max: group
                    .iter()
                    .map(|row| row.maximum_mailbox_high_water)
                    .max()
                    .unwrap_or(0),
            }
        })
        .collect()
}

fn tail_fraction_rows() -> Result<Vec<TailRow>, W2RunError> {
    let mut rows = Vec::new();
    for source_symbols in [64, 512] {
        let mut scenario = W2Scenario::screening(
            ReceiverAdmissionPolicy::NaiveBlocking,
            ReceiverServiceRate::One,
            BufferBudget::FourBdp,
            3,
            2,
            ChildOrder::SlowFirst,
            0,
        );
        scenario.scenario_id = format!("w2-tail-k{source_symbols}");
        scenario.source_symbols = source_symbols;
        scenario.slow_receiver_count = 0;
        scenario.healthy_decoder_sink_service_ns = 1;
        let outcome = run_w2(&scenario)?;
        rows.push(TailRow {
            schema_version: SCENARIO_SCHEMA_VERSION,
            simulator_version: SIMULATOR_VERSION,
            evidence_class: EVIDENCE_CLASS,
            source_symbols,
            total_emissions: outcome.total_emissions,
            ack_flight_tail_emissions: outcome.post_completion_tail_emissions,
            tail_fraction_ppm: outcome
                .post_completion_tail_emissions
                .saturating_mul(1_000_000)
                / outcome.total_emissions.max(1),
        });
    }
    Ok(rows)
}

fn no_straggler_sanity() -> Result<Vec<SanityRow>, W2ExperimentError> {
    let mut outcomes = Vec::new();
    for policy in ReceiverAdmissionPolicy::ALL {
        let mut scenario = W2Scenario::screening(
            policy,
            ReceiverServiceRate::One,
            BufferBudget::FourBdp,
            3,
            2,
            ChildOrder::SlowFirst,
            0,
        );
        scenario.scenario_id = format!("w2-sanity-{}", policy.name());
        scenario.slow_receiver_count = 0;
        scenario.healthy_decoder_sink_service_ns = 1;
        outcomes.push((policy, run_w2(&scenario)?));
    }
    let reference = &outcomes[0].1;
    let rows = outcomes
        .iter()
        .map(|(policy, outcome)| SanityRow {
            schema_version: SCENARIO_SCHEMA_VERSION,
            simulator_version: SIMULATOR_VERSION,
            evidence_class: EVIDENCE_CLASS,
            admission_policy: policy.name(),
            completion_times_ns: outcome
                .completion_times_ns
                .iter()
                .map(u64::to_string)
                .collect::<Vec<_>>()
                .join(";"),
            total_emissions: outcome.total_emissions,
            application_drops: outcome.application_drops,
            blocking_wait_events: outcome.blocking_wait_events,
            isolated_credit_deferrals: outcome.isolated_credit_deferrals,
            identical_to_hybrid: outcome.completion_times_ns == reference.completion_times_ns
                && outcome.total_emissions == reference.total_emissions,
        })
        .collect::<Vec<_>>();
    if rows.iter().any(|row| {
        !row.identical_to_hybrid
            || row.application_drops != 0
            || row.blocking_wait_events != 0
            || row.isolated_credit_deferrals != 0
    }) {
        return Err(W2ExperimentError::Sanity(
            "admission policies diverged in the provisioned all-healthy cell".to_owned(),
        ));
    }
    Ok(rows)
}

fn is_decisive_row(row: &TrialRow) -> bool {
    row.slow_service_rate == ReceiverServiceRate::Tenth.name()
        && row.buffer_budget == BufferBudget::QuarterBdp.name()
        && row.receiver_count == 8
}

fn policy_from_name(name: &str) -> ReceiverAdmissionPolicy {
    ReceiverAdmissionPolicy::ALL
        .into_iter()
        .find(|policy| policy.name() == name)
        .expect("trial policy came from the enum")
}

fn service_from_name(name: &str) -> ReceiverServiceRate {
    ReceiverServiceRate::ALL
        .into_iter()
        .find(|service| service.name() == name)
        .expect("trial service came from the enum")
}

fn budget_from_name(name: &str) -> BufferBudget {
    BufferBudget::ALL
        .into_iter()
        .find(|budget| budget.name() == name)
        .expect("trial budget came from the enum")
}

fn order_from_name(name: &str) -> ChildOrder {
    ChildOrder::ALL
        .into_iter()
        .find(|order| order.name() == name)
        .expect("trial order came from the enum")
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

fn mean_u64(values: &[u64]) -> u64 {
    integer_mean_u64(
        values.iter().map(|value| u128::from(*value)).sum(),
        values.len(),
    )
}

fn mean_usize(values: &[usize]) -> usize {
    if values.is_empty() {
        return 0;
    }
    usize::try_from(values.iter().map(|value| *value as u128).sum::<u128>() / values.len() as u128)
        .unwrap_or(usize::MAX)
}

fn mean_i128(values: &[i128]) -> i128 {
    if values.is_empty() {
        return 0;
    }
    values.iter().sum::<i128>() / values.len() as i128
}

fn integer_mean_u64(sum: u128, count: usize) -> u64 {
    if count == 0 {
        return 0;
    }
    u64::try_from(sum / count as u128).unwrap_or(u64::MAX)
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
    fn task_matrix_has_216_screening_cells_and_12_decisive_cells() {
        let tasks = build_tasks(32, 128);
        assert_eq!(tasks.iter().filter(|task| task.seed < 32).count(), 216 * 32);
        assert_eq!(tasks.iter().filter(|task| task.decisive).count(), 12 * 128);
        assert_eq!(
            tasks.iter().filter(|task| task.supporting_baseline).count(),
            12 * 128
        );
        assert_eq!(tasks.len(), 216 * 32 + 24 * (128 - 32));
    }
}
