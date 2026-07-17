use serde::Serialize;
use thiserror::Error;

use crate::metrics::MAILBOX_CAPACITY;
use crate::protocol::ProtocolKind;
use crate::scenario::{W1Outcome, W1RateProfile, W1RunError, W1Scenario, run_w1};
use crate::{SCENARIO_SCHEMA_VERSION, SIMULATOR_VERSION};

const EVIDENCE_CLASS: &str = "model-level causal evidence; not a WAN measurement";
const RECEIVER_COUNTS: [usize; 2] = [1, 3];
const SCREEN_SEED: u64 = 0;

#[derive(Clone, Debug)]
pub struct Experiment0Screen {
    pub screening_csv: String,
    pub verdict_csv: String,
    pub prediction_failed: bool,
}

#[derive(Clone, Debug)]
pub struct Experiment0Artifacts {
    pub trials_csv: String,
    pub summaries_csv: String,
    pub predictions_csv: String,
}

#[derive(Debug, Error)]
pub enum Experiment0Error {
    #[error(transparent)]
    Run(#[from] W1RunError),
    #[error("experiment 0 requires at least one seed")]
    ZeroSeeds,
    #[error("{scenario} finished locally but its sender did not finish")]
    SenderIncomplete { scenario: String },
    #[error(
        "strict-conservation scenario {scenario} recorded {application} application drops and {link} link drops"
    )]
    StrictConservationDrop {
        scenario: String,
        application: usize,
        link: usize,
    },
    #[error(
        "experiment 0 mailbox plumbing bound was reached in {scenario}: {high_water}/{capacity}"
    )]
    BindingMailbox {
        scenario: String,
        high_water: usize,
        capacity: usize,
    },
    #[error("missing experiment pairing: {0}")]
    MissingPair(String),
    #[error("screening trace is missing {event} for {receiver}")]
    MissingScreenEvent {
        receiver: String,
        event: &'static str,
    },
    #[error(transparent)]
    Csv(#[from] csv::Error),
}

#[derive(Clone, Debug, Serialize)]
struct ScreenRow {
    schema_version: u32,
    simulator_version: &'static str,
    evidence_class: &'static str,
    scenario: String,
    seed: u64,
    receiver: String,
    source_done_dispatch_ns: u64,
    rank_at_source_done: usize,
    reported_deficit: usize,
    initial_k_data_complete_ns: u64,
    source_done_overtake_ns: u64,
    application_drops: usize,
    link_drops: usize,
}

#[derive(Clone, Debug, Serialize)]
struct ScreenVerdictRow {
    schema_version: u32,
    simulator_version: &'static str,
    evidence_class: &'static str,
    prediction: &'static str,
    passed: bool,
    classification: &'static str,
    diagnosis: &'static str,
}

#[derive(Clone, Debug, Serialize)]
struct TrialRow {
    schema_version: u32,
    simulator_version: &'static str,
    evidence_class: &'static str,
    scenario: String,
    seed: u64,
    rate_profile: &'static str,
    active_receivers: usize,
    protocol: &'static str,
    source_symbols: usize,
    receiver1_completion_ns: u64,
    receiver2_completion_ns: u64,
    receiver3_completion_ns: u64,
    local_completion_sum_ns: u128,
    barrier_completion_ns: u64,
    sender_completion_ns: u64,
    total_emissions: usize,
    tree0_emissions: usize,
    tree1_emissions: usize,
    ack_flight_tail_emissions: usize,
    post_barrier_tail_emissions: usize,
    emissions_after_sender_completion: usize,
    positive_round_deficit_reports: usize,
    round_deficit_sum: usize,
    maximum_round_deficit: usize,
    application_drops: usize,
    link_drops: usize,
    mailbox_high_water: usize,
}

#[derive(Clone, Debug, Serialize)]
struct SummaryRow {
    schema_version: u32,
    simulator_version: &'static str,
    evidence_class: &'static str,
    rate_profile: &'static str,
    active_receivers: usize,
    protocol: &'static str,
    trials: usize,
    receiver_observations: usize,
    local_completion_sum_ns: u128,
    local_completion_mean_ns: u64,
    local_completion_p95_ns: u64,
    barrier_completion_sum_ns: u128,
    barrier_completion_mean_ns: u64,
    barrier_completion_p95_ns: u64,
    total_emissions_sum: u128,
    total_emissions_mean: usize,
    total_emissions_p95: usize,
    ack_flight_tail_sum: u128,
    ack_flight_tail_mean: usize,
    ack_flight_tail_p95: usize,
    positive_round_deficit_reports: usize,
    round_deficit_sum: usize,
    maximum_round_deficit: usize,
    application_drops: usize,
    link_drops: usize,
    maximum_mailbox_high_water: usize,
}

#[derive(Clone, Debug, Serialize)]
struct PredictionRow {
    schema_version: u32,
    simulator_version: &'static str,
    evidence_class: &'static str,
    prediction: &'static str,
    scope: &'static str,
    passed: bool,
    observations: usize,
    violations: usize,
    observed_min_margin_ns: i128,
    observed_max_margin_ns: i128,
    threshold_ns: u64,
    detail: String,
}

pub fn run_experiment0_screen() -> Result<Experiment0Screen, Experiment0Error> {
    let scenario = W1Scenario::experiment0(
        ProtocolKind::PooledRounds,
        W1RateProfile::Homogeneous,
        3,
        SCREEN_SEED,
    );
    let outcome = run_checked(&scenario)?;
    let quotas = scenario.quotas().expect("validated experiment quotas");
    let mut rows = Vec::with_capacity(scenario.active_receivers);
    for receiver in 1..=scenario.active_receivers {
        let component = format!("w1_receiver{receiver}");
        let source_done_dispatch_ns = outcome
            .records
            .iter()
            .find(|record| {
                record.component == component
                    && record.event == "source_done_runtime_dispatch"
                    && record.value == 0
            })
            .map(|record| record.time_ns)
            .ok_or_else(|| Experiment0Error::MissingScreenEvent {
                receiver: component.clone(),
                event: "source_done_runtime_dispatch",
            })?;
        let rank_at_source_done = outcome
            .records
            .iter()
            .filter(|record| {
                record.component == component
                    && record.event == "decoder_sink_complete"
                    && record.time_ns <= source_done_dispatch_ns
            })
            .count()
            .min(scenario.source_symbols);
        let reported_deficit = outcome
            .records
            .iter()
            .find(|record| {
                record.component == component
                    && record.event == "round_need_generated"
                    && record.sequence == 0
            })
            .map(|record| record.value)
            .ok_or_else(|| Experiment0Error::MissingScreenEvent {
                receiver: component.clone(),
                event: "round_need_generated",
            })?;
        let initial_k_data_complete_ns = outcome
            .records
            .iter()
            .filter(|record| {
                record.component == component
                    && record.event == "decoder_sink_complete"
                    && record.value < quotas.len()
                    && record.sequence < quotas[record.value]
            })
            .map(|record| record.time_ns)
            .max()
            .ok_or_else(|| Experiment0Error::MissingScreenEvent {
                receiver: component.clone(),
                event: "initial K decoder completions",
            })?;
        rows.push(ScreenRow {
            schema_version: SCENARIO_SCHEMA_VERSION,
            simulator_version: SIMULATOR_VERSION,
            evidence_class: EVIDENCE_CLASS,
            scenario: scenario.scenario_id.clone(),
            seed: SCREEN_SEED,
            receiver: component,
            source_done_dispatch_ns,
            rank_at_source_done,
            reported_deficit,
            initial_k_data_complete_ns,
            source_done_overtake_ns: initial_k_data_complete_ns
                .saturating_sub(source_done_dispatch_ns),
            application_drops: outcome.application_drops,
            link_drops: outcome.link_drops,
        });
    }
    let prediction_failed = rows.iter().any(|row| row.reported_deficit != 0);
    let verdict = ScreenVerdictRow {
        schema_version: SCENARIO_SCHEMA_VERSION,
        simulator_version: SIMULATOR_VERSION,
        evidence_class: EVIDENCE_CLASS,
        prediction: "E0-a",
        passed: !prediction_failed,
        classification: if prediction_failed {
            "real modeled mechanism"
        } else {
            "prediction retained"
        },
        diagnosis: if prediction_failed {
            "SourceDone on an independent control TCP/path overtakes data still queued or in flight; reliable transport has no cross-connection ordering guarantee"
        } else {
            "SourceDone did not overtake the initial source flight"
        },
    };
    Ok(Experiment0Screen {
        screening_csv: to_csv(&rows)?,
        verdict_csv: to_csv(&[verdict])?,
        prediction_failed,
    })
}

pub fn run_experiment0(seed_count: u64) -> Result<Experiment0Artifacts, Experiment0Error> {
    if seed_count == 0 {
        return Err(Experiment0Error::ZeroSeeds);
    }
    let mut trials = Vec::new();
    for profile in W1RateProfile::ALL {
        for active_receivers in RECEIVER_COUNTS {
            for protocol in ProtocolKind::ALL {
                for seed in 0..seed_count {
                    let scenario =
                        W1Scenario::experiment0(protocol, profile, active_receivers, seed);
                    let outcome = run_checked(&scenario)?;
                    trials.push(trial_row(&scenario, &outcome)?);
                }
            }
        }
    }
    let summaries = summarize(&trials);
    let predictions = evaluate_predictions(&trials)?;
    Ok(Experiment0Artifacts {
        trials_csv: to_csv(&trials)?,
        summaries_csv: to_csv(&summaries)?,
        predictions_csv: to_csv(&predictions)?,
    })
}

fn run_checked(scenario: &W1Scenario) -> Result<W1Outcome, Experiment0Error> {
    let outcome = run_w1(scenario)?;
    if outcome.application_drops != 0 || outcome.link_drops != 0 {
        return Err(Experiment0Error::StrictConservationDrop {
            scenario: scenario.scenario_id.clone(),
            application: outcome.application_drops,
            link: outcome.link_drops,
        });
    }
    let high_water = outcome
        .mailbox_high_water
        .values()
        .copied()
        .max()
        .unwrap_or(0);
    if high_water >= MAILBOX_CAPACITY {
        return Err(Experiment0Error::BindingMailbox {
            scenario: scenario.scenario_id.clone(),
            high_water,
            capacity: MAILBOX_CAPACITY,
        });
    }
    Ok(outcome)
}

fn trial_row(scenario: &W1Scenario, outcome: &W1Outcome) -> Result<TrialRow, Experiment0Error> {
    let sender_completion_ns =
        outcome
            .sender_completion_ns
            .ok_or_else(|| Experiment0Error::SenderIncomplete {
                scenario: scenario.scenario_id.clone(),
            })?;
    let completion = |receiver: usize| {
        outcome
            .completion_times_ns
            .get(receiver)
            .copied()
            .unwrap_or(0)
    };
    let emissions_after_sender_completion = outcome
        .records
        .iter()
        .filter(|record| {
            record.component == "w1_source"
                && record.event == "data_frame_emitted"
                && record.time_ns > sender_completion_ns
        })
        .count();
    Ok(TrialRow {
        schema_version: SCENARIO_SCHEMA_VERSION,
        simulator_version: SIMULATOR_VERSION,
        evidence_class: EVIDENCE_CLASS,
        scenario: scenario.scenario_id.clone(),
        seed: scenario.master_seed,
        rate_profile: scenario.rate_profile.name(),
        active_receivers: scenario.active_receivers,
        protocol: scenario.protocol.name(),
        source_symbols: scenario.source_symbols,
        receiver1_completion_ns: completion(0),
        receiver2_completion_ns: completion(1),
        receiver3_completion_ns: completion(2),
        local_completion_sum_ns: outcome
            .completion_times_ns
            .iter()
            .map(|value| u128::from(*value))
            .sum(),
        barrier_completion_ns: outcome.barrier_completion_ns,
        sender_completion_ns,
        total_emissions: outcome.total_emissions,
        tree0_emissions: outcome.per_tree_emissions[0],
        tree1_emissions: outcome.per_tree_emissions[1],
        ack_flight_tail_emissions: outcome.ack_flight_tail_emissions,
        post_barrier_tail_emissions: outcome.post_barrier_tail_emissions,
        emissions_after_sender_completion,
        positive_round_deficit_reports: outcome.positive_round_deficits,
        round_deficit_sum: outcome.round_deficit_sum,
        maximum_round_deficit: outcome.maximum_round_deficit,
        application_drops: outcome.application_drops,
        link_drops: outcome.link_drops,
        mailbox_high_water: outcome
            .mailbox_high_water
            .values()
            .copied()
            .max()
            .unwrap_or(0),
    })
}

fn summarize(trials: &[TrialRow]) -> Vec<SummaryRow> {
    let mut summaries = Vec::new();
    for profile in W1RateProfile::ALL {
        for active_receivers in RECEIVER_COUNTS {
            for protocol in ProtocolKind::ALL {
                let group: Vec<_> = trials
                    .iter()
                    .filter(|trial| {
                        trial.rate_profile == profile.name()
                            && trial.active_receivers == active_receivers
                            && trial.protocol == protocol.name()
                    })
                    .collect();
                let local: Vec<_> = group
                    .iter()
                    .flat_map(|trial| {
                        [
                            trial.receiver1_completion_ns,
                            trial.receiver2_completion_ns,
                            trial.receiver3_completion_ns,
                        ]
                        .into_iter()
                        .take(active_receivers)
                    })
                    .collect();
                let barriers: Vec<_> = group
                    .iter()
                    .map(|trial| trial.barrier_completion_ns)
                    .collect();
                let emissions: Vec<_> = group.iter().map(|trial| trial.total_emissions).collect();
                let tails: Vec<_> = group
                    .iter()
                    .map(|trial| trial.ack_flight_tail_emissions)
                    .collect();
                let local_sum: u128 = local.iter().map(|value| u128::from(*value)).sum();
                let barrier_sum: u128 = barriers.iter().map(|value| u128::from(*value)).sum();
                let emission_sum: u128 = emissions.iter().map(|value| *value as u128).sum();
                let tail_sum: u128 = tails.iter().map(|value| *value as u128).sum();
                summaries.push(SummaryRow {
                    schema_version: SCENARIO_SCHEMA_VERSION,
                    simulator_version: SIMULATOR_VERSION,
                    evidence_class: EVIDENCE_CLASS,
                    rate_profile: profile.name(),
                    active_receivers,
                    protocol: protocol.name(),
                    trials: group.len(),
                    receiver_observations: local.len(),
                    local_completion_sum_ns: local_sum,
                    local_completion_mean_ns: integer_mean_u64(local_sum, local.len()),
                    local_completion_p95_ns: percentile95(&local),
                    barrier_completion_sum_ns: barrier_sum,
                    barrier_completion_mean_ns: integer_mean_u64(barrier_sum, barriers.len()),
                    barrier_completion_p95_ns: percentile95(&barriers),
                    total_emissions_sum: emission_sum,
                    total_emissions_mean: integer_mean_usize(emission_sum, emissions.len()),
                    total_emissions_p95: percentile95(&emissions),
                    ack_flight_tail_sum: tail_sum,
                    ack_flight_tail_mean: integer_mean_usize(tail_sum, tails.len()),
                    ack_flight_tail_p95: percentile95(&tails),
                    positive_round_deficit_reports: group
                        .iter()
                        .map(|trial| trial.positive_round_deficit_reports)
                        .sum(),
                    round_deficit_sum: group.iter().map(|trial| trial.round_deficit_sum).sum(),
                    maximum_round_deficit: group
                        .iter()
                        .map(|trial| trial.maximum_round_deficit)
                        .max()
                        .unwrap_or(0),
                    application_drops: group.iter().map(|trial| trial.application_drops).sum(),
                    link_drops: group.iter().map(|trial| trial.link_drops).sum(),
                    maximum_mailbox_high_water: group
                        .iter()
                        .map(|trial| trial.mailbox_high_water)
                        .max()
                        .unwrap_or(0),
                });
            }
        }
    }
    summaries
}

fn evaluate_predictions(trials: &[TrialRow]) -> Result<Vec<PredictionRow>, Experiment0Error> {
    let rounds: Vec<_> = trials
        .iter()
        .filter(|trial| trial.protocol == ProtocolKind::PooledRounds.name())
        .collect();
    let positive_deficits: usize = rounds
        .iter()
        .map(|trial| trial.positive_round_deficit_reports)
        .sum();
    let deficit_sum: usize = rounds.iter().map(|trial| trial.round_deficit_sum).sum();
    let maximum_deficit = rounds
        .iter()
        .map(|trial| trial.maximum_round_deficit)
        .max()
        .unwrap_or(0);
    let e0a = PredictionRow {
        schema_version: SCENARIO_SCHEMA_VERSION,
        simulator_version: SIMULATOR_VERSION,
        evidence_class: EVIDENCE_CLASS,
        prediction: "E0-a",
        scope: "all pooled-rounds strict-conservation trials",
        passed: positive_deficits == 0,
        observations: rounds.len(),
        violations: rounds
            .iter()
            .filter(|trial| trial.positive_round_deficit_reports != 0)
            .count(),
        observed_min_margin_ns: -(positive_deficits as i128),
        observed_max_margin_ns: 0,
        threshold_ns: 0,
        detail: format!(
            "{positive_deficits} positive cached deficit reports totaling {deficit_sum} missing DoF; maximum report {maximum_deficit}; zero application/link drops"
        ),
    };

    let base =
        W1Scenario::experiment0(ProtocolKind::PooledRounds, W1RateProfile::Homogeneous, 3, 0);
    let control_latency_delta_ns = base
        .control_propagation_ns
        .iter()
        .copied()
        .max()
        .unwrap_or(0)
        .saturating_mul(2)
        .saturating_add(base.timer_interval_ns.saturating_mul(2));
    let mut local_deltas = Vec::new();
    for round in &rounds {
        let carousel = paired(trials, round, ProtocolKind::PooledCarousel.name())?;
        for receiver in 0..round.active_receivers {
            let left = receiver_completion(round, receiver);
            let right = receiver_completion(carousel, receiver);
            local_deltas.push(left.abs_diff(right));
        }
    }
    let maximum_local_delta = local_deltas.iter().copied().max().unwrap_or(0);
    let e0b_violations = local_deltas
        .iter()
        .filter(|delta| **delta > control_latency_delta_ns)
        .count();
    let e0b = PredictionRow {
        schema_version: SCENARIO_SCHEMA_VERSION,
        simulator_version: SIMULATOR_VERSION,
        evidence_class: EVIDENCE_CLASS,
        prediction: "E0-b",
        scope: "paired pooled rounds/carousel receiver-local completion",
        passed: e0b_violations == 0,
        observations: local_deltas.len(),
        violations: e0b_violations,
        observed_min_margin_ns: i128::from(control_latency_delta_ns)
            - i128::from(maximum_local_delta),
        observed_max_margin_ns: i128::from(control_latency_delta_ns),
        threshold_ns: control_latency_delta_ns,
        detail: format!(
            "maximum paired local-completion delta {maximum_local_delta} ns; tolerance is maximum modeled control RTT plus two timer quanta"
        ),
    };

    let carousel: Vec<_> = trials
        .iter()
        .filter(|trial| trial.protocol == ProtocolKind::PooledCarousel.name())
        .collect();
    let e0c_violations = carousel
        .iter()
        .filter(|trial| {
            trial.total_emissions < trial.source_symbols
                || trial.ack_flight_tail_emissions != trial.total_emissions - trial.source_symbols
                || trial.post_barrier_tail_emissions > trial.ack_flight_tail_emissions
                || trial.emissions_after_sender_completion != 0
                || trial.application_drops != 0
                || trial.link_drops != 0
        })
        .count();
    let maximum_tail = carousel
        .iter()
        .map(|trial| trial.ack_flight_tail_emissions)
        .max()
        .unwrap_or(0);
    let e0c = PredictionRow {
        schema_version: SCENARIO_SCHEMA_VERSION,
        simulator_version: SIMULATOR_VERSION,
        evidence_class: EVIDENCE_CLASS,
        prediction: "E0-c",
        scope: "all pooled-carousel strict-conservation trials",
        passed: e0c_violations == 0,
        observations: carousel.len(),
        violations: e0c_violations,
        observed_min_margin_ns: 0,
        observed_max_margin_ns: 0,
        threshold_ns: 0,
        detail: format!(
            "maximum ACK-flight tail {maximum_tail} frames; no emission may follow sender completion"
        ),
    };

    let crossed: Vec<_> = trials
        .iter()
        .filter(|trial| {
            trial.rate_profile == W1RateProfile::CrossedHeterogeneous.name()
                && trial.protocol == ProtocolKind::PooledCarousel.name()
        })
        .collect();
    let mut pooling_margins = Vec::new();
    for carousel in crossed {
        let rounds = paired(trials, carousel, ProtocolKind::PooledRounds.name())?;
        let pooled_best = carousel
            .barrier_completion_ns
            .min(rounds.barrier_completion_ns);
        let mut striped_best = u64::MAX;
        for protocol in [
            ProtocolKind::EqualSplitStriping,
            ProtocolKind::RateProportionalStriping,
            ProtocolKind::PerStripeFec,
        ] {
            striped_best =
                striped_best.min(paired(trials, carousel, protocol.name())?.barrier_completion_ns);
        }
        pooling_margins.push(i128::from(striped_best) - i128::from(pooled_best));
    }
    let e0d_violations = pooling_margins
        .iter()
        .filter(|margin| **margin <= 0)
        .count();
    let e0d = PredictionRow {
        schema_version: SCENARIO_SCHEMA_VERSION,
        simulator_version: SIMULATOR_VERSION,
        evidence_class: EVIDENCE_CLASS,
        prediction: "E0-d",
        scope: "crossed-heterogeneous paired barrier completion, best pooled vs best striped",
        passed: e0d_violations == 0,
        observations: pooling_margins.len(),
        violations: e0d_violations,
        observed_min_margin_ns: pooling_margins.iter().copied().min().unwrap_or(0),
        observed_max_margin_ns: pooling_margins.iter().copied().max().unwrap_or(0),
        threshold_ns: 0,
        detail:
            "positive margin means the fastest pooled protocol beat the fastest striped baseline"
                .to_owned(),
    };
    Ok(vec![e0a, e0b, e0c, e0d])
}

fn paired<'a>(
    trials: &'a [TrialRow],
    reference: &TrialRow,
    protocol: &str,
) -> Result<&'a TrialRow, Experiment0Error> {
    trials
        .iter()
        .find(|trial| {
            trial.rate_profile == reference.rate_profile
                && trial.active_receivers == reference.active_receivers
                && trial.seed == reference.seed
                && trial.protocol == protocol
        })
        .ok_or_else(|| {
            Experiment0Error::MissingPair(format!(
                "profile={} receivers={} seed={} protocol={protocol}",
                reference.rate_profile, reference.active_receivers, reference.seed
            ))
        })
}

fn receiver_completion(trial: &TrialRow, receiver: usize) -> u64 {
    match receiver {
        0 => trial.receiver1_completion_ns,
        1 => trial.receiver2_completion_ns,
        2 => trial.receiver3_completion_ns,
        _ => 0,
    }
}

fn percentile95<T: Copy + Ord + Default>(values: &[T]) -> T {
    if values.is_empty() {
        return T::default();
    }
    let mut ordered = values.to_vec();
    ordered.sort_unstable();
    let rank = (95usize.saturating_mul(ordered.len()).saturating_add(99)) / 100;
    ordered[rank.saturating_sub(1).min(ordered.len() - 1)]
}

fn integer_mean_u64(sum: u128, count: usize) -> u64 {
    if count == 0 {
        return 0;
    }
    u64::try_from(sum / count as u128).unwrap_or(u64::MAX)
}

fn integer_mean_usize(sum: u128, count: usize) -> usize {
    if count == 0 {
        return 0;
    }
    usize::try_from(sum / count as u128).unwrap_or(usize::MAX)
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
    fn nearest_rank_percentile_is_integer_and_deterministic() {
        let values: Vec<_> = (1..=20).collect();
        assert_eq!(percentile95(&values), 19);
        assert_eq!(percentile95::<u64>(&[]), 0);
    }

    #[test]
    fn strict_conservation_screen_classifies_cross_connection_overtake() {
        let screen = run_experiment0_screen().expect("screen completes");
        assert!(screen.prediction_failed);
        assert!(!screen.screening_csv.contains("real modeled mechanism"));
        assert!(screen.verdict_csv.contains("real modeled mechanism"));
        assert!(screen.screening_csv.contains(",0,0\n"));
    }
}
