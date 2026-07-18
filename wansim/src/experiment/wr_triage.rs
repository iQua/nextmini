use serde::Serialize;
use thiserror::Error;

use crate::metrics::Record;
use crate::scenario::{
    CloudProfileKind, CloudScenario, ReceiverAdmissionPolicy, RegistrationOrder,
    WR_FOREGROUND_START_NS, WrProtocol, WrRunConfig, WrRunError, run_wr_triage,
};
use crate::{SCENARIO_SCHEMA_VERSION, SIMULATOR_VERSION};

const EVIDENCE_CLASS: &str =
    "model-level WR failure triage; ideal DoF abstraction; not a WAN measurement";
const SOURCE_SYMBOLS: usize = 8_192;
const SLOW_PEER_ID: usize = 1;
const SLOW_PEER_INDEX: usize = 0;
const STALL_TIMEOUT_NS: u64 = 15_000_000_000;
const TIMELINE_BUCKET_NS: u64 = 250_000_000;

#[derive(Clone, Debug)]
pub struct WrTriageArtifacts {
    pub causal_events_csv: String,
    pub fatal_window_events_csv: String,
    pub timeline_csv: String,
    pub event_class_counts_csv: String,
    pub summary_csv: String,
    pub verdict: String,
}

#[derive(Debug, Error)]
pub enum WrTriageError {
    #[error("WR triage expected a liveness failure but the cell did not fail")]
    MissingFailure,
    #[error("WR triage did not record the sender liveness abort")]
    MissingAbort,
    #[error("WR triage did not record both sender liveness clocks")]
    MissingLivenessClock,
    #[error(transparent)]
    Run(#[from] WrRunError),
    #[error(transparent)]
    Csv(#[from] csv::Error),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TriageVerdict {
    AGranularity,
    BRankFrozen,
    CAckPathOrModel,
    CEmissionCapArtifact,
}

impl TriageVerdict {
    const fn name(self) -> &'static str {
        match self {
            Self::AGranularity => "a-rank-advanced-watermark-stuck",
            Self::BRankFrozen => "b-rank-frozen",
            Self::CAckPathOrModel => "c-ack-path-or-model-artifact",
            Self::CEmissionCapArtifact => "c-simulator-emission-cap-artifact",
        }
    }
}

#[derive(Clone, Debug, Serialize)]
struct SummaryRow {
    schema_version: u32,
    simulator_version: &'static str,
    evidence_class: &'static str,
    profile: &'static str,
    placement: &'static str,
    utilization_percent: u8,
    jitter: bool,
    protocol: &'static str,
    admission: &'static str,
    source_symbols: usize,
    seed: u64,
    slow_receiver: usize,
    slow_service_factor: usize,
    source_pacer_enabled: bool,
    acknowledgement_progress_units: u64,
    stopped_at_ns: u64,
    failure: String,
    abort_time_ns: u64,
    fatal_window_start_ns: u64,
    rank_at_window_start: usize,
    rank_at_abort: usize,
    innovative_rank_advance: usize,
    receiver_watermark_at_window_start: u64,
    receiver_watermark_at_abort: u64,
    sender_joined_watermark_at_window_start: u64,
    sender_joined_watermark_at_abort: u64,
    block_ack_emissions_in_window: usize,
    sender_ack_joins_in_window: usize,
    sender_progress_joins_in_window: usize,
    last_ack_seen_ns: u64,
    last_ack_progress_ns: u64,
    silence_clock_age_at_abort_ns: u64,
    progress_clock_age_at_abort_ns: u64,
    total_emissions: u64,
    total_emissions_over_k_ppm: u64,
    configured_emission_ceiling: u64,
    emission_ceiling_reached: bool,
    last_source_emission_ns: u64,
    last_slow_receiver_arrival_ns: u64,
    last_rank_observation_ns: u64,
    verdict: &'static str,
}

#[derive(Clone, Debug, Serialize)]
struct TimelineRow {
    schema_version: u32,
    simulator_version: &'static str,
    evidence_class: &'static str,
    bucket_start_ns: u64,
    bucket_end_ns: u64,
    source_emissions: usize,
    slow_receiver_data_frame_arrivals: usize,
    slow_receiver_inbox_accepts: usize,
    slow_receiver_inbox_drops: usize,
    slow_receiver_decoder_completions: usize,
    innovative_rank_advance: usize,
    rank_at_bucket_end: usize,
    quantized_receiver_watermark_at_bucket_end: u64,
    block_ack_emissions: usize,
    sender_ack_joins: usize,
    sender_progress_joins: usize,
    sender_joined_watermark_at_bucket_end: u64,
    last_ack_seen_ns: u64,
    last_ack_progress_ns: u64,
    silence_clock_age_ns: u64,
    progress_clock_age_ns: u64,
    stall_budget_remaining_ns: u64,
    liveness_abort: bool,
}

#[derive(Clone, Debug, Serialize)]
struct EventClassCountRow<'a> {
    schema_version: u32,
    simulator_version: &'static str,
    evidence_class: &'static str,
    event_class: &'a str,
    count: u64,
}

pub fn run_fixed_wr_triage() -> Result<WrTriageArtifacts, WrTriageError> {
    let config = WrRunConfig {
        cloud: CloudScenario::built_in(CloudProfileKind::DigitaloceanLike, 1)
            .map_err(WrRunError::from)?,
        protocol: WrProtocol::Carousel,
        source_symbols: SOURCE_SYMBOLS,
        background_utilization_percent: 70,
        jitter_enabled: true,
        seed: 15,
        ack_cadence_multiplier: 2,
        receiver_admission: ReceiverAdmissionPolicy::HybridDrop,
        slow_receiver: Some(0),
        concurrent_sessions: 1,
        registration_order: RegistrationOrder::Forward,
    };
    let outcome = run_wr_triage(&config)?;
    let failure = outcome.failure.ok_or(WrTriageError::MissingFailure)?;
    let abort = outcome
        .records
        .iter()
        .find(|record| {
            record.event == "carousel_liveness_stall_abort" && record.flow_id == SLOW_PEER_ID
        })
        .ok_or(WrTriageError::MissingAbort)?;
    let abort_time_ns = abort.time_ns;
    let last_ack_seen_ns = latest_liveness_clock(
        &outcome.records,
        "carousel_liveness_last_ack_seen",
        abort_time_ns,
    )
    .ok_or(WrTriageError::MissingLivenessClock)?;
    let last_ack_progress_ns = latest_liveness_clock(
        &outcome.records,
        "carousel_liveness_last_ack_progress",
        abort_time_ns,
    )
    .ok_or(WrTriageError::MissingLivenessClock)?;
    let fatal_window_start_ns = last_ack_progress_ns;
    let start_progress = receiver_progress_at(&outcome.records, fatal_window_start_ns);
    let end_progress = receiver_progress_at(&outcome.records, abort_time_ns);
    let sender_start = sender_watermark_at(&outcome.records, fatal_window_start_ns);
    let sender_end = sender_watermark_at(&outcome.records, abort_time_ns);
    let block_ack_emissions = count_between(
        &outcome.records,
        "block_ack_submitted",
        fatal_window_start_ns,
        abort_time_ns,
        |_| true,
    );
    let sender_ack_joins = count_between(
        &outcome.records,
        "carousel_ack_join_progress",
        fatal_window_start_ns,
        abort_time_ns,
        |record| record.sequence == SLOW_PEER_INDEX,
    ) + count_between(
        &outcome.records,
        "carousel_ack_join_noop",
        fatal_window_start_ns,
        abort_time_ns,
        |record| record.sequence == SLOW_PEER_INDEX,
    );
    let sender_progress_joins = count_between(
        &outcome.records,
        "carousel_ack_join_progress",
        fatal_window_start_ns,
        abort_time_ns,
        |record| record.sequence == SLOW_PEER_INDEX,
    );
    let rank_advance = end_progress.0.saturating_sub(start_progress.0);
    let final_incomplete_watermark = outcome.acknowledgement_progress_units.saturating_sub(1);
    let total_emissions = event_count(&outcome.records, "data_frame_emitted");
    let configured_emission_ceiling = u64_from_usize(outcome.configured_emission_ceiling);
    let emission_ceiling_reached = total_emissions >= configured_emission_ceiling;
    let verdict = classify(
        rank_advance,
        start_progress.1,
        end_progress.1,
        sender_start,
        sender_end,
        final_incomplete_watermark,
        block_ack_emissions,
        sender_ack_joins,
        emission_ceiling_reached,
    );
    let total_emissions_over_k_ppm = total_emissions.saturating_mul(1_000_000)
        / u64::try_from(SOURCE_SYMBOLS).expect("fixed K fits u64");
    let last_source_emission_ns = latest_event_time(&outcome.records, "data_frame_emitted");
    let last_slow_receiver_arrival_ns =
        latest_event_time(&outcome.records, "runtime_command_enqueue_data");
    let last_rank_observation_ns =
        latest_event_time(&outcome.records, "carousel_progress_observed");
    let summary = SummaryRow {
        schema_version: SCENARIO_SCHEMA_VERSION,
        simulator_version: SIMULATOR_VERSION,
        evidence_class: EVIDENCE_CLASS,
        profile: "digitalocean-like",
        placement: "west-origin",
        utilization_percent: 70,
        jitter: true,
        protocol: "carousel",
        admission: "hybrid-drop",
        source_symbols: SOURCE_SYMBOLS,
        seed: 15,
        slow_receiver: 0,
        slow_service_factor: 125,
        source_pacer_enabled: false,
        acknowledgement_progress_units: outcome.acknowledgement_progress_units,
        stopped_at_ns: outcome.stopped_at_ns,
        failure,
        abort_time_ns,
        fatal_window_start_ns,
        rank_at_window_start: start_progress.0,
        rank_at_abort: end_progress.0,
        innovative_rank_advance: rank_advance,
        receiver_watermark_at_window_start: start_progress.1,
        receiver_watermark_at_abort: end_progress.1,
        sender_joined_watermark_at_window_start: sender_start,
        sender_joined_watermark_at_abort: sender_end,
        block_ack_emissions_in_window: block_ack_emissions,
        sender_ack_joins_in_window: sender_ack_joins,
        sender_progress_joins_in_window: sender_progress_joins,
        last_ack_seen_ns,
        last_ack_progress_ns,
        silence_clock_age_at_abort_ns: abort_time_ns.saturating_sub(last_ack_seen_ns),
        progress_clock_age_at_abort_ns: abort_time_ns.saturating_sub(last_ack_progress_ns),
        total_emissions,
        total_emissions_over_k_ppm,
        configured_emission_ceiling,
        emission_ceiling_reached,
        last_source_emission_ns,
        last_slow_receiver_arrival_ns,
        last_rank_observation_ns,
        verdict: verdict.name(),
    };
    let timeline = timeline_rows(&outcome.records, outcome.stopped_at_ns);
    let event_counts = outcome
        .event_class_counts
        .iter()
        .map(|row| EventClassCountRow {
            schema_version: SCENARIO_SCHEMA_VERSION,
            simulator_version: SIMULATOR_VERSION,
            evidence_class: EVIDENCE_CLASS,
            event_class: &row.event_class,
            count: row.count,
        })
        .collect::<Vec<_>>();
    Ok(WrTriageArtifacts {
        causal_events_csv: to_csv(&outcome.records)?,
        fatal_window_events_csv: to_csv(
            &outcome
                .records
                .iter()
                .filter(|record| {
                    record.time_ns >= fatal_window_start_ns && record.time_ns <= abort_time_ns
                })
                .cloned()
                .collect::<Vec<_>>(),
        )?,
        timeline_csv: to_csv(&timeline)?,
        event_class_counts_csv: to_csv(&event_counts)?,
        summary_csv: to_csv(&[summary])?,
        verdict: verdict.name().to_owned(),
    })
}

#[allow(clippy::too_many_arguments)]
fn classify(
    rank_advance: usize,
    receiver_start: u64,
    receiver_end: u64,
    sender_start: u64,
    sender_end: u64,
    final_incomplete_watermark: u64,
    block_ack_emissions: usize,
    sender_ack_joins: usize,
    emission_ceiling_reached: bool,
) -> TriageVerdict {
    if emission_ceiling_reached {
        return TriageVerdict::CEmissionCapArtifact;
    }
    if rank_advance == 0 {
        return TriageVerdict::BRankFrozen;
    }
    if receiver_start == final_incomplete_watermark
        && receiver_end == final_incomplete_watermark
        && sender_start == final_incomplete_watermark
        && sender_end == final_incomplete_watermark
        && block_ack_emissions > 0
        && sender_ack_joins > 0
    {
        TriageVerdict::AGranularity
    } else {
        TriageVerdict::CAckPathOrModel
    }
}

fn latest_liveness_clock(records: &[Record], event: &str, through_ns: u64) -> Option<u64> {
    records
        .iter()
        .filter(|record| {
            record.event == event && record.flow_id == SLOW_PEER_ID && record.time_ns <= through_ns
        })
        .max_by_key(|record| record.time_ns)
        .map(|record| u64_from_usize(record.value))
}

fn receiver_progress_at(records: &[Record], through_ns: u64) -> (usize, u64) {
    records
        .iter()
        .filter(|record| {
            record.event == "carousel_progress_observed" && record.time_ns <= through_ns
        })
        .max_by_key(|record| record.time_ns)
        .map_or((0, 0), |record| {
            (record.value, u64_from_usize(record.bytes))
        })
}

fn sender_watermark_at(records: &[Record], through_ns: u64) -> u64 {
    records
        .iter()
        .filter(|record| {
            matches!(
                record.event,
                "carousel_ack_join_progress" | "carousel_ack_join_noop"
            ) && record.sequence == SLOW_PEER_INDEX
                && record.time_ns <= through_ns
        })
        .max_by_key(|record| record.time_ns)
        .map_or(0, |record| u64_from_usize(record.value))
}

fn count_between(
    records: &[Record],
    event: &str,
    start_ns: u64,
    end_ns: u64,
    predicate: impl Fn(&Record) -> bool,
) -> usize {
    records
        .iter()
        .filter(|record| {
            record.event == event
                && record.time_ns >= start_ns
                && record.time_ns <= end_ns
                && predicate(record)
        })
        .count()
}

fn event_count(records: &[Record], event: &str) -> u64 {
    u64::try_from(
        records
            .iter()
            .filter(|record| record.event == event)
            .count(),
    )
    .unwrap_or(u64::MAX)
}

fn latest_event_time(records: &[Record], event: &str) -> u64 {
    records
        .iter()
        .filter(|record| record.event == event)
        .map(|record| record.time_ns)
        .max()
        .unwrap_or(0)
}

fn timeline_rows(records: &[Record], stopped_at_ns: u64) -> Vec<TimelineRow> {
    let mut rows = Vec::new();
    let mut cursor = 0;
    let mut rank = 0_usize;
    let mut receiver_watermark = 0_u64;
    let mut sender_watermark = 0_u64;
    let mut last_seen = WR_FOREGROUND_START_NS;
    let mut last_progress = WR_FOREGROUND_START_NS;
    let mut bucket_start = WR_FOREGROUND_START_NS;
    while bucket_start < stopped_at_ns {
        let bucket_end = bucket_start
            .saturating_add(TIMELINE_BUCKET_NS)
            .min(stopped_at_ns);
        let rank_at_start = rank;
        let mut source_emissions = 0;
        let mut arrivals = 0;
        let mut accepts = 0;
        let mut drops = 0;
        let mut decoder_completions = 0;
        let mut block_ack_emissions = 0;
        let mut sender_ack_joins = 0;
        let mut sender_progress_joins = 0;
        let mut liveness_abort = false;
        while let Some(record) = records.get(cursor) {
            if record.time_ns >= bucket_end {
                break;
            }
            cursor += 1;
            if record.time_ns < bucket_start {
                apply_state(
                    record,
                    &mut rank,
                    &mut receiver_watermark,
                    &mut sender_watermark,
                    &mut last_seen,
                    &mut last_progress,
                );
                continue;
            }
            match record.event {
                "data_frame_emitted" => source_emissions += 1,
                "runtime_command_enqueue_data" => arrivals += 1,
                "data_inbox_enqueue" => accepts += 1,
                "data_inbox_drop_after_tcp_ack" => drops += 1,
                "decoder_sink_complete" => decoder_completions += 1,
                "block_ack_submitted" => block_ack_emissions += 1,
                "carousel_ack_join_progress" if record.sequence == SLOW_PEER_INDEX => {
                    sender_ack_joins += 1;
                    sender_progress_joins += 1;
                }
                "carousel_ack_join_noop" if record.sequence == SLOW_PEER_INDEX => {
                    sender_ack_joins += 1;
                }
                "carousel_liveness_stall_abort" | "carousel_liveness_silent_abort"
                    if record.flow_id == SLOW_PEER_ID =>
                {
                    liveness_abort = true;
                }
                _ => {}
            }
            apply_state(
                record,
                &mut rank,
                &mut receiver_watermark,
                &mut sender_watermark,
                &mut last_seen,
                &mut last_progress,
            );
        }
        let silence_age = bucket_end.saturating_sub(last_seen);
        let progress_age = bucket_end.saturating_sub(last_progress);
        rows.push(TimelineRow {
            schema_version: SCENARIO_SCHEMA_VERSION,
            simulator_version: SIMULATOR_VERSION,
            evidence_class: EVIDENCE_CLASS,
            bucket_start_ns: bucket_start,
            bucket_end_ns: bucket_end,
            source_emissions,
            slow_receiver_data_frame_arrivals: arrivals,
            slow_receiver_inbox_accepts: accepts,
            slow_receiver_inbox_drops: drops,
            slow_receiver_decoder_completions: decoder_completions,
            innovative_rank_advance: rank.saturating_sub(rank_at_start),
            rank_at_bucket_end: rank,
            quantized_receiver_watermark_at_bucket_end: receiver_watermark,
            block_ack_emissions,
            sender_ack_joins,
            sender_progress_joins,
            sender_joined_watermark_at_bucket_end: sender_watermark,
            last_ack_seen_ns: last_seen,
            last_ack_progress_ns: last_progress,
            silence_clock_age_ns: silence_age,
            progress_clock_age_ns: progress_age,
            stall_budget_remaining_ns: STALL_TIMEOUT_NS.saturating_sub(progress_age),
            liveness_abort,
        });
        bucket_start = bucket_end;
    }
    rows
}

fn apply_state(
    record: &Record,
    rank: &mut usize,
    receiver_watermark: &mut u64,
    sender_watermark: &mut u64,
    last_seen: &mut u64,
    last_progress: &mut u64,
) {
    match record.event {
        "carousel_progress_observed" => {
            *rank = record.value;
            *receiver_watermark = u64_from_usize(record.bytes);
        }
        "carousel_ack_join_progress" | "carousel_ack_join_noop"
            if record.sequence == SLOW_PEER_INDEX =>
        {
            *sender_watermark = u64_from_usize(record.value);
        }
        "carousel_liveness_last_ack_seen" if record.flow_id == SLOW_PEER_ID => {
            *last_seen = u64_from_usize(record.value);
        }
        "carousel_liveness_last_ack_progress" if record.flow_id == SLOW_PEER_ID => {
            *last_progress = u64_from_usize(record.value);
        }
        _ => {}
    }
}

fn u64_from_usize(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
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
    fn triage_classifier_requires_rank_motion_final_watermark_and_live_ack_path() {
        assert_eq!(
            classify(7, 481, 481, 481, 481, 481, 50, 50, false),
            TriageVerdict::AGranularity
        );
        assert_eq!(
            classify(0, 481, 481, 481, 481, 481, 50, 50, false),
            TriageVerdict::BRankFrozen
        );
        assert_eq!(
            classify(7, 480, 481, 480, 481, 481, 50, 1, false),
            TriageVerdict::CAckPathOrModel
        );
        assert_eq!(
            classify(7, 481, 481, 481, 481, 481, 0, 0, false),
            TriageVerdict::CAckPathOrModel
        );
    }

    #[test]
    fn emission_ceiling_takes_precedence_over_surface_rank_freeze() {
        assert_eq!(
            classify(0, 409, 409, 409, 409, 481, 92, 93, true),
            TriageVerdict::CEmissionCapArtifact
        );
    }
}
