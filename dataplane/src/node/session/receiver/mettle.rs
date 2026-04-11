use nextmini_messages::lossless_session::{
    self, FecScheme, LosslessSessionMode, MettleReplayWindow, NeedReport,
};
use tokio::time::{Duration, Instant};
use tracing::{debug, info};

use crate::node::session::api::InboundFrame;
use crate::node::session::mettle::decoder::Decoder;
use crate::node::session::mettle::encoder::MettleBin;
use crate::node::session::mettle::params::{CodedRate, MettleParams};

pub(super) struct MettleReceiver {
    decoder: Decoder,
    last_source_done_round_id: Option<u32>,
    last_round_need: Option<NeedReport>,
    complete_reported: bool,
    last_reported_stalled_source_id: Option<u64>,
    last_reported_decoded_prefix_len: Option<u64>,
    last_reported_replay_end_bin_id: Option<u64>,
    current_repair_burst_bins: u64,
    last_progress_at: Option<Instant>,
    last_streaming_need_at: Option<Instant>,
}

const MIN_REPAIR_BURST_BINS: u64 = 64;
const REPAIR_STALL_LOOKAHEAD_SOURCES: u64 = 16;
const STREAMING_STALL_INTERVAL: Duration = Duration::from_millis(250);
const STREAMING_NEED_MIN_GAP: Duration = Duration::from_millis(250);

impl MettleReceiver {
    pub(super) fn new(params: MettleParams, total_sources: u64) -> Self {
        Self {
            decoder: Decoder::new(params, total_sources),
            last_source_done_round_id: None,
            last_round_need: None,
            complete_reported: false,
            last_reported_stalled_source_id: None,
            last_reported_decoded_prefix_len: None,
            last_reported_replay_end_bin_id: None,
            current_repair_burst_bins: MIN_REPAIR_BURST_BINS,
            last_progress_at: None,
            last_streaming_need_at: None,
        }
    }

    pub(super) fn last_source_done_round_id(&self) -> Option<u32> {
        self.last_source_done_round_id
    }

    pub(super) async fn handle_mettle_symbol_frame(
        &mut self,
        shared: &mut super::ReceiverShared,
        frame: InboundFrame,
    ) {
        let Some(manifest) = shared.manifest.as_ref() else {
            return;
        };
        let LosslessSessionMode::Fec(fec_mode) = manifest.mode.clone() else {
            return;
        };
        if fec_mode.scheme_kind() != Some(FecScheme::MettleV1) {
            return;
        }
        let Some((_, symbol, payload)) = lossless_session::decode_mettle_symbol(&frame.bytes)
        else {
            return;
        };
        if manifest.validate_mettle_symbol(&symbol).is_err() {
            return;
        }
        if self.complete_reported {
            return;
        }

        let decoded_prefix_before = self.decoder.decoded_prefix_len();
        shared.mark_first_payload_unit();
        let decoded = self.decoder.receive_bin(MettleBin {
            bin_id: symbol.bin_id,
            degree: symbol.degree,
            xor_source_id: symbol.xor_source_id,
            xor_source_sig: symbol.xor_source_sig,
            payload: payload.to_vec(),
        });
        for source in decoded {
            shared.write_block(source.source_id, &source.payload).await;
            shared.complete_blocks.insert(source.source_id);
        }

        let decoded_prefix_after = self.decoder.decoded_prefix_len();
        if decoded_prefix_after > decoded_prefix_before {
            self.last_progress_at = Some(Instant::now());
            if matches!(self.last_round_need, Some(NeedReport::Mettle { .. })) {
                self.last_round_need = None;
            }
        }
        self.maybe_send_streaming_feedback(shared).await;
    }

    pub(super) async fn handle_source_done(
        &mut self,
        shared: &super::ReceiverShared,
        round_id: u32,
    ) {
        if let Some(last_round_id) = self.last_source_done_round_id {
            if round_id < last_round_id {
                debug!(
                    session_id = shared.session_id,
                    round_id, last_round_id, "Lossless METTLE receiver dropped stale SourceDone"
                );
                return;
            }
            if round_id == last_round_id {
                if let Some(report) = self.last_round_need.clone() {
                    debug!(
                        session_id = shared.session_id,
                        round_id,
                        "Lossless METTLE receiver replayed cached Need for duplicate SourceDone"
                    );
                    shared.send_fec_need(last_round_id, &report).await;
                    self.complete_reported = matches!(report, NeedReport::Complete);
                }
                return;
            }
        }

        self.last_source_done_round_id = Some(round_id);
        let Some(report) = self
            .last_round_need
            .clone()
            .or_else(|| self.need_report(shared))
        else {
            return;
        };
        match &report {
            NeedReport::Complete => {
                info!(
                    session_id = shared.session_id,
                    round_id,
                    decoded_prefix_len = self.decoder.decoded_prefix_len(),
                    "Lossless METTLE receiver reported completion after tail"
                );
            }
            NeedReport::Mettle { window } => {
                info!(
                    session_id = shared.session_id,
                    round_id,
                    stalled_source_id = window.stalled_source_id,
                    replay_start_bin_id = window.replay_start_bin_id,
                    replay_end_bin_id = window.replay_end_bin_id,
                    replay_window_bins = window
                        .replay_end_bin_id
                        .saturating_sub(window.replay_start_bin_id),
                    decoded_prefix_len = self.decoder.decoded_prefix_len(),
                    current_repair_burst_bins = self.current_repair_burst_bins,
                    "Lossless METTLE receiver replayed tail repair state"
                );
            }
            NeedReport::Plain { .. } | NeedReport::Fec { .. } => {}
        }
        self.last_round_need = Some(report.clone());
        shared.send_fec_need(round_id, &report).await;
        self.complete_reported = matches!(report, NeedReport::Complete);
    }

    pub(super) fn is_complete(&self) -> bool {
        self.complete_reported
    }

    pub(super) async fn handle_idle(&mut self, shared: &super::ReceiverShared) {
        self.maybe_send_streaming_feedback(shared).await;
    }

    fn need_report(&mut self, shared: &super::ReceiverShared) -> Option<NeedReport> {
        let plan = shared.plan?;
        if plan.total_blocks() == 0 || shared.has_all_blocks() || self.decoder.is_complete() {
            return Some(NeedReport::Complete);
        }

        let stall_hint = self.decoder.stall_hint(REPAIR_STALL_LOOKAHEAD_SOURCES)?;
        let stalled_source_id = stall_hint.stalled_source_id;
        let decoded_prefix_len = self.decoder.decoded_prefix_len();
        let replay_base_bin_id = stall_hint.replay_start_bin_id;
        let replay_window_end_bin_id = stall_hint.replay_end_limit_bin_id;
        let full_window_bins = replay_window_end_bin_id
            .saturating_sub(replay_base_bin_id)
            .max(1);
        let made_progress = match (
            self.last_reported_stalled_source_id,
            self.last_reported_decoded_prefix_len,
        ) {
            (Some(last_stalled), Some(last_prefix)) => {
                stalled_source_id != last_stalled || decoded_prefix_len > last_prefix
            }
            _ => true,
        };

        let replay_start_bin_id = if made_progress {
            replay_base_bin_id
        } else {
            self.last_reported_replay_end_bin_id
                .unwrap_or(replay_base_bin_id)
        };
        let (burst_bins, replay_start_bin_id, replay_end_bin_id) = if made_progress {
            let burst_bins = MIN_REPAIR_BURST_BINS.min(full_window_bins).max(1);
            let replay_end_bin_id =
                (replay_start_bin_id + burst_bins).min(replay_window_end_bin_id);
            (burst_bins, replay_start_bin_id, replay_end_bin_id)
        } else if replay_start_bin_id < replay_window_end_bin_id {
            let burst_bins = self
                .current_repair_burst_bins
                .saturating_mul(2)
                .min(full_window_bins)
                .max(1);
            let replay_end_bin_id =
                (replay_start_bin_id + burst_bins).min(replay_window_end_bin_id);
            (burst_bins, replay_start_bin_id, replay_end_bin_id)
        } else {
            (
                full_window_bins,
                replay_base_bin_id,
                replay_window_end_bin_id,
            )
        };

        self.current_repair_burst_bins = burst_bins;
        self.last_reported_stalled_source_id = Some(stalled_source_id);
        self.last_reported_decoded_prefix_len = Some(decoded_prefix_len);
        self.last_reported_replay_end_bin_id = Some(replay_end_bin_id);

        debug!(
            session_id = shared.session_id,
            stalled_source_id,
            decoded_prefix_len,
            full_window_bins,
            burst_bins,
            made_progress,
            hint_replay_start_bin_id = stall_hint.replay_start_bin_id,
            hint_replay_end_limit_bin_id = stall_hint.replay_end_limit_bin_id,
            replay_start_bin_id,
            replay_end_bin_id,
            "Lossless METTLE receiver computed repair burst"
        );
        Some(NeedReport::Mettle {
            window: MettleReplayWindow {
                stalled_source_id,
                replay_start_bin_id,
                replay_end_bin_id,
            },
        })
    }

    async fn maybe_send_streaming_feedback(&mut self, shared: &super::ReceiverShared) {
        if self.complete_reported {
            return;
        }

        let now = Instant::now();
        if self.decoder.is_complete() || shared.has_all_blocks() {
            let round_id = self.last_source_done_round_id.unwrap_or(0);
            let report = NeedReport::Complete;
            debug!(
                session_id = shared.session_id,
                round_id,
                "Lossless METTLE receiver emitted streaming completion"
            );
            shared.send_fec_need(round_id, &report).await;
            self.last_streaming_need_at = Some(now);
            self.complete_reported = true;
            self.last_round_need = Some(report);
            return;
        }

        let Some(last_progress_at) = self.last_progress_at else {
            self.last_progress_at = Some(now);
            return;
        };
        if now.duration_since(last_progress_at) < STREAMING_STALL_INTERVAL {
            return;
        }
        if self
            .last_streaming_need_at
            .is_some_and(|last| now.duration_since(last) < STREAMING_NEED_MIN_GAP)
        {
            return;
        }

        let Some(report) = self.need_report(shared) else {
            return;
        };
        let NeedReport::Mettle { window } = &report else {
            return;
        };

        let round_id = self.last_source_done_round_id.unwrap_or(0);
        debug!(
            session_id = shared.session_id,
            round_id,
            stalled_source_id = window.stalled_source_id,
            replay_start_bin_id = window.replay_start_bin_id,
            replay_end_bin_id = window.replay_end_bin_id,
            "Lossless METTLE receiver emitted streaming repair request"
        );
        shared.send_fec_need(round_id, &report).await;
        self.last_streaming_need_at = Some(now);
        self.last_round_need = Some(report);
    }
}

pub(super) fn params_from_manifest(
    manifest: &nextmini_messages::lossless_session::LosslessSessionManifest,
) -> Result<MettleParams, &'static str> {
    let LosslessSessionMode::Fec(fec) = &manifest.mode else {
        return Err("attempted to build mettle receiver for plain manifest");
    };
    if fec.scheme_kind() != Some(FecScheme::MettleV1) {
        return Err("attempted to build mettle receiver for non-mettle manifest");
    }
    let source_symbol_bytes =
        usize::try_from(manifest.block_size).map_err(|_| "invalid mettle block size")?;
    let rate = CodedRate::new(
        u32::from(fec.coded_rate_numerator),
        u32::from(fec.coded_rate_denominator),
    )
    .map_err(|_| "invalid mettle coded rate")?;
    MettleParams::paper_default(source_symbol_bytes, rate, fec.seed)
        .map_err(|_| "invalid mettle params")
        .map(|params| params.with_tail_compression(true))
}
