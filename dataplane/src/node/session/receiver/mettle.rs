use nextmini_messages::lossless_session::{
    self, FecScheme, LosslessSessionMode, MettleReplayWindow, NeedReport,
};
use tracing::{debug, info};

use crate::node::session::api::InboundFrame;
use crate::node::session::mettle::decoder::Decoder;
use crate::node::session::mettle::encoder::MettleBin;
use crate::node::session::mettle::params::{CodedRate, MettleParams};

pub(super) struct MettleReceiver {
    params: MettleParams,
    decoder: Decoder,
    last_source_done_round_id: Option<u32>,
    last_round_need: Option<NeedReport>,
    complete_reported: bool,
    last_reported_stalled_source_id: Option<u64>,
    last_reported_decoded_prefix_len: Option<u64>,
    current_repair_burst_bins: u64,
}

const MIN_REPAIR_BURST_BINS: u64 = 64;

impl MettleReceiver {
    pub(super) fn new(params: MettleParams, total_sources: u64) -> Self {
        Self {
            params,
            decoder: Decoder::new(params, total_sources),
            last_source_done_round_id: None,
            last_round_need: None,
            complete_reported: false,
            last_reported_stalled_source_id: None,
            last_reported_decoded_prefix_len: None,
            current_repair_burst_bins: MIN_REPAIR_BURST_BINS,
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

        let Some(report) = self.need_report(shared) else {
            return;
        };
        match &report {
            NeedReport::Complete => {
                info!(
                    session_id = shared.session_id,
                    round_id,
                    decoded_prefix_len = self.decoder.decoded_prefix_len(),
                    "Lossless METTLE receiver reported completion"
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
                    "Lossless METTLE receiver requested repair burst"
                );
            }
            NeedReport::Plain { .. } | NeedReport::Fec { .. } => {}
        }
        self.last_source_done_round_id = Some(round_id);
        self.last_round_need = Some(report.clone());
        shared.send_fec_need(round_id, &report).await;
        self.complete_reported = matches!(report, NeedReport::Complete);
    }

    pub(super) fn is_complete(&self) -> bool {
        self.complete_reported
    }

    fn need_report(&mut self, shared: &super::ReceiverShared) -> Option<NeedReport> {
        let plan = shared.plan?;
        if plan.total_blocks() == 0 || shared.has_all_blocks() || self.decoder.is_complete() {
            return Some(NeedReport::Complete);
        }

        let stalled_source_id = self.decoder.first_undecoded_source_id()?;
        let decoded_prefix_len = self.decoder.decoded_prefix_len();
        let full_window_bins = self.params.window_bins().max(1);
        let made_progress = match (
            self.last_reported_stalled_source_id,
            self.last_reported_decoded_prefix_len,
        ) {
            (Some(last_stalled), Some(last_prefix)) => {
                stalled_source_id != last_stalled || decoded_prefix_len > last_prefix
            }
            _ => true,
        };
        let burst_bins = if made_progress {
            MIN_REPAIR_BURST_BINS.min(full_window_bins).max(1)
        } else {
            self.current_repair_burst_bins
                .saturating_mul(2)
                .min(full_window_bins)
                .max(1)
        };

        self.current_repair_burst_bins = burst_bins;
        self.last_reported_stalled_source_id = Some(stalled_source_id);
        self.last_reported_decoded_prefix_len = Some(decoded_prefix_len);
        let replay_start_bin_id = self.params.base(stalled_source_id);
        let replay_end_bin_id =
            (replay_start_bin_id + burst_bins).min(self.params.right_exclusive(stalled_source_id));

        debug!(
            session_id = shared.session_id,
            stalled_source_id,
            decoded_prefix_len,
            full_window_bins,
            burst_bins,
            made_progress,
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
}
