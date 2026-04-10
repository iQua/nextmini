use std::collections::BTreeMap;

use tokio::sync::mpsc;
use tokio::time::{Duration, Instant};
use tracing::{debug, info, warn};

use nextmini_messages::lossless_session::{
    FecScheme, LosslessSessionManifest, LosslessSessionMode, MettleReplayWindow, NeedReport,
};

use crate::node::processor::SendOutcome;
use crate::node::session::api::{InboundFrame, SessionOutcome};
use crate::node::session::control;
use crate::node::session::mettle::encoder::{EncodedStream, Encoder, MettleBin};
use crate::node::session::mettle::params::{CodedRate, MettleParams};
use crate::node::session::plan::BlockPlan;

use super::mettle_symbol_frame;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    SendingNominal,
    SendingReplay,
    WaitingForReports,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EmissionKind {
    Nominal,
    Replay,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ReplayCursor {
    next_index: usize,
    end_bin_id: u64,
}

pub(super) struct MettleSender {
    stream: EncodedStream,
    tree_ids: Vec<u16>,
    next_tree_rr: usize,
    frame_scratch: Vec<u8>,
    phase: Phase,
    current_round_id: u32,
    nominal_cursor: usize,
    replay_cursor: Option<ReplayCursor>,
    round_reports: BTreeMap<usize, NeedReport>,
    first_repair_request_seen_at: Option<Instant>,
    round_complete: bool,
    protocol_error: bool,
    nominal_bins_sent: u64,
    nominal_payload_bytes_sent: u64,
    replay_rounds_started: u32,
    replay_bins_sent: u64,
    replay_payload_bytes_sent: u64,
}

const REPAIR_REPORT_COALESCE_GRACE: Duration = Duration::from_millis(100);

impl MettleSender {
    pub(super) fn new(
        manifest: &LosslessSessionManifest,
        plan: BlockPlan,
        source: &super::BlockSource,
    ) -> Result<Self, &'static str> {
        let LosslessSessionMode::Fec(fec) = &manifest.mode else {
            return Err("attempted to build mettle sender for plain manifest");
        };
        if fec.scheme_kind() != Some(FecScheme::MettleV1) {
            return Err("attempted to build mettle sender for non-mettle manifest");
        }
        let source_symbol_bytes =
            usize::try_from(manifest.block_size).map_err(|_| "invalid mettle block size")?;
        let rate = CodedRate::new(
            u32::from(fec.coded_rate_numerator),
            u32::from(fec.coded_rate_denominator),
        )
        .map_err(|_| "invalid mettle coded rate")?;
        let params = MettleParams::paper_default(source_symbol_bytes, rate, fec.seed)
            .map_err(|_| "invalid mettle params")?;

        let mut encoder = Encoder::new(params);
        for block_id in 0..plan.total_blocks() {
            let Some(span) = plan.block_span(block_id) else {
                return Err("invalid mettle block span");
            };
            let payload = source.block_payload(span);
            let _ = encoder.push_source(&payload);
        }
        let stream = encoder.finish();
        debug_assert_eq!(stream.total_sources, plan.total_blocks());

        Ok(Self {
            stream,
            tree_ids: fec.tree_ids.clone(),
            next_tree_rr: 0,
            frame_scratch: Vec::new(),
            phase: Phase::SendingNominal,
            current_round_id: 0,
            nominal_cursor: 0,
            replay_cursor: None,
            round_reports: BTreeMap::new(),
            first_repair_request_seen_at: None,
            round_complete: false,
            protocol_error: false,
            nominal_bins_sent: 0,
            nominal_payload_bytes_sent: 0,
            replay_rounds_started: 0,
            replay_bins_sent: 0,
            replay_payload_bytes_sent: 0,
        })
    }

    pub(super) async fn run(
        &mut self,
        shared: &mut super::SenderShared,
        ctrl_rx: &mut mpsc::Receiver<InboundFrame>,
    ) -> SessionOutcome {
        while !self.round_complete {
            shared.drain_controls(ctrl_rx, self);
            if self.protocol_error {
                return SessionOutcome::Aborted;
            }
            if self.round_complete {
                break;
            }

            if let Some((kind, bin)) = self.next_bin() {
                shared.pace(bin.payload.len()).await;
                if self.try_send_bin(shared, &bin) {
                    self.record_emitted_bin(kind, &bin);
                    shared.mark_payload_emitted();
                    continue;
                }
                if !shared.wait_for_signal(ctrl_rx, self).await {
                    return SessionOutcome::Aborted;
                }
                continue;
            }

            if self.phase != Phase::WaitingForReports {
                shared.send_source_done(self.current_round_id).await;
                self.phase = Phase::WaitingForReports;
                self.round_reports.clear();
                self.first_repair_request_seen_at = None;
                if shared.active_quorum_is_empty() {
                    self.round_complete = true;
                    break;
                }
                shared.start_quorum_feedback_wait();
                continue;
            }

            if self.should_finish_report_round(shared.active_quorum.active_members().len()) {
                self.finish_report_round(shared);
                continue;
            }

            if self.first_repair_request_seen_at.is_some() {
                if !shared.wait_for_signal(ctrl_rx, self).await {
                    return SessionOutcome::Aborted;
                }
                continue;
            }

            match shared
                .wait_for_quorum_feedback(ctrl_rx, self, self.current_round_id)
                .await
            {
                super::QuorumWaitOutcome::Control | super::QuorumWaitOutcome::Solicited => {}
                super::QuorumWaitOutcome::TimedOut | super::QuorumWaitOutcome::Closed => {
                    return SessionOutcome::Aborted;
                }
            }
        }

        info!(
            session_id = shared.session.session_id,
            nominal_bins_sent = self.nominal_bins_sent,
            nominal_payload_bytes_sent = self.nominal_payload_bytes_sent,
            replay_rounds_started = self.replay_rounds_started,
            replay_bins_sent = self.replay_bins_sent,
            replay_payload_bytes_sent = self.replay_payload_bytes_sent,
            "Lossless sender finished METTLE payload/replay transmission"
        );

        SessionOutcome::Completed
    }

    fn next_bin(&mut self) -> Option<(EmissionKind, MettleBin)> {
        match self.phase {
            Phase::SendingNominal => {
                let bin = self.stream.bins.get(self.nominal_cursor)?.clone();
                self.nominal_cursor += 1;
                Some((EmissionKind::Nominal, bin))
            }
            Phase::SendingReplay => {
                let cursor = self.replay_cursor.as_mut()?;
                while let Some(bin) = self.stream.bins.get(cursor.next_index) {
                    if bin.bin_id >= cursor.end_bin_id {
                        self.replay_cursor = None;
                        return None;
                    }
                    cursor.next_index += 1;
                    return Some((EmissionKind::Replay, bin.clone()));
                }
                self.replay_cursor = None;
                None
            }
            Phase::WaitingForReports => None,
        }
    }

    fn should_finish_report_round(&self, quorum_len: usize) -> bool {
        if self.round_reports.len() == quorum_len {
            return true;
        }

        self.first_repair_request_seen_at
            .is_some_and(|seen_at| seen_at.elapsed() >= REPAIR_REPORT_COALESCE_GRACE)
    }

    fn record_emitted_bin(&mut self, kind: EmissionKind, bin: &MettleBin) {
        match kind {
            EmissionKind::Nominal => {
                self.nominal_bins_sent += 1;
                self.nominal_payload_bytes_sent += bin.payload.len() as u64;
            }
            EmissionKind::Replay => {
                self.replay_bins_sent += 1;
                self.replay_payload_bytes_sent += bin.payload.len() as u64;
            }
        }
    }

    fn try_send_bin(&mut self, shared: &mut super::SenderShared, bin: &MettleBin) -> bool {
        if self.tree_ids.is_empty() {
            return false;
        }

        let tree_count = self.tree_ids.len();
        let start_idx = self.next_tree_rr;
        let initial_tree_id = self.tree_ids[start_idx];
        mettle_symbol_frame::encode_into(
            &mut self.frame_scratch,
            shared.session.session_id,
            bin.bin_id,
            bin.degree,
            bin.xor_source_id,
            bin.xor_source_sig,
            initial_tree_id,
            &bin.payload,
        );

        for offset in 0..tree_count {
            let idx = (start_idx + offset) % tree_count;
            let tree_id = self.tree_ids[idx];
            if offset > 0 {
                mettle_symbol_frame::patch_tree_id(&mut self.frame_scratch, tree_id)
                    .expect("encoded mettle symbol should accept tree-id patch");
            }
            let submission = control::try_send_frame(
                &shared.processors,
                control::FrameRoute {
                    session_id: shared.session.session_id,
                    tree_id: Some(tree_id),
                    src_ip: shared.route.src_ip,
                    src_port: shared.route.src_port,
                    dst_ip: shared.route.dst_ip,
                    dst_port: shared.route.dst_port,
                },
                &self.frame_scratch,
            );
            match submission.outcome {
                SendOutcome::Queued => {
                    self.next_tree_rr = (idx + 1) % tree_count;
                    return true;
                }
                SendOutcome::WouldBlock => {}
                SendOutcome::Closed => {
                    warn!(
                        session_id = shared.session.session_id,
                        tree_id,
                        "Lossless sender observed closed processor ingress while sending METTLE symbol"
                    );
                }
            }
        }

        false
    }

    fn finish_report_round(&mut self, shared: &mut super::SenderShared) {
        let merged = match select_replay_window(self.round_reports.values()) {
            Ok(window) => window,
            Err(()) => {
                self.protocol_error = true;
                return;
            }
        };

        shared.clear_quorum_feedback_wait();
        self.round_reports.clear();
        self.first_repair_request_seen_at = None;

        let Some(window) = merged else {
            self.round_complete = true;
            return;
        };

        let next_index = self
            .stream
            .bins
            .partition_point(|bin| bin.bin_id < window.replay_start_bin_id);
        if self
            .stream
            .bins
            .get(next_index)
            .is_none_or(|bin| bin.bin_id >= window.replay_end_bin_id)
        {
            warn!(
                session_id = shared.session.session_id,
                replay_start_bin_id = window.replay_start_bin_id,
                replay_end_bin_id = window.replay_end_bin_id,
                "Lossless sender rejected empty METTLE replay window"
            );
            self.protocol_error = true;
            return;
        }

        info!(
            session_id = shared.session.session_id,
            current_round_id = self.current_round_id,
            next_round_id = self.current_round_id.saturating_add(1),
            replay_round_index = self.replay_rounds_started + 1,
            replay_start_bin_id = window.replay_start_bin_id,
            replay_end_bin_id = window.replay_end_bin_id,
            replay_window_bins = window
                .replay_end_bin_id
                .saturating_sub(window.replay_start_bin_id),
            replay_bins_sent_so_far = self.replay_bins_sent,
            replay_payload_bytes_sent_so_far = self.replay_payload_bytes_sent,
            "Lossless sender scheduled METTLE replay window"
        );
        self.replay_rounds_started = self.replay_rounds_started.saturating_add(1);
        self.current_round_id = self.current_round_id.saturating_add(1);
        self.phase = Phase::SendingReplay;
        self.replay_cursor = Some(ReplayCursor {
            next_index,
            end_bin_id: window.replay_end_bin_id,
        });
    }
}

fn select_replay_window<'a>(
    reports: impl IntoIterator<Item = &'a NeedReport>,
) -> Result<Option<MettleReplayWindow>, ()> {
    let mut selected: Option<MettleReplayWindow> = None;
    for report in reports {
        match report {
            NeedReport::Complete => {}
            NeedReport::Mettle { window } => {
                if selected.is_none_or(|current| replay_window_order(*window, current).is_lt()) {
                    selected = Some(*window);
                }
            }
            NeedReport::Plain { .. } | NeedReport::Fec { .. } => return Err(()),
        }
    }
    Ok(selected)
}

fn replay_window_order(a: MettleReplayWindow, b: MettleReplayWindow) -> std::cmp::Ordering {
    (
        a.stalled_source_id,
        a.replay_start_bin_id,
        a.replay_end_bin_id,
    )
        .cmp(&(
            b.stalled_source_id,
            b.replay_start_bin_id,
            b.replay_end_bin_id,
        ))
}

impl super::ModeHooks for MettleSender {
    fn on_need(
        &mut self,
        shared: &mut super::SenderShared,
        peer_id: usize,
        round_id: u32,
        report: NeedReport,
    ) {
        if self.phase != Phase::WaitingForReports {
            debug!(
                session_id = shared.session.session_id,
                peer_id,
                round_id,
                "Lossless METTLE sender dropped Need because no report round is open"
            );
            return;
        }
        if round_id != self.current_round_id {
            debug!(
                session_id = shared.session.session_id,
                peer_id,
                round_id,
                current_round_id = self.current_round_id,
                "Lossless METTLE sender dropped stale or future Need"
            );
            return;
        }
        if let Some(existing) = self.round_reports.get(&peer_id) {
            if existing != &report {
                warn!(
                    session_id = shared.session.session_id,
                    peer_id,
                    round_id,
                    "Lossless METTLE sender rejected changed same-round Need from a quorum peer"
                );
                self.protocol_error = true;
            }
            return;
        }

        match &report {
            NeedReport::Complete | NeedReport::Mettle { .. } => {
                if matches!(report, NeedReport::Mettle { .. })
                    && self.first_repair_request_seen_at.is_none()
                {
                    self.first_repair_request_seen_at = Some(Instant::now());
                }
                self.round_reports.insert(peer_id, report);
            }
            NeedReport::Plain { .. } | NeedReport::Fec { .. } => {
                warn!(
                    session_id = shared.session.session_id,
                    peer_id,
                    round_id,
                    "Lossless METTLE sender rejected Need with mismatched report mode"
                );
                self.protocol_error = true;
            }
        }
    }

    fn pending_feedback_peers(&self, shared: &super::SenderShared) -> Vec<usize> {
        shared
            .active_quorum
            .active_members()
            .iter()
            .copied()
            .filter(|peer_id| !self.round_reports.contains_key(peer_id))
            .collect()
    }
}


