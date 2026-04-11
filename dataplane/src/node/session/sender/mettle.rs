use std::collections::{BTreeMap, BTreeSet, VecDeque};

use tokio::sync::mpsc;
use tokio::time::{Duration, Instant};
use tracing::{debug, info, warn};

use nextmini_messages::lossless_session::{
    FecScheme, LosslessSessionManifest, LosslessSessionMode, MettleReplayWindow, NeedReport,
};

use crate::node::processor::SendOutcome;
use crate::node::session::api::{InboundFrame, SessionOutcome};
use crate::node::session::control;
use crate::node::session::mettle::encoder::{Encoder, MettleBin};
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
    encoder: Encoder,
    total_sources: u64,
    next_source_id: u64,
    nominal_queue: VecDeque<MettleBin>,
    tail_flushed: bool,
    tree_schedule: Vec<u16>,
    next_tree_rr: usize,
    frame_scratch: Vec<u8>,
    phase: Phase,
    current_round_id: u32,
    tail_source_done_sent: bool,
    replay_cursor: Option<ReplayCursor>,
    replay_resume_phase: Option<Phase>,
    pending_reports: BTreeMap<usize, NeedReport>,
    completed_peers: BTreeSet<usize>,
    first_repair_request_seen_at: Option<Instant>,
    last_repair_activity_at: Instant,
    round_complete: bool,
    protocol_error: bool,
    nominal_bins_sent: u64,
    nominal_payload_bytes_sent: u64,
    replay_rounds_started: u32,
    replay_bins_sent: u64,
    replay_payload_bytes_sent: u64,
}

const REPAIR_REPORT_COALESCE_GRACE: Duration = Duration::from_millis(100);
const NOMINAL_QUEUE_TARGET_BINS: usize = 256;

impl MettleSender {
    pub(super) fn new(
        manifest: &LosslessSessionManifest,
        plan: BlockPlan,
        tree_schedule: Vec<u16>,
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
            .map_err(|_| "invalid mettle params")?
            .with_tail_compression(true);
        let total_sources = plan.total_blocks();

        Ok(Self {
            encoder: Encoder::new_with_total_sources(params, total_sources),
            total_sources,
            next_source_id: 0,
            nominal_queue: VecDeque::new(),
            tail_flushed: false,
            tree_schedule,
            next_tree_rr: 0,
            frame_scratch: Vec::new(),
            phase: Phase::SendingNominal,
            current_round_id: 0,
            tail_source_done_sent: false,
            replay_cursor: None,
            replay_resume_phase: None,
            pending_reports: BTreeMap::new(),
            completed_peers: BTreeSet::new(),
            first_repair_request_seen_at: None,
            last_repair_activity_at: Instant::now(),
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

            if self.phase != Phase::SendingReplay && self.repair_burst_is_ready() {
                self.schedule_replay_from_reports(shared, self.phase);
                if self.protocol_error || self.round_complete {
                    continue;
                }
            }

            self.fill_nominal_queue(shared);
            if let Some((kind, bin)) = self.next_bin() {
                shared.pace(bin.payload.len()).await;
                if self.try_send_bin(shared, &bin) {
                    self.advance_after_emit(kind);
                    self.record_emitted_bin(kind, &bin);
                    shared.mark_payload_emitted();
                    continue;
                }
                if !shared.wait_for_signal(ctrl_rx, self).await {
                    return SessionOutcome::Aborted;
                }
                continue;
            }

            if self.phase == Phase::SendingReplay {
                let resume_phase = self
                    .replay_resume_phase
                    .take()
                    .unwrap_or(Phase::SendingNominal);
                self.phase = resume_phase;
                continue;
            }

            if self.phase != Phase::WaitingForReports {
                if !self.tail_source_done_sent {
                    shared.send_source_done(self.current_round_id).await;
                    self.tail_source_done_sent = true;
                    self.last_repair_activity_at = Instant::now();
                }
                self.phase = Phase::WaitingForReports;
                if shared.active_quorum_is_empty() {
                    self.round_complete = true;
                    break;
                }
                continue;
            }

            if self.all_quorum_peers_complete(shared) {
                self.round_complete = true;
                break;
            }

            if self.repair_wait_timed_out(shared) {
                warn!(
                    session_id = shared.session.session_id,
                    reason = "peer_report_timeout",
                    missing = ?self.incomplete_quorum_peers(shared),
                    "Lossless METTLE sender timed out waiting for repair progress after tail"
                );
                return SessionOutcome::Aborted;
            }

            if !shared.wait_for_signal(ctrl_rx, self).await {
                return SessionOutcome::Aborted;
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
            Phase::SendingNominal => self
                .nominal_queue
                .front()
                .cloned()
                .map(|bin| (EmissionKind::Nominal, bin)),
            Phase::SendingReplay => {
                let cursor = self.replay_cursor.as_ref()?;
                while let Some(bin) = self.encoder.emitted_bins().get(cursor.next_index) {
                    if bin.bin_id >= cursor.end_bin_id {
                        self.replay_cursor = None;
                        return None;
                    }
                    return Some((EmissionKind::Replay, bin.clone()));
                }
                self.replay_cursor = None;
                None
            }
            Phase::WaitingForReports => None,
        }
    }

    fn advance_after_emit(&mut self, kind: EmissionKind) {
        match kind {
            EmissionKind::Nominal => {
                let _ = self.nominal_queue.pop_front();
            }
            EmissionKind::Replay => {
                if let Some(cursor) = self.replay_cursor.as_mut() {
                    cursor.next_index += 1;
                }
            }
        }
    }

    fn repair_burst_is_ready(&self) -> bool {
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
        if self.tree_schedule.is_empty() {
            return false;
        }

        let tree_count = self.tree_schedule.len();
        let start_idx = self.next_tree_rr;
        let initial_tree_id = self.tree_schedule[start_idx];
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
            let tree_id = self.tree_schedule[idx];
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

    fn all_quorum_peers_complete(&self, shared: &super::SenderShared) -> bool {
        shared
            .active_quorum
            .active_members()
            .iter()
            .all(|peer_id| self.completed_peers.contains(peer_id))
    }

    fn repair_wait_timed_out(&self, shared: &super::SenderShared) -> bool {
        self.tail_source_done_sent
            && !self.all_quorum_peers_complete(shared)
            && self.last_repair_activity_at.elapsed() >= shared.quorum_liveness.peer_report_timeout()
    }

    fn incomplete_quorum_peers(&self, shared: &super::SenderShared) -> Vec<usize> {
        shared
            .active_quorum
            .active_members()
            .iter()
            .copied()
            .filter(|peer_id| !self.completed_peers.contains(peer_id))
            .collect()
    }

    fn schedule_replay_from_reports(
        &mut self,
        shared: &mut super::SenderShared,
        resume_phase: Phase,
    ) {
        let merged = match select_replay_window(self.pending_reports.values()) {
            Ok(window) => window,
            Err(()) => {
                self.protocol_error = true;
                return;
            }
        };

        self.pending_reports.clear();
        self.first_repair_request_seen_at = None;

        let Some(window) = merged else {
            if resume_phase == Phase::WaitingForReports && self.all_quorum_peers_complete(shared) {
                self.round_complete = true;
            }
            return;
        };

        let emitted_bins = self.encoder.emitted_bins();
        let next_index = emitted_bins.partition_point(|bin| bin.bin_id < window.replay_start_bin_id);
        if self
            .encoder
            .emitted_bins()
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
        self.last_repair_activity_at = Instant::now();
        self.replay_rounds_started = self.replay_rounds_started.saturating_add(1);
        self.phase = Phase::SendingReplay;
        self.replay_resume_phase = Some(resume_phase);
        self.replay_cursor = Some(ReplayCursor {
            next_index,
            end_bin_id: window.replay_end_bin_id,
        });
    }

    fn fill_nominal_queue(&mut self, shared: &super::SenderShared) {
        if self.phase != Phase::SendingNominal
            || self.nominal_queue.len() >= NOMINAL_QUEUE_TARGET_BINS
        {
            return;
        }

        while self.nominal_queue.len() < NOMINAL_QUEUE_TARGET_BINS {
            if self.next_source_id < self.total_sources {
                let Some(span) = shared.plan.block_span(self.next_source_id) else {
                    self.protocol_error = true;
                    return;
                };
                let payload = shared.source.block_payload(span);
                let emitted = self.encoder.push_source(&payload);
                self.next_source_id += 1;
                self.nominal_queue.extend(emitted);
                continue;
            }

            if !self.tail_flushed {
                self.nominal_queue.extend(self.encoder.flush_tail());
                self.tail_flushed = true;
                continue;
            }

            break;
        }
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
        match &report {
            NeedReport::Complete | NeedReport::Mettle { .. } => {
                let now = Instant::now();
                if let Some(existing) = self.pending_reports.get(&peer_id) {
                    if existing == &report {
                        self.last_repair_activity_at = now;
                        return;
                    }
                    match (existing, &report) {
                        (NeedReport::Complete, NeedReport::Mettle { .. }) => {
                            debug!(
                                session_id = shared.session.session_id,
                                peer_id,
                                round_id,
                                "Lossless METTLE sender ignored same-round repair after completion from a quorum peer"
                            );
                            return;
                        }
                        _ => {
                            debug!(
                                session_id = shared.session.session_id,
                                peer_id,
                                round_id,
                                previous = ?existing,
                                updated = ?report,
                                "Lossless METTLE sender replaced same-round Need from a quorum peer"
                            );
                        }
                    }
                }
                match report {
                    NeedReport::Complete => {
                        self.completed_peers.insert(peer_id);
                        self.pending_reports.remove(&peer_id);
                    }
                    NeedReport::Mettle { .. } => {
                        if self.completed_peers.contains(&peer_id) {
                            debug!(
                                session_id = shared.session.session_id,
                                peer_id,
                                round_id,
                                "Lossless METTLE sender ignored repair after peer already completed"
                            );
                            self.last_repair_activity_at = now;
                            return;
                        }
                        if self.first_repair_request_seen_at.is_none() {
                            self.first_repair_request_seen_at = Some(now);
                        }
                        self.pending_reports.insert(peer_id, report);
                    }
                    NeedReport::Plain { .. } | NeedReport::Fec { .. } => unreachable!(),
                }
                self.last_repair_activity_at = now;
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
            .filter(|peer_id| !self.completed_peers.contains(peer_id))
            .collect()
    }
}


