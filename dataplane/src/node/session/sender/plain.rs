use std::collections::{BTreeMap, BTreeSet};

use nextmini_messages::lossless_session::{MissingBlockRange, NeedReport};
use tokio::sync::mpsc;
use tracing::debug;

use crate::node::session::api::InboundFrame;
use crate::node::session::api::SessionOutcome;
use crate::node::session::control;

/// Plain-mode sender state machine.
#[derive(Default)]
pub(super) struct PlainSender {
    source_blocks: Vec<u64>,
    source_cursor: usize,
    feedback_open_round_id: u32,
    current_burst_id: u32,
    feedback_round_open: bool,
    round_reports: BTreeMap<usize, NeedReport>,
    required_blocks: BTreeSet<u64>,
    emitted_blocks: BTreeSet<u64>,
    queued_retransmit_blocks: BTreeSet<u64>,
    next_burst_nonempty: bool,
    protocol_error: bool,
    complete: bool,
    initialized: bool,
}

impl super::ModeHooks for PlainSender {
    fn on_need(
        &mut self,
        _shared: &mut super::SenderShared,
        peer_id: usize,
        round_id: u32,
        report: NeedReport,
    ) {
        if !self.feedback_round_open || round_id != self.feedback_open_round_id {
            return;
        }

        if let Some(existing) = self.round_reports.get(&peer_id) {
            if existing != &report {
                self.protocol_error = true;
            }
            return;
        }
        self.round_reports.insert(peer_id, report);
        match self.round_reports.get(&peer_id) {
            Some(NeedReport::Complete) => {}
            Some(NeedReport::Plain { ranges }) => {
                let mut useful = false;
                for block_id in missing_blocks(ranges) {
                    if self.required_blocks.insert(block_id) {
                        useful = true;
                        if !self.emitted_blocks.contains(&block_id) {
                            self.queued_retransmit_blocks.insert(block_id);
                        }
                    }
                }
                if useful {
                    self.current_burst_id = self.feedback_open_round_id.saturating_add(1);
                    self.next_burst_nonempty = true;
                }
            }
            Some(NeedReport::Fec { .. }) => {
                self.protocol_error = true;
            }
            None => {}
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

impl PlainSender {
    /// Main send loop for plain mode.
    pub(super) async fn run(
        &mut self,
        shared: &mut super::SenderShared,
        ctrl_rx: &mut mpsc::Receiver<InboundFrame>,
    ) -> SessionOutcome {
        self.ensure_initial_round(shared);

        while !self.complete {
            shared.drain_controls(ctrl_rx, self);
            if self.protocol_error {
                return SessionOutcome::Aborted;
            }
            if self.complete {
                break;
            }

            if let Some(block_id) = self.next_source_block() {
                self.send_source_block(shared, block_id).await;
                continue;
            }

            if let Some(block_id) = self.next_retransmit_block() {
                self.send_retransmit_block(shared, block_id).await;
                continue;
            }

            if !self.feedback_round_open {
                shared.send_source_done(self.feedback_open_round_id).await;
                self.feedback_round_open = true;
                if shared.active_quorum_is_empty() {
                    self.complete = true;
                    break;
                }
                shared.start_quorum_feedback_wait();
                continue;
            }

            if self.round_can_close(shared) {
                if self
                    .round_reports
                    .values()
                    .all(|report| matches!(report, NeedReport::Complete))
                {
                    self.complete = true;
                    shared.clear_quorum_feedback_wait();
                    debug!(
                        round_id = self.feedback_open_round_id,
                        "Lossless plain sender completed after an all-complete feedback round"
                    );
                    break;
                }

                if self.next_burst_nonempty {
                    self.advance_round(shared);
                    continue;
                }
            }

            match shared
                .wait_for_quorum_feedback(ctrl_rx, self, self.feedback_open_round_id)
                .await
            {
                super::QuorumWaitOutcome::Control | super::QuorumWaitOutcome::Solicited => {}
                super::QuorumWaitOutcome::TimedOut | super::QuorumWaitOutcome::Closed => {
                    return SessionOutcome::Aborted;
                }
            }
            if self.protocol_error {
                return SessionOutcome::Aborted;
            }
        }

        SessionOutcome::Completed
    }

    fn ensure_initial_round(&mut self, shared: &super::SenderShared) {
        if self.initialized {
            return;
        }
        self.source_blocks = (0..shared.plan.total_blocks()).collect();
        self.initialized = true;
    }

    fn next_source_block(&mut self) -> Option<u64> {
        if self.source_cursor >= self.source_blocks.len() {
            return None;
        }
        let block_id = self.source_blocks[self.source_cursor];
        self.source_cursor += 1;
        Some(block_id)
    }

    fn next_retransmit_block(&mut self) -> Option<u64> {
        self.queued_retransmit_blocks.pop_first()
    }

    fn round_can_close(&self, shared: &super::SenderShared) -> bool {
        self.round_reports.len() == shared.active_quorum.active_members().len()
            && self
                .required_blocks
                .iter()
                .all(|block_id| self.emitted_blocks.contains(block_id))
    }

    fn advance_round(&mut self, shared: &mut super::SenderShared) {
        shared.clear_quorum_feedback_wait();
        self.feedback_open_round_id = self.feedback_open_round_id.saturating_add(1);
        self.current_burst_id = self.feedback_open_round_id;
        self.feedback_round_open = false;
        self.round_reports.clear();
        self.required_blocks.clear();
        self.emitted_blocks.clear();
        self.queued_retransmit_blocks.clear();
        self.next_burst_nonempty = false;
        debug!(
            open_round_id = self.feedback_open_round_id,
            current_burst_id = self.current_burst_id,
            "Lossless plain sender advanced to the next feedback-open round"
        );
    }

    /// Encode and send one source block from burst 0.
    async fn send_source_block(&mut self, shared: &mut super::SenderShared, block_id: u64) {
        self.send_block(shared, block_id).await;
    }

    /// Encode and send one retransmitted block from burst r + 1.
    async fn send_retransmit_block(&mut self, shared: &mut super::SenderShared, block_id: u64) {
        self.send_block(shared, block_id).await;
        self.emitted_blocks.insert(block_id);
    }

    /// Encode and send one plain data block.
    async fn send_block(&mut self, shared: &mut super::SenderShared, block_id: u64) {
        let Some(span) = shared.plan.block_span(block_id) else {
            return;
        };
        let payload = shared.source.block_payload(span);
        let frame = nextmini_messages::lossless_session::encode_block_data(
            shared.session.session_id,
            block_id,
            &payload,
        );
        shared.pace(frame.len()).await;
        control::send_frame(
            &shared.processors,
            control::FrameRoute {
                session_id: shared.session.session_id,
                tree_id: None,
                src_ip: shared.route.src_ip,
                src_port: shared.route.src_port,
                dst_ip: shared.route.dst_ip,
                dst_port: shared.route.dst_port,
            },
            &frame,
        )
        .await;
        shared.mark_payload_emitted();
    }
}

fn missing_blocks(ranges: &[MissingBlockRange]) -> impl Iterator<Item = u64> + '_ {
    ranges
        .iter()
        .flat_map(|range| range.start_block_id..range.end_block_id)
}
