use std::collections::{BTreeMap, BTreeSet};

use nextmini_messages::lossless_session::{MissingBlockRange, PlainStatus};
use tokio::sync::mpsc;

use crate::node::session::api::InboundFrame;
use crate::node::session::control;

/// Plain-mode sender state machine.
#[derive(Default)]
pub(super) struct PlainSender {
    pending_blocks: Vec<u64>,
    cursor: usize,
    round_eot_sent: bool,
    round_reports: BTreeMap<usize, PlainStatus>,
    complete: bool,
    initialized: bool,
}

impl super::ModeHooks for PlainSender {
    fn on_plain_status(
        &mut self,
        shared: &super::SenderShared,
        peer_id: usize,
        status: PlainStatus,
    ) {
        if !self.round_eot_sent {
            return;
        }

        self.round_reports.insert(peer_id, status);
        if self.round_reports.len() < shared.receiver_set.len() {
            return;
        }

        let mut next_round = BTreeSet::new();
        let mut complete = true;
        for receiver_id in &shared.receiver_ids {
            let Some(status) = self.round_reports.get(receiver_id) else {
                return;
            };
            match status {
                PlainStatus::Complete => {}
                PlainStatus::MissingBlocks { ranges } => {
                    complete = false;
                    collect_missing_blocks(&mut next_round, ranges);
                }
            }
        }

        self.complete = complete;
        self.pending_blocks = next_round.into_iter().collect();
        self.cursor = 0;
        self.round_eot_sent = false;
        self.round_reports.clear();
    }
}

impl PlainSender {
    /// Main send loop for plain mode.
    pub(super) async fn run(
        &mut self,
        shared: &mut super::SenderShared,
        ctrl_rx: &mut mpsc::Receiver<InboundFrame>,
    ) {
        self.ensure_initial_round(shared);

        while !self.complete {
            shared.drain_controls(ctrl_rx, self);
            if self.complete {
                break;
            }

            if let Some(block_id) = self.next_block() {
                self.send_block(shared, block_id).await;
                continue;
            }

            if !self.round_eot_sent {
                shared.send_eot().await;
                self.round_eot_sent = true;
                continue;
            }

            if !shared.wait_for_signal(ctrl_rx, self).await {
                break;
            }
        }
    }

    pub(super) fn is_complete(&self) -> bool {
        self.complete
    }

    /// Return the next plain block that still needs to be sent.
    fn next_block(&mut self) -> Option<u64> {
        while self.cursor < self.pending_blocks.len() {
            let block_id = self.pending_blocks[self.cursor];
            self.cursor += 1;
            return Some(block_id);
        }
        None
    }

    fn ensure_initial_round(&mut self, shared: &super::SenderShared) {
        if self.initialized {
            return;
        }
        self.pending_blocks = (0..shared.plan.total_blocks()).collect();
        self.initialized = true;
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
    }
}

fn collect_missing_blocks(out: &mut BTreeSet<u64>, ranges: &[MissingBlockRange]) {
    for range in ranges {
        for block_id in range.start_block_id..range.end_block_id {
            out.insert(block_id);
        }
    }
}
