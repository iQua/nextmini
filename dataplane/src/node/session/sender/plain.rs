use tokio::sync::mpsc;

use crate::node::session::api::InboundFrame;
use crate::node::session::control;
use crate::node::session::ledger::BlockState;

/// Plain-mode sender state machine.
#[derive(Default)]
pub(super) struct PlainSender {
    cursor: u64,
    round_eot_sent: bool,
}

impl super::ModeHooks for PlainSender {}

impl PlainSender {
    /// Main send loop for plain mode.
    pub(super) async fn run(
        &mut self,
        shared: &mut super::SenderShared,
        ctrl_rx: &mut mpsc::Receiver<InboundFrame>,
    ) {
        while !shared.ledger.is_complete() {
            shared.drain_controls(ctrl_rx, self);

            if let Some(block_id) = self.next_block(shared) {
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
            self.cursor = 0;
            self.round_eot_sent = false;
        }
    }

    /// Return the next plain block that still needs to be sent.
    fn next_block(&mut self, shared: &super::SenderShared) -> Option<u64> {
        while self.cursor < shared.plan.total_blocks() {
            let block_id = self.cursor;
            self.cursor += 1;
            if shared.ledger.block_state(block_id) != Some(BlockState::Complete) {
                self.round_eot_sent = false;
                return Some(block_id);
            }
        }
        None
    }

    /// Encode and send one plain data block.
    async fn send_block(&mut self, shared: &mut super::SenderShared, block_id: u64) {
        let Some(span) = shared.plan.block_span(block_id) else {
            return;
        };
        let payload = shared.source.block_payload(span);
        let frame = nextmini_messages::lossless_session::encode_block_data(
            shared.common.session_id,
            block_id,
            &payload,
        );
        shared.pace(frame.len()).await;
        control::send_frame(
            &shared.processors,
            control::FrameRoute {
                session_id: shared.common.session_id,
                tree_id: None,
                src_ip: shared.src_ip,
                src_port: shared.common.src_port,
                dst_ip: shared.common.dest_ip,
                dst_port: shared.common.dst_port,
            },
            &frame,
        )
        .await;
    }
}
