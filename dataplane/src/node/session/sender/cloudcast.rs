use std::collections::BTreeMap;

use nextmini_messages::lossless_session::{MissingBlockRange, NeedReport};
use tokio::sync::mpsc;
use tracing::{debug, warn};

use crate::node::session::api::{InboundFrame, SessionOutcome};
use crate::node::session::control;
use crate::node::session::runtime::CloudcastRuntimeConfig;

/// Cloudcast-mode sender: plain block payloads striped once across tree scopes.
pub(super) struct CloudcastSender {
    source_cursor: u64,
    stripe_selector: CloudcastStripeSelector,
    feedback_round_open: bool,
    round_reports: BTreeMap<usize, NeedReport>,
    protocol_error: bool,
    complete: bool,
}

impl super::ModeHooks for CloudcastSender {
    fn on_need(
        &mut self,
        shared: &mut super::SenderShared,
        peer_id: usize,
        round_id: u32,
        report: NeedReport,
    ) {
        if !self.feedback_round_open {
            debug!(
                session_id = shared.session.session_id,
                peer_id,
                round_id,
                "Lossless Cloudcast sender dropped Need because no feedback round is open"
            );
            return;
        }
        if round_id != 0 {
            debug!(
                session_id = shared.session.session_id,
                peer_id, round_id, "Lossless Cloudcast sender dropped out-of-round Need"
            );
            return;
        }

        if let Some(existing) = self.round_reports.get(&peer_id) {
            if existing != &report {
                warn!(
                    session_id = shared.session.session_id,
                    peer_id,
                    round_id,
                    "Lossless Cloudcast sender rejected changed same-round Need from a quorum peer"
                );
                self.protocol_error = true;
            }
            return;
        }

        if !need_is_complete(&report) {
            warn!(
                session_id = shared.session.session_id,
                peer_id,
                round_id,
                "Lossless Cloudcast sender observed missing plain blocks; Cloudcast mode does not retransmit"
            );
            self.protocol_error = true;
        }
        self.round_reports.insert(peer_id, report);
        shared
            .quorum_liveness
            .note_feedback_progress(tokio::time::Instant::now());
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

impl CloudcastSender {
    pub(super) fn new(config: &CloudcastRuntimeConfig) -> Result<Self, &'static str> {
        Ok(Self {
            source_cursor: 0,
            stripe_selector: CloudcastStripeSelector::new(config.stripe_tree_ids())?,
            feedback_round_open: false,
            round_reports: BTreeMap::new(),
            protocol_error: false,
            complete: false,
        })
    }

    pub(super) async fn run(
        &mut self,
        shared: &mut super::SenderShared,
        ctrl_rx: &mut mpsc::Receiver<InboundFrame>,
    ) -> SessionOutcome {
        while !self.complete {
            shared.drain_controls(ctrl_rx, self);
            if self.protocol_error {
                return SessionOutcome::Aborted;
            }

            if let Some(block_id) = self.next_source_block(shared) {
                self.send_source_block(shared, block_id).await;
                continue;
            }

            if !self.feedback_round_open {
                shared.send_source_done(0).await;
                self.feedback_round_open = true;
                if shared.active_quorum_is_empty() {
                    self.complete = true;
                    break;
                }
                shared.start_quorum_feedback_wait();
                continue;
            }

            if self.round_reports.len() == shared.active_quorum.active_members().len() {
                shared.clear_quorum_feedback_wait();
                if self.round_reports.values().all(need_is_complete) {
                    self.complete = true;
                    break;
                }
                return SessionOutcome::Aborted;
            }

            match shared.wait_for_quorum_feedback(ctrl_rx, self, 0).await {
                super::QuorumWaitOutcome::Control | super::QuorumWaitOutcome::Solicited => {}
                super::QuorumWaitOutcome::TimedOut | super::QuorumWaitOutcome::Closed => {
                    return SessionOutcome::Aborted;
                }
            }
        }

        SessionOutcome::Completed
    }

    fn next_source_block(&mut self, shared: &super::SenderShared) -> Option<u64> {
        if self.source_cursor >= shared.plan.total_blocks() {
            return None;
        }
        let block_id = self.source_cursor;
        self.source_cursor = self.source_cursor.saturating_add(1);
        Some(block_id)
    }

    async fn send_source_block(&mut self, shared: &mut super::SenderShared, block_id: u64) {
        let Some(span) = shared.plan.block_span(block_id) else {
            return;
        };
        let payload = shared.source.block_payload(span);
        let frame = nextmini_messages::lossless_session::encode_block_data(
            shared.session.session_id,
            block_id,
            &payload,
        );
        let tree_id = self.stripe_selector.tree_id_for_block(block_id);
        shared.pace(frame.len()).await;
        control::send_frame(
            &shared.processors,
            control::FrameRoute {
                session_id: shared.session.session_id,
                tree_id: Some(tree_id),
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

fn need_is_complete(report: &NeedReport) -> bool {
    match report {
        NeedReport::Complete => true,
        NeedReport::Plain { ranges } => !ranges.iter().any(nonempty_range),
        NeedReport::Fec { .. } => false,
    }
}

fn nonempty_range(range: &MissingBlockRange) -> bool {
    range.start_block_id < range.end_block_id
}

#[derive(Debug)]
struct CloudcastStripeSelector {
    stripe_tree_ids: Vec<u16>,
}

impl CloudcastStripeSelector {
    fn new(stripe_tree_ids: &[u16]) -> Result<Self, &'static str> {
        if stripe_tree_ids.is_empty() {
            return Err("cloudcast sender requires at least one stripe");
        }
        Ok(Self {
            stripe_tree_ids: stripe_tree_ids.to_vec(),
        })
    }

    fn tree_id_for_block(&self, block_id: u64) -> u16 {
        let stripe_idx = block_id as usize % self.stripe_tree_ids.len();
        self.stripe_tree_ids[stripe_idx]
    }
}

#[cfg(test)]
mod tests {
    use super::CloudcastStripeSelector;

    #[test]
    fn selector_routes_blocks_by_fixed_stripe_table() {
        let selector = CloudcastStripeSelector::new(&[3, 3, 7]).expect("valid selector");
        let emitted = (0..6)
            .map(|block_id| selector.tree_id_for_block(block_id))
            .collect::<Vec<_>>();

        assert_eq!(emitted, vec![3, 3, 7, 3, 3, 7]);
    }
}
