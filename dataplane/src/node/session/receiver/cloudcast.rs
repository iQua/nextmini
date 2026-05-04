use std::collections::{BTreeMap, BTreeSet};

use nextmini_messages::lossless_session::{LosslessSessionMode, NeedReport};
use tracing::{debug, info, warn};

use crate::node::session::api::InboundFrame;

/// Cloudcast receiver: plain blocks over tree scopes, with tree-scoped burst boundaries.
pub(super) struct CloudcastReceiver {
    complete_reported: bool,
    last_source_done_round_id: Option<u32>,
    last_round_need: Option<NeedReport>,
    expected_tree_ids: BTreeSet<u16>,
    pending_source_done_trees: BTreeMap<u32, BTreeSet<u16>>,
}

impl CloudcastReceiver {
    pub(super) fn new(tree_ids: &[u16]) -> Self {
        Self {
            complete_reported: false,
            last_source_done_round_id: None,
            last_round_need: None,
            expected_tree_ids: tree_ids.iter().copied().collect(),
            pending_source_done_trees: BTreeMap::new(),
        }
    }

    pub(super) fn last_source_done_round_id(&self) -> Option<u32> {
        self.last_source_done_round_id
    }

    pub(super) async fn handle_block_data_frame(
        &mut self,
        shared: &mut super::ReceiverShared,
        frame: InboundFrame,
    ) {
        if !self.accepts_data_tree(shared, frame.tree_id) {
            return;
        }
        let Some(manifest) = shared.manifest.as_ref() else {
            return;
        };
        if !matches!(manifest.mode, LosslessSessionMode::Plain) {
            return;
        }

        let Some((_, data, payload)) =
            nextmini_messages::lossless_session::decode_block_data(&frame.bytes)
        else {
            return;
        };
        if manifest.validate_block_data(&data, payload.len()).is_err() {
            return;
        }
        if shared.complete_blocks.contains(&data.block_id) {
            return;
        }

        shared.mark_first_payload_unit();
        shared.write_block(data.block_id, payload).await;
        shared.complete_blocks.insert(data.block_id);
        if shared.has_all_blocks() {
            shared.mark_object_complete();
        }
    }

    pub(super) async fn handle_source_done(
        &mut self,
        shared: &super::ReceiverShared,
        round_id: u32,
        tree_id: Option<u16>,
    ) {
        if let Some(last_round_id) = self.last_source_done_round_id {
            if round_id < last_round_id {
                debug!(
                    session_id = shared.session_id,
                    round_id, last_round_id, "Lossless Cloudcast receiver dropped stale SourceDone"
                );
                return;
            }
            if round_id == last_round_id {
                if let Some(report) = self.last_round_need.clone() {
                    debug!(
                        session_id = shared.session_id,
                        round_id,
                        "Lossless Cloudcast receiver replayed cached Need for duplicate SourceDone"
                    );
                    shared.send_plain_need(last_round_id, &report).await;
                    self.complete_reported = matches!(report, NeedReport::Complete);
                }
                return;
            }
        }

        if !self.source_done_boundary_ready(shared, round_id, tree_id) {
            return;
        }

        let Some(report) = shared.plain_need() else {
            return;
        };
        self.last_source_done_round_id = Some(round_id);
        self.last_round_need = Some(report.clone());
        self.pending_source_done_trees
            .retain(|pending_round, _| *pending_round > round_id);
        shared.send_plain_need(round_id, &report).await;
        self.complete_reported = matches!(report, NeedReport::Complete);
    }

    pub(super) fn is_complete(&self) -> bool {
        self.complete_reported
    }

    fn accepts_data_tree(&self, shared: &super::ReceiverShared, tree_id: Option<u16>) -> bool {
        if self.expected_tree_ids.is_empty() {
            return true;
        }
        let Some(tree_id) = tree_id else {
            warn!(
                session_id = shared.session_id,
                local_node_id = shared.local_node_id,
                "Lossless Cloudcast receiver ignored unscoped data frame"
            );
            return false;
        };
        if self.expected_tree_ids.contains(&tree_id) {
            return true;
        }
        warn!(
            session_id = shared.session_id,
            local_node_id = shared.local_node_id,
            tree_id,
            expected_tree_ids = ?self.expected_tree_ids,
            "Lossless Cloudcast receiver ignored data from unexpected tree"
        );
        false
    }

    fn source_done_boundary_ready(
        &mut self,
        shared: &super::ReceiverShared,
        round_id: u32,
        tree_id: Option<u16>,
    ) -> bool {
        if self.expected_tree_ids.is_empty() {
            return true;
        }
        let Some(tree_id) = tree_id else {
            warn!(
                session_id = shared.session_id,
                local_node_id = shared.local_node_id,
                round_id,
                "Lossless Cloudcast receiver ignored unscoped SourceDone"
            );
            return false;
        };
        if !self.expected_tree_ids.contains(&tree_id) {
            warn!(
                session_id = shared.session_id,
                local_node_id = shared.local_node_id,
                round_id,
                tree_id,
                expected_tree_ids = ?self.expected_tree_ids,
                "Lossless Cloudcast receiver ignored SourceDone from unexpected tree"
            );
            return false;
        }

        let seen = self.pending_source_done_trees.entry(round_id).or_default();
        seen.insert(tree_id);
        if seen.is_superset(&self.expected_tree_ids) {
            return true;
        }
        info!(
            session_id = shared.session_id,
            local_node_id = shared.local_node_id,
            round_id,
            tree_id,
            seen_tree_ids = ?seen,
            expected_tree_ids = ?self.expected_tree_ids,
            "Lossless Cloudcast receiver deferred SourceDone until all tree data boundaries arrive"
        );
        false
    }
}
