use nextmini_messages::lossless_session::{LosslessSessionMode, NeedReport};
use tracing::debug;

use crate::node::session::api::InboundFrame;

/// Cloudcast receiver: plain blocks over the configured runtime trees.
pub(super) struct CloudcastReceiver {
    complete_reported: bool,
    last_source_done_round_id: Option<u32>,
    last_round_need: Option<NeedReport>,
}

impl CloudcastReceiver {
    pub(super) fn new(_tree_ids: &[u16]) -> Self {
        Self {
            complete_reported: false,
            last_source_done_round_id: None,
            last_round_need: None,
        }
    }

    pub(super) fn last_source_done_round_id(&self) -> Option<u32> {
        self.last_source_done_round_id
    }

    pub(super) async fn handle_block_data_frame(
        &mut self,
        shared: &mut super::ReceiverShared,
        frame: InboundFrame,
    ) -> Result<(), super::SinkWriteError> {
        let Some(manifest) = shared.manifest.as_ref() else {
            return Ok(());
        };
        if !matches!(manifest.mode, LosslessSessionMode::Plain) {
            return Ok(());
        }

        let Some((_, data, payload)) =
            nextmini_messages::lossless_session::decode_block_data(&frame.bytes)
        else {
            return Ok(());
        };
        if manifest.validate_block_data(&data, payload.len()).is_err() {
            return Ok(());
        }
        if shared.complete_blocks.contains(&data.block_id) {
            return Ok(());
        }

        shared.mark_first_payload_unit();
        shared.write_block(data.block_id, payload).await?;
        shared.complete_blocks.insert(data.block_id);
        if shared.has_all_blocks() {
            shared.mark_object_complete();
            if let Some(round_id) = self.last_source_done_round_id {
                self.report_complete(shared, round_id).await;
            }
        }
        Ok(())
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

        self.last_source_done_round_id = Some(round_id);
        if shared.has_all_blocks() {
            self.report_complete(shared, round_id).await;
        }
    }

    pub(super) fn is_complete(&self) -> bool {
        self.complete_reported
    }

    async fn report_complete(&mut self, shared: &super::ReceiverShared, round_id: u32) {
        if self.complete_reported {
            return;
        }
        let report = NeedReport::Complete;
        self.last_round_need = Some(report.clone());
        shared.send_plain_need(round_id, &report).await;
        self.complete_reported = true;
    }
}
