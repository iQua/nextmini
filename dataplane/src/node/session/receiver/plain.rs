use nextmini_messages::lossless_session::NeedReport;
use tracing::debug;

use crate::node::session::api::InboundFrame;

/// Plain-mode receiver state machine.
#[derive(Default)]
pub(super) struct PlainReceiver {
    complete_reported: bool,
    last_source_done_round_id: Option<u32>,
    last_round_need: Option<NeedReport>,
}

impl PlainReceiver {
    pub(super) fn last_source_done_round_id(&self) -> Option<u32> {
        self.last_source_done_round_id
    }

    /// Handle one plain data block.
    pub(super) async fn handle_block_data_frame(
        &mut self,
        shared: &mut super::ReceiverShared,
        frame: InboundFrame,
    ) {
        let Some(manifest) = shared.manifest.as_ref() else {
            return;
        };
        if !matches!(
            manifest.mode,
            nextmini_messages::lossless_session::LosslessSessionMode::Plain
        ) {
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
    ) {
        if let Some(last_round_id) = self.last_source_done_round_id {
            if round_id < last_round_id {
                debug!(
                    session_id = shared.session_id,
                    round_id, last_round_id, "Lossless plain receiver dropped stale SourceDone"
                );
                return;
            }
            if round_id == last_round_id {
                if let Some(report) = self.last_round_need.clone() {
                    debug!(
                        session_id = shared.session_id,
                        round_id,
                        "Lossless plain receiver replayed cached Need for duplicate SourceDone"
                    );
                    shared.send_plain_need(last_round_id, &report).await;
                    self.complete_reported = matches!(report, NeedReport::Complete);
                }
                return;
            }
        }

        let Some(report) = shared.plain_need() else {
            return;
        };
        self.last_source_done_round_id = Some(round_id);
        self.last_round_need = Some(report.clone());
        shared.send_plain_need(round_id, &report).await;
        self.complete_reported = matches!(report, NeedReport::Complete);
    }

    pub(super) fn is_complete(&self) -> bool {
        self.complete_reported
    }
}
