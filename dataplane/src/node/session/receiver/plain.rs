use nextmini_messages::lossless_session::PlainStatus;

use crate::node::session::api::InboundFrame;

/// Plain-mode receiver state machine.
#[derive(Default)]
pub(super) struct PlainReceiver {
    complete_reported: bool,
    last_source_done_round_id: Option<u32>,
    last_round_status: Option<PlainStatus>,
}

impl PlainReceiver {
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
        if manifest.validate_block_data(&data).is_err() {
            return;
        }
        if shared.complete_blocks.contains(&data.block_id) {
            return;
        }

        shared.mark_first_payload_unit();
        shared.write_block(data.block_id, payload).await;
        shared.complete_blocks.insert(data.block_id);
    }

    pub(super) async fn handle_source_done(
        &mut self,
        shared: &super::ReceiverShared,
        round_id: u32,
    ) {
        if let Some(last_round_id) = self.last_source_done_round_id {
            if round_id < last_round_id {
                return;
            }
            if round_id == last_round_id {
                if let Some(status) = self.last_round_status.clone() {
                    shared.send_plain_status(&status).await;
                    self.complete_reported = matches!(status, PlainStatus::Complete);
                }
                return;
            }
        }

        let Some(status) = shared.plain_status() else {
            return;
        };
        self.last_source_done_round_id = Some(round_id);
        self.last_round_status = Some(status.clone());
        shared.send_plain_status(&status).await;
        self.complete_reported = matches!(status, PlainStatus::Complete);
    }

    pub(super) fn is_complete(&self) -> bool {
        self.complete_reported
    }
}
