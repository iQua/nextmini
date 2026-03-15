use nextmini_messages::lossless_session::PlainStatus;

use crate::node::session::api::InboundFrame;

/// Plain-mode receiver state machine.
#[derive(Default)]
pub(super) struct PlainReceiver {
    complete_reported: bool,
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

        shared.write_block(data.block_id, payload).await;
        shared.complete_blocks.insert(data.block_id);
    }

    pub(super) async fn handle_eot(&mut self, shared: &super::ReceiverShared) {
        let Some(status) = shared.plain_status() else {
            return;
        };
        shared.send_plain_status(&status).await;
        self.complete_reported = matches!(status, PlainStatus::Complete);
    }

    pub(super) fn is_complete(&self) -> bool {
        self.complete_reported
    }
}
