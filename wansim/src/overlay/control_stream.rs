use std::sync::Arc;

use parking_lot::Mutex;
use thiserror::Error;

use crate::protocol::ControlFrame;

#[derive(Clone, Debug, Default)]
pub(crate) struct ControlStream {
    frames: Arc<Mutex<Vec<ControlStreamFrame>>>,
}

#[derive(Clone, Debug)]
struct ControlStreamFrame {
    frame: ControlFrame,
    wire_bytes: usize,
}

impl ControlStream {
    pub(crate) fn append(&self, frame: ControlFrame) -> Result<usize, ControlStreamError> {
        let wire_bytes = frame
            .payload_bytes()
            .checked_add(4)
            .ok_or(ControlStreamError::GeometryOverflow)?;
        self.frames
            .lock()
            .push(ControlStreamFrame { frame, wire_bytes });
        Ok(wire_bytes)
    }

    fn get(&self, index: usize) -> Option<ControlStreamFrame> {
        self.frames.lock().get(index).cloned()
    }
}

#[derive(Clone, Debug, Default)]
pub(crate) struct ControlStreamCursor {
    next_stream_offset: usize,
    next_frame_index: usize,
    buffered_bytes: usize,
}

impl ControlStreamCursor {
    pub(crate) fn ingest(
        &mut self,
        stream: &ControlStream,
        stream_offset: usize,
        byte_count: usize,
    ) -> Result<Vec<ControlFrame>, ControlStreamError> {
        if stream_offset != self.next_stream_offset {
            return Err(ControlStreamError::NoncontiguousDelivery {
                expected: self.next_stream_offset,
                actual: stream_offset,
            });
        }
        self.next_stream_offset = self
            .next_stream_offset
            .checked_add(byte_count)
            .ok_or(ControlStreamError::GeometryOverflow)?;
        self.buffered_bytes = self
            .buffered_bytes
            .checked_add(byte_count)
            .ok_or(ControlStreamError::GeometryOverflow)?;

        let mut complete = Vec::new();
        loop {
            let frame = stream.get(self.next_frame_index).ok_or(
                ControlStreamError::DescriptorNotPublished {
                    index: self.next_frame_index,
                },
            )?;
            if self.buffered_bytes < frame.wire_bytes {
                break;
            }
            self.buffered_bytes -= frame.wire_bytes;
            self.next_frame_index = self
                .next_frame_index
                .checked_add(1)
                .ok_or(ControlStreamError::GeometryOverflow)?;
            complete.push(frame.frame);
            if self.buffered_bytes == 0 {
                break;
            }
        }
        Ok(complete)
    }
}

#[derive(Debug, Error, PartialEq, Eq)]
pub(crate) enum ControlStreamError {
    #[error("control stream geometry overflow")]
    GeometryOverflow,
    #[error("control TCP delivered offset {actual}, expected {expected}")]
    NoncontiguousDelivery { expected: usize, actual: usize },
    #[error("control descriptor {index} was not published before its bytes arrived")]
    DescriptorNotPublished { index: usize },
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::BlockAck;

    #[test]
    fn cursor_reassembles_variable_length_control_frames() {
        let stream = ControlStream::default();
        let first = ControlFrame::AckProbe { target_peer_id: 7 };
        let second = ControlFrame::BlockAck(BlockAck::empty());
        let first_bytes = stream.append(first.clone()).expect("first geometry");
        let second_bytes = stream.append(second.clone()).expect("second geometry");
        let mut cursor = ControlStreamCursor::default();

        assert!(
            cursor
                .ingest(&stream, 0, first_bytes - 1)
                .expect("partial frame")
                .is_empty()
        );
        assert_eq!(
            cursor
                .ingest(&stream, first_bytes - 1, second_bytes + 1)
                .expect("joined delivery"),
            vec![first, second]
        );
    }
}
