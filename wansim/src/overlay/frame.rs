use std::collections::VecDeque;

use thiserror::Error;

use crate::determinism::CounterPrf;

#[derive(Clone, Debug)]
pub(crate) struct FramedStream {
    frame_count: usize,
    payload_bytes: usize,
    frame_wire_bytes: usize,
    total_bytes: usize,
    prf: CounterPrf,
}

impl FramedStream {
    pub(crate) fn new(
        frame_count: usize,
        payload_bytes: usize,
        prf: CounterPrf,
    ) -> Result<Self, FrameError> {
        let payload_u32 = u32::try_from(payload_bytes).map_err(|_| FrameError::PayloadTooLarge)?;
        let frame_wire_bytes = payload_bytes
            .checked_add(4)
            .ok_or(FrameError::GeometryOverflow)?;
        let total_bytes = frame_count
            .checked_mul(frame_wire_bytes)
            .ok_or(FrameError::GeometryOverflow)?;
        let _ = payload_u32;
        Ok(Self {
            frame_count,
            payload_bytes,
            frame_wire_bytes,
            total_bytes,
            prf,
        })
    }

    pub(crate) fn total_bytes(&self) -> usize {
        self.total_bytes
    }

    pub(crate) fn frame_wire_bytes(&self) -> usize {
        self.frame_wire_bytes
    }

    pub(crate) fn frame_count(&self) -> usize {
        self.frame_count
    }

    pub(crate) fn bytes(&self, offset: usize, byte_count: usize) -> Result<Vec<u8>, FrameError> {
        let end = offset
            .checked_add(byte_count)
            .ok_or(FrameError::GeometryOverflow)?;
        if end > self.total_bytes {
            return Err(FrameError::RangeBeyondStream {
                end,
                stream_bytes: self.total_bytes,
            });
        }
        let mut bytes = Vec::with_capacity(byte_count);
        for stream_offset in offset..end {
            let within_frame = stream_offset % self.frame_wire_bytes;
            let frame_id = stream_offset / self.frame_wire_bytes;
            if within_frame < 4 {
                bytes.push((self.payload_bytes as u32).to_be_bytes()[within_frame]);
            } else {
                let payload_offset = within_frame - 4;
                bytes.push(self.prf.draw_byte(
                    "logical-frame-payload",
                    frame_id as u64,
                    0,
                    payload_offset as u64,
                ));
            }
        }
        Ok(bytes)
    }
}

#[derive(Debug)]
pub(crate) struct FrameAssembler {
    maximum_payload_bytes: usize,
    next_stream_offset: usize,
    next_frame_id: usize,
    buffered: VecDeque<u8>,
}

impl FrameAssembler {
    pub(crate) fn new(maximum_payload_bytes: usize) -> Self {
        Self {
            maximum_payload_bytes,
            next_stream_offset: 0,
            next_frame_id: 0,
            buffered: VecDeque::new(),
        }
    }

    pub(crate) fn buffered_bytes(&self) -> usize {
        self.buffered.len()
    }

    pub(crate) fn ingest(
        &mut self,
        stream: &FramedStream,
        stream_offset: usize,
        byte_count: usize,
    ) -> Result<Vec<AssembledFrame>, FrameError> {
        if stream_offset != self.next_stream_offset {
            return Err(FrameError::NoncontiguousDelivery {
                expected: self.next_stream_offset,
                actual: stream_offset,
            });
        }
        self.buffered
            .extend(stream.bytes(stream_offset, byte_count)?);
        self.next_stream_offset = self
            .next_stream_offset
            .checked_add(byte_count)
            .ok_or(FrameError::GeometryOverflow)?;

        let mut frames = Vec::new();
        loop {
            if self.buffered.len() < 4 {
                break;
            }
            let prefix = [
                self.buffered[0],
                self.buffered[1],
                self.buffered[2],
                self.buffered[3],
            ];
            let payload_bytes = u32::from_be_bytes(prefix) as usize;
            if payload_bytes > self.maximum_payload_bytes {
                return Err(FrameError::PayloadExceedsLimit {
                    payload_bytes,
                    maximum: self.maximum_payload_bytes,
                });
            }
            let frame_wire_bytes = payload_bytes
                .checked_add(4)
                .ok_or(FrameError::GeometryOverflow)?;
            if self.buffered.len() < frame_wire_bytes {
                break;
            }
            self.buffered.drain(..frame_wire_bytes);
            frames.push(AssembledFrame {
                frame_id: self.next_frame_id,
                wire_bytes: frame_wire_bytes,
            });
            self.next_frame_id += 1;
        }
        Ok(frames)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct AssembledFrame {
    pub(crate) frame_id: usize,
    pub(crate) wire_bytes: usize,
}

#[derive(Debug, Error)]
pub(crate) enum FrameError {
    #[error("logical-frame payload does not fit a u32 length prefix")]
    PayloadTooLarge,
    #[error("logical-frame geometry overflow")]
    GeometryOverflow,
    #[error("stream byte range ends at {end}, beyond stream length {stream_bytes}")]
    RangeBeyondStream { end: usize, stream_bytes: usize },
    #[error("TCP delivered stream offset {actual}, expected {expected}")]
    NoncontiguousDelivery { expected: usize, actual: usize },
    #[error("frame payload {payload_bytes} exceeds configured limit {maximum}")]
    PayloadExceedsLimit {
        payload_bytes: usize,
        maximum: usize,
    },
}
