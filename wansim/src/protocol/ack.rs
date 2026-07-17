use thiserror::Error;

pub const MAX_BLOCK_ACK_RANGES: usize = u8::MAX as usize;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct CompletedRange {
    pub start: u64,
    pub end: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockAck {
    pub completed_watermark: u64,
    pub extra_completed: Vec<CompletedRange>,
}

impl BlockAck {
    pub fn empty() -> Self {
        Self {
            completed_watermark: 0,
            extra_completed: Vec::new(),
        }
    }

    pub fn complete(total_blocks: u64) -> Self {
        Self {
            completed_watermark: total_blocks,
            extra_completed: Vec::new(),
        }
    }

    pub fn canonicalized(mut self, total_blocks: u64) -> Result<Self, AckError> {
        if self.completed_watermark > total_blocks {
            return Err(AckError::WatermarkOutOfRange);
        }
        for range in &self.extra_completed {
            if range.start >= range.end || range.end > total_blocks {
                return Err(AckError::RangeOutOfRange);
            }
        }
        self.extra_completed.push(CompletedRange {
            start: 0,
            end: self.completed_watermark,
        });
        self.extra_completed.sort_unstable();
        let mut merged: Vec<CompletedRange> = Vec::with_capacity(self.extra_completed.len());
        for range in self.extra_completed {
            if range.start == range.end {
                continue;
            }
            if let Some(previous) = merged.last_mut()
                && range.start <= previous.end
            {
                previous.end = previous.end.max(range.end);
                continue;
            }
            merged.push(range);
        }
        self.completed_watermark = 0;
        if merged.first().is_some_and(|range| range.start == 0) {
            self.completed_watermark = merged.remove(0).end;
        }
        self.extra_completed = merged;
        Ok(self)
    }

    pub fn for_wire(self, total_blocks: u64) -> Result<Self, AckError> {
        let mut canonical = self.canonicalized(total_blocks)?;
        canonical.extra_completed.truncate(MAX_BLOCK_ACK_RANGES);
        Ok(canonical)
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PeerBlockCompletion {
    seen_ack: bool,
    snapshot: BlockAck,
}

impl Default for BlockAck {
    fn default() -> Self {
        Self::empty()
    }
}

impl PeerBlockCompletion {
    pub fn join(&mut self, ack: &BlockAck, total_blocks: u64) -> Result<bool, AckError> {
        let mut ranges = self.snapshot.extra_completed.clone();
        ranges.extend(ack.extra_completed.iter().copied());
        if self.snapshot.completed_watermark > 0 {
            ranges.push(CompletedRange {
                start: 0,
                end: self.snapshot.completed_watermark,
            });
        }
        if ack.completed_watermark > 0 {
            ranges.push(CompletedRange {
                start: 0,
                end: ack.completed_watermark,
            });
        }
        let joined = BlockAck {
            completed_watermark: 0,
            extra_completed: ranges,
        }
        .canonicalized(total_blocks)?;
        let grew = joined != self.snapshot;
        self.seen_ack = true;
        self.snapshot = joined;
        Ok(grew)
    }

    pub fn object_complete(&self, total_blocks: u64) -> bool {
        self.seen_ack && self.snapshot.completed_watermark == total_blocks
    }

    pub fn snapshot(&self) -> &BlockAck {
        &self.snapshot
    }
}

#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub enum AckError {
    #[error("BlockAck watermark is beyond the object")]
    WatermarkOutOfRange,
    #[error("BlockAck contains an empty or out-of-range interval")]
    RangeOutOfRange,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn touching_watermark_and_ranges_fold_into_one_prefix() {
        let ack = BlockAck {
            completed_watermark: 2,
            extra_completed: vec![
                CompletedRange { start: 4, end: 6 },
                CompletedRange { start: 2, end: 4 },
            ],
        }
        .canonicalized(8)
        .expect("valid ranges");
        assert_eq!(ack, BlockAck::complete(6));
    }

    #[test]
    fn lowest_ranges_are_retained_for_wire_truncation() {
        let ranges = (0..MAX_BLOCK_ACK_RANGES + 5)
            .map(|index| CompletedRange {
                start: 2 + (index as u64 * 2),
                end: 3 + (index as u64 * 2),
            })
            .collect();
        let ack = BlockAck {
            completed_watermark: 1,
            extra_completed: ranges,
        }
        .for_wire(1_000)
        .expect("bounded ack");
        assert_eq!(ack.extra_completed.len(), MAX_BLOCK_ACK_RANGES);
        assert_eq!(ack.extra_completed[0].start, 2);
    }
}
