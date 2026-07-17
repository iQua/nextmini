use std::collections::BTreeSet;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct DofObservation {
    pub(crate) innovative: bool,
    pub(crate) rank: usize,
    pub(crate) complete: bool,
}

#[derive(Debug)]
pub(crate) struct DofBucket {
    source_symbols: usize,
    innovative_frames: BTreeSet<usize>,
}

impl DofBucket {
    pub(crate) fn new(source_symbols: usize) -> Self {
        Self {
            source_symbols,
            innovative_frames: BTreeSet::new(),
        }
    }

    pub(crate) fn observe(&mut self, frame_id: usize) -> DofObservation {
        let innovative = self.innovative_frames.insert(frame_id);
        let rank = self.innovative_frames.len().min(self.source_symbols);
        DofObservation {
            innovative,
            rank,
            complete: rank == self.source_symbols,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn duplicate_delivery_does_not_increase_ideal_rank() {
        let mut bucket = DofBucket::new(2);
        assert_eq!(bucket.observe(7).rank, 1);
        assert_eq!(bucket.observe(7).rank, 1);
        assert_eq!(bucket.observe(8).rank, 2);
    }
}
