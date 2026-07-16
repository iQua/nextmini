use std::collections::BTreeSet;

use nextmini_messages::lossless_session::{BlockAck, CompletedBlockRange};
use tokio::time::{Duration, Instant};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ActiveSessionQuorum {
    configured: BTreeSet<usize>,
    active: BTreeSet<usize>,
    frozen: bool,
}

impl ActiveSessionQuorum {
    pub(super) fn new(configured: impl IntoIterator<Item = usize>) -> Self {
        Self {
            configured: configured.into_iter().collect(),
            active: BTreeSet::new(),
            frozen: false,
        }
    }

    pub(super) fn record_ready(&mut self, peer_id: usize) {
        if self.frozen || !self.configured.contains(&peer_id) {
            return;
        }
        self.active.insert(peer_id);
    }

    pub(super) fn freeze(&mut self) {
        self.frozen = true;
    }

    pub(super) fn is_frozen(&self) -> bool {
        self.frozen
    }

    pub(super) fn active_members(&self) -> &BTreeSet<usize> {
        &self.active
    }

    pub(super) fn configured_len(&self) -> usize {
        self.configured.len()
    }

    pub(super) fn configured_members(&self) -> &BTreeSet<usize> {
        &self.configured
    }
}

/// Monotone completion knowledge held for one carousel peer.
///
/// The representation is the canonical interval union from protocol P4.  It
/// deliberately records whether an acknowledgement was observed separately:
/// for an empty object, an empty set is complete only after the peer has
/// actually acknowledged the transfer.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(super) struct PeerBlockCompletion {
    seen_ack: bool,
    completed_watermark: u64,
    extra_completed: Vec<CompletedBlockRange>,
}

impl PeerBlockCompletion {
    /// Join a validated cumulative acknowledgement into the stored set.
    ///
    /// Returns `true` only when the completion set grows. Duplicate and
    /// reordered snapshots still set `seen_ack`, but are completion no-ops.
    pub(super) fn join(&mut self, ack: &BlockAck) -> bool {
        let (completed_watermark, extra_completed) = match ack {
            BlockAck::Blocks {
                completed_watermark,
                extra_completed,
            } => (completed_watermark, extra_completed),
            _ => return false,
        };
        self.seen_ack = true;

        let previous_watermark = self.completed_watermark;
        let previous_ranges = self.extra_completed.clone();
        let mut intervals = Vec::with_capacity(
            self.extra_completed
                .len()
                .saturating_add(extra_completed.len())
                .saturating_add(2),
        );
        if self.completed_watermark > 0 {
            intervals.push(CompletedBlockRange {
                start_block_id: 0,
                end_block_id: self.completed_watermark,
            });
        }
        intervals.extend(self.extra_completed.iter().copied());
        if *completed_watermark > 0 {
            intervals.push(CompletedBlockRange {
                start_block_id: 0,
                end_block_id: *completed_watermark,
            });
        }
        intervals.extend(extra_completed.iter().copied());
        intervals.sort_unstable_by_key(|range| (range.start_block_id, range.end_block_id));

        let mut merged: Vec<CompletedBlockRange> = Vec::with_capacity(intervals.len());
        for range in intervals {
            if let Some(previous) = merged.last_mut()
                && range.start_block_id <= previous.end_block_id
            {
                previous.end_block_id = previous.end_block_id.max(range.end_block_id);
                continue;
            }
            merged.push(range);
        }

        self.completed_watermark = 0;
        self.extra_completed.clear();
        if merged
            .first()
            .is_some_and(|range| range.start_block_id == 0)
        {
            self.completed_watermark = merged.remove(0).end_block_id;
        }
        self.extra_completed = merged;

        self.completed_watermark != previous_watermark || self.extra_completed != previous_ranges
    }

    pub(super) fn contains(&self, block_id: u64) -> bool {
        if block_id < self.completed_watermark {
            return true;
        }
        let index = self
            .extra_completed
            .partition_point(|range| range.end_block_id <= block_id);
        self.extra_completed
            .get(index)
            .is_some_and(|range| range.start_block_id <= block_id && block_id < range.end_block_id)
    }

    pub(super) fn object_complete(&self, total_blocks: u64) -> bool {
        self.seen_ack && self.completed_watermark == total_blocks
    }

    #[cfg(test)]
    pub(super) fn snapshot(&self) -> BlockAck {
        BlockAck::Blocks {
            completed_watermark: self.completed_watermark,
            extra_completed: self.extra_completed.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct QuorumLiveness {
    solicitation_interval: Duration,
    peer_report_timeout: Duration,
    started_at: Option<Instant>,
    next_solicitation_at: Option<Instant>,
    solicitation_count: u32,
}

impl QuorumLiveness {
    pub(super) fn new(solicitation_interval: Duration, peer_report_timeout: Duration) -> Self {
        Self {
            solicitation_interval,
            peer_report_timeout,
            started_at: None,
            next_solicitation_at: None,
            solicitation_count: 0,
        }
    }

    pub(super) fn start(&mut self, now: Instant) {
        self.started_at = Some(now);
        self.next_solicitation_at = Some(now + self.solicitation_interval);
        self.solicitation_count = 0;
    }

    pub(super) fn clear(&mut self) {
        self.started_at = None;
        self.next_solicitation_at = None;
        self.solicitation_count = 0;
    }

    pub(super) fn started_at(&self) -> Option<Instant> {
        self.started_at
    }

    pub(super) fn next_solicitation_at(&self) -> Option<Instant> {
        self.next_solicitation_at
    }

    pub(super) fn timeout_at(&self) -> Option<Instant> {
        self.started_at
            .map(|started_at| started_at + self.peer_report_timeout)
    }

    pub(super) fn should_solicit(&self, now: Instant) -> bool {
        self.next_solicitation_at
            .is_some_and(|deadline| now >= deadline)
    }

    pub(super) fn timed_out(&self, now: Instant) -> bool {
        self.timeout_at().is_some_and(|deadline| now >= deadline)
    }

    pub(super) fn note_solicitation(&mut self, now: Instant) {
        self.next_solicitation_at = Some(now + self.solicitation_interval);
        self.solicitation_count += 1;
    }

    pub(super) fn note_feedback_progress(&mut self, now: Instant) {
        if self.started_at.is_none() {
            return;
        }
        self.next_solicitation_at = Some(now + self.solicitation_interval);
    }

    pub(super) fn solicitation_count(&self) -> u32 {
        self.solicitation_count
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quorum_freeze_preserves_only_pre_freeze_ready_peers() {
        let mut quorum = ActiveSessionQuorum::new([1, 2, 3]);
        quorum.record_ready(1);
        quorum.record_ready(2);
        quorum.freeze();
        quorum.record_ready(3);

        assert!(quorum.is_frozen());
        assert_eq!(
            quorum.active_members().iter().copied().collect::<Vec<_>>(),
            vec![1, 2]
        );
    }

    #[test]
    fn quorum_liveness_solicits_before_timing_out() {
        let mut liveness =
            QuorumLiveness::new(Duration::from_millis(10), Duration::from_millis(30));
        let now = Instant::now();

        assert!(liveness.started_at().is_none());
        liveness.start(now);
        assert_eq!(liveness.started_at(), Some(now));
        assert_eq!(
            liveness.next_solicitation_at(),
            Some(now + Duration::from_millis(10))
        );
        assert!(!liveness.should_solicit(now));
        assert!(liveness.should_solicit(now + Duration::from_millis(10)));
        assert!(!liveness.timed_out(now + Duration::from_millis(29)));
        assert!(liveness.timed_out(now + Duration::from_millis(30)));

        liveness.note_solicitation(now + Duration::from_millis(10));
        assert_eq!(
            liveness.next_solicitation_at(),
            Some(now + Duration::from_millis(20))
        );
        assert_eq!(liveness.solicitation_count(), 1);

        liveness.clear();
        assert!(liveness.started_at().is_none());
        assert!(liveness.next_solicitation_at().is_none());
        assert_eq!(liveness.solicitation_count(), 0);
    }

    #[test]
    fn quorum_liveness_feedback_progress_pushes_back_next_solicitation() {
        let mut liveness =
            QuorumLiveness::new(Duration::from_millis(10), Duration::from_millis(30));
        let now = Instant::now();

        liveness.start(now);
        let original = liveness.next_solicitation_at().unwrap();
        let progress_at = now + Duration::from_millis(5);
        liveness.note_feedback_progress(progress_at);

        assert_eq!(
            liveness.next_solicitation_at(),
            Some(progress_at + Duration::from_millis(10))
        );
        assert!(liveness.next_solicitation_at().unwrap() > original);
        assert_eq!(liveness.solicitation_count(), 0);
    }

    #[test]
    fn peer_block_completion_joins_reordered_overlapping_snapshots() {
        let mut completion = PeerBlockCompletion::default();
        let later = BlockAck::Blocks {
            completed_watermark: 2,
            extra_completed: vec![CompletedBlockRange {
                start_block_id: 5,
                end_block_id: 7,
            }],
        };
        let earlier = BlockAck::Blocks {
            completed_watermark: 5,
            extra_completed: vec![CompletedBlockRange {
                start_block_id: 7,
                end_block_id: 9,
            }],
        };

        assert!(completion.join(&later));
        assert!(completion.join(&earlier));
        assert!(!completion.join(&later));
        assert_eq!(
            completion.snapshot(),
            BlockAck::Blocks {
                completed_watermark: 9,
                extra_completed: Vec::new(),
            }
        );
        assert!(completion.contains(8));
        assert!(!completion.contains(9));
    }

    #[test]
    fn empty_object_requires_an_observed_ack() {
        let mut completion = PeerBlockCompletion::default();
        assert!(!completion.object_complete(0));
        assert!(!completion.join(&BlockAck::Blocks {
            completed_watermark: 0,
            extra_completed: Vec::new(),
        }));
        assert!(completion.object_complete(0));
    }
}
