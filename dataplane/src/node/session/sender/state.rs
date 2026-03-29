use std::collections::BTreeSet;

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
}
