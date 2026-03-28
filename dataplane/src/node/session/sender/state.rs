#![allow(dead_code)]

// T2 intentionally lands the shared round/quorum vocabulary before the sender
// behavior rewrites consume it in later issues.

use std::collections::{BTreeMap, BTreeSet};

use tokio::time::{Duration, Instant};

pub(super) type RoundId = u32;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum PeerRoundReportState {
    Pending,
    Reported,
}

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
}

impl QuorumLiveness {
    pub(super) fn new(solicitation_interval: Duration, peer_report_timeout: Duration) -> Self {
        Self {
            solicitation_interval,
            peer_report_timeout,
            started_at: None,
            next_solicitation_at: None,
        }
    }

    pub(super) fn start(&mut self, now: Instant) {
        self.started_at = Some(now);
        self.next_solicitation_at = Some(now + self.solicitation_interval);
    }

    pub(super) fn clear(&mut self) {
        self.started_at = None;
        self.next_solicitation_at = None;
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
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(super) struct PlainWorkFrontier {
    blocks: BTreeSet<u64>,
}

impl PlainWorkFrontier {
    pub(super) fn record_block(&mut self, block_id: u64) {
        self.blocks.insert(block_id);
    }

    pub(super) fn covers(&self, required: &Self) -> bool {
        required.blocks.is_subset(&self.blocks)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(super) struct FecWorkFrontier {
    deficits: BTreeMap<u64, u16>,
}

impl FecWorkFrontier {
    pub(super) fn record_required_deficit(&mut self, block_id: u64, deficit_symbols: u16) {
        let entry = self.deficits.entry(block_id).or_default();
        *entry = (*entry).max(deficit_symbols);
    }

    pub(super) fn record_emitted_symbols(&mut self, block_id: u64, emitted_symbols: u16) {
        let entry = self.deficits.entry(block_id).or_default();
        *entry = entry.saturating_add(emitted_symbols);
    }

    pub(super) fn covers(&self, required: &Self) -> bool {
        required
            .deficits
            .iter()
            .all(|(block_id, required_symbols)| {
                self.deficits.get(block_id).copied().unwrap_or(0) >= *required_symbols
            })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum WorkFrontier {
    Plain(PlainWorkFrontier),
    Fec(FecWorkFrontier),
}

impl WorkFrontier {
    pub(super) fn plain() -> Self {
        Self::Plain(PlainWorkFrontier::default())
    }

    pub(super) fn fec() -> Self {
        Self::Fec(FecWorkFrontier::default())
    }

    pub(super) fn covers(&self, required: &Self) -> bool {
        match (self, required) {
            (Self::Plain(emitted), Self::Plain(required)) => emitted.covers(required),
            (Self::Fec(emitted), Self::Fec(required)) => emitted.covers(required),
            _ => false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct SenderRoundState {
    pub(super) feedback_open_round_id: RoundId,
    pub(super) current_burst_id: RoundId,
    pub(super) peer_report_status: BTreeMap<usize, PeerRoundReportState>,
    pub(super) quorum: ActiveSessionQuorum,
    pub(super) required_work_frontier: WorkFrontier,
    pub(super) emitted_work_frontier: WorkFrontier,
    pub(super) next_burst_nonempty: bool,
    pub(super) source_done_pending: bool,
}

impl SenderRoundState {
    pub(super) fn new_plain(quorum: ActiveSessionQuorum) -> Self {
        Self::new(quorum, WorkFrontier::plain(), WorkFrontier::plain())
    }

    pub(super) fn new_fec(quorum: ActiveSessionQuorum) -> Self {
        Self::new(quorum, WorkFrontier::fec(), WorkFrontier::fec())
    }

    fn new(
        quorum: ActiveSessionQuorum,
        required_work_frontier: WorkFrontier,
        emitted_work_frontier: WorkFrontier,
    ) -> Self {
        let peer_report_status = quorum
            .active_members()
            .iter()
            .copied()
            .map(|peer_id| (peer_id, PeerRoundReportState::Pending))
            .collect();

        Self {
            feedback_open_round_id: 0,
            current_burst_id: 0,
            peer_report_status,
            quorum,
            required_work_frontier,
            emitted_work_frontier,
            next_burst_nonempty: false,
            source_done_pending: false,
        }
    }

    pub(super) fn mark_reported(&mut self, peer_id: usize) {
        if let Some(status) = self.peer_report_status.get_mut(&peer_id) {
            *status = PeerRoundReportState::Reported;
        }
    }

    pub(super) fn all_active_peers_reported(&self) -> bool {
        self.peer_report_status
            .values()
            .all(|status| *status == PeerRoundReportState::Reported)
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
    fn plain_work_frontier_uses_explicit_block_sets() {
        let mut required = PlainWorkFrontier::default();
        required.record_block(4);
        required.record_block(7);

        let mut emitted = PlainWorkFrontier::default();
        emitted.record_block(4);
        assert!(!emitted.covers(&required));

        emitted.record_block(7);
        assert!(emitted.covers(&required));
    }

    #[test]
    fn fec_work_frontier_tracks_required_and_emitted_symbols_explicitly() {
        let mut required = FecWorkFrontier::default();
        required.record_required_deficit(5, 2);
        required.record_required_deficit(5, 4);
        required.record_required_deficit(9, 1);

        let mut emitted = FecWorkFrontier::default();
        emitted.record_emitted_symbols(5, 3);
        emitted.record_emitted_symbols(9, 1);
        assert!(!emitted.covers(&required));

        emitted.record_emitted_symbols(5, 1);
        assert!(emitted.covers(&required));
    }

    #[test]
    fn sender_round_state_keeps_burst_identity_separate_from_feedback_round() {
        let mut quorum = ActiveSessionQuorum::new([11, 12]);
        quorum.record_ready(11);
        quorum.record_ready(12);
        quorum.freeze();

        let mut state = SenderRoundState::new_plain(quorum);
        state.feedback_open_round_id = 3;
        state.current_burst_id = 4;
        state.next_burst_nonempty = true;
        state.source_done_pending = true;
        state.mark_reported(11);

        assert_eq!(state.feedback_open_round_id, 3);
        assert_eq!(state.current_burst_id, 4);
        assert!(state.next_burst_nonempty);
        assert!(state.source_done_pending);
        assert!(!state.all_active_peers_reported());

        state.mark_reported(12);
        assert!(state.all_active_peers_reported());
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

        liveness.clear();
        assert!(liveness.started_at().is_none());
        assert!(liveness.next_solicitation_at().is_none());
    }
}
