use std::collections::{BTreeMap, BTreeSet};

use super::ControlFrame;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RoundsSenderState {
    Sending,
    WaitingForReports,
    Finished,
}

#[derive(Clone, Debug)]
pub struct RoundsSender {
    peers: BTreeSet<u64>,
    reports: BTreeMap<u64, usize>,
    state: RoundsSenderState,
    round_id: u32,
    emissions_remaining: usize,
    next_symbol_id: u64,
    barrier_announced: bool,
}

impl RoundsSender {
    pub fn new(source_symbols: usize, peers: impl IntoIterator<Item = u64>) -> Self {
        Self {
            peers: peers.into_iter().collect(),
            reports: BTreeMap::new(),
            state: RoundsSenderState::Sending,
            round_id: 0,
            emissions_remaining: source_symbols,
            next_symbol_id: 0,
            barrier_announced: false,
        }
    }

    pub fn state(&self) -> RoundsSenderState {
        self.state
    }

    pub fn round_id(&self) -> u32 {
        self.round_id
    }

    pub fn next_data_emission(&mut self) -> Option<u64> {
        if self.state != RoundsSenderState::Sending || self.emissions_remaining == 0 {
            return None;
        }
        let symbol_id = self.next_symbol_id;
        self.next_symbol_id = self.next_symbol_id.checked_add(1)?;
        self.emissions_remaining -= 1;
        Some(symbol_id)
    }

    pub fn poll_controls(&mut self) -> Vec<(u64, ControlFrame)> {
        if self.state != RoundsSenderState::Sending
            || self.emissions_remaining != 0
            || self.barrier_announced
        {
            return Vec::new();
        }
        self.barrier_announced = true;
        self.state = RoundsSenderState::WaitingForReports;
        self.peers
            .iter()
            .map(|peer_id| {
                (
                    *peer_id,
                    ControlFrame::SourceDone {
                        round_id: self.round_id,
                    },
                )
            })
            .collect()
    }

    pub fn on_need(&mut self, peer_id: u64, round_id: u32, deficit: usize) -> bool {
        if self.state != RoundsSenderState::WaitingForReports
            || round_id != self.round_id
            || !self.peers.contains(&peer_id)
        {
            return false;
        }
        self.reports.insert(peer_id, deficit);
        if self.reports.len() != self.peers.len() {
            return true;
        }
        let maximum_deficit = self.reports.values().copied().max().unwrap_or(0);
        self.reports.clear();
        if maximum_deficit == 0 {
            self.state = RoundsSenderState::Finished;
        } else {
            self.round_id = self.round_id.saturating_add(1);
            self.emissions_remaining = maximum_deficit;
            self.barrier_announced = false;
            self.state = RoundsSenderState::Sending;
        }
        true
    }
}

#[derive(Clone, Debug)]
pub struct RoundsReceiver {
    source_symbols: usize,
    innovative: BTreeSet<u64>,
    cached_reports: BTreeMap<u32, usize>,
    local_completion_ns: Option<u64>,
}

impl RoundsReceiver {
    pub fn new(source_symbols: usize) -> Self {
        Self {
            source_symbols,
            innovative: BTreeSet::new(),
            cached_reports: BTreeMap::new(),
            local_completion_ns: None,
        }
    }

    pub fn observe_symbol(&mut self, symbol_id: u64, now_ns: u64) -> bool {
        let innovative = self.innovative.insert(symbol_id);
        if self.rank() == self.source_symbols && self.local_completion_ns.is_none() {
            self.local_completion_ns = Some(now_ns);
        }
        innovative
    }

    pub fn rank(&self) -> usize {
        self.innovative.len().min(self.source_symbols)
    }

    pub fn local_completion_ns(&self) -> Option<u64> {
        self.local_completion_ns
    }

    pub fn on_source_done(&mut self, round_id: u32) -> ControlFrame {
        let current_deficit = self.source_symbols.saturating_sub(self.rank());
        let deficit = *self
            .cached_reports
            .entry(round_id)
            .or_insert(current_deficit);
        ControlFrame::Need { round_id, deficit }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rounds_waits_for_every_peer_and_uses_maximum_deficit() {
        let mut sender = RoundsSender::new(4, [1, 2]);
        for _ in 0..4 {
            assert!(sender.next_data_emission().is_some());
        }
        assert_eq!(sender.poll_controls().len(), 2);
        assert!(sender.on_need(1, 0, 1));
        assert_eq!(sender.state(), RoundsSenderState::WaitingForReports);
        assert!(sender.on_need(2, 0, 3));
        assert_eq!(sender.state(), RoundsSenderState::Sending);
        assert_eq!(sender.round_id(), 1);
        assert_eq!(
            (0..4).filter_map(|_| sender.next_data_emission()).count(),
            3
        );
    }

    #[test]
    fn duplicate_source_done_replays_the_original_report() {
        let mut receiver = RoundsReceiver::new(4);
        receiver.observe_symbol(0, 1);
        let first = receiver.on_source_done(0);
        receiver.observe_symbol(1, 2);
        assert_eq!(receiver.on_source_done(0), first);
    }
}
