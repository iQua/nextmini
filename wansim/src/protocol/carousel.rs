use std::collections::{BTreeMap, BTreeSet};

use thiserror::Error;

use super::{BlockAck, ControlFrame, PeerBlockCompletion};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CarouselTiming {
    pub ack_debounce_ns: u64,
    pub ack_heartbeat_ns: u64,
    pub ack_probe_interval_ns: u64,
    pub peer_silence_timeout_ns: u64,
    pub peer_stall_timeout_ns: u64,
    pub receiver_passive_window_ns: u64,
    pub session_complete_repeats: usize,
    pub session_complete_interval_ns: u64,
}

impl CarouselTiming {
    pub fn validate(self) -> Result<Self, CarouselConfigError> {
        for (field, value) in [
            ("ack_debounce_ns", self.ack_debounce_ns),
            ("ack_heartbeat_ns", self.ack_heartbeat_ns),
            ("ack_probe_interval_ns", self.ack_probe_interval_ns),
            ("peer_silence_timeout_ns", self.peer_silence_timeout_ns),
            ("peer_stall_timeout_ns", self.peer_stall_timeout_ns),
            (
                "receiver_passive_window_ns",
                self.receiver_passive_window_ns,
            ),
            (
                "session_complete_interval_ns",
                self.session_complete_interval_ns,
            ),
        ] {
            if value == 0 {
                return Err(CarouselConfigError::ZeroDuration(field));
            }
        }
        if self.session_complete_repeats == 0 {
            return Err(CarouselConfigError::ZeroCompletionRepeats);
        }
        if self.peer_stall_timeout_ns <= self.peer_silence_timeout_ns {
            return Err(CarouselConfigError::StallNotLongerThanSilence);
        }
        if self.receiver_passive_window_ns <= self.peer_stall_timeout_ns {
            return Err(CarouselConfigError::PassiveWindowTooShort);
        }
        Ok(self)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CarouselSenderState {
    Sending,
    Probing,
    Finished,
    Aborted,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CarouselReceiverState {
    Active,
    LocallyComplete,
    Finished,
}

#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub enum LivenessViolation {
    #[error("peer {peer_id} exceeded the BlockAck silence timeout")]
    Silent { peer_id: u64 },
    #[error("peer {peer_id} remained alive without completion progress")]
    Stalled { peer_id: u64 },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OutboundControl {
    pub peer_id: u64,
    pub frame: ControlFrame,
}

#[derive(Clone, Debug)]
struct SenderPeer {
    completion: PeerBlockCompletion,
    last_ack_seen_ns: u64,
    last_ack_progress_ns: u64,
    next_probe_ns: u64,
}

#[derive(Clone, Debug)]
pub struct CarouselSender {
    total_blocks: u64,
    timing: CarouselTiming,
    peers: BTreeMap<u64, SenderPeer>,
    state: CarouselSenderState,
    next_symbol_id: u64,
    completion_repeats_sent: usize,
    next_completion_repeat_ns: u64,
    completion_time_ns: Option<u64>,
}

impl CarouselSender {
    pub fn new(
        total_blocks: u64,
        peer_ids: impl IntoIterator<Item = u64>,
        ready_freeze_ns: u64,
        timing: CarouselTiming,
    ) -> Result<Self, CarouselConfigError> {
        let timing = timing.validate()?;
        let peers: BTreeMap<_, _> = peer_ids
            .into_iter()
            .map(|peer_id| {
                (
                    peer_id,
                    SenderPeer {
                        completion: PeerBlockCompletion::default(),
                        last_ack_seen_ns: ready_freeze_ns,
                        last_ack_progress_ns: ready_freeze_ns,
                        next_probe_ns: ready_freeze_ns.saturating_add(timing.ack_probe_interval_ns),
                    },
                )
            })
            .collect();
        let empty_quorum = peers.is_empty();
        Ok(Self {
            total_blocks,
            timing,
            peers,
            state: if empty_quorum {
                CarouselSenderState::Finished
            } else {
                CarouselSenderState::Sending
            },
            next_symbol_id: 0,
            completion_repeats_sent: 0,
            next_completion_repeat_ns: 0,
            completion_time_ns: empty_quorum.then_some(ready_freeze_ns),
        })
    }

    pub fn state(&self) -> CarouselSenderState {
        self.state
    }

    pub fn completion_time_ns(&self) -> Option<u64> {
        self.completion_time_ns
    }

    pub fn next_data_emission(&mut self) -> Option<u64> {
        if matches!(
            self.state,
            CarouselSenderState::Finished | CarouselSenderState::Aborted
        ) || self.all_peers_complete()
        {
            return None;
        }
        self.state = CarouselSenderState::Sending;
        let symbol_id = self.next_symbol_id;
        self.next_symbol_id = self.next_symbol_id.checked_add(1)?;
        Some(symbol_id)
    }

    pub fn on_block_ack(
        &mut self,
        peer_id: u64,
        ack: &BlockAck,
        now_ns: u64,
    ) -> Result<bool, CarouselInputError> {
        let peer = self
            .peers
            .get_mut(&peer_id)
            .ok_or(CarouselInputError::UnknownPeer(peer_id))?;
        let progressed = peer
            .completion
            .join(ack, self.total_blocks)
            .map_err(CarouselInputError::InvalidAck)?;
        peer.last_ack_seen_ns = now_ns;
        if progressed {
            peer.last_ack_progress_ns = now_ns;
        }
        peer.next_probe_ns = now_ns.saturating_add(self.timing.ack_probe_interval_ns);
        if self.all_peers_complete() && self.state != CarouselSenderState::Finished {
            self.state = CarouselSenderState::Finished;
            self.completion_time_ns = Some(now_ns);
            self.next_completion_repeat_ns = now_ns;
        }
        Ok(progressed)
    }

    pub fn poll(&mut self, now_ns: u64) -> Result<Vec<OutboundControl>, LivenessViolation> {
        if self.state == CarouselSenderState::Finished {
            let mut controls = Vec::new();
            if self.completion_repeats_sent < self.timing.session_complete_repeats
                && now_ns >= self.next_completion_repeat_ns
            {
                controls.extend(self.peers.keys().map(|peer_id| OutboundControl {
                    peer_id: *peer_id,
                    frame: ControlFrame::SessionComplete,
                }));
                self.completion_repeats_sent += 1;
                self.next_completion_repeat_ns =
                    now_ns.saturating_add(self.timing.session_complete_interval_ns);
            }
            return Ok(controls);
        }

        let mut controls = Vec::new();
        let mut probing = false;
        for (&peer_id, peer) in &mut self.peers {
            if peer.completion.object_complete(self.total_blocks) {
                continue;
            }
            if now_ns
                >= peer
                    .last_ack_seen_ns
                    .saturating_add(self.timing.peer_silence_timeout_ns)
            {
                self.state = CarouselSenderState::Aborted;
                return Err(LivenessViolation::Silent { peer_id });
            }
            if now_ns
                >= peer
                    .last_ack_progress_ns
                    .saturating_add(self.timing.peer_stall_timeout_ns)
            {
                self.state = CarouselSenderState::Aborted;
                return Err(LivenessViolation::Stalled { peer_id });
            }
            if now_ns >= peer.next_probe_ns {
                probing = true;
                controls.push(OutboundControl {
                    peer_id,
                    frame: ControlFrame::AckProbe {
                        target_peer_id: peer_id,
                    },
                });
                peer.next_probe_ns = now_ns.saturating_add(self.timing.ack_probe_interval_ns);
            }
        }
        if probing {
            self.state = CarouselSenderState::Probing;
        }
        Ok(controls)
    }

    pub fn all_peers_complete(&self) -> bool {
        self.peers
            .values()
            .all(|peer| peer.completion.object_complete(self.total_blocks))
    }
}

#[derive(Clone, Debug)]
pub struct CarouselReceiver {
    peer_id: u64,
    source_symbols: usize,
    total_blocks: u64,
    timing: CarouselTiming,
    innovative: BTreeSet<u64>,
    state: CarouselReceiverState,
    dirty: bool,
    debounce_due_ns: Option<u64>,
    next_heartbeat_ns: u64,
    passive_deadline_ns: Option<u64>,
    local_completion_ns: Option<u64>,
}

impl CarouselReceiver {
    pub fn new(
        peer_id: u64,
        source_symbols: usize,
        ready_at_ns: u64,
        timing: CarouselTiming,
    ) -> Result<Self, CarouselConfigError> {
        Self::new_with_total_blocks(peer_id, source_symbols, ready_at_ns, timing, 1)
    }

    pub fn new_with_total_blocks(
        peer_id: u64,
        source_symbols: usize,
        ready_at_ns: u64,
        timing: CarouselTiming,
        total_blocks: u64,
    ) -> Result<Self, CarouselConfigError> {
        let timing = timing.validate()?;
        if total_blocks == 0 {
            return Err(CarouselConfigError::ZeroProgressUnits);
        }
        Ok(Self {
            peer_id,
            source_symbols,
            total_blocks,
            timing,
            innovative: BTreeSet::new(),
            state: CarouselReceiverState::Active,
            dirty: false,
            debounce_due_ns: None,
            next_heartbeat_ns: ready_at_ns.saturating_add(timing.ack_heartbeat_ns),
            passive_deadline_ns: None,
            local_completion_ns: None,
        })
    }

    pub fn state(&self) -> CarouselReceiverState {
        self.state
    }

    pub fn rank(&self) -> usize {
        self.innovative.len().min(self.source_symbols)
    }

    pub fn local_completion_ns(&self) -> Option<u64> {
        self.local_completion_ns
    }

    pub fn observe_symbol(&mut self, symbol_id: u64, now_ns: u64) -> bool {
        if self.state != CarouselReceiverState::Active {
            return false;
        }
        let innovative = self.innovative.insert(symbol_id);
        if innovative {
            self.dirty = true;
            self.debounce_due_ns
                .get_or_insert(now_ns.saturating_add(self.timing.ack_debounce_ns));
            if self.rank() == self.source_symbols {
                self.state = CarouselReceiverState::LocallyComplete;
                self.local_completion_ns = Some(now_ns);
                self.passive_deadline_ns =
                    Some(now_ns.saturating_add(self.timing.receiver_passive_window_ns));
            }
        }
        innovative
    }

    pub fn on_control(&mut self, frame: &ControlFrame, now_ns: u64) -> Option<ControlFrame> {
        match frame {
            ControlFrame::AckProbe { target_peer_id } if *target_peer_id == self.peer_id => {
                Some(ControlFrame::BlockAck(self.snapshot()))
            }
            ControlFrame::SessionComplete => {
                self.state = CarouselReceiverState::Finished;
                None
            }
            _ => {
                let _ = now_ns;
                None
            }
        }
    }

    pub fn poll(&mut self, now_ns: u64) -> Option<ControlFrame> {
        if self.state == CarouselReceiverState::Finished {
            return None;
        }
        if self
            .passive_deadline_ns
            .is_some_and(|deadline| now_ns >= deadline)
        {
            self.state = CarouselReceiverState::Finished;
            return None;
        }
        let debounce_due = self
            .debounce_due_ns
            .is_some_and(|deadline| now_ns >= deadline);
        let heartbeat_due = now_ns >= self.next_heartbeat_ns;
        if !debounce_due && !heartbeat_due {
            return None;
        }
        self.dirty = false;
        self.debounce_due_ns = None;
        self.next_heartbeat_ns = now_ns.saturating_add(self.timing.ack_heartbeat_ns);
        Some(ControlFrame::BlockAck(self.snapshot()))
    }

    fn snapshot(&self) -> BlockAck {
        let completed_watermark = if self.state == CarouselReceiverState::LocallyComplete
            || (self.state == CarouselReceiverState::Finished && self.local_completion_ns.is_some())
        {
            self.total_blocks
        } else {
            u64::try_from(self.rank())
                .unwrap_or(u64::MAX)
                .saturating_mul(self.total_blocks)
                / u64::try_from(self.source_symbols)
                    .unwrap_or(u64::MAX)
                    .max(1)
        };
        BlockAck::complete(completed_watermark)
    }
}

#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub enum CarouselConfigError {
    #[error("{0} must be nonzero")]
    ZeroDuration(&'static str),
    #[error("session_complete_repeats must be nonzero")]
    ZeroCompletionRepeats,
    #[error("carousel acknowledgement progress units must be nonzero")]
    ZeroProgressUnits,
    #[error("peer stall timeout must be longer than peer silence timeout")]
    StallNotLongerThanSilence,
    #[error("receiver passive window must exceed the sender stall-abort budget")]
    PassiveWindowTooShort,
}

#[derive(Debug, Error)]
pub enum CarouselInputError {
    #[error("control arrived from unknown peer {0}")]
    UnknownPeer(u64),
    #[error("invalid cumulative BlockAck: {0}")]
    InvalidAck(#[source] super::ack::AckError),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::CompletedRange;

    fn timing() -> CarouselTiming {
        CarouselTiming {
            ack_debounce_ns: 5,
            ack_heartbeat_ns: 20,
            ack_probe_interval_ns: 10,
            peer_silence_timeout_ns: 40,
            peer_stall_timeout_ns: 100,
            receiver_passive_window_ns: 200,
            session_complete_repeats: 3,
            session_complete_interval_ns: 2,
        }
    }

    #[test]
    fn join_is_permutation_and_duplicate_invariant() {
        let snapshots = [
            BlockAck {
                completed_watermark: 1,
                extra_completed: vec![CompletedRange { start: 3, end: 4 }],
            },
            BlockAck {
                completed_watermark: 2,
                extra_completed: vec![CompletedRange { start: 3, end: 6 }],
            },
            BlockAck::complete(6),
        ];
        for order in [[0, 1, 2], [2, 0, 1], [1, 2, 0], [2, 1, 0]] {
            let mut completion = PeerBlockCompletion::default();
            for index in order {
                completion
                    .join(&snapshots[index], 6)
                    .expect("valid snapshot");
                completion
                    .join(&snapshots[index], 6)
                    .expect("valid duplicate");
            }
            assert_eq!(completion.snapshot(), &BlockAck::complete(6));
        }
    }

    #[test]
    fn multi_block_receiver_reports_monotone_intermediate_progress() {
        let mut receiver =
            CarouselReceiver::new_with_total_blocks(1, 8, 0, timing(), 4).expect("receiver");
        assert!(receiver.observe_symbol(0, 1));
        assert!(receiver.observe_symbol(1, 2));
        assert_eq!(
            receiver.poll(7),
            Some(ControlFrame::BlockAck(BlockAck::complete(1)))
        );
        for symbol in 2..8 {
            assert!(receiver.observe_symbol(symbol, 10 + symbol));
        }
        assert_eq!(receiver.snapshot(), BlockAck::complete(4));
    }

    #[test]
    fn receiver_rejects_zero_ack_progress_units() {
        assert_eq!(
            CarouselReceiver::new_with_total_blocks(1, 8, 0, timing(), 0).unwrap_err(),
            CarouselConfigError::ZeroProgressUnits
        );
    }

    #[test]
    fn sender_completes_only_after_every_frozen_peer_acknowledges() {
        let mut sender = CarouselSender::new(1, [1, 2], 0, timing()).expect("valid sender");
        sender
            .on_block_ack(1, &BlockAck::complete(1), 5)
            .expect("peer one");
        assert_ne!(sender.state(), CarouselSenderState::Finished);
        sender
            .on_block_ack(2, &BlockAck::complete(1), 6)
            .expect("peer two");
        assert_eq!(sender.state(), CarouselSenderState::Finished);
    }

    #[test]
    fn empty_frozen_quorum_is_immediate_success() {
        let mut sender = CarouselSender::new(1, [], 7, timing()).expect("valid sender");
        assert_eq!(sender.state(), CarouselSenderState::Finished);
        assert_eq!(sender.completion_time_ns(), Some(7));
        assert_eq!(sender.next_data_emission(), None);
        assert!(sender.poll(7).expect("completion poll").is_empty());
    }

    #[test]
    fn aborted_sender_cannot_emit_more_payload() {
        let mut sender = CarouselSender::new(1, [1], 0, timing()).expect("valid sender");
        assert_eq!(
            sender.poll(40),
            Err(LivenessViolation::Silent { peer_id: 1 })
        );
        assert_eq!(sender.state(), CarouselSenderState::Aborted);
        assert_eq!(sender.next_data_emission(), None);
    }

    #[test]
    fn completion_is_rechecked_immediately_before_emission() {
        let mut sender = CarouselSender::new(1, [1], 0, timing()).expect("valid sender");
        assert_eq!(sender.next_data_emission(), Some(0));
        sender
            .on_block_ack(1, &BlockAck::complete(1), 5)
            .expect("completion");
        assert_eq!(sender.next_data_emission(), None);
    }

    #[test]
    fn duplicate_heartbeats_refresh_silence_but_not_progress() {
        let mut sender = CarouselSender::new(1, [7], 0, timing()).expect("valid sender");
        for now in [20, 40, 60, 80] {
            sender
                .on_block_ack(7, &BlockAck::empty(), now)
                .expect("heartbeat");
        }
        assert_eq!(sender.poll(99).expect("still live").len(), 1);
        assert_eq!(
            sender.poll(100),
            Err(LivenessViolation::Stalled { peer_id: 7 })
        );
    }

    #[test]
    fn probe_is_targeted_and_receiver_replays_current_snapshot() {
        let mut sender = CarouselSender::new(1, [7, 9], 0, timing()).expect("valid sender");
        sender
            .on_block_ack(7, &BlockAck::complete(1), 2)
            .expect("peer seven complete");
        let probes = sender.poll(10).expect("probe due");
        assert_eq!(
            probes,
            vec![OutboundControl {
                peer_id: 9,
                frame: ControlFrame::AckProbe { target_peer_id: 9 },
            }]
        );

        let mut receiver = CarouselReceiver::new(9, 1, 0, timing()).expect("valid receiver");
        receiver.observe_symbol(3, 1);
        assert_eq!(
            receiver.on_control(&probes[0].frame, 10),
            Some(ControlFrame::BlockAck(BlockAck::complete(1)))
        );
    }

    #[test]
    fn receiver_debounces_progress_and_heartbeats_while_passive() {
        let mut receiver = CarouselReceiver::new(9, 1, 0, timing()).expect("valid receiver");
        receiver.observe_symbol(3, 1);
        assert_eq!(receiver.poll(5), None);
        assert_eq!(
            receiver.poll(6),
            Some(ControlFrame::BlockAck(BlockAck::complete(1)))
        );
        assert_eq!(receiver.poll(25), None);
        assert_eq!(
            receiver.poll(26),
            Some(ControlFrame::BlockAck(BlockAck::complete(1)))
        );
    }
}
