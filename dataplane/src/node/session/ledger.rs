use std::fmt::{Display, Formatter};

use ahash::AHashMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionLedgerError {
    EmptyPeerSet,
    DuplicatePeer { peer_id: usize },
    UnknownPeer { peer_id: usize },
    BlockOutOfRange { block_id: u64, total_blocks: u64 },
}

impl Display for SessionLedgerError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::EmptyPeerSet => write!(f, "session ledger requires at least one peer"),
            Self::DuplicatePeer { peer_id } => {
                write!(f, "peer_id {peer_id} appears more than once")
            }
            Self::UnknownPeer { peer_id } => write!(f, "peer_id {peer_id} is not tracked"),
            Self::BlockOutOfRange {
                block_id,
                total_blocks,
            } => write!(
                f,
                "block_id {block_id} is out of range for total_blocks={total_blocks}"
            ),
        }
    }
}

impl std::error::Error for SessionLedgerError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AckProgress {
    pub peer_newly_acked: bool,
    pub block_newly_complete: bool,
    pub session_newly_complete: bool,
}

#[derive(Debug, Clone)]
struct BlockAckState {
    acked: Vec<bool>,
    acked_count: usize,
    complete: bool,
}

impl BlockAckState {
    fn new(peer_count: usize) -> Self {
        Self {
            acked: vec![false; peer_count],
            acked_count: 0,
            complete: false,
        }
    }
}

#[derive(Debug, Clone)]
pub struct SessionLedger {
    peer_ids: Vec<usize>,
    peer_index: AHashMap<usize, usize>,
    blocks: Vec<BlockAckState>,
    completed_blocks: usize,
}

impl SessionLedger {
    pub fn new<I>(total_blocks: u64, peers: I) -> Result<Self, SessionLedgerError>
    where
        I: IntoIterator<Item = usize>,
    {
        let mut peer_ids = Vec::new();
        let mut peer_index = AHashMap::default();

        for peer_id in peers {
            if peer_index.insert(peer_id, peer_ids.len()).is_some() {
                return Err(SessionLedgerError::DuplicatePeer { peer_id });
            }
            peer_ids.push(peer_id);
        }

        if peer_ids.is_empty() {
            return Err(SessionLedgerError::EmptyPeerSet);
        }

        let blocks = (0..total_blocks)
            .map(|_| BlockAckState::new(peer_ids.len()))
            .collect();

        Ok(Self {
            peer_ids,
            peer_index,
            blocks,
            completed_blocks: 0,
        })
    }

    pub fn total_blocks(&self) -> u64 {
        self.blocks.len() as u64
    }

    pub fn peer_ids(&self) -> &[usize] {
        &self.peer_ids
    }

    pub fn completed_blocks(&self) -> usize {
        self.completed_blocks
    }

    pub fn is_block_complete(&self, block_id: u64) -> Result<bool, SessionLedgerError> {
        Ok(self.block(block_id)?.complete)
    }

    pub fn is_acked_by(&self, peer_id: usize, block_id: u64) -> Result<bool, SessionLedgerError> {
        let peer_idx = self.peer_slot(peer_id)?;
        Ok(self.block(block_id)?.acked[peer_idx])
    }

    pub fn all_blocks_complete(&self) -> bool {
        self.completed_blocks == self.blocks.len()
    }

    pub fn mark_block_acked(
        &mut self,
        peer_id: usize,
        block_id: u64,
    ) -> Result<AckProgress, SessionLedgerError> {
        let peer_idx = self.peer_slot(peer_id)?;
        let session_complete_before = self.all_blocks_complete();
        let block = self.block_mut(block_id)?;

        if block.acked[peer_idx] {
            return Ok(AckProgress {
                peer_newly_acked: false,
                block_newly_complete: false,
                session_newly_complete: false,
            });
        }

        block.acked[peer_idx] = true;
        block.acked_count += 1;

        let mut block_newly_complete = false;
        if !block.complete && block.acked_count == block.acked.len() {
            block.complete = true;
            self.completed_blocks += 1;
            block_newly_complete = true;
        }

        Ok(AckProgress {
            peer_newly_acked: true,
            block_newly_complete,
            session_newly_complete: !session_complete_before && self.all_blocks_complete(),
        })
    }

    fn peer_slot(&self, peer_id: usize) -> Result<usize, SessionLedgerError> {
        self.peer_index
            .get(&peer_id)
            .copied()
            .ok_or(SessionLedgerError::UnknownPeer { peer_id })
    }

    fn block(&self, block_id: u64) -> Result<&BlockAckState, SessionLedgerError> {
        self.blocks
            .get(block_id as usize)
            .ok_or(SessionLedgerError::BlockOutOfRange {
                block_id,
                total_blocks: self.total_blocks(),
            })
    }

    fn block_mut(&mut self, block_id: u64) -> Result<&mut BlockAckState, SessionLedgerError> {
        let total_blocks = self.total_blocks();
        self.blocks
            .get_mut(block_id as usize)
            .ok_or(SessionLedgerError::BlockOutOfRange {
                block_id,
                total_blocks,
            })
    }
}

#[cfg(test)]
mod tests {
    use super::{SessionLedger, SessionLedgerError};

    #[test]
    fn rejects_empty_peer_set() {
        assert_eq!(
            SessionLedger::new(2, []).expect_err("empty peers should fail"),
            SessionLedgerError::EmptyPeerSet
        );
    }

    #[test]
    fn rejects_duplicate_peer_ids() {
        assert_eq!(
            SessionLedger::new(2, [7, 7]).expect_err("duplicate peers should fail"),
            SessionLedgerError::DuplicatePeer { peer_id: 7 }
        );
    }

    #[test]
    fn duplicate_acks_are_idempotent() {
        let mut ledger = SessionLedger::new(2, [1, 2]).expect("ledger");

        let first = ledger.mark_block_acked(1, 0).expect("ack");
        assert!(first.peer_newly_acked);
        assert!(!first.block_newly_complete);
        assert!(!first.session_newly_complete);

        let duplicate = ledger.mark_block_acked(1, 0).expect("duplicate ack");
        assert!(!duplicate.peer_newly_acked);
        assert!(!duplicate.block_newly_complete);
        assert!(!duplicate.session_newly_complete);
    }

    #[test]
    fn marks_block_and_session_completion_when_all_peers_ack() {
        let mut ledger = SessionLedger::new(2, [1, 2]).expect("ledger");

        assert!(
            ledger
                .mark_block_acked(1, 0)
                .expect("ack")
                .peer_newly_acked
        );
        assert!(
            ledger
                .mark_block_acked(2, 0)
                .expect("ack")
                .block_newly_complete
        );
        assert!(ledger.is_block_complete(0).expect("complete"));
        assert!(!ledger.all_blocks_complete());

        ledger.mark_block_acked(1, 1).expect("ack");
        let final_ack = ledger.mark_block_acked(2, 1).expect("ack");
        assert!(final_ack.block_newly_complete);
        assert!(final_ack.session_newly_complete);
        assert!(ledger.all_blocks_complete());
        assert_eq!(ledger.completed_blocks(), 2);
    }

    #[test]
    fn rejects_unknown_peers_and_invalid_blocks() {
        let mut ledger = SessionLedger::new(1, [4]).expect("ledger");

        assert_eq!(
            ledger
                .mark_block_acked(9, 0)
                .expect_err("unknown peer should fail"),
            SessionLedgerError::UnknownPeer { peer_id: 9 }
        );
        assert_eq!(
            ledger
                .mark_block_acked(4, 8)
                .expect_err("out of range block should fail"),
            SessionLedgerError::BlockOutOfRange {
                block_id: 8,
                total_blocks: 1,
            }
        );
    }
}
