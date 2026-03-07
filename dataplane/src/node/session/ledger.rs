use std::collections::BTreeMap;
use std::error::Error;
use std::fmt::{Display, Formatter};

/// Construction and query failures for the shared block acknowledgement ledger.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LedgerError {
    DuplicateReceiver { receiver_id: usize },
    UnknownReceiver { receiver_id: usize },
    InvalidBlockId { block_id: u64, total_blocks: u64 },
    TooManyBlocks { total_blocks: u64 },
}

impl Display for LedgerError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::DuplicateReceiver { receiver_id } => {
                write!(f, "receiver_id {receiver_id} was registered more than once")
            }
            Self::UnknownReceiver { receiver_id } => {
                write!(f, "receiver_id {receiver_id} is not part of this session")
            }
            Self::InvalidBlockId {
                block_id,
                total_blocks,
            } => {
                write!(f, "block_id {block_id} is outside 0..{total_blocks}")
            }
            Self::TooManyBlocks { total_blocks } => {
                write!(
                    f,
                    "total_blocks {total_blocks} exceeds in-memory ledger capacity"
                )
            }
        }
    }
}

impl Error for LedgerError {}

/// Aggregate state for one block in the shared acknowledgement ledger.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BlockState {
    Pending,
    Complete,
}

/// Result of applying one receiver `BlockAck` to the shared ledger.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AckUpdate {
    pub changed: bool,
    pub block_complete: bool,
    pub block_completed_now: bool,
    pub receiver_complete: bool,
    pub receiver_completed_now: bool,
    pub session_complete: bool,
    pub session_completed_now: bool,
}

#[derive(Debug, Clone)]
struct BlockLedger {
    acked_by: Vec<bool>,
    acked_receivers: usize,
}

/// Mode-agnostic shared acknowledgement ledger.
///
/// The canonical completion rule is: the session is complete when every
/// registered receiver has acknowledged every block.
#[derive(Debug, Clone)]
pub struct SessionLedger {
    total_blocks: u64,
    receiver_ids: Vec<usize>,
    receiver_positions: BTreeMap<usize, usize>,
    acked_blocks_per_receiver: Vec<u64>,
    blocks: Vec<BlockLedger>,
    complete_blocks: u64,
}

impl SessionLedger {
    pub fn new(
        total_blocks: u64,
        receiver_ids: impl IntoIterator<Item = usize>,
    ) -> Result<Self, LedgerError> {
        let mut ordered_receivers = Vec::new();
        let mut receiver_positions = BTreeMap::new();
        for receiver_id in receiver_ids {
            if receiver_positions
                .insert(receiver_id, ordered_receivers.len())
                .is_some()
            {
                return Err(LedgerError::DuplicateReceiver { receiver_id });
            }
            ordered_receivers.push(receiver_id);
        }

        let block_count = usize::try_from(total_blocks)
            .map_err(|_| LedgerError::TooManyBlocks { total_blocks })?;
        let receiver_count = ordered_receivers.len();
        let blocks = vec![
            BlockLedger {
                acked_by: vec![false; receiver_count],
                acked_receivers: 0,
            };
            block_count
        ];
        let complete_blocks = if receiver_count == 0 { total_blocks } else { 0 };

        Ok(Self {
            total_blocks,
            receiver_ids: ordered_receivers,
            receiver_positions,
            acked_blocks_per_receiver: vec![0; receiver_count],
            blocks,
            complete_blocks,
        })
    }

    pub const fn total_blocks(&self) -> u64 {
        self.total_blocks
    }

    pub fn receiver_ids(&self) -> &[usize] {
        &self.receiver_ids
    }

    pub fn receiver_count(&self) -> usize {
        self.receiver_ids.len()
    }

    pub fn complete_blocks(&self) -> u64 {
        self.complete_blocks
    }

    pub fn is_complete(&self) -> bool {
        self.complete_blocks == self.total_blocks
    }

    pub fn block_state(&self, block_id: u64) -> Option<BlockState> {
        let block = self.blocks.get(usize::try_from(block_id).ok()?)?;
        Some(if block.acked_receivers == self.receiver_count() {
            BlockState::Complete
        } else {
            BlockState::Pending
        })
    }

    pub fn block_acked_receivers(&self, block_id: u64) -> Option<usize> {
        self.blocks
            .get(usize::try_from(block_id).ok()?)
            .map(|block| block.acked_receivers)
    }

    pub fn receiver_has_acked(
        &self,
        receiver_id: usize,
        block_id: u64,
    ) -> Result<bool, LedgerError> {
        let receiver_pos = self.receiver_pos(receiver_id)?;
        let block = self.block_ref(block_id)?;
        Ok(block.acked_by[receiver_pos])
    }

    pub fn receiver_acked_blocks(&self, receiver_id: usize) -> Result<u64, LedgerError> {
        let receiver_pos = self.receiver_pos(receiver_id)?;
        Ok(self.acked_blocks_per_receiver[receiver_pos])
    }

    pub fn receiver_is_complete(&self, receiver_id: usize) -> Result<bool, LedgerError> {
        Ok(self.receiver_acked_blocks(receiver_id)? == self.total_blocks)
    }

    pub fn ack_block(
        &mut self,
        receiver_id: usize,
        block_id: u64,
    ) -> Result<AckUpdate, LedgerError> {
        let receiver_pos = self.receiver_pos(receiver_id)?;
        let block_index = self.block_index(block_id)?;
        let receiver_count = self.receiver_count();
        let total_blocks = self.total_blocks;
        let session_complete_before = self.is_complete();

        if self.blocks[block_index].acked_by[receiver_pos] {
            let receiver_complete = self.acked_blocks_per_receiver[receiver_pos] == total_blocks;
            let block_complete = self.blocks[block_index].acked_receivers == receiver_count;
            return Ok(AckUpdate {
                changed: false,
                block_complete,
                block_completed_now: false,
                receiver_complete,
                receiver_completed_now: false,
                session_complete: session_complete_before,
                session_completed_now: false,
            });
        }

        {
            let block = &mut self.blocks[block_index];
            block.acked_by[receiver_pos] = true;
            block.acked_receivers += 1;
        }
        self.acked_blocks_per_receiver[receiver_pos] += 1;

        let block_completed_now = self.blocks[block_index].acked_receivers == receiver_count;
        if block_completed_now {
            self.complete_blocks += 1;
        }

        let receiver_complete = self.acked_blocks_per_receiver[receiver_pos] == total_blocks;
        let session_complete = self.is_complete();

        Ok(AckUpdate {
            changed: true,
            block_complete: block_completed_now,
            block_completed_now,
            receiver_complete,
            receiver_completed_now: receiver_complete,
            session_complete,
            session_completed_now: session_complete,
        })
    }

    fn receiver_pos(&self, receiver_id: usize) -> Result<usize, LedgerError> {
        self.receiver_positions
            .get(&receiver_id)
            .copied()
            .ok_or(LedgerError::UnknownReceiver { receiver_id })
    }

    fn block_ref(&self, block_id: u64) -> Result<&BlockLedger, LedgerError> {
        Ok(&self.blocks[self.block_index(block_id)?])
    }

    fn block_index(&self, block_id: u64) -> Result<usize, LedgerError> {
        let block_index = usize::try_from(block_id).ok();
        match block_index {
            Some(index) if index < self.blocks.len() => Ok(index),
            _ => Err(LedgerError::InvalidBlockId {
                block_id,
                total_blocks: self.total_blocks,
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{BlockState, LedgerError, SessionLedger};

    #[test]
    fn duplicate_ack_is_idempotent() {
        let mut ledger = SessionLedger::new(2, [7usize, 9usize]).expect("valid ledger");

        let first = ledger.ack_block(7, 0).expect("first ack should apply");
        assert!(first.changed);
        assert!(!first.block_complete);
        assert!(!first.session_complete);
        assert_eq!(ledger.block_state(0), Some(BlockState::Pending));
        assert_eq!(ledger.receiver_acked_blocks(7), Ok(1));

        let duplicate = ledger
            .ack_block(7, 0)
            .expect("duplicate ack should be accepted");
        assert!(!duplicate.changed);
        assert!(!duplicate.block_completed_now);
        assert!(!duplicate.session_completed_now);
        assert_eq!(ledger.block_acked_receivers(0), Some(1));
        assert_eq!(ledger.receiver_acked_blocks(7), Ok(1));
    }

    #[test]
    fn session_completes_only_after_all_receivers_ack_all_blocks() {
        let mut ledger = SessionLedger::new(2, [11usize, 13usize]).expect("valid ledger");

        assert!(!ledger.is_complete());

        assert!(ledger.ack_block(11, 0).expect("ack should apply").changed);
        assert!(
            ledger
                .ack_block(13, 0)
                .expect("ack should apply")
                .block_completed_now
        );
        assert_eq!(ledger.block_state(0), Some(BlockState::Complete));
        assert!(!ledger.is_complete());

        let final_ack = ledger.ack_block(11, 1).expect("ack should apply");
        assert!(!final_ack.block_complete);
        assert!(!final_ack.session_complete);

        let session_done = ledger
            .ack_block(13, 1)
            .expect("last receiver should complete");
        assert!(session_done.block_completed_now);
        assert!(session_done.session_complete);
        assert!(session_done.session_completed_now);
        assert!(ledger.is_complete());
        assert_eq!(ledger.complete_blocks(), 2);
        assert_eq!(ledger.receiver_is_complete(11), Ok(true));
        assert_eq!(ledger.receiver_is_complete(13), Ok(true));
    }

    #[test]
    fn zero_receiver_session_is_vacuously_complete() {
        let ledger = SessionLedger::new(3, []).expect("ledger without receivers should be valid");

        assert!(ledger.is_complete());
        assert_eq!(ledger.complete_blocks(), 3);
        assert_eq!(ledger.block_state(0), Some(BlockState::Complete));
    }

    #[test]
    fn ledger_rejects_duplicate_receivers_and_invalid_queries() {
        assert_eq!(
            SessionLedger::new(1, [5usize, 5usize]).unwrap_err(),
            LedgerError::DuplicateReceiver { receiver_id: 5 }
        );

        let mut ledger = SessionLedger::new(1, [3usize]).expect("valid ledger");
        assert_eq!(
            ledger.receiver_has_acked(4, 0),
            Err(LedgerError::UnknownReceiver { receiver_id: 4 })
        );
        assert_eq!(
            ledger.ack_block(3, 1),
            Err(LedgerError::InvalidBlockId {
                block_id: 1,
                total_blocks: 1,
            })
        );
    }
}
