//! Deterministic per-session carousel metrics and test observer.

use std::collections::BTreeMap;
use std::sync::{Mutex, MutexGuard};
use std::time::Duration;

/// Async states in which the carousel sender can wait without emitting data.
/// Control handling is deliberately absent: it is work, not a wait state.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum SenderWaitState {
    Backpressure,
    Pacing,
    Feedback,
    CompletionRepeat,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct WaitStateMetrics {
    pub count: u64,
    pub total_nanoseconds: u64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SenderEsiMetrics {
    pub start: Option<u32>,
    pub end: Option<u32>,
    pub count: u64,
    pub sequence_violations: u64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SessionMetricsSnapshot {
    pub queued_after_final_ack_processed: u64,
    pub carousel_backpressure_sweeps: u64,
    pub sender_wait_states: BTreeMap<SenderWaitState, WaitStateMetrics>,
    pub sender_block_esis: BTreeMap<u64, SenderEsiMetrics>,
    pub symbols_received_after_local_block_complete: u64,
    pub receiver_duplicate_symbols: u64,
    pub mettle_targeted_retransmissions: u64,
    pub mettle_full_replay_symbols: u64,
    /// Histogram keyed by `unique_symbols_at_decode - K`.
    pub symbols_at_decode_minus_k: BTreeMap<u32, u64>,
}

/// Shared observer passed through every component of one local session task.
#[derive(Debug, Default)]
pub struct SessionMetrics {
    inner: Mutex<SessionMetricsSnapshot>,
}

impl SessionMetrics {
    #[must_use]
    #[allow(dead_code)] // public observer API is exercised by integration tests
    pub fn snapshot(&self) -> SessionMetricsSnapshot {
        self.lock().clone()
    }

    pub(crate) fn record_backpressure_sweep(&self) {
        let mut metrics = self.lock();
        metrics.carousel_backpressure_sweeps =
            metrics.carousel_backpressure_sweeps.saturating_add(1);
    }

    pub(crate) fn record_queued_after_final_ack(&self) {
        let mut metrics = self.lock();
        metrics.queued_after_final_ack_processed =
            metrics.queued_after_final_ack_processed.saturating_add(1);
    }

    pub(crate) fn record_wait(&self, state: SenderWaitState, duration: Duration) {
        let nanos = u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX);
        let mut metrics = self.lock();
        let entry = metrics.sender_wait_states.entry(state).or_default();
        entry.count = entry.count.saturating_add(1);
        entry.total_nanoseconds = entry.total_nanoseconds.saturating_add(nanos);
    }

    /// Record one actually queued carousel ESI and enforce strict freshness.
    pub(crate) fn record_sender_esi(&self, block_id: u64, symbol_id: u32) -> bool {
        let mut metrics = self.lock();
        let entry = metrics.sender_block_esis.entry(block_id).or_default();
        if entry.count == 0 {
            entry.start = Some(symbol_id);
            entry.end = Some(symbol_id);
            entry.count = 1;
            return true;
        }

        let monotone = entry
            .end
            .and_then(|end| end.checked_add(1))
            .is_some_and(|next| next == symbol_id);
        entry.count = entry.count.saturating_add(1);
        entry.end = Some(symbol_id);
        if !monotone {
            entry.sequence_violations = entry.sequence_violations.saturating_add(1);
        }
        monotone
    }

    pub(crate) fn record_receiver_tail_symbol(&self) {
        let mut metrics = self.lock();
        metrics.symbols_received_after_local_block_complete = metrics
            .symbols_received_after_local_block_complete
            .saturating_add(1);
    }

    pub(crate) fn record_receiver_duplicate(&self) {
        let mut metrics = self.lock();
        metrics.receiver_duplicate_symbols = metrics.receiver_duplicate_symbols.saturating_add(1);
    }

    pub(crate) fn record_mettle_retransmission(&self, full_replay: bool) {
        let mut metrics = self.lock();
        if full_replay {
            metrics.mettle_full_replay_symbols =
                metrics.mettle_full_replay_symbols.saturating_add(1);
        } else {
            metrics.mettle_targeted_retransmissions =
                metrics.mettle_targeted_retransmissions.saturating_add(1);
        }
    }

    pub(crate) fn record_symbols_at_decode(&self, source_symbols: u32, unique_symbols: usize) {
        let unique_symbols = u64::try_from(unique_symbols).unwrap_or(u64::MAX);
        let overhead = unique_symbols.saturating_sub(u64::from(source_symbols));
        let overhead = u32::try_from(overhead).unwrap_or(u32::MAX);
        let mut metrics = self.lock();
        let entry = metrics
            .symbols_at_decode_minus_k
            .entry(overhead)
            .or_default();
        *entry = entry.saturating_add(1);
    }

    fn lock(&self) -> MutexGuard<'_, SessionMetricsSnapshot> {
        self.inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sender_esi_observer_enforces_checked_monotone_increments() {
        let metrics = SessionMetrics::default();
        assert!(metrics.record_sender_esi(7, 10));
        assert!(metrics.record_sender_esi(7, 11));
        assert!(!metrics.record_sender_esi(7, 11));

        assert_eq!(
            metrics.snapshot().sender_block_esis.get(&7),
            Some(&SenderEsiMetrics {
                start: Some(10),
                end: Some(11),
                count: 3,
                sequence_violations: 1,
            })
        );
    }

    #[test]
    fn snapshot_exposes_wait_and_receiver_event_boundaries() {
        let metrics = SessionMetrics::default();
        metrics.record_backpressure_sweep();
        metrics.record_wait(SenderWaitState::Feedback, Duration::from_nanos(12));
        metrics.record_receiver_tail_symbol();
        metrics.record_receiver_duplicate();
        metrics.record_symbols_at_decode(4, 6);

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.carousel_backpressure_sweeps, 1);
        assert_eq!(
            snapshot.sender_wait_states[&SenderWaitState::Feedback],
            WaitStateMetrics {
                count: 1,
                total_nanoseconds: 12,
            }
        );
        assert_eq!(snapshot.symbols_received_after_local_block_complete, 1);
        assert_eq!(snapshot.receiver_duplicate_symbols, 1);
        assert_eq!(snapshot.symbols_at_decode_minus_k.get(&2), Some(&1));
    }

    #[test]
    fn queued_after_final_ack_metric_hook_increments() {
        let metrics = SessionMetrics::default();

        metrics.record_queued_after_final_ack();
        metrics.record_queued_after_final_ack();

        assert_eq!(metrics.snapshot().queued_after_final_ack_processed, 2);
    }
}
