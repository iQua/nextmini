use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::time::{Duration, Instant};

use nextmini_messages::rlm::RlmControl;

/// Merge and normalize SACK gap runs encoded as `(start_delta_from_base, len)`.
#[cfg(test)]
pub fn coalesce_sack_runs(mut runs: Vec<(u16, u16)>) -> Vec<(u16, u16)> {
    if runs.is_empty() {
        return runs;
    }
    runs.sort_by_key(|r| r.0);
    let mut out: Vec<(u16, u16)> = Vec::with_capacity(runs.len());
    let mut cur = runs[0];
    for (s, l) in runs.into_iter().skip(1) {
        let cur_end = cur.0.saturating_add(cur.1);
        if s <= cur_end {
            let new_end = cur_end.max(s.saturating_add(l));
            cur.1 = new_end.saturating_sub(cur.0);
        } else {
            out.push(cur);
            cur = (s, l);
        }
    }
    out.push(cur);
    out
}

/// Build SACK gap runs given a cumulative base and the set of received indices in (base, high].
#[cfg(test)]
pub fn build_gap_runs(base: u64, highest_seen: u64, received: &BTreeSet<u64>) -> Vec<(u16, u16)> {
    if highest_seen <= base {
        return Vec::new();
    }
    let mut runs = Vec::new();
    let mut cur_start: Option<u64> = None;
    for idx in base + 1..=highest_seen {
        let have = received.contains(&idx);
        if !have {
            if cur_start.is_none() {
                cur_start = Some(idx);
            }
        } else if let Some(start) = cur_start.take() {
            let len = (idx - start) as u16;
            let delta = (start - base) as u16;
            runs.push((delta, len));
        }
    }
    if let Some(start) = cur_start {
        let len = (highest_seen + 1 - start) as u16;
        let delta = (start - base) as u16;
        runs.push((delta, len));
    }
    coalesce_sack_runs(runs)
}

/// Simple per-chunk NACK limiter.
pub struct NackLimiter {
    last: Option<(u64, Instant)>,
    min_interval: Duration,
}

impl NackLimiter {
    pub fn new(min_interval: Duration) -> Self {
        Self {
            last: None,
            min_interval,
        }
    }

    pub fn should_send(&mut self, chunk: u64, now: Instant) -> bool {
        match self.last {
            Some((c, t)) if c == chunk && now.duration_since(t) < self.min_interval => false,
            _ => {
                self.last = Some((chunk, now));
                true
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SackSnapshot {
    pub base: u64,
    pub runs: Vec<(u16, u16)>,
}

/// Timer-backed helper that throttles SACK emission.
pub struct SackScheduler {
    interval: Duration,
    last_sent: Option<Instant>,
    snapshot: Option<SackSnapshot>,
    dirty: bool,
}

impl SackScheduler {
    pub fn new(interval: Duration) -> Self {
        Self {
            interval,
            last_sent: None,
            snapshot: None,
            dirty: false,
        }
    }

    /// Update the pending SACK snapshot. Empty runs clear the state.
    pub fn record(&mut self, base: u64, runs: Vec<(u16, u16)>) {
        if runs.is_empty() {
            self.snapshot = None;
            self.dirty = false;
            return;
        }
        self.snapshot = Some(SackSnapshot { base, runs });
        self.dirty = true;
    }

    pub fn clear(&mut self) {
        self.snapshot = None;
        self.dirty = false;
        self.last_sent = None;
    }

    pub fn has_snapshot(&self) -> bool {
        self.snapshot.is_some()
    }

    pub fn ready(&self, now: Instant) -> bool {
        if self.snapshot.is_none() {
            return false;
        }
        match self.last_sent {
            None => true,
            Some(last) => {
                if self.interval.is_zero() {
                    self.dirty
                } else {
                    now.duration_since(last) >= self.interval
                }
            }
        }
    }

    pub fn next_deadline(&self, now: Instant) -> Option<Instant> {
        self.snapshot.as_ref()?;
        match (self.last_sent, self.interval.is_zero()) {
            (None, _) => Some(now),
            (Some(_), true) => self.dirty.then_some(now),
            (Some(last), false) => Some(last + self.interval),
        }
    }

    pub fn take_ready(&mut self, now: Instant) -> Option<SackSnapshot> {
        if !self.ready(now) {
            return None;
        }
        self.dirty = false;
        self.last_sent = Some(now);
        self.snapshot.clone()
    }
}

/// Determines when a chunk can be retired from the sender's inflight queue.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CompletionPolicy {
    All,
    Threshold(usize),
    #[cfg_attr(not(test), allow(dead_code))]
    Leader(usize),
}

impl CompletionPolicy {
    pub fn should_retire(&self, acked_by: &HashSet<usize>, receiver_count: usize) -> bool {
        match *self {
            CompletionPolicy::All => acked_by.len() == receiver_count,
            CompletionPolicy::Threshold(t) => acked_by.len() >= t.min(receiver_count),
            CompletionPolicy::Leader(id) => acked_by.contains(&id),
        }
    }
}

/// Process a single control event from `from_node` and update inflight/repairs.
/// Returns a list of chunk indices that should be retired after this event.
pub fn process_control_event(
    from_node: usize,
    ctrl: &RlmControl,
    inflight: &mut BTreeMap<u64, HashSet<usize>>,
    resend_queue: &mut BTreeSet<u64>,
    receiver_count: usize,
    policy: &CompletionPolicy,
) -> Vec<u64> {
    match ctrl {
        RlmControl::Ack { up_to } => {
            let up = *up_to;
            // Track which receivers have acknowledged each chunk up to the
            // cumulative pointer so we can evaluate the completion policy.
            for (_idx, acked_by) in inflight.range_mut(..=up) {
                acked_by.insert(from_node);
            }
            let mut completed = Vec::new();
            for (idx, acked_by) in inflight.range(..=up) {
                if policy.should_retire(acked_by, receiver_count) {
                    completed.push(*idx);
                }
            }
            completed
        }
        RlmControl::Sack { base, runs } => {
            // Each gap run represents missing data, so we enqueue the
            // corresponding indices for retransmission.
            tracing::debug!(
                from_node = from_node,
                base = base,
                runs_count = runs.len(),
                runs = ?runs,
                "Processing SACK - runs are GAPS (missing chunks)"
            );
            let mut added_to_queue = 0;
            let mut already_in_queue = 0;
            let mut not_inflight = 0;
            for (delta, len) in runs {
                let start = *base + (*delta as u64);
                let end = start + (*len as u64);
                tracing::trace!(
                    from_node = from_node,
                    delta = delta,
                    len = len,
                    start = start,
                    end = end,
                    "SACK gap range: [{}, {})",
                    start,
                    end
                );
                for idx in start..end {
                    if inflight.contains_key(&idx) {
                        let was_new = resend_queue.insert(idx);
                        if was_new {
                            added_to_queue += 1;
                            tracing::trace!(
                                from_node = from_node,
                                chunk_index = idx,
                                "Added missing chunk to resend_queue"
                            );
                        } else {
                            already_in_queue += 1;
                        }
                    } else {
                        not_inflight += 1;
                        tracing::trace!(
                            from_node = from_node,
                            chunk_index = idx,
                            "Skipping chunk - not in inflight"
                        );
                    }
                }
            }
            tracing::debug!(
                from_node = from_node,
                added_to_queue = added_to_queue,
                already_in_queue = already_in_queue,
                not_inflight = not_inflight,
                resend_queue_size = resend_queue.len(),
                "SACK processing complete"
            );
            Vec::new()
        }
        RlmControl::Repair { indices } => {
            // Receiver supplied explicit indices (e.g. after NACK limiter),
            // so we add them to the resend queue if they are still inflight.
            for idx in indices {
                if inflight.contains_key(idx) {
                    resend_queue.insert(*idx);
                }
            }
            Vec::new()
        }
        RlmControl::Manifest { .. }
        | RlmControl::Ready { .. }
        | RlmControl::Eot { .. }
        | RlmControl::TfmccFeedback { .. } => Vec::new(),
    }
}

/// Helper to apply retirement (removes from inflight state and resets queues).
pub fn retire_chunks(
    to_retire: &[u64],
    inflight: &mut BTreeMap<u64, HashSet<usize>>,
    resend_queue: &mut BTreeSet<u64>,
) {
    for idx in to_retire {
        inflight.remove(idx);
        resend_queue.remove(idx);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn coalesce_adjacent_and_overlapping() {
        let runs = vec![(1, 2), (3, 2), (10, 1), (2, 3)];
        let out = coalesce_sack_runs(runs);
        assert_eq!(out, vec![(1, 4), (10, 1)]);
    }

    #[test]
    fn build_gap_runs_basic() {
        let base = 5u64;
        let highest = 12u64;
        let mut recv = BTreeSet::new();
        // received 6,7,10,12; missing 8,9,11
        for i in [6u64, 7, 10, 12] {
            recv.insert(i);
        }
        let runs = build_gap_runs(base, highest, &recv);
        // gaps start at 8 (len 2) and 11 (len 1)
        assert_eq!(runs, vec![(3, 2), (6, 1)]);
    }

    #[test]
    fn nack_limiter_limits_repeated_nacks() {
        let mut limiter = NackLimiter::new(Duration::from_millis(100));
        let t0 = Instant::now();
        assert!(limiter.should_send(42, t0));
        // Within window: same chunk should be suppressed
        assert!(!limiter.should_send(42, t0 + Duration::from_millis(50)));
        // Different chunk: allowed immediately
        assert!(limiter.should_send(43, t0 + Duration::from_millis(50)));
        // After window for original chunk: allowed again
        assert!(limiter.should_send(42, t0 + Duration::from_millis(150)));
    }

    #[test]
    fn sack_scheduler_resends_after_interval() {
        let mut sched = SackScheduler::new(Duration::from_millis(40));
        let t0 = Instant::now();
        sched.record(10, vec![(1, 2)]);
        assert!(sched.ready(t0));
        let first = sched.take_ready(t0).expect("initial send");
        assert_eq!(first.base, 10);
        assert_eq!(first.runs, vec![(1, 2)]);

        assert!(!sched.ready(t0 + Duration::from_millis(20)));
        let deadline = sched
            .next_deadline(t0 + Duration::from_millis(20))
            .expect("deadline pending");
        assert_eq!(deadline, t0 + Duration::from_millis(40));

        let second = sched
            .take_ready(t0 + Duration::from_millis(45))
            .expect("resend after interval");
        assert_eq!(second.base, 10);
        assert_eq!(second.runs, vec![(1, 2)]);
    }

    #[test]
    fn sack_scheduler_clears_state_on_empty_runs() {
        let mut sched = SackScheduler::new(Duration::from_millis(10));
        let t0 = Instant::now();
        sched.record(5, vec![(1, 1)]);
        assert!(sched.ready(t0));
        sched.take_ready(t0);

        sched.record(0, Vec::new());
        assert!(!sched.has_snapshot());
        assert!(!sched.ready(t0 + Duration::from_millis(20)));
        assert!(
            sched
                .next_deadline(t0 + Duration::from_millis(20))
                .is_none()
        );
    }

    #[test]
    fn sack_scheduler_zero_interval_requires_new_info() {
        let mut sched = SackScheduler::new(Duration::from_millis(0));
        let t0 = Instant::now();
        sched.record(7, vec![(1, 1)]);
        assert!(sched.ready(t0));
        sched.take_ready(t0);
        assert!(!sched.ready(t0 + Duration::from_millis(5)));
        assert!(sched.next_deadline(t0 + Duration::from_millis(5)).is_none());

        sched.record(7, vec![(2, 1)]);
        assert!(sched.ready(t0 + Duration::from_millis(5)));
    }

    #[test]
    fn process_control_ack_retires_when_policy_all() {
        use std::collections::{BTreeMap, BTreeSet, HashSet};
        let mut inflight: BTreeMap<u64, HashSet<usize>> = BTreeMap::new();
        for idx in 1..=3u64 {
            inflight.insert(idx, HashSet::new());
        }
        let mut resend: BTreeSet<u64> = BTreeSet::new();
        let policy = CompletionPolicy::All;
        let rc = 2usize;

        // First ACK from node 1 up_to 3: should not retire yet (need both nodes)
        let retired1 = process_control_event(
            1,
            &RlmControl::Ack { up_to: 3 },
            &mut inflight,
            &mut resend,
            rc,
            &policy,
        );
        assert!(retired1.is_empty());
        retire_chunks(&retired1, &mut inflight, &mut resend);

        // Second ACK from node 2 up_to 2: retire 1 and 2 now
        let retired2 = process_control_event(
            2,
            &RlmControl::Ack { up_to: 2 },
            &mut inflight,
            &mut resend,
            rc,
            &policy,
        );
        assert_eq!(retired2, vec![1, 2]);
        retire_chunks(&retired2, &mut inflight, &mut resend);

        // Finish index 3
        let retired3 = process_control_event(
            2,
            &RlmControl::Ack { up_to: 3 },
            &mut inflight,
            &mut resend,
            rc,
            &policy,
        );
        assert_eq!(retired3, vec![3]);
        retire_chunks(&retired3, &mut inflight, &mut resend);
    }

    #[test]
    fn process_control_sack_schedules_resends() {
        use std::collections::{BTreeMap, BTreeSet, HashSet};
        let mut inflight: BTreeMap<u64, HashSet<usize>> = BTreeMap::new();
        for idx in 10..=15u64 {
            inflight.insert(idx, HashSet::new());
        }
        let mut resend: BTreeSet<u64> = BTreeSet::new();
        let policy = CompletionPolicy::All;

        let runs = vec![(1u16, 2u16), (5u16, 1u16)]; // gaps: 11-12, 15
        let _retired = process_control_event(
            2,
            &RlmControl::Sack { base: 10, runs },
            &mut inflight,
            &mut resend,
            2,
            &policy,
        );

        let scheduled: Vec<u64> = resend.iter().copied().collect();
        assert_eq!(scheduled, vec![11, 12, 15]);
    }

    #[test]
    fn completion_policy_leader_retires_on_matching_ack() {
        use std::collections::{BTreeMap, BTreeSet, HashSet};
        let mut inflight: BTreeMap<u64, HashSet<usize>> = BTreeMap::new();
        inflight.insert(1, HashSet::new());
        let mut resend: BTreeSet<u64> = BTreeSet::new();
        let policy = CompletionPolicy::Leader(42);

        let retired = process_control_event(
            7,
            &RlmControl::Ack { up_to: 1 },
            &mut inflight,
            &mut resend,
            3,
            &policy,
        );
        assert!(retired.is_empty());

        let retired = process_control_event(
            42,
            &RlmControl::Ack { up_to: 1 },
            &mut inflight,
            &mut resend,
            3,
            &policy,
        );
        assert_eq!(retired, vec![1]);
    }

    #[test]
    fn retire_chunks_removes_from_inflight_and_resend() {
        use std::collections::{BTreeMap, BTreeSet, HashSet};
        let mut inflight: BTreeMap<u64, HashSet<usize>> = BTreeMap::new();
        inflight.insert(1, HashSet::new());
        inflight.insert(2, HashSet::new());
        inflight.insert(3, HashSet::new());
        let mut resend: BTreeSet<u64> = [1u64, 3u64].into_iter().collect();

        retire_chunks(&[1, 3], &mut inflight, &mut resend);

        assert!(!inflight.contains_key(&1));
        assert!(!inflight.contains_key(&3));
        assert!(inflight.contains_key(&2));
        assert!(resend.is_empty());
    }
}
