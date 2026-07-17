//! Local timestamped tie resolution for deadline-sensitive decisions.

/// One nanosecond of simulator bookkeeping. It is excluded from modeled latency.
pub const DECISION_DELTA_NS: u64 = 1;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TimerGeneration(u64);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Decision {
    EventWon,
    DeadlineWon,
    Stale,
    TooEarly,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct DeferredDeadline {
    generation: u64,
    deadline_ns: u64,
    qualifying_event_seen: bool,
    armed: bool,
}

impl DeferredDeadline {
    pub fn arm(&mut self, deadline_ns: u64) -> (TimerGeneration, u64) {
        self.generation = self.generation.wrapping_add(1);
        self.deadline_ns = deadline_ns;
        self.qualifying_event_seen = false;
        self.armed = true;
        (
            TimerGeneration(self.generation),
            deadline_ns.saturating_add(DECISION_DELTA_NS),
        )
    }

    pub fn observe(&mut self, event_timestamp_ns: u64) {
        if self.armed && event_timestamp_ns <= self.deadline_ns {
            self.qualifying_event_seen = true;
        }
    }

    pub fn decide(&mut self, generation: TimerGeneration, now_ns: u64) -> Decision {
        if generation.0 != self.generation || !self.armed {
            return Decision::Stale;
        }
        if now_ns <= self.deadline_ns {
            return Decision::TooEarly;
        }
        self.armed = false;
        if self.qualifying_event_seen {
            Decision::EventWon
        } else {
            Decision::DeadlineWon
        }
    }
}
