use std::collections::{BTreeMap, HashSet};

use nextmini_messages::rlm::RlmControl;

/// Process a single control event from `from_node` and update inflight state.
/// Returns a list of chunk indices that should be retired after this event.
pub fn process_control_event(
    from_node: usize,
    ctrl: &RlmControl,
    inflight: &mut BTreeMap<u64, HashSet<usize>>,
    receiver_count: usize,
) -> Vec<u64> {
    match ctrl {
        RlmControl::Ack { up_to } => {
            let up = *up_to;
            for (_idx, acked_by) in inflight.range_mut(..=up) {
                acked_by.insert(from_node);
            }
            let mut completed = Vec::new();
            for (idx, acked_by) in inflight.range(..=up) {
                // a chunk should be retired when all receivers have acknowledged it
                if acked_by.len() == receiver_count {
                    completed.push(*idx);
                }
            }
            completed
        }
        RlmControl::Manifest { .. } | RlmControl::Ready { .. } | RlmControl::Eot { .. } => {
            Vec::new()
        }
    }
}

/// Helper to apply retirement (removes from inflight state).
pub fn retire_chunks(to_retire: &[u64], inflight: &mut BTreeMap<u64, HashSet<usize>>) {
    for idx in to_retire {
        inflight.remove(idx);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn process_control_ack_retires_when_all_acknowledge() {
        let mut inflight: BTreeMap<u64, HashSet<usize>> = BTreeMap::new();
        for idx in 1..=3u64 {
            inflight.insert(idx, HashSet::new());
        }
        let rc = 2usize;

        let retired1 =
            process_control_event(1, &RlmControl::Ack { up_to: 3 }, &mut inflight, rc);
        assert!(retired1.is_empty());
        retire_chunks(&retired1, &mut inflight);

        let retired2 =
            process_control_event(2, &RlmControl::Ack { up_to: 2 }, &mut inflight, rc);
        assert_eq!(retired2, vec![1, 2]);
        retire_chunks(&retired2, &mut inflight);

        let retired3 =
            process_control_event(2, &RlmControl::Ack { up_to: 3 }, &mut inflight, rc);
        assert_eq!(retired3, vec![3]);
    }


    #[test]
    fn retire_chunks_removes_indices() {
        let mut inflight: BTreeMap<u64, HashSet<usize>> = BTreeMap::new();
        inflight.insert(1, HashSet::new());
        inflight.insert(2, HashSet::new());
        inflight.insert(3, HashSet::new());

        retire_chunks(&[1, 3], &mut inflight);

        assert!(!inflight.contains_key(&1));
        assert!(!inflight.contains_key(&3));
        assert!(inflight.contains_key(&2));
    }
}
