use std::collections::BTreeMap;

use nextmini_messages::rlm::RlmControl;

/// Track cumulative ACK progress for each receiver. Returns `Some(new_value)`
/// when the receiver reports forward progress, `None` otherwise.
pub fn update_receiver_progress(
    from_node: usize,
    ctrl: &RlmControl,
    progress: &mut BTreeMap<usize, u64>,
) -> Option<u64> {
    match ctrl {
        RlmControl::Ack { up_to } => {
            if let Some(entry) = progress.get_mut(&from_node)
                && *up_to > *entry
            {
                *entry = *up_to;
                return Some(*entry);
            }
            None
        }
        RlmControl::Manifest { .. } | RlmControl::Ready { .. } | RlmControl::Eot { .. } => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn update_receiver_progress_advances_when_monotonic() {
        let mut progress: BTreeMap<usize, u64> = BTreeMap::new();
        progress.insert(7, 2);

        let updated = update_receiver_progress(7, &RlmControl::Ack { up_to: 5 }, &mut progress);

        assert_eq!(updated, Some(5));
        assert_eq!(progress.get(&7).copied(), Some(5));
    }

    #[test]
    fn update_receiver_progress_ignores_missing_or_regressions() {
        let mut progress: BTreeMap<usize, u64> = BTreeMap::new();
        progress.insert(1, 4);

        let regression = update_receiver_progress(1, &RlmControl::Ack { up_to: 2 }, &mut progress);
        assert!(regression.is_none());
        assert_eq!(progress.get(&1).copied(), Some(4));

        let missing = update_receiver_progress(99, &RlmControl::Ack { up_to: 10 }, &mut progress);
        assert!(missing.is_none());
        assert!(!progress.contains_key(&99));
    }
}
