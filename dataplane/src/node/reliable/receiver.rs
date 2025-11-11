use bytes::Bytes;
use std::collections::{BTreeMap, BTreeSet};

use crate::node::network::interface::NetworkInterfaceHandle;

use super::session::ReceiverConfig;
use super::control::build_gap_runs;

pub async fn run(cfg: ReceiverConfig, _net: Option<NetworkInterfaceHandle>) {
    tracing::info!(session_id=cfg.common.session_id, expected_bytes=cfg.expected_bytes, "RLM receiver started (no-net baseline)");
}

#[allow(dead_code)]
pub fn build_ack_and_sack(
    expected: u64,
    received: &BTreeSet<u64>,
    highest_seen: u64,
) -> (u64, Vec<(u16, u16)>) {
    let base = expected.saturating_sub(1);
    let runs = build_gap_runs(base, highest_seen, received);
    (base, runs)
}

#[allow(dead_code)]
pub fn choose_repair_indices(expected: u64) -> Vec<u64> {
    vec![expected]
}

#[allow(dead_code)]
pub fn on_chunk(
    idx: u64,
    bytes: Bytes,
    pending: &mut std::collections::BTreeMap<u64, Bytes>,
    expected: &mut u64,
) {
    if idx < *expected {
        return;
    }
    pending.insert(idx, bytes);
    while pending.contains_key(expected) {
        pending.remove(expected);
        *expected += 1;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ack_and_sack_basic() {
        let mut received = BTreeSet::new();
        received.insert(2);
        received.insert(3);
        received.insert(6);
        let (base, runs) = build_ack_and_sack(2, &received, 6);
        assert_eq!(base, 1);
        // gaps: 4-5 beyond base (1) → deltas 3.., plus no gap for 2/3
        assert_eq!(runs, vec![(3, 2)]);
    }

    #[test]
    fn choose_repair_picks_expected() {
        assert_eq!(choose_repair_indices(7), vec![7]);
    }
}
