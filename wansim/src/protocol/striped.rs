use std::collections::{BTreeMap, BTreeSet};

use super::ControlFrame;

pub fn equal_quotas(source_symbols: usize, tree_count: usize) -> Option<Vec<usize>> {
    if tree_count == 0 {
        return None;
    }
    let quotient = source_symbols / tree_count;
    let remainder = source_symbols % tree_count;
    Some(
        (0..tree_count)
            .map(|tree| quotient + usize::from(tree < remainder))
            .collect(),
    )
}

pub fn proportional_quotas(source_symbols: usize, weights: &[u64]) -> Option<Vec<usize>> {
    let total_weight: u128 = weights.iter().map(|weight| u128::from(*weight)).sum();
    if weights.is_empty() || total_weight == 0 {
        return None;
    }
    let source = source_symbols as u128;
    let mut quotas = Vec::with_capacity(weights.len());
    let mut remainders = Vec::with_capacity(weights.len());
    let mut assigned = 0usize;
    for (index, weight) in weights.iter().copied().enumerate() {
        let product = source.checked_mul(u128::from(weight))?;
        let floor = usize::try_from(product / total_weight).ok()?;
        quotas.push(floor);
        assigned = assigned.checked_add(floor)?;
        remainders.push((product % total_weight, index));
    }
    remainders.sort_unstable_by(|lhs, rhs| rhs.cmp(lhs));
    for (_, index) in remainders.into_iter().take(source_symbols - assigned) {
        quotas[index] = quotas[index].checked_add(1)?;
    }
    Some(quotas)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StripeSenderMode {
    Finite,
    ContinuousFec,
}

#[derive(Clone, Debug)]
pub struct StripeSender {
    mode: StripeSenderMode,
    quotas: Vec<usize>,
    emitted: Vec<usize>,
    next_tree: usize,
    peers: BTreeSet<u64>,
    completed: BTreeMap<(u64, usize), bool>,
}

impl StripeSender {
    pub fn new(
        mode: StripeSenderMode,
        quotas: Vec<usize>,
        peers: impl IntoIterator<Item = u64>,
    ) -> Self {
        let peers: BTreeSet<_> = peers.into_iter().collect();
        let completed = peers
            .iter()
            .flat_map(|peer| (0..quotas.len()).map(move |tree| ((*peer, tree), false)))
            .collect();
        Self {
            mode,
            emitted: vec![0; quotas.len()],
            quotas,
            next_tree: 0,
            peers,
            completed,
        }
    }

    pub fn next_emission(&mut self, writable_trees: &BTreeSet<usize>) -> Option<(usize, usize)> {
        if self.quotas.is_empty() {
            return None;
        }
        for offset in 0..self.quotas.len() {
            let tree = (self.next_tree + offset) % self.quotas.len();
            let needs_emission = match self.mode {
                StripeSenderMode::Finite => self.emitted[tree] < self.quotas[tree],
                StripeSenderMode::ContinuousFec => self
                    .peers
                    .iter()
                    .any(|peer| !self.completed[&(*peer, tree)]),
            };
            if needs_emission && writable_trees.contains(&tree) {
                let symbol = self.emitted[tree];
                self.emitted[tree] = self.emitted[tree].checked_add(1)?;
                self.next_tree = (tree + 1) % self.quotas.len();
                return Some((tree, symbol));
            }
        }
        None
    }

    pub fn on_ack(&mut self, peer_id: u64, stripe_id: usize) -> bool {
        let Some(completed) = self.completed.get_mut(&(peer_id, stripe_id)) else {
            return false;
        };
        *completed = true;
        true
    }

    pub fn all_complete(&self) -> bool {
        self.completed.values().all(|completed| *completed)
    }
}

#[derive(Clone, Debug)]
pub struct StripeReceiver {
    quotas: Vec<usize>,
    innovative: Vec<BTreeSet<usize>>,
    acknowledged: Vec<bool>,
    local_completion_ns: Option<u64>,
}

impl StripeReceiver {
    pub fn new(quotas: Vec<usize>) -> Self {
        let tree_count = quotas.len();
        Self {
            quotas,
            innovative: (0..tree_count).map(|_| BTreeSet::new()).collect(),
            acknowledged: vec![false; tree_count],
            local_completion_ns: None,
        }
    }

    pub fn observe_symbol(
        &mut self,
        tree: usize,
        symbol: usize,
        now_ns: u64,
    ) -> Option<ControlFrame> {
        let bucket = self.innovative.get_mut(tree)?;
        bucket.insert(symbol);
        let complete = bucket.len() >= self.quotas[tree];
        let ack = if complete && !self.acknowledged[tree] {
            self.acknowledged[tree] = true;
            Some(ControlFrame::StripeAck { stripe_id: tree })
        } else {
            None
        };
        if self
            .innovative
            .iter()
            .zip(&self.quotas)
            .all(|(bucket, quota)| bucket.len() >= *quota)
            && self.local_completion_ns.is_none()
        {
            self.local_completion_ns = Some(now_ns);
        }
        ack
    }

    pub fn local_completion_ns(&self) -> Option<u64> {
        self.local_completion_ns
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn largest_remainder_quotas_are_exact() {
        assert_eq!(proportional_quotas(17, &[4, 1]), Some(vec![14, 3]));
        assert_eq!(equal_quotas(17, 2), Some(vec![9, 8]));
    }

    #[test]
    fn continuous_stripe_sender_stops_each_owned_bucket_independently() {
        let mut sender = StripeSender::new(StripeSenderMode::ContinuousFec, vec![2, 2], [7]);
        let both = BTreeSet::from([0, 1]);
        assert_eq!(sender.next_emission(&both), Some((0, 0)));
        assert!(sender.on_ack(7, 0));
        assert_eq!(sender.next_emission(&both), Some((1, 0)));
    }
}
