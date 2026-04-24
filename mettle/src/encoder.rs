#![cfg_attr(not(test), allow(dead_code))]

use std::collections::VecDeque;
use std::num::NonZeroUsize;

use crate::MettleParams;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct MettleBin {
    bin_id: u128,
    payload: Vec<u8>,
}

impl MettleBin {
    pub(crate) fn new(bin_id: u128, payload: Vec<u8>) -> Self {
        Self { bin_id, payload }
    }

    pub(crate) fn into_parts(self) -> (u128, Vec<u8>) {
        (self.bin_id, self.payload)
    }
}

#[derive(Debug)]
pub(crate) struct MettleEncoder {
    params: MettleParams,
    source_symbol_bytes: NonZeroUsize,
    next_source_id: u64,
    next_departure_bin_id: u128,
    seed: u64,
    terminal_source_count: Option<u64>,
    open_bins: VecDeque<Option<Vec<u8>>>,
    source_scratch: Vec<u8>,
}

impl MettleEncoder {
    pub(crate) fn new(params: MettleParams, source_symbol_bytes: NonZeroUsize, seed: u64) -> Self {
        Self::new_with_terminal_source_count(params, source_symbol_bytes, seed, None)
    }

    pub(crate) fn new_terminated(
        params: MettleParams,
        source_symbol_bytes: NonZeroUsize,
        seed: u64,
        terminal_source_count: u64,
    ) -> Self {
        Self::new_with_terminal_source_count(
            params,
            source_symbol_bytes,
            seed,
            Some(terminal_source_count),
        )
    }

    fn new_with_terminal_source_count(
        params: MettleParams,
        source_symbol_bytes: NonZeroUsize,
        seed: u64,
        terminal_source_count: Option<u64>,
    ) -> Self {
        Self {
            params,
            source_symbol_bytes,
            next_source_id: 0,
            next_departure_bin_id: 0,
            seed,
            terminal_source_count,
            open_bins: VecDeque::new(),
            source_scratch: vec![0; source_symbol_bytes.get()],
        }
    }

    pub(crate) fn push_source(&mut self, payload: &[u8]) -> Vec<MettleBin> {
        assert!(payload.len() <= self.source_symbol_bytes.get());
        if let Some(terminal_source_count) = self.terminal_source_count {
            assert!(self.next_source_id < terminal_source_count);
        }

        self.source_scratch.fill(0);
        self.source_scratch[..payload.len()].copy_from_slice(payload);
        self.apply_previous_fake_tle_payload(self.next_source_id);

        let (edge_bin_ids, edge_count) = self.unique_edge_bin_id_buffer(self.next_source_id);
        for &bin_id in &edge_bin_ids[..edge_count] {
            let slot_index = self.ensure_open_bin_slot(bin_id);
            let entry = self.open_bins[slot_index]
                .get_or_insert_with(|| vec![0; self.source_symbol_bytes.get()]);
            xor_payload(entry, &self.source_scratch);
        }

        self.next_source_id += 1;
        self.take_finalized_bins_until(
            self.params
                .departure_frontier_after_source_count(self.next_source_id),
        )
    }

    pub(crate) fn finish(mut self) -> Vec<MettleBin> {
        if let Some(terminal_source_count) = self.terminal_source_count {
            return self.take_finalized_bins_until(
                self.params
                    .terminal_departure_end_exclusive(terminal_source_count),
            );
        }

        self.flush_open_bins()
    }

    fn apply_previous_fake_tle_payload(&mut self, source_id: u64) {
        let tle_bin_id = self.params.tle_bin_id(source_id);
        if tle_bin_id < self.next_departure_bin_id {
            return;
        }
        let slot_index =
            usize::try_from(tle_bin_id - self.next_departure_bin_id).expect("open bin index fits");
        let Self {
            open_bins,
            source_scratch,
            ..
        } = self;
        if let Some(Some(previous_fake_tle_payloads)) = open_bins.get(slot_index) {
            xor_payload(source_scratch, previous_fake_tle_payloads);
        }
    }

    fn fake_source_payload(&self, source_id: u64, mut source_payload: Vec<u8>) -> Vec<u8> {
        if let Some(previous_fake_tle_payloads) =
            self.open_bin_payload(self.params.tle_bin_id(source_id))
        {
            xor_payload(&mut source_payload, previous_fake_tle_payloads);
        }

        source_payload
    }

    fn unique_edge_bin_id_buffer(
        &self,
        source_id: u64,
    ) -> ([u128; MettleParams::EDGE_COUNT], usize) {
        self.params
            .unique_edge_bin_id_buffer_with_terminal_source_count(
                source_id,
                self.seed,
                self.terminal_source_count,
            )
    }

    fn ensure_open_bin_slot(&mut self, bin_id: u128) -> usize {
        assert!(bin_id >= self.next_departure_bin_id);
        let slot_index =
            usize::try_from(bin_id - self.next_departure_bin_id).expect("open bin index fits");
        while self.open_bins.len() <= slot_index {
            self.open_bins.push_back(None);
        }
        slot_index
    }

    fn open_bin_payload(&self, bin_id: u128) -> Option<&Vec<u8>> {
        if bin_id < self.next_departure_bin_id {
            return None;
        }
        let slot_index = usize::try_from(bin_id - self.next_departure_bin_id).ok()?;
        self.open_bins.get(slot_index)?.as_ref()
    }

    fn take_finalized_bins_until(&mut self, end_exclusive: u128) -> Vec<MettleBin> {
        let emit_count =
            usize::try_from(end_exclusive - self.next_departure_bin_id).expect("emit count fits");
        while self.open_bins.len() < emit_count {
            self.open_bins.push_back(None);
        }
        let mut emitted = Vec::with_capacity(emit_count);

        for _ in 0..emit_count {
            let bin_id = self.next_departure_bin_id;
            let payload = self
                .open_bins
                .pop_front()
                .flatten()
                .unwrap_or_else(|| vec![0; self.source_symbol_bytes.get()]);
            emitted.push(MettleBin { bin_id, payload });
            self.next_departure_bin_id += 1;
        }

        emitted
    }

    fn flush_open_bins(&mut self) -> Vec<MettleBin> {
        let start_bin_id = self.next_departure_bin_id;
        std::mem::take(&mut self.open_bins)
            .into_iter()
            .enumerate()
            .filter_map(|(offset, payload)| {
                Some(MettleBin {
                    bin_id: start_bin_id + offset as u128,
                    payload: payload?,
                })
            })
            .collect()
    }
}

fn xor_payload(dst: &mut [u8], src: &[u8]) {
    for (dst_byte, src_byte) in dst.iter_mut().zip(src) {
        *dst_byte ^= *src_byte;
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use crate::{MettleParams, OverheadRatio};

    use super::{MettleBin, MettleEncoder, xor_payload};

    #[test]
    fn encoder_keeps_constructor_fields() {
        let params = MettleParams::new(OverheadRatio::new(1, 20).expect("valid overhead"));
        let encoder = MettleEncoder::new(params, NonZeroUsize::new(1500).expect("non-zero"), 7);

        assert_eq!(encoder.params, params);
        assert_eq!(encoder.source_symbol_bytes.get(), 1500);
        assert_eq!(encoder.next_source_id, 0);
        assert_eq!(encoder.next_departure_bin_id, 0);
        assert_eq!(encoder.seed, 7);
        assert_eq!(encoder.terminal_source_count, None);
        assert!(encoder.open_bins.is_empty());
    }

    #[test]
    fn push_source_emits_finalized_tle_bin() {
        let params = MettleParams::new(OverheadRatio::new(1, 20).expect("valid overhead"));
        let mut encoder =
            MettleEncoder::new(params, NonZeroUsize::new(4).expect("non-zero"), 0x1234);

        let emitted = encoder.push_source(&[1, 2]);

        assert_eq!(
            emitted,
            vec![MettleBin {
                bin_id: 0,
                payload: vec![1, 2, 0, 0],
            }]
        );
        assert_eq!(encoder.next_source_id, 1);
    }

    #[test]
    fn push_source_keeps_overlapping_future_bin_open() {
        let params = MettleParams::new(OverheadRatio::new(1, 20).expect("valid overhead"));
        let (first_source_id, second_source_id, shared_bin) = (0..64)
            .find_map(|first_source_id| {
                let first_edges = params.edge_bin_ids(first_source_id, 0);
                ((first_source_id + 1)..64).find_map(|second_source_id| {
                    let second_edges = params.edge_bin_ids(second_source_id, 0);
                    first_edges
                        .into_iter()
                        .find(|&bin_id| {
                            bin_id >= params.tle_bin_id(second_source_id + 1)
                                && second_edges.contains(&bin_id)
                        })
                        .map(|bin_id| (first_source_id, second_source_id, bin_id))
                })
            })
            .expect("expected a future overlapping bin");
        let mut encoder = MettleEncoder::new(params, NonZeroUsize::new(1).expect("non-zero"), 0);

        for _ in 0..first_source_id {
            encoder.push_source(&[0]);
        }

        encoder.push_source(&[0b1010_0000]);
        assert_eq!(
            encoder.open_bin_payload(shared_bin),
            Some(&vec![0b1010_0000])
        );

        for _ in (first_source_id + 1)..second_source_id {
            encoder.push_source(&[0]);
        }

        let mut expected_shared_payload = encoder
            .open_bin_payload(shared_bin)
            .cloned()
            .expect("first source opened shared bin");
        let second_fake_payload = encoder.fake_source_payload(second_source_id, vec![0b1100_0000]);
        xor_payload(&mut expected_shared_payload, &second_fake_payload);

        let second_emitted = encoder.push_source(&[0b1100_0000]);

        assert!(!second_emitted.iter().any(|bin| bin.bin_id == shared_bin));
        assert_eq!(
            encoder.open_bin_payload(shared_bin),
            Some(&expected_shared_payload)
        );
    }

    #[test]
    fn terminated_encoder_releases_dense_departure_prefix() {
        let params = MettleParams::new(OverheadRatio::new(1, 20).expect("valid overhead"));
        let source_symbol_bytes = NonZeroUsize::new(1).expect("non-zero");
        let mut encoder = MettleEncoder::new_terminated(params, source_symbol_bytes, 0, 20);
        let mut emitted_bin_ids = Vec::new();

        for _ in 0..20 {
            emitted_bin_ids.extend(encoder.push_source(&[1]).into_iter().map(|bin| bin.bin_id));
        }

        assert_eq!(
            emitted_bin_ids,
            (0..params.departure_frontier_after_source_count(20)).collect::<Vec<_>>()
        );
    }

    #[test]
    fn finish_emits_remaining_open_bins_in_order() {
        let params = MettleParams::new(OverheadRatio::new(1, 20).expect("valid overhead"));
        let mut encoder = MettleEncoder::new(params, NonZeroUsize::new(1).expect("non-zero"), 0);
        let mut expected_remaining_bin_ids = params.edge_bin_ids(0, 0);
        expected_remaining_bin_ids.sort_unstable();

        let emitted = encoder.push_source(&[0b1010_0000]);
        let remaining = encoder.finish();

        assert_eq!(
            emitted,
            vec![MettleBin {
                bin_id: expected_remaining_bin_ids[0],
                payload: vec![0b1010_0000],
            }]
        );
        assert_eq!(remaining.len(), expected_remaining_bin_ids.len() - 1);
        assert_eq!(
            remaining.iter().map(|bin| bin.bin_id).collect::<Vec<_>>(),
            expected_remaining_bin_ids[1..]
        );
        assert!(remaining.iter().all(|bin| bin.payload == vec![0b1010_0000]));
    }
}
