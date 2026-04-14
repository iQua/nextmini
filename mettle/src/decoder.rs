#![cfg_attr(not(test), allow(dead_code))]

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::num::NonZeroUsize;

use crate::encoder::MettleBin;
use crate::MettleParams;

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct DecodedSource {
    source_id: u64,
    payload: Vec<u8>,
}

#[cfg(test)]
impl DecodedSource {
    pub(super) fn as_parts(&self) -> (u64, &[u8]) {
        (self.source_id, &self.payload)
    }

    pub(crate) fn into_parts(self) -> (u64, Vec<u8>) {
        (self.source_id, self.payload)
    }
}

#[derive(Debug)]
struct BufferedBin {
    payload: Vec<u8>,
    remaining_touchers: u16,
}

#[derive(Debug)]
pub(crate) struct MettleDecoder {
    params: MettleParams,
    source_symbol_bytes: NonZeroUsize,
    next_decoded_source_id: u64,
    seed: u64,
    terminal_source_count: Option<u64>,
    decoded_prefix_start_source_id: u64,
    decoded_prefix_payloads: VecDeque<Vec<u8>>,
    decoded_tle_prefix_xors: HashMap<u64, Vec<u8>>,
    seen_bin_ids: HashSet<u128>,
    received_bins: BTreeMap<u128, BufferedBin>,
}

impl MettleDecoder {
    const DECODED_PREFIX_WINDOW: usize = MettleParams::COUPLING_WINDOW as usize + 1;

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
            next_decoded_source_id: 0,
            seed,
            terminal_source_count,
            decoded_prefix_start_source_id: 0,
            decoded_prefix_payloads: VecDeque::new(),
            decoded_tle_prefix_xors: HashMap::new(),
            seen_bin_ids: HashSet::new(),
            received_bins: BTreeMap::new(),
        }
    }

    pub(crate) fn push_bin(&mut self, bin: MettleBin) -> Vec<DecodedSource> {
        let (bin_id, mut payload) = bin.into_parts();
        if self
            .terminal_source_count
            .is_some_and(|terminal_source_count| self.next_decoded_source_id >= terminal_source_count)
        {
            return Vec::new();
        }
        if payload.len() != self.source_symbol_bytes.get()
            || self.params.latest_source_id_for_bin(bin_id).is_none()
        {
            return Vec::new();
        }
        if !self.seen_bin_ids.insert(bin_id) {
            return Vec::new();
        }
        if bin_id == self.params.tle_bin_id(self.next_decoded_source_id)
            && self.received_bins.is_empty()
        {
            if let Some(peeled_prefix) = self
                .decoded_tle_prefix_xors
                .remove(&self.next_decoded_source_id)
            {
                xor_payload(&mut payload, &peeled_prefix);
            }
            return vec![self.decode_next_source(payload)];
        }
        if !self.is_tle_bin_id(bin_id) && self.bin_has_no_undecoded_touchers(bin_id) {
            return Vec::new();
        }
        self.peel_known_prefix_from_bin(bin_id, &mut payload);
        if bin_id == self.params.tle_bin_id(self.next_decoded_source_id) {
            let mut decoded = vec![self.decode_next_source(payload)];
            decoded.extend(self.drain_decodable_prefix());
            return decoded;
        }
        let remaining_touchers = self.count_remaining_touchers(bin_id);
        if remaining_touchers == 0 {
            return Vec::new();
        }
        self.received_bins.insert(
            bin_id,
            BufferedBin {
                payload,
                remaining_touchers,
            },
        );
        self.drain_decodable_prefix()
    }

    fn drain_decodable_prefix(&mut self) -> Vec<DecodedSource> {
        let mut decoded = Vec::new();

        while let Some(bin_id) = self.find_unique_bin_for_next_source() {
            let payload = self
                .received_bins
                .remove(&bin_id)
                .expect("just matched decodable bin");
            decoded.push(self.decode_next_source(payload.payload));
        }

        decoded
    }

    fn decode_next_source(&mut self, payload: Vec<u8>) -> DecodedSource {
        let source_id = self.next_decoded_source_id;
        self.apply_decoded_source_edges(source_id, &payload);
        self.push_decoded_prefix_payload(payload.clone());
        self.next_decoded_source_id += 1;
        self.drop_bins_closed_by_prefix();
        DecodedSource { source_id, payload }
    }

    fn find_unique_bin_for_next_source(&self) -> Option<u128> {
        self.edge_bin_ids(self.next_decoded_source_id)
            .into_iter()
            .find(|&bin_id| self.received_bins.get(&bin_id).is_some_and(|bin| bin.remaining_touchers == 1))
    }

    fn apply_decoded_source_edges(&mut self, source_id: u64, payload: &[u8]) {
        let edge_bin_ids = self.edge_bin_ids(source_id);

        for bin_id in edge_bin_ids {
            if let Some(bin) = self.received_bins.get_mut(&bin_id) {
                xor_payload(&mut bin.payload, payload);
                bin.remaining_touchers = bin.remaining_touchers.saturating_sub(1);
            }
            if let Some(tle_source_id) = self.tle_source_id_for_bin(bin_id) {
                if tle_source_id <= source_id {
                    continue;
                }
                let peeled_prefix = self
                    .decoded_tle_prefix_xors
                    .entry(tle_source_id)
                    .or_insert_with(|| vec![0; self.source_symbol_bytes.get()]);
                xor_payload(peeled_prefix, payload);
            }
        }
    }

    fn peel_known_prefix_from_bin(&self, bin_id: u128, payload: &mut [u8]) {
        if let Some(tle_source_id) = self.tle_source_id_for_bin(bin_id) {
            if let Some(peeled_prefix) = self.decoded_tle_prefix_xors.get(&tle_source_id) {
                xor_payload(payload, peeled_prefix);
            }
            return;
        }
        let Some((earliest_source_id, latest_source_id)) =
            self.possible_source_id_range_for_bin(bin_id)
        else {
            return;
        };

        for source_id in earliest_source_id..self.next_decoded_source_id.min(latest_source_id + 1) {
            if self.edge_bin_ids(source_id).contains(&bin_id) {
                xor_payload(
                    payload,
                    self.decoded_prefix_payload(source_id)
                        .expect("possible source range stays inside the bounded prefix window"),
                );
            }
        }
    }

    fn bin_has_no_undecoded_touchers(&self, bin_id: u128) -> bool {
        if self
            .terminal_source_count
            .is_some_and(|terminal_source_count| self.next_decoded_source_id >= terminal_source_count)
        {
            return true;
        }
        bin_id < self.params.tle_bin_id(self.next_decoded_source_id)
    }

    fn drop_bins_closed_by_prefix(&mut self) {
        if self
            .terminal_source_count
            .is_some_and(|terminal_source_count| self.next_decoded_source_id >= terminal_source_count)
        {
            self.received_bins.clear();
            self.decoded_tle_prefix_xors.clear();
            return;
        }
        let frontier = self.params.tle_bin_id(self.next_decoded_source_id);
        self.received_bins = self.received_bins.split_off(&frontier);
        self.decoded_tle_prefix_xors
            .retain(|&source_id, _| source_id >= self.next_decoded_source_id);
    }

    fn count_remaining_touchers(&self, bin_id: u128) -> u16 {
        let Some((earliest_source_id, latest_source_id)) =
            self.possible_source_id_range_for_bin(bin_id)
        else {
            return 0;
        };
        let mut count = 0u16;

        for source_id in earliest_source_id.max(self.next_decoded_source_id)..=latest_source_id {
            if self.edge_bin_ids(source_id).contains(&bin_id) {
                count += 1;
            }
        }

        count
    }

    fn is_tle_bin_id(&self, bin_id: u128) -> bool {
        self.tle_source_id_for_bin(bin_id).is_some()
    }

    fn edge_bin_ids(&self, source_id: u64) -> [u128; MettleParams::EDGE_COUNT] {
        self.params.edge_bin_ids_with_terminal_source_count(
            source_id,
            self.seed,
            self.terminal_source_count,
        )
    }

    fn push_decoded_prefix_payload(&mut self, payload: Vec<u8>) {
        self.decoded_prefix_payloads.push_back(payload);
        if self.decoded_prefix_payloads.len() > Self::DECODED_PREFIX_WINDOW {
            self.decoded_prefix_payloads.pop_front();
            self.decoded_prefix_start_source_id += 1;
        }
    }

    fn decoded_prefix_payload(&self, source_id: u64) -> Option<&[u8]> {
        let offset = source_id.checked_sub(self.decoded_prefix_start_source_id)?;
        let index = usize::try_from(offset).ok()?;
        self.decoded_prefix_payloads.get(index).map(Vec::as_slice)
    }

    fn possible_source_id_range_for_bin(&self, bin_id: u128) -> Option<(u64, u64)> {
        self.params
            .possible_source_id_range_for_bin(bin_id, self.terminal_source_count)
    }

    fn tle_source_id_for_bin(&self, bin_id: u128) -> Option<u64> {
        self.params.latest_source_id_for_bin(bin_id).filter(|&source_id| {
            self.terminal_source_count
                .is_none_or(|terminal_source_count| source_id < terminal_source_count)
                && self.params.tle_bin_id(source_id) == bin_id
        })
    }

    pub(crate) fn next_source_id(&self) -> u64 {
        self.next_decoded_source_id
    }

    pub(crate) fn skip_next_source_without_edges(&mut self) -> Vec<DecodedSource> {
        self.push_decoded_prefix_payload(vec![0; self.source_symbol_bytes.get()]);
        self.next_decoded_source_id += 1;
        self.drop_bins_closed_by_prefix();
        self.drain_decodable_prefix()
    }
}

fn xor_payload(dst: &mut [u8], src: &[u8]) {
    for (dst_byte, src_byte) in dst.iter_mut().zip(src) {
        *dst_byte ^= *src_byte;
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::num::NonZeroUsize;

    use crate::encoder::MettleEncoder;
    use crate::{MettleParams, OverheadRatio};

    use crate::encoder::MettleBin;

    use super::{DecodedSource, MettleDecoder};

    #[test]
    fn push_bin_deduplicates_by_bin_id() {
        let params = MettleParams::new(OverheadRatio::new(1, 20).expect("valid overhead"));
        let mut decoder =
            MettleDecoder::new(params, NonZeroUsize::new(4).expect("non-zero"), 0x1234);

        assert!(decoder.push_bin(MettleBin::new(17, vec![1, 2, 3, 4])).is_empty());
        assert!(decoder.push_bin(MettleBin::new(17, vec![1, 2, 3, 4])).is_empty());

        assert_eq!(decoder.received_bins.len(), 1);
        let buffered = decoder.received_bins.get(&17).expect("buffered bin");
        assert_eq!(buffered.payload, vec![1, 2, 3, 4]);
    }

    #[test]
    fn push_bin_rejects_wrong_payload_length() {
        let params = MettleParams::new(OverheadRatio::new(1, 20).expect("valid overhead"));
        let mut decoder =
            MettleDecoder::new(params, NonZeroUsize::new(4).expect("non-zero"), 0x1234);

        assert!(decoder.push_bin(MettleBin::new(17, vec![1, 2, 3])).is_empty());
        assert!(decoder.seen_bin_ids.is_empty());
        assert!(decoder.received_bins.is_empty());
    }

    #[test]
    fn push_bin_decodes_a_unique_prefix_bin() {
        let params = MettleParams::new(OverheadRatio::new(1, 20).expect("valid overhead"));
        let mut decoder = MettleDecoder::new(params, NonZeroUsize::new(4).expect("non-zero"), 0);

        let decoded = decoder.push_bin(MettleBin::new(0, vec![1, 2, 3, 4]));

        assert_eq!(
            decoded,
            vec![DecodedSource {
                source_id: 0,
                payload: vec![1, 2, 3, 4],
            }]
        );
        assert_eq!(decoder.next_decoded_source_id, 1);
        assert!(decoder.seen_bin_ids.contains(&0));
        assert!(decoder.received_bins.is_empty());
    }

    #[test]
    fn push_bin_drains_waiting_unique_prefix_bins_in_order() {
        let params = MettleParams::new(OverheadRatio::new(1, 20).expect("valid overhead"));
        let mut decoder = MettleDecoder::new(params, NonZeroUsize::new(4).expect("non-zero"), 0);

        assert!(decoder.push_bin(MettleBin::new(1, vec![5, 6, 7, 8])).is_empty());

        let decoded = decoder.push_bin(MettleBin::new(0, vec![1, 2, 3, 4]));

        assert_eq!(
            decoded,
            vec![
                DecodedSource {
                    source_id: 0,
                    payload: vec![1, 2, 3, 4],
                },
                DecodedSource {
                    source_id: 1,
                    payload: vec![5, 6, 7, 8],
                },
            ]
        );
        assert_eq!(decoder.next_decoded_source_id, 2);
        assert!(decoder.seen_bin_ids.contains(&0));
        assert!(decoder.seen_bin_ids.contains(&1));
        assert!(decoder.received_bins.is_empty());
    }

    #[test]
    fn push_bin_peels_a_decoded_source_out_of_overlap_bins() {
        let params = MettleParams::new(OverheadRatio::new(1, 20).expect("valid overhead"));
        let mut decoder = MettleDecoder::new(params, NonZeroUsize::new(1).expect("non-zero"), 0);
        decoder.next_decoded_source_id = 537;
        decoder.decoded_prefix_payloads = VecDeque::from(vec![vec![0]; 537]);

        assert!(decoder.push_bin(MettleBin::new(1116, vec![0b0110_0000])).is_empty());

        let decoded = decoder.push_bin(MettleBin::new(563, vec![0b1010_0000]));

        assert_eq!(
            decoded,
            vec![DecodedSource {
                source_id: 537,
                payload: vec![0b1010_0000],
            }]
        );
        assert_eq!(decoder.next_decoded_source_id, 538);
        assert!(decoder.seen_bin_ids.contains(&563));
        let buffered = decoder.received_bins.get(&1116).expect("buffered bin");
        assert_eq!(buffered.payload, vec![0b1100_0000]);
    }

    #[test]
    fn push_bin_ignores_duplicates_after_prefix_decode() {
        let params = MettleParams::new(OverheadRatio::new(1, 20).expect("valid overhead"));
        let mut decoder = MettleDecoder::new(params, NonZeroUsize::new(4).expect("non-zero"), 0);

        assert_eq!(
            decoder.push_bin(MettleBin::new(0, vec![1, 2, 3, 4])),
            vec![DecodedSource {
                source_id: 0,
                payload: vec![1, 2, 3, 4],
            }]
        );

        assert!(decoder.push_bin(MettleBin::new(0, vec![1, 2, 3, 4])).is_empty());
        assert_eq!(
            decoder.decoded_prefix_payloads.iter().cloned().collect::<Vec<_>>(),
            vec![vec![1, 2, 3, 4]]
        );
        assert!(decoder.received_bins.is_empty());
    }

    #[test]
    fn push_bin_rejects_max_bin_id() {
        let params = MettleParams::new(OverheadRatio::new(1, 20).expect("valid overhead"));
        let mut decoder =
            MettleDecoder::new(params, NonZeroUsize::new(4).expect("non-zero"), 0x1234);

        assert!(decoder
            .push_bin(MettleBin::new(u128::MAX, vec![1, 2, 3, 4]))
            .is_empty());
        assert!(decoder.seen_bin_ids.is_empty());
        assert!(decoder.received_bins.is_empty());
    }

    #[test]
    fn push_bin_rejects_unrepresentable_large_bin_id() {
        let params = MettleParams::new(OverheadRatio::new(1, 20).expect("valid overhead"));
        let mut decoder =
            MettleDecoder::new(params, NonZeroUsize::new(4).expect("non-zero"), 0x1234);

        assert!(decoder
            .push_bin(MettleBin::new(u128::MAX - 1, vec![1, 2, 3, 4]))
            .is_empty());
        assert!(decoder.seen_bin_ids.is_empty());
        assert!(decoder.received_bins.is_empty());
    }

    #[test]
    fn encoder_round_trip_decodes_no_loss_stream() {
        let params = MettleParams::new(OverheadRatio::new(1, 20).expect("valid overhead"));
        let source_symbol_bytes = NonZeroUsize::new(2).expect("non-zero");
        let mut encoder = MettleEncoder::new(params, source_symbol_bytes, 0);
        let mut bins = Vec::new();
        let sources = [vec![1, 2], vec![3, 4], vec![5, 6]];

        for source in &sources {
            bins.extend(encoder.push_source(source));
        }
        bins.extend(encoder.finish());

        let mut decoder = MettleDecoder::new(params, source_symbol_bytes, 0);
        let mut decoded = Vec::new();
        for bin in bins {
            decoded.extend(decoder.push_bin(bin));
        }

        assert_eq!(
            decoded,
            vec![
                DecodedSource {
                    source_id: 0,
                    payload: vec![1, 2],
                },
                DecodedSource {
                    source_id: 1,
                    payload: vec![3, 4],
                },
                DecodedSource {
                    source_id: 2,
                    payload: vec![5, 6],
                },
            ]
        );
        assert_eq!(decoder.next_decoded_source_id, 3);
        assert_eq!(decoder.decoded_prefix_start_source_id, 0);
        assert_eq!(
            decoder.decoded_prefix_payloads.iter().cloned().collect::<Vec<_>>(),
            sources
        );
        let frontier = params.tle_bin_id(decoder.next_decoded_source_id);
        assert!(decoder.received_bins.keys().all(|&bin_id| bin_id >= frontier));
    }

    #[test]
    fn fast_tle_path_still_peels_future_tle_bins() {
        let params = MettleParams::new(OverheadRatio::new(1, 20).expect("valid overhead"));
        let source_symbol_bytes = NonZeroUsize::new(1).expect("non-zero");
        let future_tle_source_id = 320u64;
        let future_tle_bin_id = params.tle_bin_id(future_tle_source_id);
        let source_count = future_tle_source_id + 1;
        let mut encoder =
            MettleEncoder::new_terminated(params, source_symbol_bytes, 0, source_count);
        let mut bins = Vec::new();
        let mut sources = vec![vec![0]; source_count as usize];
        sources[0] = vec![0b1010_0000];
        sources[future_tle_source_id as usize] = vec![0b1100_0000];

        assert!(params.edge_bin_ids(0, 0).contains(&future_tle_bin_id));

        for source in &sources {
            bins.extend(encoder.push_source(source));
        }
        bins.extend(encoder.finish());

        let mut decoder =
            MettleDecoder::new_terminated(params, source_symbol_bytes, 0, source_count);
        let decoded = bins
            .into_iter()
            .flat_map(|bin| decoder.push_bin(bin))
            .map(DecodedSource::into_parts)
            .collect::<Vec<_>>();
        let expected = sources
            .into_iter()
            .enumerate()
            .map(|(source_id, payload)| (source_id as u64, payload))
            .collect::<Vec<_>>();

        assert_eq!(decoded, expected);
    }

    #[test]
    fn terminated_decoder_rejects_post_terminal_tle_bins() {
        let params = MettleParams::new(OverheadRatio::new(1, 20).expect("valid overhead"));
        let source_symbol_bytes = NonZeroUsize::new(1).expect("non-zero");
        let mut decoder = MettleDecoder::new_terminated(params, source_symbol_bytes, 0, 1);

        assert_eq!(
            decoder.push_bin(MettleBin::new(params.tle_bin_id(0), vec![0b1010_0000])),
            vec![DecodedSource {
                source_id: 0,
                payload: vec![0b1010_0000],
            }]
        );
        assert!(decoder
            .push_bin(MettleBin::new(params.tle_bin_id(1), vec![0b1100_0000]))
            .is_empty());
        assert_eq!(decoder.next_decoded_source_id, 1);
    }

    #[test]
    fn decoder_bounds_prefix_history_to_the_local_window() {
        let params = MettleParams::new(OverheadRatio::new(1, 20).expect("valid overhead"));
        let source_symbol_bytes = NonZeroUsize::new(1).expect("non-zero");
        let extra_sources = 3u64;
        let total_sources = MettleDecoder::DECODED_PREFIX_WINDOW as u64 + extra_sources;
        let mut decoder = MettleDecoder::new(params, source_symbol_bytes, 0);

        for source_id in 0..total_sources {
            let decoded = decoder.push_bin(MettleBin::new(params.tle_bin_id(source_id), vec![0]));
            assert_eq!(decoded.len(), 1);
        }

        assert_eq!(
            decoder.decoded_prefix_payloads.len(),
            MettleDecoder::DECODED_PREFIX_WINDOW
        );
        assert_eq!(decoder.decoded_prefix_start_source_id, extra_sources);
    }

    #[test]
    fn missing_prefix_bin_stalls_until_it_is_replayed() {
        let params = MettleParams::new(OverheadRatio::new(1, 20).expect("valid overhead"));
        let source_symbol_bytes = NonZeroUsize::new(2).expect("non-zero");
        let mut encoder = MettleEncoder::new(params, source_symbol_bytes, 0);
        let mut bins = Vec::new();
        let sources = [vec![1, 2], vec![3, 4], vec![5, 6]];

        for source in &sources {
            bins.extend(encoder.push_source(source));
        }
        bins.extend(encoder.finish());

        let missing_prefix_bin = bins.remove(0);
        let mut decoder = MettleDecoder::new(params, source_symbol_bytes, 0);
        let mut decoded = Vec::new();
        for bin in bins {
            decoded.extend(decoder.push_bin(bin));
        }

        assert!(decoded.is_empty());
        assert_eq!(decoder.next_decoded_source_id, 0);

        decoded.extend(decoder.push_bin(missing_prefix_bin));
        assert_eq!(
            decoded,
            vec![
                DecodedSource {
                    source_id: 0,
                    payload: vec![1, 2],
                },
                DecodedSource {
                    source_id: 1,
                    payload: vec![3, 4],
                },
                DecodedSource {
                    source_id: 2,
                    payload: vec![5, 6],
                },
            ]
        );
    }
}
