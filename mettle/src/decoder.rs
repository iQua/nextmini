#![cfg_attr(not(test), allow(dead_code))]

use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroUsize;

use crate::encoder::MettleBin;
use crate::MettleParams;

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct DecodedSource {
    source_id: u64,
    payload: Vec<u8>,
}

#[derive(Debug)]
pub(crate) struct MettleDecoder {
    params: MettleParams,
    source_symbol_bytes: NonZeroUsize,
    next_decoded_source_id: u64,
    seed: u64,
    decoded_prefix_payloads: Vec<Vec<u8>>,
    seen_bin_ids: BTreeSet<u128>,
    received_bins: BTreeMap<u128, Vec<u8>>,
}

impl MettleDecoder {
    pub(crate) fn new(params: MettleParams, source_symbol_bytes: NonZeroUsize, seed: u64) -> Self {
        Self {
            params,
            source_symbol_bytes,
            next_decoded_source_id: 0,
            seed,
            decoded_prefix_payloads: Vec::new(),
            seen_bin_ids: BTreeSet::new(),
            received_bins: BTreeMap::new(),
        }
    }

    pub(crate) fn push_bin(&mut self, bin: MettleBin) -> Vec<DecodedSource> {
        let (bin_id, mut payload) = bin.into_parts();
        if payload.len() != self.source_symbol_bytes.get()
            || self.latest_source_id_for_bin(bin_id).is_none()
        {
            return Vec::new();
        }
        if !self.seen_bin_ids.insert(bin_id) {
            return Vec::new();
        }
        self.peel_known_prefix_from_bin(bin_id, &mut payload);
        if self.bin_has_no_undecoded_touchers(bin_id) {
            return Vec::new();
        }
        self.received_bins.insert(bin_id, payload);
        self.drain_decodable_prefix()
    }

    fn drain_decodable_prefix(&mut self) -> Vec<DecodedSource> {
        let mut decoded = Vec::new();

        while let Some(bin_id) = self.find_unique_bin_for_next_source() {
            let payload = self
                .received_bins
                .remove(&bin_id)
                .expect("just matched decodable bin");
            self.peel_source_from_open_bins(self.next_decoded_source_id, &payload);
            self.decoded_prefix_payloads.push(payload.clone());
            decoded.push(DecodedSource {
                source_id: self.next_decoded_source_id,
                payload,
            });
            self.next_decoded_source_id += 1;
            self.drop_bins_closed_by_prefix();
        }

        decoded
    }

    fn find_unique_bin_for_next_source(&self) -> Option<u128> {
        self.received_bins.keys().copied().find(|&bin_id| {
            let Some(latest_source_id) = self.latest_source_id_for_bin(bin_id) else {
                return false;
            };
            let earliest_source_id =
                latest_source_id.saturating_sub(MettleParams::COUPLING_WINDOW - 1);
            let mut unique_source_id = None;

            for candidate_source_id in earliest_source_id..=latest_source_id {
                if candidate_source_id < self.next_decoded_source_id {
                    continue;
                }
                if !self
                    .params
                    .edge_bin_ids(candidate_source_id, self.seed)
                    .contains(&bin_id)
                {
                    continue;
                }
                if unique_source_id.is_some() {
                    return false;
                }
                unique_source_id = Some(candidate_source_id);
            }

            unique_source_id == Some(self.next_decoded_source_id)
        })
    }

    fn peel_source_from_open_bins(&mut self, source_id: u64, payload: &[u8]) {
        for bin_id in self.params.edge_bin_ids(source_id, self.seed) {
            if let Some(bin_payload) = self.received_bins.get_mut(&bin_id) {
                xor_payload(bin_payload, payload);
            }
        }
    }

    fn peel_known_prefix_from_bin(&self, bin_id: u128, payload: &mut [u8]) {
        let Some(latest_source_id) = self.latest_source_id_for_bin(bin_id) else {
            return;
        };
        let earliest_source_id =
            latest_source_id.saturating_sub(MettleParams::COUPLING_WINDOW - 1);

        for source_id in earliest_source_id..self.next_decoded_source_id.min(latest_source_id + 1) {
            if self.params.edge_bin_ids(source_id, self.seed).contains(&bin_id) {
                xor_payload(payload, &self.decoded_prefix_payloads[source_id as usize]);
            }
        }
    }

    fn bin_has_no_undecoded_touchers(&self, bin_id: u128) -> bool {
        self.latest_source_id_for_bin(bin_id)
            .is_some_and(|latest_source_id| latest_source_id < self.next_decoded_source_id)
    }

    fn drop_bins_closed_by_prefix(&mut self) {
        let closed_bin_ids = self
            .received_bins
            .keys()
            .copied()
            .filter(|&bin_id| self.bin_has_no_undecoded_touchers(bin_id))
            .collect::<Vec<_>>();
        for bin_id in closed_bin_ids {
            self.received_bins.remove(&bin_id);
        }
    }

    fn latest_source_id_for_bin(&self, bin_id: u128) -> Option<u64> {
        let denominator = u128::from(self.params.overhead().denominator());
        let expansion_numerator = u128::from(self.params.overhead().numerator()) + denominator;
        let scaled = bin_id
            .checked_add(1)?
            .checked_mul(denominator)?
            .checked_sub(1)?;
        u64::try_from(scaled / expansion_numerator).ok()
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
        assert_eq!(decoder.received_bins.get(&17), Some(&vec![1, 2, 3, 4]));
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
        decoder.decoded_prefix_payloads = vec![vec![0]; 537];

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
        assert_eq!(decoder.received_bins.get(&1116), Some(&vec![0b1100_0000]));
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
        assert_eq!(decoder.decoded_prefix_payloads, vec![vec![1, 2, 3, 4]]);
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
        assert_eq!(decoder.decoded_prefix_payloads, sources);
        assert!(decoder
            .received_bins
            .values()
            .all(|payload| payload.iter().all(|&byte| byte == 0)));
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
