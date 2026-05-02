use std::num::NonZeroUsize;

use mettle::block::BlockParams;

fn source_block(k: usize, symbol_size: usize) -> Vec<u8> {
    (0..k)
        .flat_map(|source_index| {
            (0..symbol_size)
                .map(move |byte_index| ((source_index * 37 + byte_index * 11 + 0x41) % 251) as u8)
        })
        .collect()
}

fn expected_sources(source_block: &[u8], symbol_size: usize) -> Vec<(u64, Vec<u8>)> {
    source_block
        .chunks_exact(symbol_size)
        .enumerate()
        .map(|(source_id, payload)| (source_id as u64, payload.to_vec()))
        .collect()
}

fn encoded_bins(params: BlockParams, source_block: &[u8]) -> Vec<(u128, Vec<u8>)> {
    assert_eq!(
        source_block.len(),
        params.source_symbols * params.symbol_size
    );
    let source_symbol_bytes = NonZeroUsize::new(params.symbol_size).expect("non-zero symbol size");
    let mut encoder = mettle::stream::Encoder::new_terminated(
        params.mettle_params(),
        source_symbol_bytes,
        params.seed,
        params.source_symbols as u64,
    );
    let mut bins = Vec::new();

    for source_payload in source_block.chunks_exact(params.symbol_size) {
        bins.extend(
            encoder
                .push_source(source_payload)
                .into_iter()
                .map(mettle::stream::EncodedBin::into_parts),
        );
    }
    bins.extend(
        encoder
            .finish()
            .into_iter()
            .map(mettle::stream::EncodedBin::into_parts),
    );
    bins
}

fn decode_stream(
    params: BlockParams,
    bins: impl IntoIterator<Item = (u128, Vec<u8>)>,
) -> Vec<(u64, Vec<u8>)> {
    let source_symbol_bytes = NonZeroUsize::new(params.symbol_size).expect("non-zero symbol size");
    let mut decoder = mettle::stream::Decoder::new_terminated(
        params.mettle_params(),
        source_symbol_bytes,
        params.seed,
        params.source_symbols as u64,
    );
    let mut decoded = Vec::new();

    for (bin_id, payload) in bins {
        decoded.extend(
            decoder
                .push_bin(bin_id, payload)
                .into_iter()
                .map(mettle::stream::DecodedSource::into_parts),
        );
    }

    assert_eq!(decoder.next_source_id(), decoded.len() as u64);
    decoded
}

#[test]
fn streaming_decoder_releases_sources_incrementally_without_replay() {
    let params = BlockParams::new(128, 8, 0xBEEF);
    let source_block = source_block(params.source_symbols, params.symbol_size);
    let decoded = decode_stream(params, encoded_bins(params, &source_block));

    assert_eq!(decoded, expected_sources(&source_block, params.symbol_size));
}

#[test]
fn missing_coded_bins_are_recovered_by_later_bins() {
    let params = BlockParams::new(96, 8, 0x1234_5678);
    let source_block = source_block(params.source_symbols, params.symbol_size);
    let missing_bins = [7u128, 31, 64];
    let bins = encoded_bins(params, &source_block)
        .into_iter()
        .filter(|(bin_id, _)| !missing_bins.contains(bin_id));
    let decoded = decode_stream(params, bins);

    assert_eq!(decoded, expected_sources(&source_block, params.symbol_size));
}

#[test]
fn initial_prefix_loss_converges_with_future_coded_bins() {
    let params = BlockParams::new(128, 8, 0xA55A);
    let source_block = source_block(params.source_symbols, params.symbol_size);
    let metadata = params.metadata().expect("metadata");
    let initial_symbol_count = metadata.initial_symbol_count() as u128;
    let mut initial_only_decoder = mettle::stream::Decoder::new_terminated(
        params.mettle_params(),
        NonZeroUsize::new(params.symbol_size).expect("non-zero symbol size"),
        params.seed,
        params.source_symbols as u64,
    );
    let bins = encoded_bins(params, &source_block);

    for (bin_id, payload) in bins
        .iter()
        .filter(|(bin_id, _)| *bin_id < initial_symbol_count && bin_id % 97 != 0)
    {
        let _ = initial_only_decoder.push_bin(*bin_id, payload.clone());
    }
    assert!(
        initial_only_decoder.next_source_id() < params.source_symbols as u64,
        "dropped initial bins should stall before full source recovery"
    );

    let decoded = decode_stream(
        params,
        bins.into_iter()
            .filter(|(bin_id, _)| *bin_id >= initial_symbol_count || bin_id % 97 != 0),
    );
    assert_eq!(decoded, expected_sources(&source_block, params.symbol_size));
}

#[test]
fn initial_symbols_are_coded_bins_not_raw_sources() {
    let params = BlockParams::new(1024, 1, 0);
    let source_block = source_block(params.source_symbols, params.symbol_size);
    let source_symbols = expected_sources(&source_block, params.symbol_size);
    let bins = encoded_bins(params, &source_block);

    assert!(
        (0..params.source_symbols)
            .any(|symbol_id| { bins[symbol_id].1 != source_symbols[symbol_id].1 }),
        "paper-native METTLE should expose coded bins, not raw systematic source observations"
    );

    let mut rng = XorShift64::new(0xC0DE_CAFE_F00D_BAAD);
    let decoded = decode_stream(params, bins.into_iter().filter(|_| rng.delivers(0.005)));
    assert_eq!(decoded, source_symbols);
}

struct XorShift64 {
    state: u64,
}

impl XorShift64 {
    fn new(seed: u64) -> Self {
        Self { state: seed | 1 }
    }

    fn delivers(&mut self, loss_rate: f64) -> bool {
        self.next_unit_f64() >= loss_rate
    }

    fn next_unit_f64(&mut self) -> f64 {
        let mut x = self.state;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.state = x;
        (x as f64) / (u64::MAX as f64)
    }
}
