use mettle::block::{BlockParams, DecodeError, Decoder, Encoder};

fn source_block(k: usize, symbol_size: usize) -> Vec<u8> {
    (0..k)
        .flat_map(|source_index| {
            (0..symbol_size)
                .map(move |byte_index| ((source_index * 37 + byte_index * 11 + 0x41) % 251) as u8)
        })
        .collect()
}

fn expected_symbols(source_block: &[u8], symbol_size: usize) -> Vec<Vec<u8>> {
    source_block
        .chunks_exact(symbol_size)
        .map(<[u8]>::to_vec)
        .collect()
}

fn decode_with_missing_sources(
    params: BlockParams,
    source_block: &[u8],
    missing_sources: &[usize],
) -> Vec<Vec<u8>> {
    let encoder = Encoder::from_block(params, source_block).expect("valid block encoder");
    let decoder = Decoder::from_block(params);
    let source_symbols = expected_symbols(source_block, params.symbol_size);
    let missing_sources = missing_sources.to_vec();
    let mut symbols = (0..params.source_symbols)
        .filter(|source_index| !missing_sources.contains(source_index))
        .map(|source_index| {
            decoder.source_symbol(source_index, source_symbols[source_index].clone())
        })
        .collect::<Vec<_>>();

    let metadata = params.metadata().expect("metadata");
    for repair_index in 0..metadata.repair_symbol_count() {
        symbols.push(
            decoder.repair_symbol(
                repair_index,
                encoder
                    .repair_symbol(repair_index)
                    .expect("repair symbol exists"),
            ),
        );
    }

    decoder
        .decode(&symbols)
        .expect("finite repair stream recovers missing sources")
        .source_symbols
}

#[test]
fn missing_sources_are_recovered_by_repair_symbols() {
    let params = BlockParams::new(96, 8, 0x1234_5678);
    let source_block = source_block(params.source_symbols, params.symbol_size);
    let decoded = decode_with_missing_sources(params, &source_block, &[7, 31, 64]);

    assert_eq!(decoded, expected_symbols(&source_block, params.symbol_size));
}

#[test]
fn non_contiguous_repair_arrival_converges_with_future_repairs() {
    let params = BlockParams::new(128, 8, 0xA55A);
    let source_block = source_block(params.source_symbols, params.symbol_size);
    let encoder = Encoder::from_block(params, &source_block).expect("valid block encoder");
    let decoder = Decoder::from_block(params);
    let source_symbols = expected_symbols(&source_block, params.symbol_size);
    let metadata = params.metadata().expect("metadata");
    let missing_source = 59usize;
    let mut symbols = (0..params.source_symbols)
        .filter(|&source_index| source_index != missing_source)
        .map(|source_index| {
            decoder.source_symbol(source_index, source_symbols[source_index].clone())
        })
        .collect::<Vec<_>>();

    let mut later_repair_index = None;
    for repair_index in 1..metadata.repair_symbol_count() {
        let mut trial = symbols.clone();
        trial.push(
            decoder.repair_symbol(
                repair_index,
                encoder
                    .repair_symbol(repair_index)
                    .expect("repair symbol exists"),
            ),
        );
        if matches!(
            decoder.decode(&trial),
            Err(DecodeError::InsufficientSymbols)
        ) {
            later_repair_index = Some(repair_index);
            break;
        }
    }
    let later_repair_index = later_repair_index.expect("later repair that does not decode alone");

    symbols.push(
        decoder.repair_symbol(
            later_repair_index,
            encoder
                .repair_symbol(later_repair_index)
                .expect("repair symbol exists"),
        ),
    );
    assert!(matches!(
        decoder.decode(&symbols),
        Err(DecodeError::InsufficientSymbols)
    ));

    for repair_index in (later_repair_index + 1)..metadata.repair_symbol_count() {
        symbols.push(
            decoder.repair_symbol(
                repair_index,
                encoder
                    .repair_symbol(repair_index)
                    .expect("repair symbol exists"),
            ),
        );
    }

    let output = decoder
        .decode(&symbols)
        .expect("future repairs after the gap converge");
    assert_eq!(
        output.source_symbols,
        expected_symbols(&source_block, params.symbol_size)
    );
}

#[test]
fn metadata_only_large_k_maps_repair_indexes_without_payload_pressure() {
    let params = BlockParams::new(4096, 1, 0xCAFE_BABE);
    let metadata = params.metadata().expect("metadata only");

    assert!(metadata.repair_symbol_count() > 128);

    let first = metadata.repair_bin_id(0).expect("first repair bin");
    let middle = metadata
        .repair_bin_id(metadata.repair_symbol_count() / 2)
        .expect("middle repair bin");
    let last = metadata
        .repair_bin_id(metadata.repair_symbol_count() - 1)
        .expect("last repair bin");

    assert!(first < middle);
    assert!(middle < last);

    let deficit = metadata
        .estimate_repair_deficit(0..4090, [10, 12, 17])
        .expect("deficit estimate");
    assert!(deficit.is_some());
}

#[test]
fn fixed_k_padded_block_boundary_is_not_trimmed() {
    let params = BlockParams::new(4, 4, 0);
    let source_block = [
        vec![1, 2, 3, 4],
        vec![5, 6, 7, 8],
        vec![9, 10, 0, 0],
        vec![0, 0, 0, 0],
    ]
    .concat();
    let decoder = Decoder::from_block(params);
    let symbols = expected_symbols(&source_block, params.symbol_size)
        .into_iter()
        .enumerate()
        .map(|(source_index, payload)| decoder.source_symbol(source_index, payload))
        .collect::<Vec<_>>();

    let output = decoder.decode(&symbols).expect("decode padded block");

    assert_eq!(output.source_symbols.len(), params.source_symbols);
    assert_eq!(output.source_symbols.concat(), source_block);
}

#[test]
fn encoder_rejects_non_exact_source_block_length() {
    let params = BlockParams::new(4, 4, 0);
    let source_block = vec![0; params.source_symbols * params.symbol_size - 1];

    assert!(Encoder::from_block(params, &source_block).is_err());
}
