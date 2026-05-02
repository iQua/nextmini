use mettle::OverheadRatio;
use mettle::block::BlockParams;

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
fn explicit_overhead_changes_finite_repair_budget() {
    let paper = BlockParams::new(4096, 1, 0xCAFE_BABE);
    let higher = BlockParams::with_overhead(
        4096,
        1,
        0xCAFE_BABE,
        OverheadRatio::new(1, 4).expect("valid overhead"),
    );

    let paper_repairs = paper
        .metadata()
        .expect("paper metadata")
        .repair_symbol_count();
    let higher_repairs = higher
        .metadata()
        .expect("higher-overhead metadata")
        .repair_symbol_count();

    assert!(higher_repairs > paper_repairs);
}

#[test]
fn small_k_prefix_loss_can_require_a_large_repair_burst() {
    let params = BlockParams::new(64, 1, 0x1234_5678);
    let metadata = params.metadata().expect("metadata");
    let present_sources = 1..params.source_symbols;

    let deficit = metadata
        .estimate_repair_deficit(present_sources, [])
        .expect("deficit estimate")
        .expect("finite repair stream should recover this erasure pattern");

    assert!(
        deficit > 100,
        "K=64 is below the paper coupling window and can require a large repair burst"
    );
    assert!(deficit < metadata.repair_symbol_count());
}
