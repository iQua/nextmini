use std::collections::BTreeSet;

use mettle::test_support::{
    edge_bin_ids_with_terminal_source_count, source_window_with_terminal_source_count,
    terminal_departure_end_exclusive, tle_bin_id,
};
use mettle::{MettleParams, OverheadRatio};

const GRAPH_SEEDS: [u64; 4] = [0, 1, 0x1234_5678_9ABC_DEF0, u64::MAX];
const STREAM_PREFIX_SOURCE_COUNT: u64 = 10_000;
const WINDOW_CHECK_SOURCE_COUNT: u64 = 1_500;

fn paper_params() -> MettleParams {
    MettleParams::new(OverheadRatio::new(1, 20).expect("valid overhead"))
}

fn tle_formula_bin_id(overhead: OverheadRatio, source_id: u64) -> u128 {
    let numerator = u128::from(overhead.numerator()) + u128::from(overhead.denominator());
    u128::from(source_id) * numerator / u128::from(overhead.denominator())
}

#[test]
fn paper_kernel_constants_match_profile() {
    assert_eq!(MettleParams::EDGE_COUNT, 4);
    assert_eq!(MettleParams::COUPLING_WINDOW, 600);
    assert_eq!(MettleParams::NON_TLE_PROFILE, [(1, 2), (1, 4), (1, 8)]);
}

#[test]
fn tle_bin_ids_are_deterministic_and_injective_over_stream_prefix() {
    let params = paper_params();
    let mut seen_bin_ids = BTreeSet::new();
    let mut previous_bin_id = None;

    for source_id in 0..STREAM_PREFIX_SOURCE_COUNT {
        let first = tle_bin_id(params, source_id);
        let second = tle_bin_id(params, source_id);

        assert_eq!(first, second, "source_id={source_id}");
        assert!(
            seen_bin_ids.insert(first),
            "duplicate TLE bin_id={first} for source_id={source_id}"
        );
        if let Some(previous_bin_id) = previous_bin_id {
            assert!(
                previous_bin_id < first,
                "TLE bin ids must be strictly increasing: previous={previous_bin_id} current={first}"
            );
        }
        previous_bin_id = Some(first);
    }
}

#[test]
fn tle_bin_ids_follow_paper_formula_at_denominator_boundaries() {
    let overhead = OverheadRatio::new(1, 20).expect("valid overhead");
    let params = MettleParams::new(overhead);

    for source_id in [0, 1, 19, 20, 21, 39, 40, 41, 999, 1_000] {
        assert_eq!(
            tle_bin_id(params, source_id),
            tle_formula_bin_id(overhead, source_id),
            "source_id={source_id}"
        );
    }
}

#[test]
fn non_tle_edge_bins_stay_inside_half_open_coupling_window() {
    for params in [
        paper_params(),
        MettleParams::new(OverheadRatio::new(1, 7).expect("valid overhead")),
    ] {
        for seed in GRAPH_SEEDS {
            for source_id in 0..WINDOW_CHECK_SOURCE_COUNT {
                let window = source_window_with_terminal_source_count(params, source_id, None);
                let edge_bin_ids =
                    edge_bin_ids_with_terminal_source_count(params, source_id, seed, None);

                assert_eq!(edge_bin_ids[0], window.start(), "source_id={source_id}");
                for bin_id in edge_bin_ids.into_iter().skip(1) {
                    assert!(
                        window.contains(bin_id),
                        "source_id={source_id} seed={seed} bin_id={bin_id} window=[{}, {})",
                        window.start(),
                        window.end_exclusive()
                    );
                }
            }
        }
    }
}

#[test]
fn terminal_tail_compression_changes_only_tail_windows() {
    let params = paper_params();
    let terminal_source_count = MettleParams::COUPLING_WINDOW * 3 + 17;
    let tail_start = terminal_source_count - MettleParams::COUPLING_WINDOW;

    for source_id in 0..tail_start {
        assert_eq!(
            source_window_with_terminal_source_count(
                params,
                source_id,
                Some(terminal_source_count)
            ),
            source_window_with_terminal_source_count(params, source_id, None),
            "pre-tail source_id={source_id} should not be compressed"
        );
    }

    let mut changed_tail_windows = 0usize;
    for source_id in tail_start..terminal_source_count {
        let uncompressed = source_window_with_terminal_source_count(params, source_id, None);
        let compressed = source_window_with_terminal_source_count(
            params,
            source_id,
            Some(terminal_source_count),
        );

        assert_eq!(
            compressed.start(),
            uncompressed.start(),
            "source_id={source_id}"
        );
        assert!(
            compressed.end_exclusive() <= uncompressed.end_exclusive(),
            "source_id={source_id} compressed window must not expand"
        );
        changed_tail_windows += usize::from(compressed != uncompressed);
    }

    assert!(changed_tail_windows > 0);
}

#[test]
fn terminal_departure_end_matches_max_source_window_end() {
    let params = paper_params();
    let terminal_source_count = MettleParams::COUPLING_WINDOW * 3 + 17;

    let expected = (0..terminal_source_count)
        .map(|source_id| {
            source_window_with_terminal_source_count(
                params,
                source_id,
                Some(terminal_source_count),
            )
            .end_exclusive()
        })
        .max()
        .expect("non-empty terminal source range");

    assert_eq!(
        terminal_departure_end_exclusive(params, terminal_source_count),
        expected
    );
}

#[test]
fn seeded_graph_generation_is_deterministic() {
    let params = paper_params();
    let terminal_source_count = MettleParams::COUPLING_WINDOW * 2 + 31;

    for seed in GRAPH_SEEDS {
        let first = (0..terminal_source_count)
            .map(|source_id| {
                edge_bin_ids_with_terminal_source_count(
                    params,
                    source_id,
                    seed,
                    Some(terminal_source_count),
                )
            })
            .collect::<Vec<_>>();
        let second = (0..terminal_source_count)
            .map(|source_id| {
                edge_bin_ids_with_terminal_source_count(
                    params,
                    source_id,
                    seed,
                    Some(terminal_source_count),
                )
            })
            .collect::<Vec<_>>();

        assert_eq!(first, second, "seed={seed}");
    }
}
