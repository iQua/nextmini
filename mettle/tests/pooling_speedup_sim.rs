//! Conformance tests for the model-level pooled-FEC speedup simulator.

#[path = "support/pooling_speedup_model.rs"]
mod pooling_speedup_model;

use pooling_speedup_model::{LossModel, Protocol, RateProfile, simulate_protocols};

fn metrics(
    outcomes: &[(Protocol, pooling_speedup_model::TrialMetrics)],
    protocol: Protocol,
) -> &pooling_speedup_model::TrialMetrics {
    &outcomes
        .iter()
        .find(|(candidate, _)| *candidate == protocol)
        .expect("protocol result must exist")
        .1
}

#[test]
fn homogeneous_no_loss_rate_proportional_striping_matches_pooling() {
    for trees in [2usize, 4, 8] {
        let outcomes = simulate_protocols(
            trees,
            RateProfile::Static { ratio: 1 },
            LossModel::None,
            3,
            64,
            0xA11C_E001,
        )
        .expect("homogeneous deterministic cell");
        assert_eq!(
            metrics(&outcomes, Protocol::RateProportionalStriping).barrier_completion_tick,
            metrics(&outcomes, Protocol::PooledCarousel).barrier_completion_tick,
        );
        assert_eq!(
            metrics(&outcomes, Protocol::RateProportionalStriping).total_emissions,
            pooling_speedup_model::SOURCE_DOF.into(),
        );
    }
}

#[test]
fn coupled_seed_reproduces_every_protocol_metric_exactly() {
    let first = simulate_protocols(
        4,
        RateProfile::BoundedRandomWalk,
        LossModel::GilbertElliottShort,
        8,
        64,
        0xA11C_E002,
    )
    .expect("first deterministic run");
    let second = simulate_protocols(
        4,
        RateProfile::BoundedRandomWalk,
        LossModel::GilbertElliottShort,
        8,
        64,
        0xA11C_E002,
    )
    .expect("second deterministic run");
    assert_eq!(first, second);
}

#[test]
fn ownership_waste_appears_under_static_misallocation() {
    let outcomes = simulate_protocols(
        8,
        RateProfile::Static { ratio: 8 },
        LossModel::None,
        1,
        64,
        0xA11C_E003,
    )
    .expect("heterogeneous deterministic cell");
    let stripe = metrics(&outcomes, Protocol::PerStripeFec);
    let pooled = metrics(&outcomes, Protocol::PooledCarousel);
    assert!(stripe.ownership_wasted_deliveries > 0);
    assert!(stripe.barrier_completion_tick > pooled.barrier_completion_tick);
}

#[test]
fn carousel_feedback_tail_grows_with_rtt_but_not_local_completion() {
    let short = simulate_protocols(
        4,
        RateProfile::Static { ratio: 2 },
        LossModel::BecTwoPercent,
        3,
        8,
        0xA11C_E004,
    )
    .expect("short RTT cell");
    let long = simulate_protocols(
        4,
        RateProfile::Static { ratio: 2 },
        LossModel::BecTwoPercent,
        3,
        512,
        0xA11C_E004,
    )
    .expect("long RTT cell");
    let short = metrics(&short, Protocol::PooledCarousel);
    let long = metrics(&long, Protocol::PooledCarousel);
    assert_eq!(short.barrier_completion_tick, long.barrier_completion_tick);
    assert!(long.post_completion_tail_deliveries > short.post_completion_tail_deliveries);
    assert!(long.post_completion_tail_emissions > short.post_completion_tail_emissions);
    assert!(long.total_emissions > short.total_emissions);
}

#[test]
fn integer_accounting_reconciles_for_every_protocol() {
    let outcomes = simulate_protocols(
        8,
        RateProfile::BoundedRandomWalk,
        LossModel::BecHalfPercent,
        8,
        512,
        0xA11C_E006,
    )
    .expect("accounting cell");
    for (protocol, metrics) in outcomes {
        assert_eq!(metrics.receiver_completion_ticks.len(), 8);
        assert_eq!(
            metrics.barrier_completion_tick,
            *metrics
                .receiver_completion_ticks
                .iter()
                .max()
                .expect("at least one receiver"),
        );
        assert!(metrics.sender_stop_tick >= metrics.barrier_completion_tick);
        assert_eq!(
            metrics.total_emissions,
            metrics.tree_emissions.iter().sum::<u64>(),
        );
        assert!(
            metrics
                .tree_emissions
                .iter()
                .zip(&metrics.tree_available_opportunities)
                .all(|(used, available)| used <= available),
        );
        if protocol != Protocol::PooledCarousel {
            assert_eq!(metrics.post_completion_tail_deliveries, 0);
            assert_eq!(metrics.post_completion_tail_emissions, 0);
        }
        if matches!(protocol, Protocol::PooledRounds | Protocol::PooledCarousel) {
            assert_eq!(metrics.ownership_wasted_deliveries, 0);
        }
    }
}

#[test]
fn burst_profiles_reuse_stage_three_stationary_loss_definition() {
    assert_eq!(
        LossModel::GilbertElliottShort.stationary_erasure(),
        LossModel::GilbertElliottLong.stationary_erasure(),
    );
    assert_eq!(
        LossModel::GilbertElliottShort.stationary_erasure(),
        pooling_speedup_model::Rate::new_const(11, 1_010),
    );
}

#[test]
fn coupled_trace_has_a_stable_committed_metric_vector() {
    let outcomes = simulate_protocols(
        4,
        RateProfile::PeriodicAlternation,
        LossModel::GilbertElliottLong,
        3,
        64,
        0xA11C_E005,
    )
    .expect("stable-vector cell");
    let actual = outcomes
        .iter()
        .map(|(protocol, metrics)| {
            (
                *protocol,
                metrics.receiver_completion_ticks.clone(),
                metrics.barrier_completion_tick,
                metrics.sender_stop_tick,
                metrics.total_emissions,
                metrics.ownership_wasted_deliveries,
                metrics.post_completion_tail_deliveries,
                metrics.post_completion_tail_emissions,
                metrics.tree_emissions.clone(),
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(
        actual,
        vec![
            (
                Protocol::EqualSplitStriping,
                vec![6224, 6392, 7458],
                7458,
                7522,
                8884,
                1258,
                0,
                0,
                vec![2314, 2467, 2053, 2050],
            ),
            (
                Protocol::RateProportionalStriping,
                vec![6224, 6392, 7458],
                7458,
                7522,
                8884,
                1258,
                0,
                0,
                vec![2314, 2467, 2053, 2050],
            ),
            (
                Protocol::PerStripeFec,
                vec![6161, 6082, 7192],
                7192,
                7193,
                9887,
                4267,
                0,
                0,
                vec![2488, 2467, 2466, 2466],
            ),
            (
                Protocol::PooledRounds,
                vec![6087, 6049, 6523],
                6523,
                6587,
                8881,
                0,
                0,
                0,
                vec![2376, 2259, 2151, 2095],
            ),
            (
                Protocol::PooledCarousel,
                vec![6024, 5986, 6460],
                6460,
                6524,
                8968,
                0,
                1510,
                87,
                vec![2383, 2267, 2159, 2159],
            ),
        ]
    );
}
