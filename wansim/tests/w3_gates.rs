use wansim::protocol::ProtocolKind;
use wansim::scenario::{
    BufferBudget, ChildOrder, ReceiverAdmissionPolicy, ReceiverServiceRate, RegistrationOrder,
    W1Scenario, W2ControlAsymmetry, W2Scenario, W2SharedLeafBottleneck, run_w1, run_w2,
};

#[test]
fn coupled_carousel_smoke_completes_over_explicit_tcp_background() {
    let scenario = W1Scenario::w3_coupling(ProtocolKind::PooledCarousel, 50, None, 7);
    let outcome = run_w1(&scenario).expect("coupled W3 run");
    assert_eq!(outcome.completion_times_ns.len(), 3);
    assert!(outcome.sender_completion_ns.is_some());
    assert!(outcome.records.iter().any(|record| {
        record.component == "w3_coupled_data_forward" && record.event == "coupled_path_exit"
    }));
    assert!(
        outcome
            .records
            .iter()
            .any(|record| { record.event == "background_bytes_delivered" })
    );
}

#[test]
fn best_single_tree_uses_the_existing_second_tcp_connection_for_flow_matching() {
    let scenario = W1Scenario::w3_coupling(ProtocolKind::PerStripeFec, 100, Some(0), 3);
    let outcome = run_w1(&scenario).expect("matched single-tree run");
    let matched = outcome
        .records
        .iter()
        .filter(|record| record.event == "flow_count_match_frame_emitted")
        .count();
    assert!(matched > 0);
    assert!(
        outcome
            .per_tree_emissions
            .into_iter()
            .all(|count| count > 0)
    );
    assert_eq!(
        outcome.total_emissions,
        outcome.per_tree_emissions.iter().sum::<usize>()
    );
}

#[test]
fn coupled_w3_trace_is_repeatable_and_registration_order_invariant() {
    let mut forward = W1Scenario::w3_coupling(ProtocolKind::PooledCarousel, 25, None, 19);
    forward.source_symbols = 128;
    forward.scenario_id = "w3-registration-metamorphic".to_owned();
    let mut reverse = forward.clone();
    reverse.registration_order = RegistrationOrder::Reverse;
    let first = run_w1(&forward).expect("first forward run");
    let second = run_w1(&forward).expect("second forward run");
    let reversed = run_w1(&reverse).expect("reverse-registration run");
    assert_eq!(first.csv.as_bytes(), second.csv.as_bytes());
    assert_eq!(first.csv.as_bytes(), reversed.csv.as_bytes());
}

#[test]
fn eight_receiver_control_incast_and_reverse_bursts_use_the_shared_tcp_path() {
    let mut scenario = W2Scenario::screening(
        ReceiverAdmissionPolicy::HybridDrop,
        ReceiverServiceRate::One,
        BufferBudget::FourBdp,
        8,
        4,
        ChildOrder::SlowFirst,
        9,
    );
    scenario.scenario_id = "w3-control-incast-smoke".to_owned();
    scenario.slow_receiver_count = 0;
    scenario.w3_control_asymmetry = Some(W2ControlAsymmetry {
        reverse_rate_bps: 20_000_000,
        reverse_propagation_ns: 16_000_000,
        reverse_background_bursts: true,
    });
    let outcome = run_w2(&scenario).expect("control-asymmetry run");
    assert_eq!(outcome.completion_times_ns.len(), 8);
    assert!(outcome.records.iter().any(|record| {
        record.component == "w3_control_reverse_shared" && record.event == "coupled_path_exit"
    }));
    assert!(outcome.records.iter().any(|record| {
        record.component == "w3_control_reverse_onoff"
            && record.event == "background_bytes_delivered"
    }));
}

#[test]
fn harsh_shared_leaf_slice_runs_both_w2_reconsideration_policies() {
    for policy in [
        ReceiverAdmissionPolicy::HybridDrop,
        ReceiverAdmissionPolicy::IsolatedCredit,
    ] {
        let mut scenario = W2Scenario::screening(
            policy,
            ReceiverServiceRate::Tenth,
            BufferBudget::QuarterBdp,
            8,
            2,
            ChildOrder::SlowFirst,
            5,
        );
        scenario.scenario_id = format!("w3-shared-leaf-{}", policy.name());
        scenario.source_symbols = 64;
        scenario.w3_shared_leaf_bottleneck = Some(W2SharedLeafBottleneck { receivers: [0, 1] });
        let outcome = run_w2(&scenario).expect("shared-leaf run");
        assert_eq!(outcome.completion_times_ns.len(), 8);
        assert!(outcome.records.iter().any(|record| {
            record.component.starts_with("w3_shared_leaf_") && record.event == "coupled_path_exit"
        }));
    }
}
