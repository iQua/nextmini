use wansim::metrics::MAILBOX_CAPACITY;
use wansim::scenario::{
    BufferBudget, ChildOrder, ReceiverAdmissionPolicy, ReceiverServiceRate, W2Scenario, run_w2,
};

fn scenario(policy: ReceiverAdmissionPolicy) -> W2Scenario {
    W2Scenario::screening(
        policy,
        ReceiverServiceRate::Tenth,
        BufferBudget::QuarterBdp,
        3,
        2,
        ChildOrder::SlowFirst,
        17,
    )
}

#[test]
fn hybrid_drop_remains_after_transport_delivery_and_creates_deficits() {
    let outcome = run_w2(&scenario(ReceiverAdmissionPolicy::HybridDrop)).expect("hybrid run");
    assert!(outcome.application_drops > 0);
    assert_eq!(outcome.blocking_wait_events, 0);
    let drop = outcome
        .records
        .iter()
        .find(|record| record.event == "data_inbox_drop_after_tcp_ack")
        .expect("forced hybrid drop");
    let delivery = outcome
        .records
        .iter()
        .find(|record| {
            record.component == drop.component
                && record.event == "runtime_command_enqueue_data"
                && record.flow_id == drop.flow_id
                && record.sequence == drop.sequence
        })
        .expect("transport-delivered frame enters the runtime mailbox first");
    assert!(delivery.time_ns <= drop.time_ns);
    assert!(drop.value > 0, "drop records the cumulative TCP ACK point");
    assert!(outcome.total_emissions > 512);
}

#[test]
fn naive_blocking_fills_and_blocks_the_reliable_chain_without_application_loss() {
    let outcome = run_w2(&scenario(ReceiverAdmissionPolicy::NaiveBlocking)).expect("blocking run");
    assert_eq!(outcome.application_drops, 0);
    assert!(outcome.blocking_wait_events > 0);
    assert_eq!(outcome.isolated_credit_deferrals, 0);
    assert!(
        outcome.records.iter().any(|record| {
            record.event == "child_admission_blocked" && record.component.starts_with("w2_t")
        }),
        "receiver pressure must propagate into a relay child queue"
    );
}

#[test]
fn isolated_credit_replays_every_deferred_frame_without_blocking_healthy_receivers() {
    let slow = scenario(ReceiverAdmissionPolicy::IsolatedCredit);
    let mut healthy = slow.clone();
    healthy.scenario_id = "w2-isolated-all-healthy".to_owned();
    healthy.slow_receiver_count = 0;
    let slow = run_w2(&slow).expect("isolated straggler run");
    let healthy = run_w2(&healthy).expect("isolated baseline run");

    assert_eq!(slow.application_drops, 0);
    assert_eq!(slow.blocking_wait_events, 0);
    assert!(slow.isolated_credit_deferrals > 0);
    assert!(slow.isolated_credit_replays > 0);
    assert_eq!(
        &slow.completion_times_ns[1..],
        &healthy.completion_times_ns[1..]
    );
}

#[test]
fn no_straggler_sanity_cell_is_policy_invariant() {
    let mut outcomes = Vec::new();
    for policy in ReceiverAdmissionPolicy::ALL {
        let mut cell = scenario(policy);
        cell.scenario_id = format!("w2-no-straggler-{}", policy.name());
        cell.slow_receiver_count = 0;
        cell.buffer_budget = BufferBudget::FourBdp;
        cell.healthy_decoder_sink_service_ns = 1;
        outcomes.push(run_w2(&cell).expect("no-straggler policy run"));
    }
    for outcome in &outcomes {
        assert_eq!(outcome.application_drops, 0);
        assert_eq!(outcome.blocking_wait_events, 0);
        assert_eq!(outcome.isolated_credit_deferrals, 0);
    }
    assert_eq!(
        outcomes[0].completion_times_ns,
        outcomes[1].completion_times_ns
    );
    assert_eq!(
        outcomes[0].completion_times_ns,
        outcomes[2].completion_times_ns
    );
    assert_eq!(outcomes[0].total_emissions, outcomes[1].total_emissions);
    assert_eq!(outcomes[0].total_emissions, outcomes[2].total_emissions);
}

#[test]
fn larger_k_shrinks_the_ack_flight_tail_fraction() {
    let mut small = scenario(ReceiverAdmissionPolicy::NaiveBlocking);
    small.scenario_id = "w2-tail-k64".to_owned();
    small.source_symbols = 64;
    small.slow_receiver_count = 0;
    let mut large = small.clone();
    large.scenario_id = "w2-tail-k512".to_owned();
    large.source_symbols = 512;
    let small = run_w2(&small).expect("K=64 run");
    let large = run_w2(&large).expect("K=512 run");
    let small_ppm = small.post_completion_tail_emissions * 1_000_000 / small.total_emissions;
    let large_ppm = large.post_completion_tail_emissions * 1_000_000 / large.total_emissions;
    assert!(large_ppm < small_ppm, "small={small_ppm} large={large_ppm}");
}

#[test]
fn w2_is_byte_reproducible() {
    let cell = scenario(ReceiverAdmissionPolicy::IsolatedCredit);
    let first = run_w2(&cell).expect("first run");
    let second = run_w2(&cell).expect("second run");
    assert_eq!(first.csv.as_bytes(), second.csv.as_bytes());
}

#[test]
fn w2_mailbox_plumbing_is_nonbinding_at_eight_receivers() {
    let cell = W2Scenario::screening(
        ReceiverAdmissionPolicy::IsolatedCredit,
        ReceiverServiceRate::Tenth,
        BufferBudget::FourBdp,
        8,
        4,
        ChildOrder::SlowLast,
        23,
    );
    let outcome = run_w2(&cell).expect("eight-receiver run");
    let maximum = outcome
        .mailbox_high_water
        .values()
        .copied()
        .max()
        .unwrap_or(0);
    assert!(maximum < MAILBOX_CAPACITY, "mailbox high-water {maximum}");
}
