use wansim::determinism::{Decision, DeferredDeadline};
use wansim::metrics::MAILBOX_CAPACITY;
use wansim::protocol::{BlockAck, CarouselSender, CarouselTiming, ProtocolKind};
use wansim::scenario::{RegistrationOrder, W1RateProfile, W1Scenario, run_w1};

fn carousel_timing() -> CarouselTiming {
    CarouselTiming {
        ack_debounce_ns: 5,
        ack_heartbeat_ns: 20,
        ack_probe_interval_ns: 10,
        peer_silence_timeout_ns: 40,
        peer_stall_timeout_ns: 100,
        receiver_passive_window_ns: 200,
        session_complete_repeats: 3,
        session_complete_interval_ns: 2,
    }
}

fn small_scenario(protocol: ProtocolKind, receivers: usize) -> W1Scenario {
    let mut scenario =
        W1Scenario::experiment0(protocol, W1RateProfile::CrossedHeterogeneous, receivers, 7);
    scenario.scenario_id = format!("w1-gate-{}-r{receivers}", protocol.name());
    scenario.source_symbols = 8;
    scenario.runtime_command_capacity_frames = 256;
    scenario.receiver_data_inbox_capacity_frames = 256;
    scenario.simulation_end_ns = 500_000_000;
    scenario
}

#[test]
fn ack_stamped_at_emission_deadline_wins_before_submission() {
    let mut sender = CarouselSender::new(1, [1], 0, carousel_timing()).expect("valid endpoint");
    let mut deadline = DeferredDeadline::default();
    let (generation, decision_at) = deadline.arm(10);
    sender
        .on_block_ack(1, &BlockAck::complete(1), 10)
        .expect("valid cumulative ack");
    deadline.observe(10);

    assert_eq!(deadline.decide(generation, decision_at), Decision::EventWon);
    assert_eq!(sender.next_data_emission(), None);
}

#[test]
fn carousel_control_uses_per_peer_tcp_and_repeats_completion() {
    let outcome = run_w1(&small_scenario(ProtocolKind::PooledCarousel, 1))
        .expect("carousel scenario completes");
    let control_flow_ids: std::collections::BTreeSet<_> = outcome
        .records
        .iter()
        .filter(|record| record.component.starts_with("w1_control_receiver1"))
        .map(|record| record.flow_id)
        .filter(|flow_id| *flow_id != 0)
        .collect();
    let repeats = outcome
        .records
        .iter()
        .filter(|record| record.event == "session_complete_submitted")
        .count();

    assert!(control_flow_ids.contains(&40_001));
    assert!(control_flow_ids.contains(&40_002));
    assert_eq!(repeats, 3);
    assert_eq!(outcome.application_drops, 0);
    assert_eq!(outcome.link_drops, 0);
}

#[test]
fn separate_control_scope_exposes_source_done_overtake_without_any_drop() {
    let outcome =
        run_w1(&small_scenario(ProtocolKind::PooledRounds, 3)).expect("rounds scenario completes");
    assert_eq!(outcome.application_drops, 0);
    assert_eq!(outcome.link_drops, 0);
    assert!(outcome.positive_round_deficits > 0);
    assert!(outcome.total_emissions > 8);
    assert!(outcome.sender_completion_ns.is_some());
}

#[test]
fn w1_registration_order_is_metamorphically_irrelevant() {
    let forward = small_scenario(ProtocolKind::PooledCarousel, 1);
    let mut reverse = forward.clone();
    reverse.registration_order = RegistrationOrder::Reverse;
    let forward = run_w1(&forward).expect("forward registration");
    let reverse = run_w1(&reverse).expect("reverse registration");
    let causal_projection = |outcome: &wansim::scenario::W1Outcome| {
        outcome
            .records
            .iter()
            .map(|record| {
                (
                    record.time_ns,
                    record.component,
                    record.event,
                    record.flow_id,
                    record.sequence,
                    record.bytes,
                )
            })
            .collect::<Vec<_>>()
    };
    assert_eq!(causal_projection(&forward), causal_projection(&reverse));
    assert_eq!(forward.completion_times_ns, reverse.completion_times_ns);
    assert_eq!(forward.total_emissions, reverse.total_emissions);
    assert_eq!(
        forward.ack_flight_tail_emissions,
        reverse.ack_flight_tail_emissions
    );
}

#[test]
fn w1_mailboxes_remain_nonbinding() {
    let outcome = run_w1(&small_scenario(ProtocolKind::PooledCarousel, 3))
        .expect("carousel scenario completes");
    let maximum = outcome
        .mailbox_high_water
        .values()
        .copied()
        .max()
        .unwrap_or(0);
    assert!(maximum < MAILBOX_CAPACITY, "mailbox high-water {maximum}");
}
