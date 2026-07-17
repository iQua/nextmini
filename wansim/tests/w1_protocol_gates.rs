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

#[test]
fn repeated_w1_executions_are_byte_identical() {
    let scenario = small_scenario(ProtocolKind::PooledCarousel, 3);
    let first = run_w1(&scenario).expect("first execution");
    let second = run_w1(&scenario).expect("second execution");
    assert_eq!(first.csv.as_bytes(), second.csv.as_bytes());
}

#[test]
fn tandem_store_and_forward_matches_the_integer_serialization_closed_form() {
    let mut scenario = small_scenario(ProtocolKind::EqualSplitStriping, 1);
    scenario.scenario_id = "w1-tandem-closed-form".to_owned();
    scenario.source_symbols = 2;
    scenario.data_rate_override_bps = Some([[10_000_000; 5]; 2]);
    let outcome = run_w1(&scenario).expect("tandem scenario");
    let segment_bytes = scenario.frame_wire_bytes().expect("geometry") + 40;
    let serialization_ns = (segment_bytes as u64 * 8 * 1_000_000_000).div_ceil(10_000_000);

    let event_time = |component: &str, event: &str, flow_id: usize, sequence: usize| {
        outcome
            .records
            .iter()
            .find(|record| {
                record.component == component
                    && record.event == event
                    && record.flow_id == flow_id
                    && record.sequence == sequence
                    && record.bytes == segment_bytes
            })
            .map(|record| record.time_ns)
            .unwrap_or_else(|| panic!("missing {component}/{event}/{flow_id}/{sequence}"))
    };
    let emitted = outcome
        .records
        .iter()
        .find(|record| {
            record.component == "w1_source"
                && record.event == "data_frame_emitted"
                && record.flow_id == 30_001
        })
        .expect("tree-zero source emission")
        .time_ns;
    let hop1_start = event_time(
        "w1_t0_source_relay_a_forward",
        "serialization_start",
        30_001,
        0,
    );
    let hop1_end = event_time(
        "w1_t0_source_relay_a_forward",
        "serialization_end",
        30_001,
        0,
    );
    let assembled = outcome
        .records
        .iter()
        .find(|record| {
            record.component == "w1_tree0_relay_a"
                && record.event == "frame_assembled"
                && record.sequence == 0
        })
        .expect("relay assembly")
        .time_ns;
    let hop2_start = event_time(
        "w1_t0_relay_a_receiver1_forward",
        "serialization_start",
        30_002,
        0,
    );
    let hop2_end = event_time(
        "w1_t0_relay_a_receiver1_forward",
        "serialization_end",
        30_002,
        0,
    );
    let receiver_delivery = outcome
        .records
        .iter()
        .find(|record| {
            record.component == "w1_receiver1"
                && record.event == "runtime_command_enqueue_data"
                && record.flow_id == 30_002
                && record.sequence == 0
        })
        .expect("receiver delivery")
        .time_ns;

    // The one-nanosecond deterministic resolution delta is deliberately excluded from modeled
    // timestamps, so admission starts at the causal arrival timestamp.
    assert_eq!(hop1_start, emitted);
    assert_eq!(hop1_end - hop1_start, serialization_ns);
    assert_eq!(assembled, hop1_end + scenario.link_propagation_ns);
    assert_eq!(hop2_start, assembled);
    assert_eq!(hop2_end - hop2_start, serialization_ns);
    assert_eq!(receiver_delivery, hop2_end + scenario.link_propagation_ns);
    assert_eq!(
        receiver_delivery - emitted,
        2 * serialization_ns + 2 * scenario.link_propagation_ns
    );
}

#[test]
fn zero_extra_and_effectively_infinite_buffers_converge_toward_opportunity_model() {
    let mut minimal = small_scenario(ProtocolKind::EqualSplitStriping, 1);
    minimal.scenario_id = "w1-buffer-limit-minimal".to_owned();
    minimal.source_symbols = 16;
    minimal.socket_send_buffer_bytes = 512;
    minimal.socket_receive_buffer_bytes = 512;
    minimal.relay_application_buffer_bytes = 512;
    minimal.relay_child_queue_bytes = 512;
    minimal.link_queue_bytes = 552;
    minimal.link_propagation_ns = 2;
    minimal.control_propagation_ns = [2; 3];
    minimal.runtime_command_service_ns = 1;
    minimal.decoder_sink_service_ns = 1;
    minimal.simulation_end_ns = 100_000;
    minimal.data_rate_override_bps = Some([[8_000_000_000; 5]; 2]);
    minimal.control_rate_bps = 8_000_000_000;
    minimal.timer_interval_ns = 100;
    minimal.carousel.ack_debounce_ns = 100;
    minimal.carousel.ack_heartbeat_ns = 1_000;
    minimal.carousel.ack_probe_interval_ns = 800;
    minimal.carousel.peer_silence_timeout_ns = 10_000;
    minimal.carousel.peer_stall_timeout_ns = 20_000;
    minimal.carousel.receiver_passive_window_ns = 40_000;
    minimal.carousel.session_complete_interval_ns = 100;
    let mut unbounded = minimal.clone();
    unbounded.scenario_id = "w1-buffer-limit-unbounded".to_owned();
    unbounded.socket_send_buffer_bytes = 1 << 20;
    unbounded.socket_receive_buffer_bytes = 1 << 20;
    unbounded.relay_application_buffer_bytes = 1 << 20;
    unbounded.relay_child_queue_bytes = 1 << 20;
    unbounded.link_queue_bytes = 1 << 20;

    let minimal = run_w1(&minimal).expect("one-frame buffers");
    let unbounded = run_w1(&unbounded).expect("effectively infinite buffers");
    assert_eq!(minimal.application_drops, 0);
    assert_eq!(unbounded.application_drops, 0);
    assert_eq!(minimal.link_drops, 0);
    assert_eq!(unbounded.link_drops, 0);
    assert_eq!(minimal.total_emissions, unbounded.total_emissions);
    // At effectively zero propagation/service time, buffer placement may shift at most one
    // aggregate delivery opportunity per source symbol; it cannot manufacture a throughput gap.
    let one_frame_opportunity_ns = 552_u64;
    assert!(
        minimal
            .barrier_completion_ns
            .abs_diff(unbounded.barrier_completion_ns)
            <= scenario_opportunity_bound(16, one_frame_opportunity_ns)
    );
}

#[test]
fn edge_disjoint_unused_tree_cannot_change_completion() {
    let mut baseline = small_scenario(ProtocolKind::RateProportionalStriping, 1);
    baseline.scenario_id = "w1-edge-disjoint-baseline".to_owned();
    baseline.source_symbols = 8;
    baseline.quota_override = Some([0, 8]);
    baseline.data_rate_override_bps = Some([[80_000_000; 5]; 2]);
    let mut perturbed = baseline.clone();
    perturbed.scenario_id = "w1-edge-disjoint-perturbed".to_owned();
    let mut rates = [[80_000_000; 5]; 2];
    rates[0] = [1; 5];
    perturbed.data_rate_override_bps = Some(rates);

    let baseline = run_w1(&baseline).expect("baseline");
    let perturbed = run_w1(&perturbed).expect("unrelated-tree perturbation");
    assert_eq!(baseline.per_tree_emissions, [0, 8]);
    assert_eq!(perturbed.per_tree_emissions, [0, 8]);
    assert_eq!(baseline.completion_times_ns, perturbed.completion_times_ns);
    assert_eq!(
        baseline.barrier_completion_ns,
        perturbed.barrier_completion_ns
    );
    assert_eq!(
        baseline.sender_completion_ns,
        perturbed.sender_completion_ns
    );
}

#[test]
fn fork_join_strict_conservation_completes_every_active_leaf() {
    let outcome =
        run_w1(&small_scenario(ProtocolKind::PooledCarousel, 3)).expect("fork/join scenario");
    assert_eq!(outcome.completion_times_ns.len(), 3);
    assert!(outcome.completion_times_ns.iter().all(|time| *time > 0));
    assert_eq!(
        outcome.barrier_completion_ns,
        *outcome.completion_times_ns.iter().max().expect("receivers")
    );
    assert_eq!(outcome.application_drops, 0);
    assert_eq!(outcome.link_drops, 0);
}

const fn scenario_opportunity_bound(symbols: u64, one_frame_ns: u64) -> u64 {
    symbols * one_frame_ns
}
