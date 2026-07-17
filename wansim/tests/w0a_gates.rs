use std::collections::BTreeSet;

use wansim::determinism::{CounterPrf, Decision, DeferredDeadline};
use wansim::metrics::MAILBOX_CAPACITY;
use wansim::scenario::{ChainScenario, RegistrationOrder, run_chain};
use wansim::{DAYS_UPSTREAM_REV, NEXOSIM_VENDORED_TREE};

fn golden_scenario() -> ChainScenario {
    let scenario: ChainScenario =
        toml::from_str(include_str!("golden/w0a_chain.toml")).expect("valid golden scenario");
    scenario.validate().expect("valid scenario geometry");
    scenario
}

#[test]
fn dependency_identity_is_pinned_to_vendored_days_and_nexosim() {
    assert_eq!(
        DAYS_UPSTREAM_REV,
        "d6a473b555d4f129c1eb62c36cb4525cdd5240ad"
    );
    assert_eq!(
        NEXOSIM_VENDORED_TREE,
        "9cad1c1ee25dac8b31c1bb215ce3801eacfc7e29"
    );
    assert_eq!(days::flows::tcp_socket::TCP_IP_HEADER_BYTES, 40);

    let manifest = include_str!("../Cargo.toml");
    assert!(manifest.contains("days = { path = \"vendor/days\", version = \"0.4.3\" }"));
    assert!(manifest.contains("nexosim = { path = \"vendor/days/crates/nexosim\" }"));

    let lockfile = include_str!("../Cargo.lock");
    let days_package = lockfile
        .split("[[package]]")
        .find(|package| package.contains("name = \"days\""))
        .expect("days lock entry");
    assert!(days_package.contains("version = \"0.4.3\""));
    assert!(!days_package.contains("source = "));
    let nexosim_package = lockfile
        .split("[[package]]")
        .find(|package| package.contains("name = \"nexosim\""))
        .expect("nexosim lock entry");
    assert!(nexosim_package.contains("version = \"1.0.0\""));
    assert!(!nexosim_package.contains("source = "));

    let divergence = include_str!("../vendor/DIVERGENCE.md");
    assert!(divergence.contains(DAYS_UPSTREAM_REV));
    assert!(divergence.contains(NEXOSIM_VENDORED_TREE));
}

#[test]
fn counter_prf_has_stable_domain_separated_vectors() {
    let prf = CounterPrf::new(0x1234_5678_9abc_def0, "w0a-chain-golden");
    assert_eq!(
        prf.draw_u64("segment-loss", 7, 11, 0),
        0xb9fe_a139_8db9_b535
    );
    assert_eq!(
        prf.draw_u64("segment-loss", 7, 11, 1),
        0x0c72_bbdf_b229_9505
    );
    assert_ne!(
        prf.draw_u64("segment-loss", 7, 11, 0),
        prf.draw_u64("logical-frame-payload", 7, 11, 0)
    );
}

#[test]
fn scenario_schema_and_geometry_are_checked() {
    let mut scenario = golden_scenario();
    scenario.schema_version += 1;
    assert!(scenario.validate().is_err());

    let mut scenario = golden_scenario();
    scenario.relay_application_buffer_bytes = scenario.frame_payload_bytes;
    assert!(scenario.validate().is_err());
}

#[test]
fn four_byte_length_prefix_is_counted_once_per_frame() {
    let scenario = golden_scenario();
    let outcome = run_chain(&scenario).expect("chain completes");
    assert_eq!(outcome.stream_bytes, scenario.frame_count * (508 + 4));
    assert_eq!(outcome.stream_bytes, 8_192);
    assert!(outcome.records.iter().any(|record| {
        record.component == "simulation"
            && record.event == "logical_length_prefix_bytes"
            && record.bytes == 4
            && record.value == 512
    }));
}

#[test]
fn paused_reads_reach_the_exact_buffer_chain_plateau() {
    let scenario = golden_scenario();
    let outcome = run_chain(&scenario).expect("chain completes");
    let closed_form = scenario.socket_send_buffer_bytes
        + scenario.socket_receive_buffer_bytes
        + scenario.relay_application_buffer_bytes
        + scenario.socket_send_buffer_bytes
        + scenario.socket_receive_buffer_bytes;
    assert_eq!(closed_form, 5_120);
    assert_eq!(outcome.expected_backpressure_plateau_bytes, closed_form);
    assert_eq!(outcome.source_bytes_admitted_before_resume, closed_form);
    assert!(outcome.records.iter().any(|record| {
        record.component == "source"
            && record.event == "writer_blocked"
            && record.time_ns < scenario.receiver_resume_at_ns
            && record.value == closed_form
    }));
}

#[test]
fn resume_drains_the_chain_and_unblocks_all_remaining_writes() {
    let scenario = golden_scenario();
    let outcome = run_chain(&scenario).expect("chain completes");
    assert!(outcome.completed);
    assert_eq!(
        outcome
            .records
            .iter()
            .filter(|record| {
                record.component == "source" && record.event == "application_write"
            })
            .map(|record| record.value)
            .max(),
        Some(outcome.stream_bytes)
    );
    let completion = outcome
        .records
        .iter()
        .find(|record| record.component == "receiver" && record.event == "stream_complete")
        .expect("completion record");
    assert!(completion.time_ns > scenario.receiver_resume_at_ns);
    assert_eq!(completion.bytes, outcome.stream_bytes);
}

#[test]
fn saturated_link_has_the_exact_serialization_rate() {
    let scenario = golden_scenario();
    let outcome = run_chain(&scenario).expect("chain completes");
    let mut departures: Vec<_> = outcome
        .records
        .iter()
        .filter(|record| {
            record.component == "hop1_forward"
                && record.event == "serialization_end"
                && record.bytes == 552
        })
        .map(|record| record.time_ns)
        .collect();
    departures.sort_unstable();
    assert!(departures.len() >= 2);
    let expected_segment_ns = 552_u64 * 8 * 1_000_000_000 / scenario.link_rate_bps;
    assert_eq!(expected_segment_ns, 4_416_000);
    assert_eq!(departures[1] - departures[0], expected_segment_ns);

    // Each 512-byte framed stream unit carries 508 innovative/application bytes.
    let payload_bytes_per_second = 508.0 / (expected_segment_ns as f64 / 1_000_000_000.0);
    assert!((payload_bytes_per_second - 115_036.231_884).abs() < 0.001);
}

#[test]
fn relay_never_writes_a_frame_before_full_prefix_bounded_assembly() {
    let outcome = run_chain(&golden_scenario()).expect("chain completes");
    let assembly_times: Vec<_> = (0..16)
        .map(|frame_id| {
            outcome
                .records
                .iter()
                .find(|record| {
                    record.component == "relay"
                        && record.event == "frame_assembled"
                        && record.sequence == frame_id
                })
                .expect("assembled frame")
                .time_ns
        })
        .collect();
    for write in outcome
        .records
        .iter()
        .filter(|record| record.component == "relay" && record.event == "downstream_socket_write")
    {
        assert!(write.time_ns >= assembly_times[write.value]);
    }
}

#[test]
fn persistent_hops_use_independent_flow_and_congestion_state() {
    let outcome = run_chain(&golden_scenario()).expect("chain completes");
    let source_flows: BTreeSet<_> = outcome
        .records
        .iter()
        .filter(|record| record.component == "source" && record.event == "segment_emit")
        .map(|record| record.flow_id)
        .collect();
    let relay_flows: BTreeSet<_> = outcome
        .records
        .iter()
        .filter(|record| record.component == "relay" && record.event == "downstream_segment_emit")
        .map(|record| record.flow_id)
        .collect();
    assert_eq!(source_flows, BTreeSet::from([10_001]));
    assert_eq!(relay_flows, BTreeSet::from([10_002]));
}

#[test]
fn deterministic_segment_loss_recovers_without_application_gap() {
    let mut scenario = golden_scenario();
    scenario.scenario_id = "w0a-loss-recovery".to_owned();
    scenario.receiver_resume_at_ns = 1;
    scenario.hop1_drop_attempts = BTreeSet::from([0]);
    let outcome = run_chain(&scenario).expect("loss recovers");
    assert!(outcome.completed);
    assert_eq!(
        outcome
            .records
            .iter()
            .filter(|record| record.event == "segment_drop")
            .count(),
        1
    );
    assert!(
        outcome
            .records
            .iter()
            .filter(|record| {
                record.component == "source"
                    && record.event == "segment_emit"
                    && record.sequence == 0
            })
            .count()
            >= 2
    );

    let reads: Vec<_> = outcome
        .records
        .iter()
        .filter(|record| record.component == "receiver" && record.event == "application_read")
        .collect();
    let mut expected_offset = 0;
    for read in reads {
        assert_eq!(read.sequence, expected_offset);
        expected_offset += read.bytes;
    }
    assert_eq!(expected_offset, outcome.stream_bytes);
}

#[test]
fn loss_recovery_does_not_duplicate_application_frames() {
    let mut scenario = golden_scenario();
    scenario.scenario_id = "w0a-loss-no-duplicate".to_owned();
    scenario.receiver_resume_at_ns = 1;
    scenario.hop1_drop_attempts = BTreeSet::from([0]);
    let outcome = run_chain(&scenario).expect("loss recovers");
    let frame_ids: Vec<_> = outcome
        .records
        .iter()
        .filter(|record| record.component == "receiver" && record.event == "frame_delivered")
        .map(|record| record.sequence)
        .collect();
    assert_eq!(frame_ids, (0..scenario.frame_count).collect::<Vec<_>>());
}

#[test]
fn ack_stamped_at_deadline_wins_after_one_delta() {
    let mut deadline = DeferredDeadline::default();
    let (generation, decision_at) = deadline.arm(1_000);
    assert_eq!(decision_at, 1_001);
    deadline.observe(1_000);
    assert_eq!(deadline.decide(generation, 1_000), Decision::TooEarly);
    assert_eq!(deadline.decide(generation, decision_at), Decision::EventWon);
}

#[test]
fn event_after_deadline_does_not_win_tie_resolution() {
    let mut deadline = DeferredDeadline::default();
    let (generation, decision_at) = deadline.arm(1_000);
    deadline.observe(1_001);
    assert_eq!(
        deadline.decide(generation, decision_at),
        Decision::DeadlineWon
    );
}

#[test]
fn rearming_deadline_invalidates_stale_timer_generation() {
    let mut deadline = DeferredDeadline::default();
    let (stale, _) = deadline.arm(1_000);
    let (current, decision_at) = deadline.arm(2_000);
    deadline.observe(2_000);
    assert_eq!(deadline.decide(stale, 2_001), Decision::Stale);
    assert_eq!(deadline.decide(current, decision_at), Decision::EventWon);
}

#[test]
fn repeated_executions_produce_byte_identical_csv() {
    let scenario = golden_scenario();
    let first = run_chain(&scenario).expect("first run");
    let second = run_chain(&scenario).expect("second run");
    assert_eq!(first.csv.as_bytes(), second.csv.as_bytes());
}

#[test]
fn model_registration_order_is_metamorphically_irrelevant() {
    let forward_scenario = golden_scenario();
    let mut reverse_scenario = forward_scenario.clone();
    reverse_scenario.registration_order = RegistrationOrder::Reverse;
    let forward = run_chain(&forward_scenario).expect("forward registration");
    let reverse = run_chain(&reverse_scenario).expect("reverse registration");
    assert_eq!(forward.csv.as_bytes(), reverse.csv.as_bytes());
}

#[test]
fn committed_csv_golden_is_byte_identical() {
    let outcome = run_chain(&golden_scenario()).expect("golden run");
    assert_eq!(
        outcome.csv.as_bytes(),
        include_bytes!("golden/w0a_chain.csv")
    );
}

#[test]
fn nexosim_mailboxes_are_provably_nonbinding() {
    let outcome = run_chain(&golden_scenario()).expect("chain completes");
    for mailbox in [
        "source",
        "hop1_forward",
        "relay",
        "hop2_forward",
        "receiver",
        "hop2_reverse",
        "hop1_reverse",
    ] {
        let high_water = outcome
            .mailbox_high_water
            .get(mailbox)
            .copied()
            .expect("tracked mailbox");
        assert!(high_water <= 2, "{mailbox} high-water was {high_water}");
        assert!(high_water < MAILBOX_CAPACITY);
    }
}

#[test]
fn simulation_records_exactly_one_nexosim_worker() {
    let outcome = run_chain(&golden_scenario()).expect("chain completes");
    assert!(outcome.records.iter().any(|record| {
        record.component == "simulation" && record.event == "single_worker" && record.value == 1
    }));
}

#[test]
fn golden_path_has_no_physical_queue_drop() {
    let outcome = run_chain(&golden_scenario()).expect("chain completes");
    assert!(
        !outcome
            .records
            .iter()
            .any(|record| { record.event == "queue_drop" || record.event == "segment_drop" })
    );
}
