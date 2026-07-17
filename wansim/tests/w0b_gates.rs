use std::collections::{BTreeMap, BTreeSet};

use wansim::metrics::MAILBOX_CAPACITY;
use wansim::scenario::{FanoutAdmission, RegistrationOrder, TreeEndpoint, TreeScenario, run_tree};

const FRAME_WIRE_BYTES: usize = 512;
const RECEIVER_1_RESUME_NS: u64 = 200_000_000;

fn golden_scenario() -> TreeScenario {
    let scenario: TreeScenario =
        toml::from_str(include_str!("golden/w0b_tree.toml")).expect("valid golden tree scenario");
    scenario.validate().expect("valid tree geometry");
    scenario
}

fn externality_scenario(admission: FanoutAdmission) -> TreeScenario {
    let mut scenario = golden_scenario();
    scenario.scenario_id = match admission {
        FanoutAdmission::Sequential => "w0b-sequential-externality".to_owned(),
        FanoutAdmission::Concurrent => "w0b-concurrent-externality".to_owned(),
    };
    scenario.fanout_admission = admission;
    scenario.relay_application_buffer_bytes = scenario.stream_bytes().expect("stream geometry");
    scenario.runtime_command_capacity_frames = scenario.frame_count;
    scenario.receiver_data_inbox_capacity_frames = scenario.frame_count;
    scenario.runtime_command_service_ns = 10_000;
    scenario.decoder_sink_service_ns = 100_000;
    scenario.receiver2.transport_resume_at_ns = 1;
    scenario.receiver2.service_start_at_ns = 1;
    scenario.receiver3.transport_resume_at_ns = 1;
    scenario.receiver3.service_start_at_ns = 1;
    scenario.validate().expect("valid externality scenario");
    scenario
}

#[test]
fn tree_scenario_rejects_non_topological_or_reordered_duplicates() {
    let mut scenario = golden_scenario();
    scenario.relay_a_children = vec![TreeEndpoint::Receiver1, TreeEndpoint::Receiver1];
    assert!(scenario.validate().is_err());
}

#[test]
fn paused_tree_reaches_the_exact_enumerated_byte_ownership_plateau() {
    let scenario = golden_scenario();
    let outcome = run_tree(&scenario).expect("tree completes");
    let expected = BTreeMap::from([
        ("source.sndbuf", 1_024),
        ("relay_a.upstream_rcv", 1_024),
        ("relay_a.application", 1_024),
        ("relay_a.receiver1.queue", 1_024),
        ("relay_a.receiver1.sndbuf", 1_024),
        ("relay_a.relay_b.queue", 0),
        ("relay_a.relay_b.sndbuf", 0),
        ("relay_b.upstream_rcv", 0),
        ("relay_b.application", 0),
        ("relay_b.receiver2.queue", 1_024),
        ("relay_b.receiver2.sndbuf", 1_024),
        ("relay_b.receiver3.queue", 1_024),
        ("relay_b.receiver3.sndbuf", 1_024),
        ("receiver1.tcp_rcv", 1_024),
        ("receiver1.runtime_command", 0),
        ("receiver1.data_inbox", 0),
        ("receiver1.decoder_sink", 0),
        ("receiver2.tcp_rcv", 1_024),
        ("receiver2.runtime_command", 0),
        ("receiver2.data_inbox", 0),
        ("receiver2.decoder_sink", 0),
        ("receiver3.tcp_rcv", 1_024),
        ("receiver3.runtime_command", 0),
        ("receiver3.data_inbox", 0),
        ("receiver3.decoder_sink", 0),
    ]);
    assert_eq!(outcome.ownership_at_probe, expected);
    assert_eq!(outcome.source_bytes_admitted_at_probe, 6_144);
    assert_eq!(outcome.expected_source_plateau_bytes, 6_144);
    assert_eq!(outcome.ownership_total_at_probe, 12_288);
    assert_eq!(outcome.expected_resident_copy_plateau_bytes, 12_288);
}

#[test]
fn sequential_admission_preserves_controller_child_order_without_sorting() {
    let mut scenario = golden_scenario();
    scenario.scenario_id = "w0b-route-order".to_owned();
    scenario.relay_a_children = vec![TreeEndpoint::RelayB, TreeEndpoint::Receiver1];
    scenario.relay_b_children = vec![TreeEndpoint::Receiver3, TreeEndpoint::Receiver2];
    let outcome = run_tree(&scenario).expect("tree completes");
    let mut relay_a: Vec<_> = outcome
        .records
        .iter()
        .filter(|record| record.component == "relay_a" && record.event == "fanout_child_configured")
        .map(|record| (record.sequence, record.flow_id))
        .collect();
    relay_a.sort_unstable();
    let mut relay_b: Vec<_> = outcome
        .records
        .iter()
        .filter(|record| record.component == "relay_b" && record.event == "fanout_child_configured")
        .map(|record| (record.sequence, record.flow_id))
        .collect();
    relay_b.sort_unstable();
    assert_eq!(relay_a, vec![(0, 20_003), (1, 20_002)]);
    assert_eq!(relay_b, vec![(0, 20_005), (1, 20_004)]);
}

#[test]
fn sequential_first_child_blocks_later_child_only_after_real_chain_fills() {
    let scenario = externality_scenario(FanoutAdmission::Sequential);
    let outcome = run_tree(&scenario).expect("sequential tree completes");
    let first_block = outcome
        .records
        .iter()
        .find(|record| {
            record.component == "relay_a"
                && record.event == "child_admission_blocked"
                && record.flow_id == 20_002
                && record.time_ns < RECEIVER_1_RESUME_NS
        })
        .expect("receiver1 chain eventually blocks");
    let relay_b_frames_before_block: Vec<_> = outcome
        .records
        .iter()
        .filter(|record| {
            record.component == "relay_a"
                && record.event == "child_queue_admit"
                && record.flow_id == 20_003
                && record.time_ns <= first_block.time_ns
        })
        .map(|record| record.sequence)
        .collect();
    assert_eq!(first_block.sequence, 6);
    assert_eq!(first_block.value, 3_072);
    assert_eq!(relay_b_frames_before_block, (0..6).collect::<Vec<_>>());
    assert!(
        outcome.completion_times_ns["receiver2"] > RECEIVER_1_RESUME_NS,
        "later branch completed before the blocked first child reopened"
    );
}

#[test]
fn concurrent_admission_removes_the_configured_order_externality() {
    let sequential = run_tree(&externality_scenario(FanoutAdmission::Sequential))
        .expect("sequential tree completes");
    let concurrent = run_tree(&externality_scenario(FanoutAdmission::Concurrent))
        .expect("concurrent tree completes");
    let forwarded_before_resume: Vec<_> = concurrent
        .records
        .iter()
        .filter(|record| {
            record.component == "relay_a"
                && record.event == "child_queue_admit"
                && record.flow_id == 20_003
                && record.time_ns < RECEIVER_1_RESUME_NS
        })
        .map(|record| record.sequence)
        .collect();
    assert_eq!(forwarded_before_resume, (0..16).collect::<Vec<_>>());
    assert!(concurrent.completion_times_ns["receiver2"] < RECEIVER_1_RESUME_NS);
    assert!(concurrent.completion_times_ns["receiver3"] < RECEIVER_1_RESUME_NS);
    assert!(
        sequential.completion_times_ns["receiver2"] > concurrent.completion_times_ns["receiver2"]
    );
}

#[test]
fn hybrid_data_drop_occurs_after_transport_ack_reaches_the_hop_sender() {
    let mut scenario = golden_scenario();
    scenario.scenario_id = "w0b-hybrid-drop-order".to_owned();
    scenario.receiver1.transport_resume_at_ns = 1;
    scenario.receiver1.service_start_at_ns = 500_000_000;
    scenario.receiver2.transport_resume_at_ns = 1;
    scenario.receiver2.service_start_at_ns = 500_000_000;
    scenario.receiver3.transport_resume_at_ns = 1;
    scenario.receiver3.service_start_at_ns = 500_000_000;
    scenario.receiver_data_inbox_capacity_frames = 1;
    scenario.runtime_command_service_ns = 20_000_000;
    let outcome = run_tree(&scenario).expect("drop scenario remains transport-live");
    let dropped = outcome
        .records
        .iter()
        .find(|record| {
            record.component == "receiver1" && record.event == "data_inbox_drop_after_tcp_ack"
        })
        .expect("data inbox should drop while decoder is paused");
    let frame_end = (dropped.sequence + 1) * FRAME_WIRE_BYTES;
    let ack_emit = outcome
        .records
        .iter()
        .filter(|record| {
            record.component == "receiver1"
                && record.event == "tcp_ack_emit"
                && record.sequence >= frame_end
        })
        .min_by_key(|record| record.time_ns)
        .expect("receiver emitted covering ACK");
    let ack_arrival = outcome
        .records
        .iter()
        .filter(|record| {
            record.component == "relay_a"
                && record.event == "child_ack_arrival"
                && record.flow_id == 20_002
                && record.sequence >= frame_end
        })
        .min_by_key(|record| record.time_ns)
        .expect("hop sender received covering ACK");
    assert!(ack_emit.time_ns < ack_arrival.time_ns);
    assert!(ack_arrival.time_ns < dropped.time_ns);
    assert!(dropped.value >= frame_end);
}

#[test]
fn shared_runtime_command_mailbox_precedes_the_control_data_split() {
    let outcome = run_tree(&golden_scenario()).expect("tree completes");
    for receiver in ["receiver1", "receiver2", "receiver3"] {
        for frame_id in 0..16 {
            let transport = outcome
                .records
                .iter()
                .find(|record| {
                    record.component == receiver
                        && record.event == "transport_frame_delivered"
                        && record.sequence == frame_id
                })
                .expect("transport delivery");
            let command = outcome
                .records
                .iter()
                .find(|record| {
                    record.component == receiver
                        && record.event == "runtime_command_enqueue"
                        && record.sequence == frame_id
                })
                .expect("shared command enqueue");
            let data = outcome
                .records
                .iter()
                .find(|record| {
                    record.component == receiver
                        && record.event == "data_inbox_enqueue"
                        && record.sequence == frame_id
                })
                .expect("data-lane enqueue");
            assert!(transport.time_ns <= command.time_ns);
            assert!(command.time_ns < data.time_ns);
        }
    }
}

#[test]
fn decoder_sink_is_an_explicit_serial_service_center() {
    let scenario = golden_scenario();
    let outcome = run_tree(&scenario).expect("tree completes");
    let starts: BTreeMap<_, _> = outcome
        .records
        .iter()
        .filter(|record| {
            record.component == "receiver1" && record.event == "decoder_sink_service_start"
        })
        .map(|record| (record.sequence, record.time_ns))
        .collect();
    let finishes: BTreeMap<_, _> = outcome
        .records
        .iter()
        .filter(|record| record.component == "receiver1" && record.event == "frame_delivered")
        .map(|record| (record.sequence, record.time_ns))
        .collect();
    for frame_id in 0..scenario.frame_count {
        assert_eq!(
            finishes[&frame_id] - starts[&frame_id],
            scenario.decoder_sink_service_ns
        );
        if frame_id > 0 {
            assert!(starts[&frame_id] >= finishes[&(frame_id - 1)]);
        }
    }
}

#[test]
fn provisioned_no_drop_tree_conserves_all_k_frames_in_hop_order() {
    let scenario = golden_scenario();
    let outcome = run_tree(&scenario).expect("tree completes");
    assert!(
        !outcome
            .records
            .iter()
            .any(|record| record.event == "data_inbox_drop_after_tcp_ack")
    );
    for (receiver, flow_id) in [
        ("receiver1", 20_002),
        ("receiver2", 20_004),
        ("receiver3", 20_005),
    ] {
        let delivered: Vec<_> = outcome
            .records
            .iter()
            .filter(|record| record.component == receiver && record.event == "frame_delivered")
            .map(|record| record.sequence)
            .collect();
        assert_eq!(
            delivered,
            (0..scenario.source_symbols_k).collect::<Vec<_>>()
        );
        assert!(outcome.records.iter().any(|record| {
            record.component == receiver
                && record.event == "stream_complete"
                && record.flow_id == flow_id
                && record.value == scenario.source_symbols_k
        }));
    }
    for (relay, flow_id) in [
        ("relay_a", 20_002),
        ("relay_a", 20_003),
        ("relay_b", 20_004),
        ("relay_b", 20_005),
    ] {
        let admitted: Vec<_> = outcome
            .records
            .iter()
            .filter(|record| {
                record.component == relay
                    && record.event == "child_queue_admit"
                    && record.flow_id == flow_id
            })
            .map(|record| record.sequence)
            .collect();
        assert_eq!(admitted, (0..scenario.source_symbols_k).collect::<Vec<_>>());
    }
}

#[test]
fn every_overlay_hop_has_independent_tcp_state_and_no_segment_multicast() {
    let outcome = run_tree(&golden_scenario()).expect("tree completes");
    let expected = [
        ("source_relay_a_forward", 20_001),
        ("relay_a_receiver1_forward", 20_002),
        ("relay_a_relay_b_forward", 20_003),
        ("relay_b_receiver2_forward", 20_004),
        ("relay_b_receiver3_forward", 20_005),
    ];
    let mut observed_flows = BTreeSet::new();
    for (link, expected_flow) in expected {
        let flows: BTreeSet<_> = outcome
            .records
            .iter()
            .filter(|record| record.component == link && record.event == "arrival")
            .map(|record| record.flow_id)
            .collect();
        assert_eq!(flows, BTreeSet::from([expected_flow]));
        observed_flows.extend(flows);
    }
    assert_eq!(
        observed_flows,
        BTreeSet::from([20_001, 20_002, 20_003, 20_004, 20_005])
    );
}

#[test]
fn fanout_fanin_nexosim_mailboxes_remain_nonbinding() {
    let outcome = run_tree(&golden_scenario()).expect("tree completes");
    let expected = [
        "tree_source",
        "relay_a",
        "relay_b",
        "receiver1",
        "receiver2",
        "receiver3",
        "source_relay_a_forward",
        "source_relay_a_reverse",
        "relay_a_receiver1_forward",
        "relay_a_receiver1_reverse",
        "relay_a_relay_b_forward",
        "relay_a_relay_b_reverse",
        "relay_b_receiver2_forward",
        "relay_b_receiver2_reverse",
        "relay_b_receiver3_forward",
        "relay_b_receiver3_reverse",
    ];
    assert_eq!(outcome.mailbox_high_water.len(), expected.len());
    for mailbox in expected {
        let high_water = outcome.mailbox_high_water[mailbox];
        assert!(high_water <= 3, "{mailbox} high-water was {high_water}");
        assert!(high_water < MAILBOX_CAPACITY);
    }
}

#[test]
fn repeated_tree_executions_produce_byte_identical_csv() {
    let scenario = golden_scenario();
    let first = run_tree(&scenario).expect("first tree run");
    let second = run_tree(&scenario).expect("second tree run");
    assert_eq!(first.csv.as_bytes(), second.csv.as_bytes());
}

#[test]
fn tree_registration_order_is_metamorphically_irrelevant() {
    let forward_scenario = golden_scenario();
    let mut reverse_scenario = forward_scenario.clone();
    reverse_scenario.registration_order = RegistrationOrder::Reverse;
    let forward = run_tree(&forward_scenario).expect("forward registration");
    let reverse = run_tree(&reverse_scenario).expect("reverse registration");
    assert_eq!(forward.csv.as_bytes(), reverse.csv.as_bytes());
}

#[test]
fn committed_tree_csv_golden_is_byte_identical() {
    let outcome = run_tree(&golden_scenario()).expect("golden tree run");
    assert_eq!(
        outcome.csv.as_bytes(),
        include_bytes!("golden/w0b_tree.csv")
    );
}
