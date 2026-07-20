use std::process::ExitCode;

use wansim::scenario::{
    CloudProfileKind, CloudScenario, ReceiverAdmissionPolicy, RegistrationOrder,
    WR_FOREGROUND_START_NS, WrProtocol, WrRunConfig, run_wr,
};

const USAGE: &str = "usage: wr_probe <profile> <placement> <util> <jitter:0|1> <protocol> <K> <seed> <cadence:1|2|4> <admission> <slow:-1|0|1|2> <sessions>";

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("wr_probe: {error}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<(), Box<dyn std::error::Error>> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    if args.len() != 11 {
        return Err(USAGE.into());
    }
    let profile = match args[0].as_str() {
        "aws-like" => CloudProfileKind::AwsLike,
        "gcp-like" => CloudProfileKind::GcpLike,
        "digitalocean-like" => CloudProfileKind::DigitaloceanLike,
        _ => return Err(USAGE.into()),
    };
    let protocol = match args[4].as_str() {
        "carousel" => WrProtocol::Carousel,
        "rounds" => WrProtocol::Rounds,
        "per-stripe-fec" => WrProtocol::PerStripeFec,
        "best-single-tree0" => WrProtocol::BestSingleTree0,
        "best-single-tree1" => WrProtocol::BestSingleTree1,
        _ => return Err(USAGE.into()),
    };
    let admission = match args[8].as_str() {
        "hybrid-drop" => ReceiverAdmissionPolicy::HybridDrop,
        "naive-blocking" => ReceiverAdmissionPolicy::NaiveBlocking,
        _ => return Err(USAGE.into()),
    };
    let slow = args[9].parse::<i8>()?;
    let config = WrRunConfig {
        cloud: CloudScenario::built_in(profile, args[1].parse()?)?,
        protocol,
        cloudcast_plan: None,
        egress_prices: None,
        background_anchor_regions: None,
        flow_count_match_single_tree: true,
        source_symbols: args[5].parse()?,
        background_utilization_percent: args[2].parse()?,
        jitter_enabled: match args[3].as_str() {
            "0" => false,
            "1" => true,
            _ => return Err(USAGE.into()),
        },
        seed: args[6].parse()?,
        ack_cadence_multiplier: args[7].parse()?,
        receiver_admission: admission,
        slow_receiver: if slow < 0 {
            None
        } else {
            Some(usize::try_from(slow)?)
        },
        concurrent_sessions: args[10].parse()?,
        registration_order: RegistrationOrder::Forward,
    };
    let outcome = run_wr(&config)?;
    for (index, session) in outcome.sessions.iter().enumerate() {
        println!(
            "session={index},barrier_ns={},sender_ns={},emissions={},drops={},blocking_waits={},round_deficits={},ack_probes={},stall_ppm={}",
            session.barrier_completion_ns,
            session.sender_completion_ns,
            session.total_emissions,
            session.application_drops,
            session.blocking_waits,
            session.positive_round_deficits,
            session.ack_probes,
            session.stall_budget_consumption_ppm,
        );
    }
    let foreground_ns = outcome.sessions[0].barrier_completion_ns.max(1);
    let probe_mbps = [200_000, 200_004].map(|flow_id| {
        let bytes: u128 = outcome
            .records
            .iter()
            .filter(|record| {
                record.event == "wr_tree_rate_sample"
                    && record.flow_id == flow_id
                    && record.time_ns > WR_FOREGROUND_START_NS
            })
            .map(|record| record.bytes as u128)
            .sum();
        bytes.saturating_mul(8_000) / u128::from(foreground_ns)
    });
    println!(
        "link_drops={},mailboxes={},max_background_trunk_utilization_ppm={},probe_mbps={}/{}",
        outcome.link_drops,
        outcome.mailbox_high_water.len(),
        outcome.maximum_background_trunk_utilization_ppm,
        probe_mbps[0],
        probe_mbps[1],
    );
    Ok(())
}
