//! Reproducible Stage 3.1 sweep for an extension **BEYOND the METTLE paper**.
//!
//! This example does not define or exercise any wire/manifest surface. It emits CSV to stdout:
//! `cargo run --release -p mettle --example reservoir_simulation -- sweep 256 8192 1400`.

use std::env;

use mettle::experimental_reservoir::simulation::{ChannelModel, SimulationSummary, run_case};
use mettle::experimental_reservoir::{
    FiniteReservoirGeometry, ReservoirRate, checked_reserve_payload_bytes,
};

const EXPERIMENT_SCOPE: &str = "BEYOND the METTLE paper";
const RESERVE_PAYLOAD_BUDGET_BYTES: usize = 8 * 1024 * 1024;
const BASE_SEED: u64 = 0x5354_4147_4533_2026;

#[derive(Clone, Debug)]
struct Config {
    phase: String,
    trials: usize,
    source_count: u64,
    production_symbol_bytes: usize,
    cases: Vec<(ReservoirRate, ReservoirRate)>,
}

impl Config {
    fn parse() -> Result<Self, String> {
        let arguments = env::args().skip(1).collect::<Vec<_>>();
        match arguments.as_slice() {
            [mode, trials, source_count, symbol_bytes] if mode == "sweep" => {
                let wire_rates = [rate(1, 100)?, rate(2, 100)?, rate(4, 100)?];
                let reserve_rates = [rate(3, 100)?, rate(5, 100)?, rate(7, 100)?];
                let cases = wire_rates
                    .into_iter()
                    .flat_map(|wire| {
                        reserve_rates
                            .iter()
                            .copied()
                            .map(move |reserve| (wire, reserve))
                    })
                    .collect();
                Ok(Self {
                    phase: "grid".to_owned(),
                    trials: parse(trials, "trials")?,
                    source_count: parse(source_count, "source count")?,
                    production_symbol_bytes: parse(symbol_bytes, "symbol bytes")?,
                    cases,
                })
            }
            [mode, phase, trials, source_count, symbol_bytes, wire_num, wire_den, reserve_num, reserve_den]
                if mode == "case" =>
            {
                Ok(Self {
                    phase: phase.clone(),
                    trials: parse(trials, "trials")?,
                    source_count: parse(source_count, "source count")?,
                    production_symbol_bytes: parse(symbol_bytes, "symbol bytes")?,
                    cases: vec![(
                        rate(
                            parse(wire_num, "wire numerator")?,
                            parse(wire_den, "wire denominator")?,
                        )?,
                        rate(
                            parse(reserve_num, "reserve numerator")?,
                            parse(reserve_den, "reserve denominator")?,
                        )?,
                    )],
                })
            }
            _ => Err(
                "usage: reservoir_simulation sweep <trials> <source-count> <production-symbol-bytes>\n       reservoir_simulation case <phase> <trials> <source-count> <production-symbol-bytes> <wire-num> <wire-den> <reserve-num> <reserve-den>"
                    .to_owned(),
            ),
        }
    }

    fn validate(&self) -> Result<(), String> {
        if self.trials == 0 {
            return Err("trials must be non-zero".to_owned());
        }
        if self.source_count == 0 {
            return Err("source count must be non-zero".to_owned());
        }
        if self.production_symbol_bytes == 0 {
            return Err("production symbol bytes must be non-zero".to_owned());
        }
        Ok(())
    }
}

fn main() -> Result<(), String> {
    let config = Config::parse()?;
    config.validate()?;
    let channels = channels()?;

    println!(
        "research_scope,phase,source_count,production_symbol_bytes,trials,c_wire_num,c_wire_den,c_reserve_num,c_reserve_den,channel,stationary_erasure_probability,interior_c_num,interior_c_den,wire_bin_count,reserve_cardinality,terminal_bin_count,actual_wire_overhead,actual_total_overhead,initial_completions,initial_completion_probability,final_completions,completion_probability,cp95_lower,cp95_upper,repair_successes,mean_repair_emissions,p95_repair_emissions,mean_reserve_emissions,mean_reserve_losses,duplicate_transmissions,sender_reserve_payload_bytes,sender_reserve_budget_bytes,budget_pass"
    );

    for (case_index, &(wire_rate, reserve_rate)) in config.cases.iter().enumerate() {
        let geometry = FiniteReservoirGeometry::solve(config.source_count, wire_rate, reserve_rate)
            .map_err(|error| format!("finite reservoir geometry failed: {error:?}"))?;
        let reserve_payload_bytes = checked_reserve_payload_bytes(
            geometry.reserve_cardinality(),
            config.production_symbol_bytes,
            RESERVE_PAYLOAD_BUDGET_BYTES,
        )
        .map_err(|error| format!("independent sender reserve budget rejected case: {error:?}"))?;
        let seed = BASE_SEED
            ^ u64::try_from(case_index).map_err(|_| "case index does not fit u64".to_owned())?;

        for channel in &channels {
            let summary = run_case(geometry, *channel, config.trials, seed)
                .map_err(|error| format!("simulation failed: {error:?}"))?;
            print_row(&config, geometry, *channel, &summary, reserve_payload_bytes);
        }
    }
    Ok(())
}

fn print_row(
    config: &Config,
    geometry: FiniteReservoirGeometry,
    channel: ChannelModel,
    summary: &SimulationSummary,
    reserve_payload_bytes: usize,
) {
    let source_count = geometry.source_count() as f64;
    let actual_wire_overhead = geometry.wire_bin_count() as f64 / source_count - 1.0;
    let actual_total_overhead = geometry.terminal_bin_count() as f64 / source_count - 1.0;
    println!(
        "{EXPERIMENT_SCOPE},{},{},{},{},{},{},{},{},{},{:.9},{},{},{},{},{},{:.9},{:.9},{},{:.9},{},{:.9},{:.9},{:.9},{},{:.6},{},{:.6},{:.6},{},{},{},{}",
        config.phase,
        geometry.source_count(),
        config.production_symbol_bytes,
        summary.trials,
        geometry.wire_rate().numerator(),
        geometry.wire_rate().denominator(),
        geometry.reserve_rate().numerator(),
        geometry.reserve_rate().denominator(),
        channel.name(),
        channel.stationary_erasure_probability(),
        geometry.interior_overhead().numerator(),
        geometry.interior_overhead().denominator(),
        geometry.wire_bin_count(),
        geometry.reserve_cardinality(),
        geometry.terminal_bin_count(),
        actual_wire_overhead,
        actual_total_overhead,
        summary.initial_completions,
        summary.initial_completion_probability(),
        summary.final_completions,
        summary.completion_probability(),
        summary.completion_interval_95.lower,
        summary.completion_interval_95.upper,
        summary.repair_successes,
        summary.mean_repair_emissions_for_success,
        summary.p95_repair_emissions_for_success,
        summary.mean_reserve_emissions,
        summary.mean_reserve_losses,
        summary.duplicate_transmissions,
        reserve_payload_bytes,
        RESERVE_PAYLOAD_BUDGET_BYTES,
        reserve_payload_bytes <= RESERVE_PAYLOAD_BUDGET_BYTES,
    );
}

fn channels() -> Result<Vec<ChannelModel>, String> {
    Ok(vec![
        ChannelModel::bec("bec-0.1pct", rate(1, 1000)?)
            .map_err(|error| format!("invalid BEC: {error:?}"))?,
        ChannelModel::bec("bec-0.5pct", rate(5, 1000)?)
            .map_err(|error| format!("invalid BEC: {error:?}"))?,
        ChannelModel::bec("bec-1.0pct", rate(1, 100)?)
            .map_err(|error| format!("invalid BEC: {error:?}"))?,
        ChannelModel::bec("bec-1.5pct", rate(15, 1000)?)
            .map_err(|error| format!("invalid BEC: {error:?}"))?,
        ChannelModel::bec("bec-2.0pct", rate(2, 100)?)
            .map_err(|error| format!("invalid BEC: {error:?}"))?,
        ChannelModel::gilbert_elliott(
            "ge-short-1.089pct",
            rate(1, 1000)?,
            rate(1, 10)?,
            rate(1, 1000)?,
            rate(1, 1)?,
        )
        .map_err(|error| format!("invalid short GE model: {error:?}"))?,
        ChannelModel::gilbert_elliott(
            "ge-long-1.089pct",
            rate(1, 10_000)?,
            rate(1, 100)?,
            rate(1, 1000)?,
            rate(1, 1)?,
        )
        .map_err(|error| format!("invalid long GE model: {error:?}"))?,
    ])
}

fn rate(numerator: u32, denominator: u32) -> Result<ReservoirRate, String> {
    ReservoirRate::new(numerator, denominator)
        .map_err(|error| format!("invalid rate {numerator}/{denominator}: {error:?}"))
}

fn parse<T>(value: &str, label: &str) -> Result<T, String>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    value
        .parse()
        .map_err(|error| format!("invalid {label} `{value}`: {error}"))
}
