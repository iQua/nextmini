use std::path::{Path, PathBuf};
use std::process::ExitCode;

use wansim::experiment::run_w3_experiment;

const USAGE: &str =
    "usage: w3_coupling <screening-seeds> <decisive-seeds> <workers> <output-directory>";

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("w3_coupling: {error}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args_os().skip(1);
    let screening_seeds = parse_u64(args.next())?;
    let decisive_seeds = parse_u64(args.next())?;
    let workers = parse_usize(args.next())?;
    let output = args.next().map(PathBuf::from).ok_or(USAGE)?;
    if args.next().is_some() {
        return Err(USAGE.into());
    }
    let artifacts = run_w3_experiment(screening_seeds, decisive_seeds, workers)?;
    std::fs::create_dir_all(&output)?;
    write(&output, "trials.csv", &artifacts.trials_csv)?;
    write(&output, "summaries.csv", &artifacts.summaries_csv)?;
    write(
        &output,
        "advantage-decomposition.csv",
        &artifacts.advantage_decomposition_csv,
    )?;
    write(&output, "a4-correlation.csv", &artifacts.a4_correlation_csv)?;
    write(&output, "critical-paths.csv", &artifacts.critical_paths_csv)?;
    write(&output, "flow-counts.csv", &artifacts.flow_counts_csv)?;
    write(
        &output,
        "isolated-credit-trials.csv",
        &artifacts.isolated_credit_trials_csv,
    )?;
    write(
        &output,
        "isolated-credit-summary.csv",
        &artifacts.isolated_credit_summary_csv,
    )?;
    write(
        &output,
        "control-asymmetry-trials.csv",
        &artifacts.control_asymmetry_trials_csv,
    )?;
    write(
        &output,
        "control-asymmetry-summary.csv",
        &artifacts.control_asymmetry_summary_csv,
    )?;
    println!(
        "wrote deterministic W3 coupling matrix: {screening_seeds} screening seeds, {decisive_seeds} decisive seeds, {workers} workers"
    );
    Ok(())
}

fn parse_u64(value: Option<std::ffi::OsString>) -> Result<u64, Box<dyn std::error::Error>> {
    Ok(value
        .and_then(|value| value.into_string().ok())
        .ok_or(USAGE)?
        .parse()?)
}

fn parse_usize(value: Option<std::ffi::OsString>) -> Result<usize, Box<dyn std::error::Error>> {
    Ok(value
        .and_then(|value| value.into_string().ok())
        .ok_or(USAGE)?
        .parse()?)
}

fn write(output: &Path, name: &str, contents: &str) -> Result<(), std::io::Error> {
    std::fs::write(output.join(name), contents.as_bytes())
}
