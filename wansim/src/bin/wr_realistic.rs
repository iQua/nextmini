use std::path::{Path, PathBuf};
use std::process::ExitCode;

use wansim::experiment::run_wr_experiment;

const USAGE: &str = "usage: wr_realistic <seeds>=16.. <workers> <output-directory>";

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("wr_realistic: {error}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args_os().skip(1);
    let seeds = parse_u64(args.next())?;
    let workers = parse_usize(args.next())?;
    let output = args.next().map(PathBuf::from).ok_or(USAGE)?;
    if args.next().is_some() {
        return Err(USAGE.into());
    }
    let artifacts = run_wr_experiment(seeds, workers)?;
    std::fs::create_dir_all(&output)?;
    write(&output, "trials.csv", &artifacts.trials_csv)?;
    write(&output, "summaries.csv", &artifacts.summaries_csv)?;
    write(
        &output,
        "advantage-decomposition.csv",
        &artifacts.advantage_csv,
    )?;
    write(&output, "rounds-vs-carousel.csv", &artifacts.rounds_csv)?;
    write(&output, "a4-correlation.csv", &artifacts.a4_csv)?;
    write(&output, "straggler.csv", &artifacts.straggler_csv)?;
    write(&output, "cadence.csv", &artifacts.cadence_csv)?;
    write(
        &output,
        "concurrent-sessions.csv",
        &artifacts.concurrent_csv,
    )?;
    write(&output, "k65536-scaling.csv", &artifacts.scaling_csv)?;
    write(&output, "sharing-structure.csv", &artifacts.sharing_csv)?;
    println!("wrote deterministic WR realistic envelope with {seeds} seeds and {workers} workers");
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
