use std::path::{Path, PathBuf};
use std::process::ExitCode;

use wansim::experiment::run_w2_experiment;

const USAGE: &str =
    "usage: w2_straggler <screening-seeds> <decisive-seeds> <workers> <output-directory>";

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("w2_straggler: {error}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args_os().skip(1);
    let screening_seeds = parse_u64(args.next(), USAGE)?;
    let decisive_seeds = parse_u64(args.next(), USAGE)?;
    let workers = parse_usize(args.next(), USAGE)?;
    let output = args.next().map(PathBuf::from).ok_or(USAGE)?;
    if args.next().is_some() {
        return Err(USAGE.into());
    }
    let artifacts = run_w2_experiment(screening_seeds, decisive_seeds, workers)?;
    std::fs::create_dir_all(&output)?;
    write(&output, "trials.csv", &artifacts.trials_csv)?;
    write(
        &output,
        "screening-summary.csv",
        &artifacts.screening_summary_csv,
    )?;
    write(
        &output,
        "decisive-summary.csv",
        &artifacts.decisive_summary_csv,
    )?;
    write(&output, "decision-table.csv", &artifacts.decision_table_csv)?;
    write(&output, "critical-paths.csv", &artifacts.critical_paths_csv)?;
    write(&output, "tail-fraction.csv", &artifacts.tail_fraction_csv)?;
    write(&output, "sanity.csv", &artifacts.sanity_csv)?;
    println!(
        "wrote deterministic W2 matrix: {screening_seeds} screening seeds, {decisive_seeds} decisive seeds, {workers} workers"
    );
    Ok(())
}

fn parse_u64(
    value: Option<std::ffi::OsString>,
    usage: &str,
) -> Result<u64, Box<dyn std::error::Error>> {
    Ok(value
        .and_then(|value| value.into_string().ok())
        .ok_or(usage)?
        .parse()?)
}

fn parse_usize(
    value: Option<std::ffi::OsString>,
    usage: &str,
) -> Result<usize, Box<dyn std::error::Error>> {
    Ok(value
        .and_then(|value| value.into_string().ok())
        .ok_or(usage)?
        .parse()?)
}

fn write(output: &Path, name: &str, contents: &str) -> Result<(), std::io::Error> {
    std::fs::write(output.join(name), contents.as_bytes())
}
