use std::path::{Path, PathBuf};
use std::process::ExitCode;

use wansim::experiment::{WrArtifacts, run_wr_experiment_persistent};

const USAGE: &str = "usage: wr_realistic <seeds>=16.. <workers> <output-directory> [--resume]";

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
    let resume = match args.next().as_deref() {
        None => false,
        Some(value) if value == "--resume" => true,
        Some(_) => return Err(USAGE.into()),
    };
    if args.next().is_some() {
        return Err(USAGE.into());
    }
    let run = run_wr_experiment_persistent(seeds, workers, &output, resume)?;
    write_atomic(&output, "failures.csv", &run.failures_csv)?;
    if let Some(artifacts) = run.artifacts {
        write_artifacts(&output, &artifacts)?;
    }
    println!(
        "WR cells total={} successful={} failed={} skipped={} seeds={} workers={}",
        run.total_cells, run.successful_cells, run.failed_cells, run.skipped_cells, seeds, workers
    );
    if run.failed_cells != 0 {
        return Err(format!(
            "{} WR cell(s) failed; completed cell shards and failures.csv are durable",
            run.failed_cells
        )
        .into());
    }
    Ok(())
}

fn write_artifacts(output: &Path, artifacts: &WrArtifacts) -> Result<(), std::io::Error> {
    for (name, contents) in [
        ("trials.csv", artifacts.trials_csv.as_str()),
        ("summaries.csv", artifacts.summaries_csv.as_str()),
        (
            "advantage-decomposition.csv",
            artifacts.advantage_csv.as_str(),
        ),
        ("rounds-vs-carousel.csv", artifacts.rounds_csv.as_str()),
        ("a4-correlation.csv", artifacts.a4_csv.as_str()),
        ("straggler.csv", artifacts.straggler_csv.as_str()),
        ("cadence.csv", artifacts.cadence_csv.as_str()),
        ("concurrent-sessions.csv", artifacts.concurrent_csv.as_str()),
        ("k65536-scaling.csv", artifacts.scaling_csv.as_str()),
        ("sharing-structure.csv", artifacts.sharing_csv.as_str()),
    ] {
        write_atomic(output, name, contents)?;
    }
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

fn write_atomic(output: &Path, name: &str, contents: &str) -> Result<(), std::io::Error> {
    let final_path = output.join(name);
    let temporary_path = output.join(format!(".{name}.tmp"));
    std::fs::write(&temporary_path, contents.as_bytes())?;
    std::fs::File::open(&temporary_path)?.sync_all()?;
    std::fs::rename(temporary_path, final_path)?;
    std::fs::File::open(output)?.sync_all()
}
