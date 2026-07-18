use std::path::{Path, PathBuf};
use std::process::ExitCode;

use wansim::experiment::run_fixed_wr_triage;

const USAGE: &str = "usage: wr_triage <output-directory>";

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("wr_triage: {error}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args_os().skip(1);
    let output = args.next().map(PathBuf::from).ok_or(USAGE)?;
    if args.next().is_some() {
        return Err(USAGE.into());
    }
    std::fs::create_dir_all(&output)?;
    let artifacts = run_fixed_wr_triage()?;
    for (name, contents) in [
        ("causal-events.csv", artifacts.causal_events_csv.as_str()),
        (
            "fatal-window-events.csv",
            artifacts.fatal_window_events_csv.as_str(),
        ),
        ("timeline-250ms.csv", artifacts.timeline_csv.as_str()),
        (
            "event-class-counts.csv",
            artifacts.event_class_counts_csv.as_str(),
        ),
        ("summary.csv", artifacts.summary_csv.as_str()),
    ] {
        write_atomic(&output, name, contents)?;
    }
    println!("WR triage verdict: {}", artifacts.verdict);
    Ok(())
}

fn write_atomic(output: &Path, name: &str, contents: &str) -> Result<(), std::io::Error> {
    let final_path = output.join(name);
    let temporary_path = output.join(format!(".{name}.tmp"));
    std::fs::write(&temporary_path, contents.as_bytes())?;
    std::fs::File::open(&temporary_path)?.sync_all()?;
    std::fs::rename(temporary_path, final_path)?;
    std::fs::File::open(output)?.sync_all()
}
