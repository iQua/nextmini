use std::path::{Path, PathBuf};
use std::process::ExitCode;

use wansim::experiment::{run_experiment0, run_experiment0_screen};

const USAGE: &str = "usage:\n  w1_experiment0 screen <output-directory>\n  w1_experiment0 sweep <seed-count> <output-directory> --accept-source-done-overtake";

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("w1_experiment0: {error}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args_os().skip(1);
    let mode = args.next().and_then(|value| value.into_string().ok());
    match mode.as_deref() {
        Some("screen") => {
            let output = args.next().map(PathBuf::from).ok_or(USAGE)?;
            if args.next().is_some() {
                return Err(USAGE.into());
            }
            let screen = run_experiment0_screen()?;
            create_output(&output)?;
            write(&output, "screening.csv", &screen.screening_csv)?;
            write(&output, "screen-verdict.csv", &screen.verdict_csv)?;
            if screen.prediction_failed {
                println!(
                    "E0-a failed as screened; sweep stopped. Review screen-verdict.csv before the explicit diagnosed resume."
                );
            } else {
                println!("E0-a passed its stopping screen.");
            }
        }
        Some("sweep") => {
            let seed_count: u64 = args
                .next()
                .and_then(|value| value.into_string().ok())
                .ok_or(USAGE)?
                .parse()?;
            let output = args.next().map(PathBuf::from).ok_or(USAGE)?;
            let acceptance = args.next().and_then(|value| value.into_string().ok());
            if acceptance.as_deref() != Some("--accept-source-done-overtake")
                || args.next().is_some()
            {
                return Err(USAGE.into());
            }
            let artifacts = run_experiment0(seed_count)?;
            create_output(&output)?;
            write(&output, "trials.csv", &artifacts.trials_csv)?;
            write(&output, "summary.csv", &artifacts.summaries_csv)?;
            write(&output, "predictions.csv", &artifacts.predictions_csv)?;
            println!("wrote deterministic experiment 0 sweep for {seed_count} seeds");
        }
        _ => return Err(USAGE.into()),
    }
    Ok(())
}

fn create_output(output: &Path) -> Result<(), std::io::Error> {
    std::fs::create_dir_all(output)
}

fn write(output: &Path, name: &str, contents: &str) -> Result<(), std::io::Error> {
    std::fs::write(output.join(name), contents.as_bytes())
}
