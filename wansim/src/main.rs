use std::path::PathBuf;
use std::process::ExitCode;

use wansim::scenario::{ChainScenario, run_chain};

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("wansim: {error}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args_os().skip(1);
    let scenario_path = args
        .next()
        .map(PathBuf::from)
        .ok_or("usage: wansim <scenario.toml> <output.csv>")?;
    let output_path = args
        .next()
        .map(PathBuf::from)
        .ok_or("usage: wansim <scenario.toml> <output.csv>")?;
    if args.next().is_some() {
        return Err("usage: wansim <scenario.toml> <output.csv>".into());
    }

    let scenario = ChainScenario::from_path(&scenario_path)?;
    let outcome = run_chain(&scenario)?;
    std::fs::write(output_path, outcome.csv.as_bytes())?;
    Ok(())
}
