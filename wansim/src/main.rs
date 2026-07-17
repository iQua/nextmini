use std::path::PathBuf;
use std::process::ExitCode;

use wansim::scenario::{ChainScenario, TreeScenario, run_chain, run_tree};

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

    let source = std::fs::read_to_string(&scenario_path)?;
    let document: toml::Value = toml::from_str(&source)?;
    if document.get("scenario_kind").and_then(toml::Value::as_str) == Some("tree") {
        let scenario = TreeScenario::from_path(&scenario_path)?;
        let outcome = run_tree(&scenario)?;
        std::fs::write(output_path, outcome.csv.as_bytes())?;
    } else {
        let scenario = ChainScenario::from_path(&scenario_path)?;
        let outcome = run_chain(&scenario)?;
        std::fs::write(output_path, outcome.csv.as_bytes())?;
    }
    Ok(())
}
