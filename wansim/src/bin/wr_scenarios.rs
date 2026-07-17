use std::error::Error;
use std::fs;
use std::path::PathBuf;

use wansim::scenario::CloudScenario;

fn main() {
    if let Err(error) = run() {
        eprintln!("wr_scenarios: {error}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), Box<dyn Error>> {
    let output = std::env::args_os()
        .nth(1)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("scenarios/wr"));
    fs::create_dir_all(&output)?;
    for scenario in CloudScenario::all_built_in()? {
        fs::write(
            output.join(scenario.file_name()),
            scenario.to_commented_toml()?,
        )?;
    }
    Ok(())
}
