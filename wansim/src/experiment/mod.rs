//! Reproducible stage experiments built on the validated simulator substrate.

mod w1_experiment0;
mod w2_straggler;
mod w3_coupling;
mod wr_realistic;

pub use w1_experiment0::{
    Experiment0Artifacts, Experiment0Error, Experiment0Screen, run_experiment0,
    run_experiment0_screen,
};
pub use w2_straggler::{W2Artifacts, W2ExperimentError, run_w2_experiment};
pub use w3_coupling::{W3Artifacts, W3ExperimentError, run_w3_experiment};
pub use wr_realistic::{
    WrArtifacts, WrExperimentError, WrPersistentRun, run_wr_experiment,
    run_wr_experiment_persistent,
};
