//! Reproducible stage experiments built on the validated simulator substrate.

mod w1_experiment0;
mod w2_straggler;

pub use w1_experiment0::{
    Experiment0Artifacts, Experiment0Error, Experiment0Screen, run_experiment0,
    run_experiment0_screen,
};
pub use w2_straggler::{W2Artifacts, W2ExperimentError, run_w2_experiment};
