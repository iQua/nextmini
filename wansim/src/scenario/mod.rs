mod chain;
mod config;
mod tree;
mod tree_config;

pub use chain::{ChainOutcome, ChainRunError, run_chain};
pub use config::{ChainScenario, RegistrationOrder, ScenarioError};
pub use tree::{TreeOutcome, TreeRunError, run_tree};
pub use tree_config::{
    FanoutAdmission, ReceiverTiming, TreeEndpoint, TreeScenario, TreeScenarioError,
};
