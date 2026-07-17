//! Standalone deterministic ideal-DoF simulation of pooled cross-tree FEC speedup.
//!
//! This executable produces model-level evidence, not codec results and not WAN measurements.
//! It deliberately lives in `mettle/examples` because the abstraction is codec-independent but
//! follows the existing METTLE research-harness discipline without linking dataplane production.

#[path = "../tests/support/pooling_speedup_model.rs"]
mod pooling_speedup_model;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    pooling_speedup_model::run_cli(std::env::args().skip(1))?;
    Ok(())
}
