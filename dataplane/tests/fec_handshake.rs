use std::collections::BTreeMap;

use nextmini::node::session::control::should_abort_fec_preflight;
use nextmini_messages::lossless_session::{FecCapabilities, FecManifest};

#[test]
fn aborts_when_peer_incompatible() {
    let manifest = FecManifest::new_raptorq(64, 1400);
    let required = vec![11usize, 12usize];

    let mut capabilities = BTreeMap::new();
    capabilities.insert(11usize, FecCapabilities::default());
    capabilities.insert(12usize, FecCapabilities::empty());

    assert!(
        should_abort_fec_preflight(&required, &manifest, &capabilities),
        "strict FEC session must abort before first FEC data frame when any required peer is incompatible"
    );
}
