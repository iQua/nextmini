use std::collections::BTreeMap;
use std::sync::Arc;

use parking_lot::Mutex;

#[derive(Clone, Debug, Default)]
pub(crate) struct OwnershipLedger {
    owners: Arc<Mutex<BTreeMap<&'static str, usize>>>,
}

impl OwnershipLedger {
    pub(crate) fn set(&self, owner: &'static str, bytes: usize) {
        self.owners.lock().insert(owner, bytes);
    }

    pub(crate) fn get(&self, owner: &'static str) -> usize {
        self.owners.lock().get(owner).copied().unwrap_or(0)
    }

    pub(crate) fn snapshot(&self) -> BTreeMap<&'static str, usize> {
        self.owners.lock().clone()
    }
}
