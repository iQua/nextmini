use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

const BYTES_PER_GB: u64 = 1_000_000_000;

#[derive(Clone, Debug, Default)]
pub(crate) struct EgressLedger {
    inner: Arc<EgressLedgerInner>,
}

#[derive(Debug, Default)]
struct EgressLedgerInner {
    wire_bytes: AtomicU64,
    weighted_numerator: AtomicU64,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct EgressSnapshot {
    pub(crate) wire_bytes: u64,
    pub(crate) modeled_nano_usd: u64,
}

impl EgressLedger {
    pub(crate) fn charge(
        &self,
        wire_bytes: usize,
        nano_usd_per_gb: u64,
    ) -> Result<(), EgressLedgerOverflow> {
        let wire_bytes = u64::try_from(wire_bytes).map_err(|_| EgressLedgerOverflow)?;
        let weighted = wire_bytes
            .checked_mul(nano_usd_per_gb)
            .ok_or(EgressLedgerOverflow)?;
        checked_add(&self.inner.wire_bytes, wire_bytes)?;
        checked_add(&self.inner.weighted_numerator, weighted)
    }

    pub(crate) fn snapshot(&self) -> EgressSnapshot {
        EgressSnapshot {
            wire_bytes: self.inner.wire_bytes.load(Ordering::Relaxed),
            modeled_nano_usd: self.inner.weighted_numerator.load(Ordering::Relaxed) / BYTES_PER_GB,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct EgressLedgerOverflow;

fn checked_add(target: &AtomicU64, amount: u64) -> Result<(), EgressLedgerOverflow> {
    target
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
            current.checked_add(amount)
        })
        .map(|_| ())
        .map_err(|_| EgressLedgerOverflow)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wire_cost_accumulates_exact_integer_nano_usd() {
        let ledger = EgressLedger::default();
        ledger
            .charge(250_000_000, 20_000_000)
            .expect("first charge");
        ledger
            .charge(750_000_000, 10_000_000)
            .expect("second charge");
        assert_eq!(
            ledger.snapshot(),
            EgressSnapshot {
                wire_bytes: 1_000_000_000,
                modeled_nano_usd: 12_500_000,
            }
        );
    }
}
