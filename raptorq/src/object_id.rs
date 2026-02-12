//! Minimal object identity type used by decoder/proof APIs.

use core::fmt;

/// A 128-bit object identifier split into high/low words.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct ObjectId {
    high: u64,
    low: u64,
}

impl ObjectId {
    /// The nil (zero) object ID.
    pub const NIL: Self = Self { high: 0, low: 0 };

    /// Creates a new object ID from high/low 64-bit words.
    #[must_use]
    pub const fn new(high: u64, low: u64) -> Self {
        Self { high, low }
    }

    /// Creates an object ID from a 128-bit integer.
    #[must_use]
    pub const fn from_u128(value: u128) -> Self {
        Self {
            high: (value >> 64) as u64,
            low: value as u64,
        }
    }

    /// Returns this object ID as a 128-bit integer.
    #[must_use]
    pub const fn as_u128(self) -> u128 {
        ((self.high as u128) << 64) | (self.low as u128)
    }

    /// Returns the high 64 bits.
    #[must_use]
    pub const fn high(self) -> u64 {
        self.high
    }

    /// Returns the low 64 bits.
    #[must_use]
    pub const fn low(self) -> u64 {
        self.low
    }

    /// Creates a random object ID from a deterministic RNG.
    #[must_use]
    pub fn new_random(rng: &mut crate::deterministic::DetRng) -> Self {
        Self {
            high: rng.next_u64(),
            low: rng.next_u64(),
        }
    }

    /// Creates an object ID for tests.
    #[doc(hidden)]
    #[must_use]
    pub const fn new_for_test(value: u64) -> Self {
        Self {
            high: 0,
            low: value,
        }
    }
}

impl fmt::Debug for ObjectId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ObjectId({:016x}{:016x})", self.high, self.low)
    }
}

impl fmt::Display for ObjectId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Obj-{:08x}", (self.high >> 32) as u32)
    }
}
