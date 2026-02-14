//! Deterministic randomness + hashing helpers used by migrated RaptorQ core.

use core::hash::{BuildHasher, Hasher};

/// A deterministic pseudo-random generator using xorshift64.
///
/// This is intentionally simple and reproducible. It is not cryptographically secure.
#[derive(Debug, Clone)]
pub struct DetRng {
    state: u64,
}

impl DetRng {
    /// Creates a new generator from a stable seed.
    #[must_use]
    pub const fn new(seed: u64) -> Self {
        Self {
            state: if seed == 0 { 1 } else { seed },
        }
    }

    /// Returns the next deterministic `u64`.
    pub fn next_u64(&mut self) -> u64 {
        let mut x = self.state;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.state = x;
        x
    }

    /// Returns the next deterministic `u32`.
    #[must_use]
    pub fn next_u32(&mut self) -> u32 {
        (self.next_u64() >> 32) as u32
    }

    /// Returns a deterministic index in `[0, bound)`.
    ///
    /// Uses rejection sampling to avoid modulo bias.
    pub fn next_usize(&mut self, bound: usize) -> usize {
        assert!(bound > 0, "bound must be non-zero");
        let bound_u64 = bound as u64;
        let threshold = u64::MAX - (u64::MAX % bound_u64);
        loop {
            let value = self.next_u64();
            if value < threshold {
                return (value % bound_u64) as usize;
            }
        }
    }

    /// Returns the next deterministic boolean.
    #[must_use]
    pub fn next_bool(&mut self) -> bool {
        self.next_u64() & 1 == 1
    }

    /// Fills a mutable byte slice with deterministic bytes.
    pub fn fill_bytes(&mut self, dest: &mut [u8]) {
        let mut i = 0;
        while i < dest.len() {
            let rand = self.next_u64();
            let bytes = rand.to_le_bytes();
            let n = core::cmp::min(dest.len() - i, 8);
            dest[i..i + n].copy_from_slice(&bytes[..n]);
            i += n;
        }
    }

    /// Shuffles a slice in place using Fisher-Yates.
    pub fn shuffle<T>(&mut self, slice: &mut [T]) {
        for i in (1..slice.len()).rev() {
            let j = self.next_usize(i + 1);
            slice.swap(i, j);
        }
    }
}

/// Backward-compatible alias for earlier scaffolding.
pub type DeterministicRng = DetRng;

/// Deterministic non-cryptographic hasher.
#[derive(Debug, Clone)]
pub struct DetHasher {
    state: u64,
}

impl DetHasher {
    const SEED: u64 = 0x16f1_1fe8_9b0d_677c;
    const MULTIPLIER: u64 = 0x517c_c1b7_2722_0a95;

    #[inline]
    fn mix_byte(&mut self, byte: u8) {
        self.state = self.state.wrapping_mul(Self::MULTIPLIER);
        self.state ^= u64::from(byte);
    }
}

impl Default for DetHasher {
    fn default() -> Self {
        Self { state: Self::SEED }
    }
}

impl Hasher for DetHasher {
    fn write(&mut self, bytes: &[u8]) {
        for &byte in bytes {
            self.mix_byte(byte);
        }
    }

    fn write_u8(&mut self, i: u8) {
        self.mix_byte(i);
    }

    fn write_u16(&mut self, i: u16) {
        let bytes = i.to_ne_bytes();
        self.mix_byte(bytes[0]);
        self.mix_byte(bytes[1]);
    }

    fn write_u32(&mut self, i: u32) {
        for b in i.to_ne_bytes() {
            self.mix_byte(b);
        }
    }

    fn write_u64(&mut self, i: u64) {
        for b in i.to_ne_bytes() {
            self.mix_byte(b);
        }
    }

    fn write_u128(&mut self, i: u128) {
        for b in i.to_ne_bytes() {
            self.mix_byte(b);
        }
    }

    fn finish(&self) -> u64 {
        let mut h = self.state;
        h ^= h >> 33;
        h = h.wrapping_mul(0xff51_afd7_ed55_8ccd);
        h ^= h >> 33;
        h = h.wrapping_mul(0xc4ce_b9fe_1a85_ec53);
        h ^= h >> 33;
        h
    }
}

/// BuildHasher for deterministic maps/sets.
#[derive(Clone, Default)]
pub struct DetBuildHasher;

impl BuildHasher for DetBuildHasher {
    type Hasher = DetHasher;

    fn build_hasher(&self) -> Self::Hasher {
        DetHasher::default()
    }
}

/// Deterministic-hasher `HashMap`.
pub type DetHashMap<K, V> = std::collections::HashMap<K, V, DetBuildHasher>;
/// Deterministic-hasher `HashSet`.
pub type DetHashSet<K> = std::collections::HashSet<K, DetBuildHasher>;

/// Deterministic ordered collection re-exports.
pub use std::collections::{BTreeMap, BTreeSet};

#[cfg(test)]
mod tests {
    use super::{DetHasher, DetRng};
    use core::hash::{Hash, Hasher};

    #[test]
    fn det_rng_stream_is_reproducible() {
        let mut left = DetRng::new(7);
        let mut right = DetRng::new(7);
        for _ in 0..32 {
            assert_eq!(left.next_u64(), right.next_u64());
        }
    }

    #[test]
    fn det_hasher_is_reproducible() {
        let mut h1 = DetHasher::default();
        "nextmini".hash(&mut h1);
        let mut h2 = DetHasher::default();
        "nextmini".hash(&mut h2);
        assert_eq!(h1.finish(), h2.finish());
    }
}
