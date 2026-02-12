/// Deterministic helper for reproducible symbol scheduling during tests and proofs.
#[derive(Debug, Clone)]
pub struct DeterministicRng {
    state: u64,
}

impl DeterministicRng {
    /// Creates a deterministic pseudo-random stream from a stable seed.
    pub const fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    /// Produces the next `u64` in the stream.
    pub fn next_u64(&mut self) -> u64 {
        // SplitMix64 step with fixed constants for reproducibility.
        self.state = self.state.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut value = self.state;
        value ^= value >> 30;
        value = value.wrapping_mul(0xbf58_476d_1ce4_e5b9);
        value ^= value >> 27;
        value = value.wrapping_mul(0x94d0_49bb_1331_11eb);
        value ^ (value >> 31)
    }
}

#[cfg(test)]
mod tests {
    use super::DeterministicRng;

    #[test]
    fn stream_is_reproducible() {
        let mut left = DeterministicRng::new(7);
        let mut right = DeterministicRng::new(7);
        for _ in 0..16 {
            assert_eq!(left.next_u64(), right.next_u64());
        }
    }
}
