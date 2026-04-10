//! Deterministic helpers shared by METTLE sender and receiver.

#[must_use]
pub fn mix64(mut x: u64) -> u64 {
    x ^= x >> 30;
    x = x.wrapping_mul(0xBF58_476D_1CE4_E5B9);
    x ^= x >> 27;
    x = x.wrapping_mul(0x94D0_49BB_1331_11EB);
    x ^ (x >> 31)
}

#[must_use]
pub fn derive_u64(seed: u64, source_id: u64, edge_index: u8, salt: u64) -> u64 {
    mix64(
        seed ^ source_id.wrapping_mul(0x9E37_79B9_7F4A_7C15)
            ^ u64::from(edge_index).wrapping_mul(0xBF58_476D_1CE4_E5B9)
            ^ salt,
    )
}

#[must_use]
pub fn source_signature(seed: u64, source_id: u64) -> u64 {
    let sig = derive_u64(seed ^ 0xA5A5_A5A5_A5A5_A5A5, source_id, 0, 0xD1CE_BA5E);
    if sig == 0 { 1 } else { sig }
}

#[must_use]
pub fn uniform_below(seed: u64, source_id: u64, edge_index: u8, salt: u64, upper: u64) -> u64 {
    if upper == 0 {
        return 0;
    }
    derive_u64(seed, source_id, edge_index, salt) % upper
}
