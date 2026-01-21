//! Serde helpers for compact `Ipv4Addr` encoding.
//!
//! `toml` and `rmp-serde` can serialize `Ipv4Addr` in different formats; these helpers ensure we
//! use a stable 4-byte representation on the wire and in config files when needed.

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::net::Ipv4Addr;

pub fn serialize<S>(ip: &Ipv4Addr, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    // Directly serialize the 4 octets as a byte array
    ip.octets().serialize(serializer)
}

pub fn deserialize<'de, D>(deserializer: D) -> Result<Ipv4Addr, D::Error>
where
    D: Deserializer<'de>,
{
    // Expect exactly 4 bytes, then reconstruct
    let bytes: [u8; 4] = Deserialize::deserialize(deserializer)?;
    Ok(Ipv4Addr::from(bytes))
}
