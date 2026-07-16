//! Wire protocol definitions for dataplane lossless sessions.
//!
//! # Version and layout history
//!
//! | Version | Fixed header | Manifest body | Completion controls |
//! |---|---|---|---|
//! | 6 | 20 bytes | 32-bit FEC `symbols_per_block` | `SourceDone` / `Need` rounds |
//! | 7 | 20 bytes | Adds finite-stream FEC coded-rate numerator and denominator | `SourceDone` / `Need` rounds |
//! | 8 | 20 bytes | `mode:u8, scheme:u8, tree_count:u8, feedback:u8, block_size:u32, total_bytes:u64, total_blocks:u64, symbols_per_block:u32, coded_rate_num:u32, coded_rate_den:u32, tree_ids:[u16; tree_count]` | Rounds remain available; carousel adds cumulative completion controls |
//!
//! Versions are flag-day incompatible: every header is checked against
//! [`LOSSLESS_SESSION_VERSION`], and manifest decoding rejects unknown modes.
//! The normative v8 state machines live in `plans/perfect-fec-runtime.md` §P.
//! V8 carousel control IDs are `7 = BlockAck`, `8 = AckProbe`, and
//! `9 = SessionComplete`. `BlockAck` variant 1 contains
//! `variant:u8, reserved:u8, range_count:u16, completed_watermark:u64`, then
//! `range_count` half-open `(start:u64, end:u64)` ranges. Variant 2 is reserved
//! for METTLE stream progress and is rejected until that layout is specified.

mod block_frames;
mod control_frames;
mod fec_geometry;
mod header;
#[cfg(test)]
mod test_support;
mod types;
mod validation;

pub use block_frames::{
    decode_block_data, decode_block_symbol, encode_block_data, encode_block_symbol,
};
pub use control_frames::{decode_control, encode_control};
pub use fec_geometry::*;
pub use header::{LosslessSessionHeader, LosslessSessionRawHeader, peek_header};
pub use types::*;
