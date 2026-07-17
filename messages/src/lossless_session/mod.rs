//! Wire protocol definitions for dataplane lossless sessions.
//!
//! # Version and layout history
//!
//! | Version | Fixed header | Manifest body | Completion controls |
//! |---|---|---|---|
//! | 6 | 20 bytes | 32-bit FEC `symbols_per_block` | `SourceDone` / `Need` rounds |
//! | 7 | 20 bytes | Adds finite-stream FEC coded-rate numerator and denominator | `SourceDone` / `Need` rounds |
//! | 8 | 20 bytes | `mode:u8, scheme:u8, tree_count:u8, feedback:u8, block_size:u32, total_bytes:u64, total_blocks:u64, symbols_per_block:u32, coded_rate_num:u32, coded_rate_den:u32, tree_ids:[u16; tree_count]` | Rounds remain available; carousel adds cumulative completion controls |
//! | 9 | 20 bytes | V8 fields plus `object_symbol_bytes:u32, object_sources_per_stream:u32, object_stream_count:u64, object_final_stream_sources:u32` before `tree_ids` | Carousel adds METTLE per-stream progress/stall acknowledgements |
//! | 10 | 20 bytes | V9 manifest | Carousel adds epoch-tagged METTLE departure checkpoints |
//!
//! Versions are flag-day incompatible: every header is checked against
//! [`LOSSLESS_SESSION_VERSION`], and manifest decoding rejects unknown modes.
//! The normative v10 state machines live in `plans/perfect-fec-runtime.md` §P.
//! Carousel control IDs are `7 = BlockAck`, `8 = AckProbe`, and
//! `9 = SessionComplete`; METTLE also uses `10 = DepartureCheckpoint`.
//! `BlockAck` variant 1 contains
//! `variant:u8, reserved:u8, range_count:u16, completed_watermark:u64`, then
//! `range_count` half-open `(start:u64, end:u64)` ranges. Variant 2 contains
//! `flags:u8, range_count:u16, stream_id:u64, decoded_source_watermark:u32,
//! repair_epoch:u32`, followed by epoch-tagged half-open missing-bin ranges.
//! `AckProbe` carries one `target_peer_id:u64`, so multicast control routing
//! solicits only the missing peer named by the sender.

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
