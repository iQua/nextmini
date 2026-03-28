//! Wire protocol definitions for dataplane lossless sessions.
//!
//! Version 5 is the flag-day `Manifest -> Ready -> payload sweep ->
//! SourceDone -> Need` protocol. The normative rewrite rules live in
//! `plans/simple-lossless.md`.

mod block_frames;
mod control_frames;
mod header;
#[cfg(test)]
mod test_support;
mod types;
mod validation;

pub use block_frames::{
    decode_block_data, decode_block_symbol, encode_block_data, encode_block_symbol,
    encode_block_symbol_into, set_block_symbol_tree_id,
};
pub use control_frames::{
    MAX_CONTROL_FRAME_SIZE, decode_control, encode_control, encode_control_into,
};
pub use header::{LosslessSessionHeader, LosslessSessionRawHeader, peek_header};
pub use types::*;
