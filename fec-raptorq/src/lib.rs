//! Core RaptorQ FEC primitives for staged migration into nextmini.
//!
//! This crate is runtime-agnostic and only depends on Rust std/core.

pub mod decoder;
pub mod deterministic;
pub mod gf256;
pub mod linalg;
pub mod object_id;
pub mod primitives;
pub mod proof;
pub mod rfc6330;
pub mod systematic;

pub use decoder::{
    DecodeError, DecodeResult, DecodeResultWithProof, DecodeStats, InactivationDecoder,
    ReceivedSymbol,
};
pub use deterministic::{
    DetBuildHasher, DetHashMap, DetHashSet, DetHasher, DetRng, DeterministicRng,
};
pub use object_id::ObjectId;
pub use primitives::{BlockId, SymbolId};
pub use proof::{
    DecodeConfig, DecodeProof, DecodeProofBuilder, FailureReason, ProofOutcome, ReplayError,
};
pub use systematic::{
    ConstraintMatrix, EmittedSymbol, EncodingStats, RobustSoliton, SystematicEncoder,
    SystematicParams,
};
