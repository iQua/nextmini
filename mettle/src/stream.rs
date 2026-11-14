//! Streaming METTLE encoder API.
//!
//! This is the paper-native encoder surface: callers push source packets in
//! order and receive finalized coded bins in increasing bin-id order.

use std::num::NonZeroUsize;
use std::sync::Arc;

use crate::MettleParams;
use crate::decoder::{DecodedSource as InnerDecodedSource, MettleDecoder};
use crate::encoder::{MettleBin, MettleEncoder};

/// One finalized METTLE coded bin.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EncodedBin {
    bin_id: u128,
    payload: Vec<u8>,
}

impl EncodedBin {
    /// Return this coded bin's paper bin id.
    #[must_use]
    pub const fn bin_id(&self) -> u128 {
        self.bin_id
    }

    /// Consume the bin into its id and payload.
    #[must_use]
    pub fn into_parts(self) -> (u128, Vec<u8>) {
        (self.bin_id, self.payload)
    }
}

impl From<MettleBin> for EncodedBin {
    fn from(bin: MettleBin) -> Self {
        let (bin_id, payload) = bin.into_parts();
        Self { bin_id, payload }
    }
}

/// One decoded METTLE source packet released by the streaming peeling decoder.
///
/// The payload is held behind an `Arc` so the decoder can share the same
/// byte buffer between its coupling-window prefix (used to XOR future bins)
/// and the value handed back to the consumer. Cloning a `DecodedSource` is a
/// reference-count bump, not an `O(symbol_size)` memcpy.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DecodedSource {
    source_id: u64,
    payload: Arc<Vec<u8>>,
}

impl DecodedSource {
    /// Return this source packet's stream position.
    #[must_use]
    pub const fn source_id(&self) -> u64 {
        self.source_id
    }

    /// Consume the decoded packet into its source id and refcounted payload.
    ///
    /// The returned `Arc<Vec<u8>>` may still be referenced by the decoder's
    /// internal coupling-window prefix; callers should treat the bytes as
    /// read-only and access them via `as_slice()` / deref. To take an owned
    /// `Vec<u8>` (incurring one memcpy), use `(*payload).clone()`.
    #[must_use]
    pub fn into_parts(self) -> (u64, Arc<Vec<u8>>) {
        (self.source_id, self.payload)
    }
}

impl From<InnerDecodedSource> for DecodedSource {
    fn from(source: InnerDecodedSource) -> Self {
        let (source_id, payload) = source.into_parts();
        Self { source_id, payload }
    }
}

/// Incremental METTLE encoder for a source stream or terminated source prefix.
#[derive(Debug)]
pub struct Encoder {
    inner: MettleEncoder,
}

impl Encoder {
    /// Construct an unterminated streaming encoder.
    #[must_use]
    pub fn new(params: MettleParams, source_symbol_bytes: NonZeroUsize, seed: u64) -> Self {
        Self {
            inner: MettleEncoder::new(params, source_symbol_bytes, seed),
        }
    }

    /// Construct an encoder for a finite source prefix of known length.
    #[must_use]
    pub fn new_terminated(
        params: MettleParams,
        source_symbol_bytes: NonZeroUsize,
        seed: u64,
        terminal_source_count: u64,
    ) -> Self {
        Self {
            inner: MettleEncoder::new_terminated(
                params,
                source_symbol_bytes,
                seed,
                terminal_source_count,
            ),
        }
    }

    /// Push the next source symbol and return newly finalized coded bins.
    pub fn push_source(&mut self, payload: &[u8]) -> Vec<EncodedBin> {
        self.inner
            .push_source(payload)
            .into_iter()
            .map(EncodedBin::from)
            .collect()
    }

    /// Finish a finite stream and return the remaining finalized coded bins.
    #[must_use]
    pub fn finish(self) -> Vec<EncodedBin> {
        self.inner
            .finish()
            .into_iter()
            .map(EncodedBin::from)
            .collect()
    }
}

/// Incremental METTLE peeling decoder for a source stream or terminated prefix.
#[derive(Debug)]
pub struct Decoder {
    inner: MettleDecoder,
}

impl Decoder {
    /// Construct an unterminated streaming decoder.
    #[must_use]
    pub fn new(params: MettleParams, source_symbol_bytes: NonZeroUsize, seed: u64) -> Self {
        Self {
            inner: MettleDecoder::new(params, source_symbol_bytes, seed),
        }
    }

    /// Construct a decoder for a finite source prefix of known length.
    ///
    /// Pre-computes the dense Tanner graph (source-to-bins and bin-to-sources)
    /// up front. This is identical, paper-faithful XOR-edge information that
    /// the rolling-graph path would otherwise rebuild lazily inside a BTreeMap
    /// keyed by bin id. For finite streams the upfront cost is `O(N * l)` and
    /// the per-bin lookup drops from `O(log B)` BTreeMap probes to a single
    /// `Vec` index, which matters in WAN throughput experiments where many
    /// bins are received within the receive task's tokio loop.
    #[must_use]
    pub fn new_terminated(
        params: MettleParams,
        source_symbol_bytes: NonZeroUsize,
        seed: u64,
        terminal_source_count: u64,
    ) -> Self {
        Self {
            inner: MettleDecoder::new_terminated_with_precomputed_graph(
                params,
                source_symbol_bytes,
                seed,
                terminal_source_count,
            ),
        }
    }

    /// Push one received coded bin and return any newly released source prefix.
    pub fn push_bin(&mut self, bin_id: u128, payload: Vec<u8>) -> Vec<DecodedSource> {
        self.inner
            .push_bin(MettleBin::new(bin_id, payload))
            .into_iter()
            .map(DecodedSource::from)
            .collect()
    }

    /// Return the next not-yet-released source id.
    #[must_use]
    pub fn next_source_id(&self) -> u64 {
        self.inner.next_source_id()
    }
}
