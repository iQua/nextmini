#![cfg_attr(not(test), allow(dead_code))]

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::num::NonZeroUsize;
use std::sync::Arc;

use crate::MettleParams;
use crate::encoder::MettleBin;

/// Shared, refcounted view of a decoded source payload.
///
/// The decoder needs to hold each decoded source's bytes until the coupling
/// window slides past them so future bins can XOR them out. The caller also
/// receives the bytes via `DecodedSource`. We share both views through `Arc`
/// so the decoder does not have to clone the payload on release; the cost
/// drops from one `symbol_size`-byte memcpy per decoded source to a
/// reference-count bump.
pub(crate) type DecodedPayload = Arc<Vec<u8>>;

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct DecodedSource {
    source_id: u64,
    payload: DecodedPayload,
}

impl DecodedSource {
    pub(crate) fn into_parts(self) -> (u64, DecodedPayload) {
        (self.source_id, self.payload)
    }

    #[cfg(test)]
    pub(super) fn as_parts(&self) -> (u64, &[u8]) {
        (self.source_id, self.payload.as_slice())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct MettleDecoderStats {
    pub(crate) next_source_id: u64,
    pub(crate) terminal_source_count: Option<u64>,
    pub(crate) received_bins: usize,
    pub(crate) ready_bins: usize,
    pub(crate) seen_bins: usize,
    pub(crate) decoded_future_sources: usize,
    pub(crate) decoded_prefix_sources: usize,
    pub(crate) decoded_prefix_start_source_id: u64,
    pub(crate) graph_bins: Option<usize>,
    pub(crate) bin_cleanup_frontier: u128,
}

#[derive(Debug)]
struct BufferedBin {
    payload: Vec<u8>,
    remaining_touchers: u16,
    undecoded_source_xor: u64,
}

impl BufferedBin {
    fn unique_source_id(&self) -> Option<u64> {
        (self.remaining_touchers == 1).then_some(self.undecoded_source_xor)
    }
}

#[derive(Debug)]
struct DecoderGraph {
    source_bins: Vec<SourceEdgeIds>,
    bin_touchers: Vec<Vec<u64>>,
}

impl DecoderGraph {
    fn bin_count(&self) -> usize {
        self.bin_touchers.len()
    }

    fn source_edge_ids(&self, source_id: u64) -> Option<SourceEdgeIds> {
        let source_index = usize::try_from(source_id).ok()?;
        self.source_bins.get(source_index).copied()
    }

    fn bin_touchers(&self, bin_id: u128) -> Option<&[u64]> {
        let bin_index = usize::try_from(bin_id).ok()?;
        self.bin_touchers.get(bin_index).map(Vec::as_slice)
    }
}

#[derive(Clone, Copy, Debug)]
struct SourceEdgeIds {
    bin_ids: [u128; MettleParams::EDGE_COUNT],
    count: usize,
}

impl SourceEdgeIds {
    fn new(bin_ids: [u128; MettleParams::EDGE_COUNT], count: usize) -> Self {
        Self { bin_ids, count }
    }
}

#[derive(Debug, Default)]
struct RollingDecoderGraph {
    indexed_source_count: u64,
    bin_touchers: BTreeMap<u128, Vec<u64>>,
}

impl RollingDecoderGraph {
    fn clear(&mut self) {
        self.bin_touchers.clear();
    }

    fn drop_before(&mut self, frontier: u128) {
        while self
            .bin_touchers
            .first_key_value()
            .is_some_and(|(&bin_id, _)| bin_id < frontier)
        {
            self.bin_touchers.pop_first();
        }
    }
}

#[derive(Debug)]
enum SeenBinIds {
    Sparse(BTreeSet<u128>),
    Dense(Vec<bool>),
}

impl SeenBinIds {
    fn new(bin_count: Option<usize>) -> Self {
        match bin_count {
            Some(bin_count) => Self::Dense(vec![false; bin_count]),
            None => Self::Sparse(BTreeSet::new()),
        }
    }

    fn insert(&mut self, bin_id: u128) -> bool {
        match self {
            Self::Sparse(seen_bin_ids) => seen_bin_ids.insert(bin_id),
            Self::Dense(seen_bin_ids) => {
                let Ok(bin_index) = usize::try_from(bin_id) else {
                    return true;
                };
                let Some(seen) = seen_bin_ids.get_mut(bin_index) else {
                    return true;
                };
                if *seen {
                    false
                } else {
                    *seen = true;
                    true
                }
            }
        }
    }

    fn clear(&mut self) {
        match self {
            Self::Sparse(seen_bin_ids) => seen_bin_ids.clear(),
            Self::Dense(seen_bin_ids) => seen_bin_ids.fill(false),
        }
    }

    fn drop_before(&mut self, frontier: u128) {
        if let Self::Sparse(seen_bin_ids) = self {
            while seen_bin_ids
                .first()
                .is_some_and(|&seen_bin_id| seen_bin_id < frontier)
            {
                seen_bin_ids.pop_first();
            }
        }
    }

    #[cfg(test)]
    fn contains(&self, bin_id: &u128) -> bool {
        match self {
            Self::Sparse(seen_bin_ids) => seen_bin_ids.contains(bin_id),
            Self::Dense(seen_bin_ids) => usize::try_from(*bin_id)
                .ok()
                .and_then(|bin_index| seen_bin_ids.get(bin_index))
                .copied()
                .unwrap_or(false),
        }
    }

    fn len(&self) -> usize {
        match self {
            Self::Sparse(seen_bin_ids) => seen_bin_ids.len(),
            Self::Dense(seen_bin_ids) => seen_bin_ids.iter().filter(|&&seen| seen).count(),
        }
    }

    #[cfg(test)]
    fn is_empty(&self) -> bool {
        match self {
            Self::Sparse(seen_bin_ids) => seen_bin_ids.is_empty(),
            Self::Dense(seen_bin_ids) => seen_bin_ids.iter().all(|&seen| !seen),
        }
    }
}

#[derive(Debug)]
enum ReceivedBins {
    Sparse(BTreeMap<u128, BufferedBin>),
    Dense(Vec<Option<BufferedBin>>),
}

impl ReceivedBins {
    fn new(bin_count: Option<usize>) -> Self {
        match bin_count {
            Some(bin_count) => {
                Self::Dense(std::iter::repeat_with(|| None).take(bin_count).collect())
            }
            None => Self::Sparse(BTreeMap::new()),
        }
    }

    fn insert(&mut self, bin_id: u128, bin: BufferedBin) {
        match self {
            Self::Sparse(received_bins) => {
                received_bins.insert(bin_id, bin);
            }
            Self::Dense(received_bins) => {
                if let Ok(bin_index) = usize::try_from(bin_id)
                    && let Some(slot) = received_bins.get_mut(bin_index)
                {
                    *slot = Some(bin);
                }
            }
        }
    }

    fn get(&self, bin_id: &u128) -> Option<&BufferedBin> {
        match self {
            Self::Sparse(received_bins) => received_bins.get(bin_id),
            Self::Dense(received_bins) => usize::try_from(*bin_id)
                .ok()
                .and_then(|bin_index| received_bins.get(bin_index))
                .and_then(Option::as_ref),
        }
    }

    fn get_mut(&mut self, bin_id: &u128) -> Option<&mut BufferedBin> {
        match self {
            Self::Sparse(received_bins) => received_bins.get_mut(bin_id),
            Self::Dense(received_bins) => usize::try_from(*bin_id)
                .ok()
                .and_then(|bin_index| received_bins.get_mut(bin_index))
                .and_then(Option::as_mut),
        }
    }

    fn remove(&mut self, bin_id: &u128) -> Option<BufferedBin> {
        match self {
            Self::Sparse(received_bins) => received_bins.remove(bin_id),
            Self::Dense(received_bins) => usize::try_from(*bin_id)
                .ok()
                .and_then(|bin_index| received_bins.get_mut(bin_index))
                .and_then(Option::take),
        }
    }

    fn clear(&mut self) {
        match self {
            Self::Sparse(received_bins) => received_bins.clear(),
            Self::Dense(received_bins) => {
                for bin in received_bins {
                    *bin = None;
                }
            }
        }
    }

    fn drop_before(&mut self, frontier: u128, cleanup_frontier: &mut u128) {
        match self {
            Self::Sparse(received_bins) => {
                while received_bins
                    .first_key_value()
                    .is_some_and(|(&bin_id, _)| bin_id < frontier)
                {
                    received_bins.pop_first();
                }
            }
            Self::Dense(received_bins) => {
                let start = usize::try_from(*cleanup_frontier)
                    .unwrap_or(usize::MAX)
                    .min(received_bins.len());
                let end = usize::try_from(frontier)
                    .unwrap_or(usize::MAX)
                    .min(received_bins.len());
                for bin in &mut received_bins[start..end] {
                    *bin = None;
                }
                *cleanup_frontier = frontier;
            }
        }
    }

    fn len(&self) -> usize {
        match self {
            Self::Sparse(received_bins) => received_bins.len(),
            Self::Dense(received_bins) => received_bins.iter().filter(|bin| bin.is_some()).count(),
        }
    }

    #[cfg(test)]
    fn is_empty(&self) -> bool {
        match self {
            Self::Sparse(received_bins) => received_bins.is_empty(),
            Self::Dense(received_bins) => received_bins.iter().all(Option::is_none),
        }
    }

    #[cfg(test)]
    fn keys(&self) -> Vec<u128> {
        match self {
            Self::Sparse(received_bins) => received_bins.keys().copied().collect(),
            Self::Dense(received_bins) => received_bins
                .iter()
                .enumerate()
                .filter_map(|(bin_id, bin)| bin.as_ref().map(|_| bin_id as u128))
                .collect(),
        }
    }
}

#[derive(Debug)]
pub(crate) struct MettleDecoder {
    params: MettleParams,
    source_symbol_bytes: NonZeroUsize,
    next_decoded_source_id: u64,
    seed: u64,
    terminal_source_count: Option<u64>,
    decoded_prefix_start_source_id: u64,
    decoded_prefix_equation_payloads: VecDeque<DecodedPayload>,
    decoded_future_equation_payloads: BTreeMap<u64, DecodedPayload>,
    seen_bin_ids: SeenBinIds,
    received_bins: ReceivedBins,
    ready_bin_ids: VecDeque<u128>,
    graph: Option<DecoderGraph>,
    rolling_graph: RollingDecoderGraph,
    bin_cleanup_frontier: u128,
}

impl MettleDecoder {
    const DECODED_PREFIX_WINDOW: usize = MettleParams::COUPLING_WINDOW as usize + 1;

    pub(crate) fn new(params: MettleParams, source_symbol_bytes: NonZeroUsize, seed: u64) -> Self {
        Self::new_with_terminal_source_count(params, source_symbol_bytes, seed, None)
    }

    pub(crate) fn new_terminated(
        params: MettleParams,
        source_symbol_bytes: NonZeroUsize,
        seed: u64,
        terminal_source_count: u64,
    ) -> Self {
        Self::new_with_terminal_source_count(
            params,
            source_symbol_bytes,
            seed,
            Some(terminal_source_count),
        )
    }

    fn new_with_terminal_source_count(
        params: MettleParams,
        source_symbol_bytes: NonZeroUsize,
        seed: u64,
        terminal_source_count: Option<u64>,
    ) -> Self {
        Self::new_with_optional_graph(
            params,
            source_symbol_bytes,
            seed,
            terminal_source_count,
            None,
        )
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn new_terminated_with_precomputed_graph(
        params: MettleParams,
        source_symbol_bytes: NonZeroUsize,
        seed: u64,
        terminal_source_count: u64,
    ) -> Self {
        let graph = Some(precompute_graph(params, seed, terminal_source_count));
        Self::new_with_optional_graph(
            params,
            source_symbol_bytes,
            seed,
            Some(terminal_source_count),
            graph,
        )
    }

    fn new_with_optional_graph(
        params: MettleParams,
        source_symbol_bytes: NonZeroUsize,
        seed: u64,
        terminal_source_count: Option<u64>,
        graph: Option<DecoderGraph>,
    ) -> Self {
        let bin_count = graph.as_ref().map(DecoderGraph::bin_count);
        Self {
            params,
            source_symbol_bytes,
            next_decoded_source_id: 0,
            seed,
            terminal_source_count,
            decoded_prefix_start_source_id: 0,
            decoded_prefix_equation_payloads: VecDeque::new(),
            decoded_future_equation_payloads: BTreeMap::new(),
            seen_bin_ids: SeenBinIds::new(bin_count),
            received_bins: ReceivedBins::new(bin_count),
            ready_bin_ids: VecDeque::new(),
            graph,
            rolling_graph: RollingDecoderGraph::default(),
            bin_cleanup_frontier: 0,
        }
    }

    pub(crate) fn push_bin(&mut self, bin: MettleBin) -> Vec<DecodedSource> {
        let (bin_id, payload) = bin.into_parts();
        if self
            .terminal_source_count
            .is_some_and(|terminal_source_count| {
                self.next_decoded_source_id >= terminal_source_count
            })
        {
            return Vec::new();
        }
        if payload.len() != self.source_symbol_bytes.get()
            || self.params.latest_source_id_for_bin(bin_id).is_none()
        {
            return Vec::new();
        }
        if !self.seen_bin_ids.insert(bin_id) {
            return Vec::new();
        }

        self.push_equation_bin(bin_id, payload)
    }

    fn push_equation_bin(&mut self, bin_id: u128, payload: Vec<u8>) -> Vec<DecodedSource> {
        if self.bin_has_no_undecoded_touchers(bin_id) {
            return Vec::new();
        }
        let Some(bin) = self.buffer_bin(bin_id, payload) else {
            return Vec::new();
        };
        if bin.remaining_touchers == 1 {
            self.ready_bin_ids.push_back(bin_id);
        }
        self.received_bins.insert(bin_id, bin);
        self.drain_decodable_sources()
    }

    fn buffer_bin(&mut self, bin_id: u128, mut payload: Vec<u8>) -> Option<BufferedBin> {
        let mut remaining_touchers = 0u16;
        let mut undecoded_source_xor = 0u64;

        if let Some(graph) = &self.graph {
            let touchers = graph.bin_touchers(bin_id)?;
            for &source_id in touchers {
                if self.source_is_decoded(source_id) {
                    if let Some(decoded_payload) = self.decoded_equation_payload(source_id) {
                        xor_payload(&mut payload, decoded_payload);
                    }
                } else {
                    remaining_touchers += 1;
                    undecoded_source_xor ^= source_id;
                }
            }

            return (remaining_touchers != 0).then_some(BufferedBin {
                payload,
                remaining_touchers,
                undecoded_source_xor,
            });
        }

        let touchers = self.take_rolling_bin_touchers(bin_id)?;
        for source_id in touchers {
            if self.source_is_decoded(source_id) {
                if let Some(decoded_payload) = self.decoded_equation_payload(source_id) {
                    xor_payload(&mut payload, decoded_payload);
                }
            } else {
                remaining_touchers += 1;
                undecoded_source_xor ^= source_id;
            }
        }

        (remaining_touchers != 0).then_some(BufferedBin {
            payload,
            remaining_touchers,
            undecoded_source_xor,
        })
    }

    fn take_rolling_bin_touchers(&mut self, bin_id: u128) -> Option<Vec<u64>> {
        let mut latest_source_id = self.params.latest_source_id_for_bin(bin_id)?;
        if let Some(0) = self.terminal_source_count {
            return None;
        }
        if let Some(terminal_source_count) = self.terminal_source_count {
            latest_source_id = latest_source_id.min(terminal_source_count - 1);
        }
        let target_source_count = latest_source_id.saturating_add(1);

        while self.rolling_graph.indexed_source_count < target_source_count {
            let source_id = self.rolling_graph.indexed_source_count;
            let (edge_bin_ids, edge_count) = self
                .params
                .unique_edge_bin_id_buffer_with_terminal_source_count(
                    source_id,
                    self.seed,
                    self.terminal_source_count,
                );
            for &edge_bin_id in &edge_bin_ids[..edge_count] {
                self.rolling_graph
                    .bin_touchers
                    .entry(edge_bin_id)
                    .or_default()
                    .push(source_id);
            }
            self.rolling_graph.indexed_source_count += 1;
        }

        self.rolling_graph.bin_touchers.remove(&bin_id)
    }

    fn drain_decodable_sources(&mut self) -> Vec<DecodedSource> {
        let mut decoded = Vec::new();

        loop {
            if let Some(bin_id) = self.find_unique_bin_for_next_source() {
                let payload = self
                    .received_bins
                    .remove(&bin_id)
                    .expect("just matched decodable bin");
                decoded.extend(
                    self.decode_source_and_release(self.next_decoded_source_id, payload.payload),
                );
                continue;
            }
            let Some((source_id, bin_id)) = self.find_unique_future_bin() else {
                break;
            };
            let payload = self
                .received_bins
                .remove(&bin_id)
                .expect("just matched decodable future unique bin");
            decoded.extend(self.decode_source_and_release(source_id, payload.payload));
        }

        decoded
    }

    fn decode_source_and_release(
        &mut self,
        source_id: u64,
        equation_payload: Vec<u8>,
    ) -> Vec<DecodedSource> {
        self.apply_decoded_source_edges(source_id, &equation_payload);
        let shared: DecodedPayload = Arc::new(equation_payload);
        if source_id == self.next_decoded_source_id {
            let mut released = vec![self.release_prefix_source(source_id, shared)];
            while let Some(equation_payload) = self
                .decoded_future_equation_payloads
                .remove(&self.next_decoded_source_id)
            {
                let source_id = self.next_decoded_source_id;
                released.push(self.release_prefix_source(source_id, equation_payload));
            }
            return released;
        }

        self.decoded_future_equation_payloads
            .insert(source_id, shared);
        Vec::new()
    }

    fn release_prefix_source(
        &mut self,
        source_id: u64,
        equation_payload: DecodedPayload,
    ) -> DecodedSource {
        self.push_decoded_prefix_equation_payload(Arc::clone(&equation_payload));
        self.next_decoded_source_id += 1;
        self.drop_bins_closed_by_prefix();
        DecodedSource {
            source_id,
            payload: equation_payload,
        }
    }

    fn find_unique_bin_for_next_source(&self) -> Option<u128> {
        let (edge_bin_ids, edge_count) = self.edge_bin_id_buffer(self.next_decoded_source_id);
        edge_bin_ids[..edge_count].iter().copied().find(|&bin_id| {
            self.received_bins
                .get(&bin_id)
                .and_then(BufferedBin::unique_source_id)
                == Some(self.next_decoded_source_id)
        })
    }

    fn find_unique_future_bin(&mut self) -> Option<(u64, u128)> {
        while let Some(bin_id) = self.ready_bin_ids.pop_front() {
            let Some(bin) = self.received_bins.get(&bin_id) else {
                continue;
            };
            let Some(source_id) = bin.unique_source_id() else {
                continue;
            };
            if self.source_is_decoded(source_id) {
                continue;
            }
            return Some((source_id, bin_id));
        }

        None
    }

    fn apply_decoded_source_edges(&mut self, source_id: u64, payload: &[u8]) {
        let (edge_bin_ids, edge_count) = self.edge_bin_id_buffer(source_id);
        let mut drained_bin_ids = Vec::new();
        let mut ready_bin_ids = Vec::new();

        for &bin_id in &edge_bin_ids[..edge_count] {
            if let Some(bin) = self.received_bins.get_mut(&bin_id) {
                xor_payload(&mut bin.payload, payload);
                if bin.remaining_touchers > 0 {
                    bin.remaining_touchers -= 1;
                    bin.undecoded_source_xor ^= source_id;
                }
                if bin.remaining_touchers == 0 {
                    drained_bin_ids.push(bin_id);
                } else if bin.remaining_touchers == 1 {
                    ready_bin_ids.push(bin_id);
                }
            }
        }
        for bin_id in drained_bin_ids {
            self.received_bins.remove(&bin_id);
        }
        self.ready_bin_ids.extend(ready_bin_ids);
    }

    fn bin_has_no_undecoded_touchers(&self, bin_id: u128) -> bool {
        if self
            .terminal_source_count
            .is_some_and(|terminal_source_count| {
                self.next_decoded_source_id >= terminal_source_count
            })
        {
            return true;
        }
        bin_id < self.params.tle_bin_id(self.next_decoded_source_id)
    }

    fn edge_bin_id_buffer(&self, source_id: u64) -> ([u128; MettleParams::EDGE_COUNT], usize) {
        if let Some(graph) = &self.graph
            && let Some(cached_edge_bin_ids) = graph.source_edge_ids(source_id)
        {
            return (cached_edge_bin_ids.bin_ids, cached_edge_bin_ids.count);
        }

        self.params
            .unique_edge_bin_id_buffer_with_terminal_source_count(
                source_id,
                self.seed,
                self.terminal_source_count,
            )
    }

    fn drop_bins_closed_by_prefix(&mut self) {
        if self
            .terminal_source_count
            .is_some_and(|terminal_source_count| {
                self.next_decoded_source_id >= terminal_source_count
            })
        {
            self.received_bins.clear();
            self.seen_bin_ids.clear();
            self.ready_bin_ids.clear();
            self.rolling_graph.clear();
            return;
        }
        let frontier = self.params.tle_bin_id(self.next_decoded_source_id);
        self.received_bins
            .drop_before(frontier, &mut self.bin_cleanup_frontier);
        self.seen_bin_ids.drop_before(frontier);
        if self.graph.is_none() {
            self.rolling_graph.drop_before(frontier);
        }
    }

    fn push_decoded_prefix_equation_payload(&mut self, payload: DecodedPayload) {
        self.decoded_prefix_equation_payloads.push_back(payload);
        if self.decoded_prefix_equation_payloads.len() > Self::DECODED_PREFIX_WINDOW {
            self.decoded_prefix_equation_payloads.pop_front();
            self.decoded_prefix_start_source_id += 1;
        }
    }

    fn decoded_prefix_equation_payload(&self, source_id: u64) -> Option<&[u8]> {
        let offset = source_id.checked_sub(self.decoded_prefix_start_source_id)?;
        let index = usize::try_from(offset).ok()?;
        self.decoded_prefix_equation_payloads
            .get(index)
            .map(|payload| payload.as_slice())
    }

    fn decoded_equation_payload(&self, source_id: u64) -> Option<&[u8]> {
        self.decoded_prefix_equation_payload(source_id).or_else(|| {
            self.decoded_future_equation_payloads
                .get(&source_id)
                .map(|payload| payload.as_slice())
        })
    }

    fn source_is_decoded(&self, source_id: u64) -> bool {
        source_id < self.next_decoded_source_id
            || self
                .decoded_future_equation_payloads
                .contains_key(&source_id)
    }

    pub(crate) fn next_source_id(&self) -> u64 {
        self.next_decoded_source_id
    }

    pub(crate) fn stats(&self) -> MettleDecoderStats {
        MettleDecoderStats {
            next_source_id: self.next_decoded_source_id,
            terminal_source_count: self.terminal_source_count,
            received_bins: self.received_bins.len(),
            ready_bins: self.ready_bin_ids.len(),
            seen_bins: self.seen_bin_ids.len(),
            decoded_future_sources: self.decoded_future_equation_payloads.len(),
            decoded_prefix_sources: self.decoded_prefix_equation_payloads.len(),
            decoded_prefix_start_source_id: self.decoded_prefix_start_source_id,
            graph_bins: self.graph.as_ref().map(DecoderGraph::bin_count),
            bin_cleanup_frontier: self.bin_cleanup_frontier,
        }
    }

    pub(crate) fn buffered_bin_remaining_touchers(&self, bin_id: u128) -> Option<u16> {
        self.received_bins
            .get(&bin_id)
            .map(|bin| bin.remaining_touchers)
    }

    pub(crate) fn skip_next_source_without_edges(&mut self) -> Vec<DecodedSource> {
        let payload = vec![0; self.source_symbol_bytes.get()];
        let mut released = self.decode_source_and_release(self.next_decoded_source_id, payload);
        released.extend(self.drain_decodable_sources());
        released
    }
}

fn xor_payload(dst: &mut [u8], src: &[u8]) {
    for (dst_byte, src_byte) in dst.iter_mut().zip(src) {
        *dst_byte ^= *src_byte;
    }
}

fn precompute_graph(params: MettleParams, seed: u64, terminal_source_count: u64) -> DecoderGraph {
    let source_count = usize::try_from(terminal_source_count).expect("source count fits usize");
    let bin_count = usize::try_from(params.terminal_departure_end_exclusive(terminal_source_count))
        .expect("terminal bin count fits usize");
    let mut source_bins = Vec::with_capacity(source_count);
    let mut bin_touchers = vec![Vec::new(); bin_count];

    for source_id in 0..terminal_source_count {
        let (edge_bin_ids, edge_count) = params
            .unique_edge_bin_id_buffer_with_terminal_source_count(
                source_id,
                seed,
                Some(terminal_source_count),
            );
        for &bin_id in &edge_bin_ids[..edge_count] {
            let bin_index = usize::try_from(bin_id).expect("bin id fits usize");
            bin_touchers
                .get_mut(bin_index)
                .expect("bin id is inside terminal departure range")
                .push(source_id);
        }
        source_bins.push(SourceEdgeIds::new(edge_bin_ids, edge_count));
    }

    DecoderGraph {
        source_bins,
        bin_touchers,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::num::NonZeroUsize;
    use std::sync::Arc;

    use crate::encoder::MettleEncoder;
    use crate::{MettleParams, OverheadRatio};

    use crate::encoder::MettleBin;

    use super::{DecodedSource, MettleDecoder};

    #[test]
    fn push_bin_deduplicates_by_bin_id() {
        let params = MettleParams::new(OverheadRatio::new(1, 20).expect("valid overhead"));
        let mut decoder =
            MettleDecoder::new(params, NonZeroUsize::new(4).expect("non-zero"), 0x1234);

        assert!(
            decoder
                .push_bin(MettleBin::new(17, vec![1, 2, 3, 4]))
                .is_empty()
        );
        let buffered_bin_count = decoder.received_bins.len();
        let next_decoded_source_id = decoder.next_decoded_source_id;
        let decoded_future_count = decoder.decoded_future_equation_payloads.len();
        assert!(
            decoder
                .push_bin(MettleBin::new(17, vec![1, 2, 3, 4]))
                .is_empty()
        );

        assert_eq!(decoder.seen_bin_ids.len(), 1);
        assert_eq!(decoder.received_bins.len(), buffered_bin_count);
        assert_eq!(decoder.next_decoded_source_id, next_decoded_source_id);
        assert_eq!(
            decoder.decoded_future_equation_payloads.len(),
            decoded_future_count
        );
    }

    #[test]
    fn push_bin_rejects_wrong_payload_length() {
        let params = MettleParams::new(OverheadRatio::new(1, 20).expect("valid overhead"));
        let mut decoder =
            MettleDecoder::new(params, NonZeroUsize::new(4).expect("non-zero"), 0x1234);

        assert!(
            decoder
                .push_bin(MettleBin::new(17, vec![1, 2, 3]))
                .is_empty()
        );
        assert!(decoder.seen_bin_ids.is_empty());
        assert!(decoder.received_bins.is_empty());
    }

    #[test]
    fn push_bin_decodes_a_unique_prefix_bin() {
        let params = MettleParams::new(OverheadRatio::new(1, 20).expect("valid overhead"));
        let mut decoder = MettleDecoder::new(params, NonZeroUsize::new(4).expect("non-zero"), 0);

        let decoded = decoder.push_bin(MettleBin::new(0, vec![1, 2, 3, 4]));

        assert_eq!(
            decoded,
            vec![DecodedSource {
                source_id: 0,
                payload: Arc::new(vec![1, 2, 3, 4]),
            }]
        );
        assert_eq!(decoder.next_decoded_source_id, 1);
        assert!(!decoder.seen_bin_ids.contains(&0));
        assert!(decoder.received_bins.is_empty());
    }

    #[test]
    fn push_bin_drains_waiting_unique_prefix_bins_in_order() {
        let params = MettleParams::new(OverheadRatio::new(1, 20).expect("valid overhead"));
        let mut decoder = MettleDecoder::new(params, NonZeroUsize::new(4).expect("non-zero"), 0);

        assert!(
            decoder
                .push_bin(MettleBin::new(1, vec![5, 6, 7, 8]))
                .is_empty()
        );

        let decoded = decoder.push_bin(MettleBin::new(0, vec![1, 2, 3, 4]));

        assert_eq!(
            decoded,
            vec![
                DecodedSource {
                    source_id: 0,
                    payload: Arc::new(vec![1, 2, 3, 4]),
                },
                DecodedSource {
                    source_id: 1,
                    payload: Arc::new(vec![5, 6, 7, 8]),
                },
            ]
        );
        assert_eq!(decoder.next_decoded_source_id, 2);
        assert!(decoder.seen_bin_ids.is_empty());
        assert!(decoder.received_bins.is_empty());
    }

    #[test]
    fn push_bin_uses_decoded_prefix_bin_to_unblock_overlap_bins() {
        let params = MettleParams::new(OverheadRatio::new(1, 20).expect("valid overhead"));
        let source_id = 537u64;
        let future_overlap_bin_id = params
            .edge_bin_ids(source_id, 0)
            .into_iter()
            .find(|&bin_id| bin_id >= params.tle_bin_id(source_id + 1))
            .expect("expected a future overlap bin");
        let mut decoder = MettleDecoder::new(params, NonZeroUsize::new(1).expect("non-zero"), 0);
        decoder.next_decoded_source_id = source_id;
        let shared_zero_payload = Arc::new(vec![0u8]);
        decoder.decoded_prefix_equation_payloads =
            VecDeque::from(vec![shared_zero_payload; source_id as usize]);

        assert!(
            decoder
                .push_bin(MettleBin::new(future_overlap_bin_id, vec![0b0110_0000]))
                .is_empty()
        );

        let decoded = decoder.push_bin(MettleBin::new(
            params.tle_bin_id(source_id),
            vec![0b1010_0000],
        ));

        assert_eq!(
            decoded,
            vec![DecodedSource {
                source_id,
                payload: Arc::new(vec![0b1010_0000]),
            }]
        );
        assert_eq!(decoder.next_decoded_source_id, source_id + 1);
        assert!(!decoder.seen_bin_ids.contains(&params.tle_bin_id(source_id)));
        assert!(decoder.seen_bin_ids.contains(&future_overlap_bin_id));
    }

    #[test]
    fn push_bin_ignores_duplicates_after_prefix_decode() {
        let params = MettleParams::new(OverheadRatio::new(1, 20).expect("valid overhead"));
        let mut decoder = MettleDecoder::new(params, NonZeroUsize::new(4).expect("non-zero"), 0);

        assert_eq!(
            decoder.push_bin(MettleBin::new(0, vec![1, 2, 3, 4])),
            vec![DecodedSource {
                source_id: 0,
                payload: Arc::new(vec![1, 2, 3, 4]),
            }]
        );

        assert!(
            decoder
                .push_bin(MettleBin::new(0, vec![1, 2, 3, 4]))
                .is_empty()
        );
        assert_eq!(
            decoder
                .decoded_prefix_equation_payloads
                .iter()
                .map(|payload| payload.as_slice().to_vec())
                .collect::<Vec<_>>(),
            vec![vec![1, 2, 3, 4]]
        );
        assert!(decoder.received_bins.is_empty());
    }

    #[test]
    fn push_bin_rejects_max_bin_id() {
        let params = MettleParams::new(OverheadRatio::new(1, 20).expect("valid overhead"));
        let mut decoder =
            MettleDecoder::new(params, NonZeroUsize::new(4).expect("non-zero"), 0x1234);

        assert!(
            decoder
                .push_bin(MettleBin::new(u128::MAX, vec![1, 2, 3, 4]))
                .is_empty()
        );
        assert!(decoder.seen_bin_ids.is_empty());
        assert!(decoder.received_bins.is_empty());
    }

    #[test]
    fn push_bin_rejects_unrepresentable_large_bin_id() {
        let params = MettleParams::new(OverheadRatio::new(1, 20).expect("valid overhead"));
        let mut decoder =
            MettleDecoder::new(params, NonZeroUsize::new(4).expect("non-zero"), 0x1234);

        assert!(
            decoder
                .push_bin(MettleBin::new(u128::MAX - 1, vec![1, 2, 3, 4]))
                .is_empty()
        );
        assert!(decoder.seen_bin_ids.is_empty());
        assert!(decoder.received_bins.is_empty());
    }

    #[test]
    fn encoder_round_trip_decodes_no_loss_stream() {
        let params = MettleParams::new(OverheadRatio::new(1, 20).expect("valid overhead"));
        let source_symbol_bytes = NonZeroUsize::new(2).expect("non-zero");
        let mut encoder = MettleEncoder::new(params, source_symbol_bytes, 0);
        let mut bins = Vec::new();
        let sources = [vec![1, 2], vec![3, 4], vec![5, 6]];

        for source in &sources {
            bins.extend(encoder.push_source(source));
        }
        bins.extend(encoder.finish());

        let mut decoder = MettleDecoder::new(params, source_symbol_bytes, 0);
        let mut decoded = Vec::new();
        for bin in bins {
            decoded.extend(decoder.push_bin(bin));
        }

        assert_eq!(
            decoded,
            vec![
                DecodedSource {
                    source_id: 0,
                    payload: Arc::new(vec![1, 2]),
                },
                DecodedSource {
                    source_id: 1,
                    payload: Arc::new(vec![3, 4]),
                },
                DecodedSource {
                    source_id: 2,
                    payload: Arc::new(vec![5, 6]),
                },
            ]
        );
        assert_eq!(decoder.next_decoded_source_id, 3);
        assert_eq!(decoder.decoded_prefix_start_source_id, 0);
        assert_eq!(
            decoder
                .decoded_prefix_equation_payloads
                .iter()
                .map(|payload| payload.as_slice().to_vec())
                .collect::<Vec<_>>(),
            sources
        );
        let frontier = params.tle_bin_id(decoder.next_decoded_source_id);
        assert!(
            decoder
                .received_bins
                .keys()
                .into_iter()
                .all(|bin_id| bin_id >= frontier)
        );
    }

    #[test]
    fn terminated_decoder_defaults_to_rolling_state() {
        let params = MettleParams::new(OverheadRatio::new(1, 20).expect("valid overhead"));
        let decoder =
            MettleDecoder::new_terminated(params, NonZeroUsize::new(1).expect("non-zero"), 0, 64);

        assert_eq!(decoder.stats().terminal_source_count, Some(64));
        assert_eq!(decoder.stats().graph_bins, None);
    }

    #[test]
    fn precomputed_graph_decoder_matches_rolling_decoder() {
        let params = MettleParams::new(OverheadRatio::new(1, 20).expect("valid overhead"));
        let source_symbol_bytes = NonZeroUsize::new(2).expect("non-zero");
        let source_count = 64u64;
        let sources = (0..source_count)
            .map(|source_id| vec![source_id as u8, source_id.wrapping_mul(17) as u8])
            .collect::<Vec<_>>();
        let mut encoder =
            MettleEncoder::new_terminated(params, source_symbol_bytes, 0, source_count);
        let mut bins = Vec::new();

        for source in &sources {
            bins.extend(encoder.push_source(source));
        }
        bins.extend(encoder.finish());

        let mut rolling =
            MettleDecoder::new_terminated(params, source_symbol_bytes, 0, source_count);
        let mut precomputed = MettleDecoder::new_terminated_with_precomputed_graph(
            params,
            source_symbol_bytes,
            0,
            source_count,
        );
        assert_eq!(rolling.stats().graph_bins, None);
        assert!(precomputed.stats().graph_bins.is_some());

        let mut rolling_decoded = Vec::new();
        let mut precomputed_decoded = Vec::new();
        for bin in bins {
            let (bin_id, payload) = bin.into_parts();
            rolling_decoded.extend(rolling.push_bin(MettleBin::new(bin_id, payload.clone())));
            precomputed_decoded.extend(precomputed.push_bin(MettleBin::new(bin_id, payload)));
        }

        assert_eq!(rolling_decoded, precomputed_decoded);
        assert_eq!(rolling.next_source_id(), source_count);
        assert_eq!(precomputed.next_source_id(), source_count);
    }

    #[test]
    fn coded_tle_bins_peel_future_tle_bins() {
        let params = MettleParams::new(OverheadRatio::new(1, 20).expect("valid overhead"));
        let source_symbol_bytes = NonZeroUsize::new(1).expect("non-zero");
        let future_tle_source_id = (1..=1024)
            .find(|&source_id| {
                params
                    .edge_bin_ids(0, 0)
                    .contains(&params.tle_bin_id(source_id))
            })
            .expect("source 0 should touch some future TLE bin");
        let future_tle_bin_id = params.tle_bin_id(future_tle_source_id);
        let source_count = future_tle_source_id + 1;
        let mut encoder =
            MettleEncoder::new_terminated(params, source_symbol_bytes, 0, source_count);
        let mut bins = Vec::new();
        let mut sources = vec![vec![0]; source_count as usize];
        sources[0] = vec![0b1010_0000];
        sources[future_tle_source_id as usize] = vec![0b1100_0000];

        assert!(params.edge_bin_ids(0, 0).contains(&future_tle_bin_id));

        for source in &sources {
            bins.extend(encoder.push_source(source));
        }
        bins.extend(encoder.finish());

        let mut decoder =
            MettleDecoder::new_terminated(params, source_symbol_bytes, 0, source_count);
        let decoded = bins
            .into_iter()
            .flat_map(|bin| decoder.push_bin(bin))
            .map(DecodedSource::into_parts)
            .map(|(source_id, payload)| (source_id, payload.as_slice().to_vec()))
            .collect::<Vec<_>>();
        let expected = sources
            .into_iter()
            .enumerate()
            .map(|(source_id, payload)| (source_id as u64, payload))
            .collect::<Vec<_>>();

        assert_eq!(decoded, expected);
    }

    #[test]
    fn terminated_decoder_rejects_post_terminal_tle_bins() {
        let params = MettleParams::new(OverheadRatio::new(1, 20).expect("valid overhead"));
        let source_symbol_bytes = NonZeroUsize::new(1).expect("non-zero");
        let mut decoder = MettleDecoder::new_terminated(params, source_symbol_bytes, 0, 1);

        assert_eq!(
            decoder.push_bin(MettleBin::new(params.tle_bin_id(0), vec![0b1010_0000])),
            vec![DecodedSource {
                source_id: 0,
                payload: Arc::new(vec![0b1010_0000]),
            }]
        );
        assert!(
            decoder
                .push_bin(MettleBin::new(params.tle_bin_id(1), vec![0b1100_0000]))
                .is_empty()
        );
        assert_eq!(decoder.next_decoded_source_id, 1);
    }

    #[test]
    fn decoder_bounds_prefix_history_to_the_local_window() {
        let params = MettleParams::new(OverheadRatio::new(1, 20).expect("valid overhead"));
        let source_symbol_bytes = NonZeroUsize::new(1).expect("non-zero");
        let extra_sources = 3u64;
        let total_sources = MettleDecoder::DECODED_PREFIX_WINDOW as u64 + extra_sources;
        let mut decoder = MettleDecoder::new(params, source_symbol_bytes, 0);

        for source_id in 0..total_sources {
            let decoded = decoder.push_bin(MettleBin::new(params.tle_bin_id(source_id), vec![0]));
            assert_eq!(decoded.len(), 1);
        }

        assert_eq!(
            decoder.decoded_prefix_equation_payloads.len(),
            MettleDecoder::DECODED_PREFIX_WINDOW
        );
        assert_eq!(decoder.decoded_prefix_start_source_id, extra_sources);
    }

    #[test]
    fn missing_prefix_bin_stalls_until_it_is_replayed() {
        let params = MettleParams::new(OverheadRatio::new(1, 20).expect("valid overhead"));
        let source_symbol_bytes = NonZeroUsize::new(2).expect("non-zero");
        let mut encoder = MettleEncoder::new(params, source_symbol_bytes, 0);
        let mut bins = Vec::new();
        let sources = [vec![1, 2], vec![3, 4], vec![5, 6]];

        for source in &sources {
            bins.extend(encoder.push_source(source));
        }
        bins.extend(encoder.finish());

        let missing_prefix_bin = bins.remove(0);
        let mut decoder = MettleDecoder::new(params, source_symbol_bytes, 0);
        let mut decoded = Vec::new();
        for bin in bins {
            decoded.extend(decoder.push_bin(bin));
        }

        assert!(decoded.is_empty());
        assert_eq!(decoder.next_decoded_source_id, 0);

        decoded.extend(decoder.push_bin(missing_prefix_bin));
        assert_eq!(
            decoded,
            vec![
                DecodedSource {
                    source_id: 0,
                    payload: Arc::new(vec![1, 2]),
                },
                DecodedSource {
                    source_id: 1,
                    payload: Arc::new(vec![3, 4]),
                },
                DecodedSource {
                    source_id: 2,
                    payload: Arc::new(vec![5, 6]),
                },
            ]
        );
    }
}
