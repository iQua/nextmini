use std::collections::BTreeSet;
use std::num::NonZeroUsize;
use std::sync::OnceLock;

use nextmini_messages::lossless_session::{
    self, BlockAck, LosslessSessionFecMode, LosslessSessionMode, MettleStallEvidence,
    MissingMettleBinRange,
};
use tokio::time::Instant;
use tracing::warn;

use crate::node::session::api::InboundFrame;
use crate::node::session::fec as session_fec;
use crate::node::session::plan::ObjectSymbolPlan;
use crate::node::session::runtime::{
    MettleDecoderBudget, MettleDecoderBudgetError, MettleDecoderPermit,
};

/// Carousel-only paper-native receiver. The decoder permit spans the whole
/// session, while exactly one dense prefix decoder is live at a time.
pub(super) struct MettleCarouselReceiver {
    plan: ObjectSymbolPlan,
    fec_mode: LosslessSessionFecMode,
    current_stream_id: u64,
    current_source_count: u32,
    committed_watermark: u32,
    decoder: Option<mettle::stream::Decoder>,
    seen_bin_ids: BTreeSet<u32>,
    terminal_bin_count: u32,
    checkpoint: Option<DepartureCheckpointState>,
    _permit: Option<MettleDecoderPermit>,
    complete: bool,
    aborted: bool,
}

struct DepartureCheckpointState {
    repair_epoch: u32,
    departure_bin_exclusive: u32,
    aging_deadline: Option<Instant>,
    aged: bool,
    missing_bin_ranges: OnceLock<Vec<MissingMettleBinRange>>,
}

#[derive(Debug)]
pub(super) enum MettleCarouselInstallError {
    MissingBudget,
    PrefixExceedsReservation,
    Admission(MettleDecoderBudgetError),
    Decoder(mettle::DecoderBuildError),
    WorkerStopped,
}

impl std::fmt::Display for MettleCarouselInstallError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingBudget => formatter.write_str("METTLE decoder budget is unavailable"),
            Self::PrefixExceedsReservation => {
                formatter.write_str("METTLE prefix exceeds the decoder reservation")
            }
            Self::Admission(error) => write!(formatter, "METTLE decoder admission failed: {error}"),
            Self::Decoder(error) => {
                write!(formatter, "METTLE decoder construction failed: {error}")
            }
            Self::WorkerStopped => formatter.write_str("METTLE decoder worker stopped"),
        }
    }
}

impl MettleCarouselReceiver {
    pub(super) async fn install(
        session_id: u64,
        plan: ObjectSymbolPlan,
        fec_mode: LosslessSessionFecMode,
        budget: Option<&MettleDecoderBudget>,
    ) -> Result<Self, MettleCarouselInstallError> {
        if plan.stream_count() == 0 {
            return Ok(Self {
                plan,
                fec_mode,
                current_stream_id: 0,
                current_source_count: 0,
                committed_watermark: 0,
                decoder: None,
                seen_bin_ids: BTreeSet::new(),
                terminal_bin_count: 0,
                checkpoint: None,
                _permit: None,
                complete: true,
                aborted: false,
            });
        }

        let budget = budget.ok_or(MettleCarouselInstallError::MissingBudget)?;
        let reserved_payload_copies = plan
            .maximum_stream_payload_bytes()
            .and_then(|bytes| bytes.checked_mul(2))
            .ok_or(MettleCarouselInstallError::PrefixExceedsReservation)?;
        if reserved_payload_copies > budget.reservation_bytes() {
            return Err(MettleCarouselInstallError::PrefixExceedsReservation);
        }
        let permit = budget
            .try_acquire()
            .map_err(MettleCarouselInstallError::Admission)?;
        let current_source_count = plan
            .stream_source_count(0)
            .ok_or(MettleCarouselInstallError::PrefixExceedsReservation)?;
        let decoder = build_decoder(session_id, plan, &fec_mode, 0).await?;
        let terminal_bin_count = terminal_bin_count(&fec_mode, current_source_count)
            .ok_or(MettleCarouselInstallError::PrefixExceedsReservation)?;

        Ok(Self {
            plan,
            fec_mode,
            current_stream_id: 0,
            current_source_count,
            committed_watermark: 0,
            decoder: Some(decoder),
            seen_bin_ids: BTreeSet::new(),
            terminal_bin_count,
            checkpoint: None,
            _permit: Some(permit),
            complete: false,
            aborted: false,
        })
    }

    pub(super) fn block_ack(&self) -> Option<BlockAck> {
        if self.plan.stream_count() == 0 {
            return None;
        }
        let stalled = self.checkpoint.as_ref().and_then(|checkpoint| {
            let aged = checkpoint.aged
                || checkpoint
                    .aging_deadline
                    .is_some_and(|deadline| deadline <= Instant::now());
            aged.then(|| MettleStallEvidence {
                repair_epoch: checkpoint.repair_epoch,
                missing_bin_ranges: checkpoint
                    .missing_bin_ranges
                    .get_or_init(|| {
                        missing_bin_ranges(&self.seen_bin_ids, checkpoint.departure_bin_exclusive)
                    })
                    .clone(),
            })
        });
        BlockAck::MettleStream {
            stream_id: self.current_stream_id,
            decoded_source_watermark: self.committed_watermark,
            stalled,
        }
        .for_wire(self.plan.stream_count())
        .ok()
    }

    pub(super) fn repair_deadline(&self) -> Option<Instant> {
        self.checkpoint
            .as_ref()
            .and_then(|checkpoint| checkpoint.aging_deadline)
    }

    pub(super) fn note_ack_sent(&mut self) {
        let Some(checkpoint) = self.checkpoint.as_mut() else {
            return;
        };
        if checkpoint
            .aging_deadline
            .is_some_and(|deadline| deadline <= Instant::now())
        {
            checkpoint.aged = true;
            checkpoint.aging_deadline = None;
        }
    }

    pub(super) fn handle_departure_checkpoint(
        &mut self,
        stream_id: u64,
        repair_epoch: u32,
        departure_bin_exclusive: u32,
        reorder_budget: std::time::Duration,
    ) {
        if self.complete || self.aborted || stream_id < self.current_stream_id {
            return;
        }
        if stream_id != self.current_stream_id
            || departure_bin_exclusive == 0
            || departure_bin_exclusive > self.terminal_bin_count
        {
            return;
        }
        if self
            .checkpoint
            .as_ref()
            .is_some_and(|checkpoint| repair_epoch <= checkpoint.repair_epoch)
        {
            return;
        }
        self.checkpoint = Some(DepartureCheckpointState {
            repair_epoch,
            departure_bin_exclusive,
            aging_deadline: Some(
                Instant::now()
                    .checked_add(reorder_budget)
                    .unwrap_or_else(Instant::now),
            ),
            aged: false,
            missing_bin_ranges: OnceLock::new(),
        });
    }

    pub(super) const fn is_complete(&self) -> bool {
        self.complete
    }

    pub(super) const fn aborted(&self) -> bool {
        self.aborted
    }

    pub(super) async fn handle_block_symbol_frame(
        &mut self,
        shared: &mut super::ReceiverShared,
        frame: InboundFrame,
    ) -> Result<(), super::SinkWriteError> {
        if self.aborted {
            return Ok(());
        }
        let Some(manifest) = shared.manifest.as_ref() else {
            return Ok(());
        };
        let Some((_, symbol, payload)) = lossless_session::decode_block_symbol(&frame.bytes) else {
            return Ok(());
        };
        if manifest.validate_block_symbol(&symbol).is_err()
            || payload.len() != self.plan.symbol_size()
        {
            return Ok(());
        }
        if self.complete || symbol.block_id < self.current_stream_id {
            shared.metrics.record_receiver_tail_symbol();
            return Ok(());
        }
        if symbol.block_id != self.current_stream_id {
            return Ok(());
        }
        if symbol.symbol_id >= self.terminal_bin_count {
            shared.metrics.record_receiver_invalid_symbol();
            return Ok(());
        }
        if !self.insert_seen_bin_id(symbol.symbol_id) {
            shared.metrics.record_receiver_duplicate();
            return Ok(());
        }
        shared.mark_first_payload_unit();

        let Some(decoder) = self.decoder.as_mut() else {
            self.aborted = true;
            return Ok(());
        };
        let decoded = decoder.push_bin(u128::from(symbol.symbol_id), payload.to_vec());
        for source in decoded {
            let (source_id, payload) = source.into_parts();
            let Ok(source_id) = u32::try_from(source_id) else {
                self.aborted = true;
                return Ok(());
            };
            if source_id != self.committed_watermark || source_id >= self.current_source_count {
                self.aborted = true;
                return Ok(());
            }
            let Some(global_source_id) = self
                .plan
                .global_source_id(self.current_stream_id, source_id)
            else {
                self.aborted = true;
                return Ok(());
            };
            shared
                .write_object_symbol(self.plan, global_source_id, payload.as_ref())
                .await?;
            self.committed_watermark = match self.committed_watermark.checked_add(1) {
                Some(next) => next,
                None => {
                    self.aborted = true;
                    return Ok(());
                }
            };
        }

        if self.committed_watermark == self.current_source_count {
            shared
                .metrics
                .record_symbols_at_decode(self.current_source_count, self.seen_bin_ids.len());
            self.finish_prefix(shared.session_id).await;
        }
        Ok(())
    }

    fn insert_seen_bin_id(&mut self, bin_id: u32) -> bool {
        if !self.seen_bin_ids.insert(bin_id) {
            return false;
        }
        if let Some(checkpoint) = self.checkpoint.as_mut() {
            checkpoint.missing_bin_ranges = OnceLock::new();
        }
        true
    }

    async fn finish_prefix(&mut self, session_id: u64) {
        if self.current_stream_id.checked_add(1) == Some(self.plan.stream_count()) {
            self.complete = true;
            self.decoder = None;
            self.seen_bin_ids.clear();
            self.checkpoint = None;
            return;
        }

        // Drop the old graph before allocating the successor, preserving the
        // one-decoder-per-session lifetime measured by the Stage 2.0 spike.
        self.decoder = None;
        let Some(next_stream_id) = self.current_stream_id.checked_add(1) else {
            self.aborted = true;
            return;
        };
        let Some(source_count) = self.plan.stream_source_count(next_stream_id) else {
            self.aborted = true;
            return;
        };
        match build_decoder(session_id, self.plan, &self.fec_mode, next_stream_id).await {
            Ok(decoder) => {
                self.current_stream_id = next_stream_id;
                self.current_source_count = source_count;
                self.committed_watermark = 0;
                self.seen_bin_ids.clear();
                self.terminal_bin_count = match terminal_bin_count(&self.fec_mode, source_count) {
                    Some(count) => count,
                    None => {
                        self.aborted = true;
                        return;
                    }
                };
                self.checkpoint = None;
                self.decoder = Some(decoder);
            }
            Err(error) => {
                warn!(%error, next_stream_id, "METTLE successor decoder construction failed");
                self.aborted = true;
            }
        }
    }
}

fn terminal_bin_count(fec_mode: &LosslessSessionFecMode, source_count: u32) -> Option<u32> {
    let overhead = session_fec::mettle_overhead_from_fec_mode(fec_mode)?;
    mettle::stream::terminal_bin_count(mettle::MettleParams::new(overhead), u64::from(source_count))
}

fn missing_bin_ranges(
    seen_bin_ids: &BTreeSet<u32>,
    departure_bin_exclusive: u32,
) -> Vec<MissingMettleBinRange> {
    let mut ranges = Vec::new();
    let mut cursor = 0;
    while cursor < departure_bin_exclusive {
        if seen_bin_ids.contains(&cursor) {
            cursor += 1;
            continue;
        }
        let start_bin_id = cursor;
        while cursor < departure_bin_exclusive && !seen_bin_ids.contains(&cursor) {
            cursor += 1;
        }
        ranges.push(MissingMettleBinRange {
            start_bin_id,
            end_bin_id: cursor,
        });
    }

    ranges
}

async fn build_decoder(
    session_id: u64,
    plan: ObjectSymbolPlan,
    fec_mode: &LosslessSessionFecMode,
    stream_id: u64,
) -> Result<mettle::stream::Decoder, MettleCarouselInstallError> {
    let source_count = plan
        .stream_source_count(stream_id)
        .ok_or(MettleCarouselInstallError::PrefixExceedsReservation)?;
    let source_symbol_bytes = NonZeroUsize::new(plan.symbol_size())
        .ok_or(MettleCarouselInstallError::PrefixExceedsReservation)?;
    let overhead = session_fec::mettle_overhead_from_fec_mode(fec_mode)
        .ok_or(MettleCarouselInstallError::PrefixExceedsReservation)?;
    tokio::task::spawn_blocking(move || {
        mettle::stream::Decoder::try_new_terminated(
            mettle::MettleParams::new(overhead),
            source_symbol_bytes,
            session_fec::block_seed(session_id, stream_id),
            u64::from(source_count),
        )
    })
    .await
    .map_err(|_| MettleCarouselInstallError::WorkerStopped)?
    .map_err(MettleCarouselInstallError::Decoder)
}

pub(super) fn is_mettle_carousel(mode: &LosslessSessionMode) -> bool {
    matches!(
        mode,
        LosslessSessionMode::Fec(fec)
            if fec.scheme_kind() == Some(nextmini_messages::lossless_session::FecScheme::Mettle)
                && fec.feedback_mode
                    == nextmini_messages::lossless_session::FecFeedbackMode::Carousel
    )
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};
    use std::net::Ipv4Addr;
    use std::num::NonZeroUsize;
    use std::sync::Arc;
    use std::time::Duration;

    use nextmini_messages::lossless_session::{
        FecFeedbackMode, LosslessSessionFecMode, MettleObjectStreamGeometry,
    };

    use super::*;
    use crate::node::config::LosslessConfig;
    use crate::node::processor::ProcessorHandle;
    use crate::node::session::metrics::SessionMetrics;
    use crate::node::session::plan::BlockPlan;
    use crate::node::session::runtime::{ReceiverConfig, TransportRoute};

    #[tokio::test]
    async fn fifth_concurrent_dense_decoder_is_rejected_at_permit_exhaustion() {
        let budget = MettleDecoderBudget::from_lossless(&LosslessConfig::default())
            .expect("default decoder budget");
        let plan =
            ObjectSymbolPlan::from_negotiated(8, MettleObjectStreamGeometry::new(2, 4, 1, 4))
                .expect("small object-stream plan");
        let fec_mode = LosslessSessionFecMode::new_mettle(4, vec![0])
            .with_feedback_mode(FecFeedbackMode::Carousel)
            .with_mettle_object_stream(plan.geometry());

        let mut receivers = Vec::new();
        for session_id in 0..4 {
            receivers.push(
                MettleCarouselReceiver::install(session_id, plan, fec_mode.clone(), Some(&budget))
                    .await
                    .expect("one of four admitted dense decoders"),
            );
        }
        assert!(matches!(
            MettleCarouselReceiver::install(4, plan, fec_mode, Some(&budget)).await,
            Err(MettleCarouselInstallError::Admission(
                MettleDecoderBudgetError::Exhausted
            ))
        ));
        assert_eq!(receivers.len(), 4);
    }

    #[tokio::test]
    async fn checkpoint_ages_only_after_reorder_budget_and_classifies_no_loss_as_empty() {
        tokio::time::pause();
        let budget = MettleDecoderBudget::from_lossless(&LosslessConfig::default())
            .expect("default decoder budget");
        let plan =
            ObjectSymbolPlan::from_negotiated(128, MettleObjectStreamGeometry::new(2, 64, 1, 64))
                .expect("single-prefix plan");
        let fec_mode = LosslessSessionFecMode::new_mettle(4, vec![0])
            .with_feedback_mode(FecFeedbackMode::Carousel)
            .with_mettle_object_stream(plan.geometry());
        let mut receiver = MettleCarouselReceiver::install(9, plan, fec_mode, Some(&budget))
            .await
            .expect("decoder admission");
        let terminal = receiver.terminal_bin_count;
        receiver.handle_departure_checkpoint(0, 0, terminal, Duration::from_millis(100));

        // Model extreme cross-tree reorder: the checkpoint arrives first, then
        // every payload arrives in reverse order within the negotiated budget.
        receiver.seen_bin_ids.extend((0..terminal).rev());
        assert!(matches!(
            receiver.block_ack(),
            Some(BlockAck::MettleStream { stalled: None, .. })
        ));
        tokio::time::advance(Duration::from_millis(100)).await;
        let Some(BlockAck::MettleStream {
            stalled: Some(evidence),
            ..
        }) = receiver.block_ack()
        else {
            panic!("aged checkpoint must carry epoch evidence");
        };
        assert_eq!(evidence.repair_epoch, 0);
        assert!(
            evidence.missing_bin_ranges.is_empty(),
            "zero loss must classify zero targeted retransmissions"
        );
    }

    #[tokio::test]
    async fn checkpoint_reports_known_near_and_far_losses_as_targeted_ranges() {
        let budget = MettleDecoderBudget::from_lossless(&LosslessConfig::default())
            .expect("default decoder budget");
        let plan =
            ObjectSymbolPlan::from_negotiated(128, MettleObjectStreamGeometry::new(2, 64, 1, 64))
                .expect("single-prefix plan");
        let fec_mode = LosslessSessionFecMode::new_mettle(4, vec![0])
            .with_feedback_mode(FecFeedbackMode::Carousel)
            .with_mettle_object_stream(plan.geometry());
        let mut receiver = MettleCarouselReceiver::install(10, plan, fec_mode, Some(&budget))
            .await
            .expect("decoder admission");
        let terminal = receiver.terminal_bin_count;
        let far = terminal - 2;
        receiver
            .seen_bin_ids
            .extend((0..terminal).filter(|bin_id| *bin_id != 1 && *bin_id != far));
        receiver.handle_departure_checkpoint(0, 11, terminal, Duration::ZERO);

        let Some(BlockAck::MettleStream {
            stalled: Some(evidence),
            ..
        }) = receiver.block_ack()
        else {
            panic!("zero test budget makes the report immediately eligible");
        };
        assert_eq!(
            evidence.missing_bin_ranges,
            vec![
                MissingMettleBinRange {
                    start_bin_id: 1,
                    end_bin_id: 2,
                },
                MissingMettleBinRange {
                    start_bin_id: far,
                    end_bin_id: far + 1,
                },
            ]
        );
    }

    #[tokio::test]
    async fn range_overflow_converges_in_lowest_bin_id_windows() {
        let geometry = MettleObjectStreamGeometry::new(1, 1_024, 1, 1_024);
        let plan = ObjectSymbolPlan::from_negotiated(1_024, geometry).expect("single-prefix plan");
        let fec_mode = LosslessSessionFecMode::new_mettle(1_024, vec![0])
            .with_feedback_mode(FecFeedbackMode::Carousel)
            .with_mettle_object_stream(geometry);
        let budget = MettleDecoderBudget::from_lossless(&LosslessConfig::default())
            .expect("default decoder budget");
        let mut receiver = MettleCarouselReceiver::install(11, plan, fec_mode, Some(&budget))
            .await
            .expect("decoder admission");
        let terminal = receiver.terminal_bin_count;
        for bin_id in (1..terminal).step_by(2) {
            assert!(receiver.insert_seen_bin_id(bin_id));
        }
        let expected = missing_bin_ranges(&receiver.seen_bin_ids, terminal);
        assert!(
            expected.len() > nextmini_messages::lossless_session::MAX_METTLE_MISSING_BIN_RANGES
        );
        receiver.handle_departure_checkpoint(0, 12, terminal, Duration::ZERO);

        let mut reported = Vec::new();
        loop {
            let Some(BlockAck::MettleStream {
                stalled: Some(evidence),
                ..
            }) = receiver.block_ack()
            else {
                panic!("aged checkpoint must retain stall evidence")
            };
            if evidence.missing_bin_ranges.is_empty() {
                break;
            }
            assert!(
                evidence.missing_bin_ranges.len()
                    <= nextmini_messages::lossless_session::MAX_METTLE_MISSING_BIN_RANGES
            );
            for range in &evidence.missing_bin_ranges {
                for bin_id in range.start_bin_id..range.end_bin_id {
                    assert!(receiver.insert_seen_bin_id(bin_id));
                }
            }
            reported.extend(evidence.missing_bin_ranges);
        }

        assert_eq!(reported, expected);
    }

    #[tokio::test]
    async fn out_of_range_peer_bin_is_rejected_before_seen_set_or_decoder() {
        let session_id = 0xBAD;
        let geometry = MettleObjectStreamGeometry::new(2, 4, 1, 4);
        let plan = ObjectSymbolPlan::from_negotiated(8, geometry).expect("single-prefix plan");
        let fec_mode = LosslessSessionFecMode::new_mettle(4, vec![0])
            .with_feedback_mode(FecFeedbackMode::Carousel)
            .with_mettle_object_stream(geometry);
        let manifest = nextmini_messages::lossless_session::LosslessSessionManifest {
            block_size: 8,
            total_bytes: 8,
            total_blocks: 1,
            mode: LosslessSessionMode::Fec(fec_mode.clone()),
        };
        manifest.validate().expect("valid manifest");
        let budget = MettleDecoderBudget::from_lossless(&LosslessConfig::default())
            .expect("default decoder budget");
        let mut receiver =
            MettleCarouselReceiver::install(session_id, plan, fec_mode, Some(&budget))
                .await
                .expect("decoder admission");
        let invalid_bin_id = receiver.terminal_bin_count;
        let route = TransportRoute {
            src_ip: Ipv4Addr::new(10, 0, 0, 2),
            dst_ip: Ipv4Addr::new(10, 0, 0, 1),
            src_port: 4752,
            dst_port: 5752,
        };
        let metrics = Arc::new(SessionMetrics::default());
        let mut shared = super::super::ReceiverShared {
            session_id,
            route,
            local_node_id: 2,
            cfg: ReceiverConfig {
                session_id,
                route,
                local_node_id: 2,
                sink_buffer: Some(Arc::new(tokio::sync::Mutex::new(vec![0; 8]))),
                sink_file: None,
                progress: None,
                peer_report_timeout_ms: 200,
                fec_enabled: true,
                cloudcast: None,
                carousel: Default::default(),
                mettle_decoder_budget: Some(budget),
            },
            processors: ProcessorHandle::new(Default::default()),
            manifest: Some(manifest),
            plan: BlockPlan::new(8, 8).ok(),
            complete_blocks: BTreeSet::new(),
            metrics: metrics.clone(),
        };

        receiver
            .handle_block_symbol_frame(
                &mut shared,
                InboundFrame {
                    bytes: lossless_session::encode_block_symbol(
                        session_id,
                        0,
                        invalid_bin_id,
                        0,
                        &[1, 2],
                    ),
                    peer_id: Some(1),
                },
            )
            .await
            .expect("invalid peer input is dropped, not surfaced as a sink error");

        assert!(receiver.seen_bin_ids.is_empty());
        assert_eq!(metrics.snapshot().receiver_invalid_symbols, 1);
        assert_eq!(receiver.committed_watermark, 0);
    }

    #[tokio::test]
    async fn known_near_and_far_losses_recover_from_only_epoch_targeted_bins() {
        let session_id = 0xC5;
        let source = (0..128).map(|byte| byte as u8).collect::<Vec<_>>();
        let geometry = MettleObjectStreamGeometry::new(2, 64, 1, 64);
        let object_plan = ObjectSymbolPlan::from_negotiated(source.len() as u64, geometry)
            .expect("single-prefix plan");
        let fec_mode = LosslessSessionFecMode::new_mettle(64, vec![0])
            .with_feedback_mode(FecFeedbackMode::Carousel)
            .with_mettle_object_stream(geometry);
        let manifest = nextmini_messages::lossless_session::LosslessSessionManifest {
            block_size: 128,
            total_bytes: source.len() as u64,
            total_blocks: 1,
            mode: LosslessSessionMode::Fec(fec_mode.clone()),
        };
        manifest.validate().expect("valid manifest");
        let budget = MettleDecoderBudget::from_lossless(&LosslessConfig::default())
            .expect("default decoder budget");
        let mut receiver = MettleCarouselReceiver::install(
            session_id,
            object_plan,
            fec_mode.clone(),
            Some(&budget),
        )
        .await
        .expect("decoder admission");
        let route = TransportRoute {
            src_ip: Ipv4Addr::new(10, 0, 0, 2),
            dst_ip: Ipv4Addr::new(10, 0, 0, 1),
            src_port: 4752,
            dst_port: 5752,
        };
        let sink = Arc::new(tokio::sync::Mutex::new(vec![0; source.len()]));
        let metrics = Arc::new(SessionMetrics::default());
        let mut shared = super::super::ReceiverShared {
            session_id,
            route,
            local_node_id: 2,
            cfg: ReceiverConfig {
                session_id,
                route,
                local_node_id: 2,
                sink_buffer: Some(sink.clone()),
                sink_file: None,
                progress: None,
                peer_report_timeout_ms: 200,
                fec_enabled: true,
                cloudcast: None,
                carousel: Default::default(),
                mettle_decoder_budget: Some(budget),
            },
            processors: ProcessorHandle::new(Default::default()),
            manifest: Some(manifest),
            plan: BlockPlan::new(source.len() as u64, 128).ok(),
            complete_blocks: BTreeSet::new(),
            metrics: metrics.clone(),
        };

        let mut encoder = mettle::stream::Encoder::new_terminated(
            mettle::MettleParams::new(
                session_fec::mettle_overhead_from_fec_mode(&fec_mode).expect("valid rate"),
            ),
            NonZeroUsize::new(2).expect("non-zero symbol"),
            session_fec::block_seed(session_id, 0),
            64,
        );
        let mut encoded = BTreeMap::new();
        for payload in source.chunks_exact(2) {
            for bin in encoder.push_source(payload) {
                let (bin_id, payload) = bin.into_parts();
                encoded.insert(u32::try_from(bin_id).expect("wire bin id"), payload);
            }
        }
        for bin in encoder.finish() {
            let (bin_id, payload) = bin.into_parts();
            encoded.insert(u32::try_from(bin_id).expect("wire bin id"), payload);
        }
        let terminal = u32::try_from(encoded.len()).expect("test terminal fits");
        let pivot = terminal / 2;
        let lost = [
            MissingMettleBinRange {
                start_bin_id: 0,
                end_bin_id: pivot,
            },
            MissingMettleBinRange {
                start_bin_id: pivot + 1,
                end_bin_id: terminal,
            },
        ];
        let mut duplicate_measured = false;
        for (&bin_id, payload) in &encoded {
            if lost
                .iter()
                .any(|range| (range.start_bin_id..range.end_bin_id).contains(&bin_id))
            {
                continue;
            }
            receiver
                .handle_block_symbol_frame(
                    &mut shared,
                    InboundFrame {
                        bytes: lossless_session::encode_block_symbol(
                            session_id, 0, bin_id, 0, payload,
                        ),
                        peer_id: Some(1),
                    },
                )
                .await
                .expect("initial payload sink write");
            if !duplicate_measured {
                receiver
                    .handle_block_symbol_frame(
                        &mut shared,
                        InboundFrame {
                            bytes: lossless_session::encode_block_symbol(
                                session_id, 0, bin_id, 0, payload,
                            ),
                            peer_id: Some(1),
                        },
                    )
                    .await
                    .expect("duplicate initial payload is harmless");
                duplicate_measured = true;
            }
        }
        assert!(!receiver.is_complete());
        receiver.handle_departure_checkpoint(0, 0, terminal, Duration::ZERO);
        let Some(BlockAck::MettleStream {
            stalled: Some(evidence),
            ..
        }) = receiver.block_ack()
        else {
            panic!("losses must produce epoch-tagged repair evidence");
        };
        assert_eq!(evidence.missing_bin_ranges, lost);

        for range in evidence.missing_bin_ranges {
            for bin_id in range.start_bin_id..range.end_bin_id {
                let payload = encoded.get(&bin_id).expect("targeted bin cache");
                receiver
                    .handle_block_symbol_frame(
                        &mut shared,
                        InboundFrame {
                            bytes: lossless_session::encode_block_symbol(
                                session_id, 0, bin_id, 0, payload,
                            ),
                            peer_id: Some(1),
                        },
                    )
                    .await
                    .expect("targeted repair sink write");
            }
        }
        assert!(receiver.is_complete());
        assert_eq!(*sink.lock().await, source);
        assert_eq!(
            metrics.snapshot().receiver_duplicate_symbols,
            1,
            "duplicate repair traffic is measured explicitly"
        );
    }
}
