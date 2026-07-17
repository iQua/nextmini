use std::collections::BTreeSet;
use std::num::NonZeroUsize;

use nextmini_messages::lossless_session::{
    self, BlockAck, LosslessSessionFecMode, LosslessSessionMode,
};
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
    _permit: Option<MettleDecoderPermit>,
    complete: bool,
    aborted: bool,
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

        Ok(Self {
            plan,
            fec_mode,
            current_stream_id: 0,
            current_source_count,
            committed_watermark: 0,
            decoder: Some(decoder),
            seen_bin_ids: BTreeSet::new(),
            _permit: Some(permit),
            complete: false,
            aborted: false,
        })
    }

    pub(super) fn block_ack(&self) -> Option<BlockAck> {
        if self.plan.stream_count() == 0 {
            return None;
        }
        Some(BlockAck::MettleStream {
            stream_id: self.current_stream_id,
            decoded_source_watermark: self.committed_watermark,
            stalled: None,
        })
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
        if !self.seen_bin_ids.insert(symbol.symbol_id) {
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

    async fn finish_prefix(&mut self, session_id: u64) {
        if self.current_stream_id.checked_add(1) == Some(self.plan.stream_count()) {
            self.complete = true;
            self.decoder = None;
            self.seen_bin_ids.clear();
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
                self.decoder = Some(decoder);
            }
            Err(error) => {
                warn!(%error, next_stream_id, "METTLE successor decoder construction failed");
                self.aborted = true;
            }
        }
    }
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
    use nextmini_messages::lossless_session::{
        FecFeedbackMode, LosslessSessionFecMode, MettleObjectStreamGeometry,
    };

    use super::*;
    use crate::node::config::LosslessConfig;

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
}
