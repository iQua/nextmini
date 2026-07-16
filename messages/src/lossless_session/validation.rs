use super::{
    BlockAck, FecFeedbackMode, FecScheme, LosslessSessionBlockData, LosslessSessionBlockSymbol,
    LosslessSessionControl, LosslessSessionManifest, LosslessSessionMode,
    LosslessSessionValidationError, MAX_BLOCK_ACK_RANGES, MAX_MANIFEST_TREE_IDS,
    MAX_METTLE_MISSING_BIN_RANGES, MAX_NEED_BLOCKS, MAX_NEED_RANGES,
    METTLE_STREAM_PAYLOAD_CAP_BYTES, METTLE_STREAM_SOURCE_CAP, MettleObjectStreamGeometry,
    MettleStallEvidence, NeedReport, WireFecGeometry, WireFecGeometryError,
};

impl MettleObjectStreamGeometry {
    pub fn validate(
        self,
        total_bytes: u64,
        expected_symbol_bytes: u32,
    ) -> Result<(), LosslessSessionValidationError> {
        if self.source_symbol_bytes != expected_symbol_bytes {
            return Err(
                LosslessSessionValidationError::MettleObjectSymbolSizeMismatch {
                    expected: expected_symbol_bytes,
                    actual: self.source_symbol_bytes,
                },
            );
        }
        if self.source_symbols_per_stream == 0
            || self.source_symbols_per_stream > METTLE_STREAM_SOURCE_CAP
        {
            return Err(
                LosslessSessionValidationError::MettleStreamSourceLimitOutOfRange {
                    configured: self.source_symbols_per_stream,
                    max: METTLE_STREAM_SOURCE_CAP,
                },
            );
        }
        let stream_payload_bytes = u64::from(self.source_symbol_bytes)
            .checked_mul(u64::from(self.source_symbols_per_stream))
            .ok_or(LosslessSessionValidationError::MettleStreamGeometryOverflow)?;
        if stream_payload_bytes > METTLE_STREAM_PAYLOAD_CAP_BYTES {
            return Err(
                LosslessSessionValidationError::MettleStreamPayloadTooLarge {
                    configured: stream_payload_bytes,
                    max: METTLE_STREAM_PAYLOAD_CAP_BYTES,
                },
            );
        }

        let total_sources = if total_bytes == 0 {
            0
        } else {
            total_bytes.div_ceil(u64::from(self.source_symbol_bytes))
        };
        let source_limit = u64::from(self.source_symbols_per_stream);
        let expected_stream_count = if total_sources == 0 {
            0
        } else {
            total_sources.div_ceil(source_limit)
        };
        if self.stream_count != expected_stream_count {
            return Err(LosslessSessionValidationError::MettleStreamCountMismatch {
                expected: expected_stream_count,
                actual: self.stream_count,
            });
        }
        let expected_final_stream_source_symbols = if total_sources == 0 {
            0
        } else {
            let remainder = total_sources % source_limit;
            u32::try_from(if remainder == 0 {
                source_limit
            } else {
                remainder
            })
            .map_err(|_| LosslessSessionValidationError::MettleStreamGeometryOverflow)?
        };
        if self.final_stream_source_symbols != expected_final_stream_source_symbols {
            return Err(
                LosslessSessionValidationError::MettleFinalStreamSourceCountMismatch {
                    expected: expected_final_stream_source_symbols,
                    actual: self.final_stream_source_symbols,
                },
            );
        }
        Ok(())
    }
}

impl MettleStallEvidence {
    pub fn validate(&self) -> Result<(), LosslessSessionValidationError> {
        if self.missing_bin_ranges.is_empty() {
            return Err(LosslessSessionValidationError::MettleMissingBinRangesEmpty);
        }
        if self.missing_bin_ranges.len() > MAX_METTLE_MISSING_BIN_RANGES {
            return Err(
                LosslessSessionValidationError::TooManyMettleMissingBinRanges {
                    configured: self.missing_bin_ranges.len(),
                    max: MAX_METTLE_MISSING_BIN_RANGES,
                },
            );
        }
        let mut previous_end = None;
        for range in &self.missing_bin_ranges {
            if range.start_bin_id >= range.end_bin_id {
                return Err(
                    LosslessSessionValidationError::MettleMissingBinRangeInvalid {
                        start_bin_id: range.start_bin_id,
                        end_bin_id: range.end_bin_id,
                    },
                );
            }
            if previous_end.is_some_and(|end| range.start_bin_id <= end) {
                return Err(
                    LosslessSessionValidationError::MettleMissingBinRangesMustBeSortedMerged,
                );
            }
            previous_end = Some(range.end_bin_id);
        }
        Ok(())
    }
}

impl NeedReport {
    pub fn validate(&self) -> Result<(), LosslessSessionValidationError> {
        match self {
            Self::Complete => Ok(()),
            Self::Plain { ranges } => {
                if ranges.len() > MAX_NEED_RANGES {
                    return Err(LosslessSessionValidationError::TooManyNeedRanges {
                        configured: ranges.len(),
                        max: MAX_NEED_RANGES,
                    });
                }
                let mut previous_end = None;
                for range in ranges {
                    if range.start_block_id >= range.end_block_id {
                        return Err(LosslessSessionValidationError::MissingBlockRangeInvalid {
                            start_block_id: range.start_block_id,
                            end_block_id: range.end_block_id,
                        });
                    }
                    if let Some(prev_end) = previous_end
                        && range.start_block_id <= prev_end
                    {
                        return Err(LosslessSessionValidationError::NeedRangesMustBeSortedMerged);
                    }
                    previous_end = Some(range.end_block_id);
                }
                Ok(())
            }
            Self::Fec { blocks } => {
                if blocks.len() > MAX_NEED_BLOCKS {
                    return Err(LosslessSessionValidationError::TooManyNeedBlocks {
                        configured: blocks.len(),
                        max: MAX_NEED_BLOCKS,
                    });
                }
                if blocks.iter().any(|status| status.deficit_symbols == 0) {
                    return Err(LosslessSessionValidationError::ZeroDeficitSymbols);
                }
                if !blocks
                    .windows(2)
                    .all(|pair| pair[0].block_id < pair[1].block_id)
                {
                    return Err(LosslessSessionValidationError::NeedBlocksMustBeSortedUnique);
                }
                Ok(())
            }
        }
    }

    pub fn validate_against_total_blocks(
        &self,
        total_blocks: u64,
    ) -> Result<(), LosslessSessionValidationError> {
        self.validate()?;
        match self {
            Self::Complete => Ok(()),
            Self::Plain { ranges } => {
                for range in ranges {
                    if range.end_block_id > total_blocks {
                        return Err(LosslessSessionValidationError::NeedRangeOutOfRange {
                            end_block_id: range.end_block_id,
                            total_blocks,
                        });
                    }
                }
                Ok(())
            }
            Self::Fec { blocks } => {
                for status in blocks {
                    if status.block_id >= total_blocks {
                        return Err(LosslessSessionValidationError::BlockIdOutOfRange {
                            block_id: status.block_id,
                            total_blocks,
                        });
                    }
                }
                Ok(())
            }
        }
    }
}

impl BlockAck {
    /// Canonicalize a received cumulative snapshot against the manifest's block
    /// count. Completion islands that touch the watermark are folded into it.
    pub fn canonicalized(self, total_blocks: u64) -> Result<Self, LosslessSessionValidationError> {
        match self {
            Self::Blocks {
                completed_watermark,
                extra_completed,
            } => {
                if completed_watermark > total_blocks {
                    return Err(
                        LosslessSessionValidationError::BlockAckWatermarkOutOfRange {
                            completed_watermark,
                            total_blocks,
                        },
                    );
                }

                let original_watermark = completed_watermark;
                let mut completed_watermark = completed_watermark;
                let mut canonical = Vec::with_capacity(extra_completed.len());
                let mut previous_extra_end = None;

                for range in extra_completed {
                    if range.start_block_id >= range.end_block_id {
                        return Err(LosslessSessionValidationError::CompletedBlockRangeInvalid {
                            start_block_id: range.start_block_id,
                            end_block_id: range.end_block_id,
                        });
                    }
                    if range.start_block_id < original_watermark {
                        return Err(
                            LosslessSessionValidationError::BlockAckRangeBeforeWatermark {
                                start_block_id: range.start_block_id,
                                completed_watermark: original_watermark,
                            },
                        );
                    }
                    if range.end_block_id > total_blocks {
                        return Err(LosslessSessionValidationError::BlockAckRangeOutOfRange {
                            end_block_id: range.end_block_id,
                            total_blocks,
                        });
                    }

                    if canonical.is_empty() && range.start_block_id == completed_watermark {
                        completed_watermark = range.end_block_id;
                        continue;
                    }
                    if range.start_block_id < completed_watermark
                        || previous_extra_end
                            .is_some_and(|previous_end| range.start_block_id <= previous_end)
                    {
                        return Err(
                            LosslessSessionValidationError::BlockAckRangesMustBeSortedMerged,
                        );
                    }
                    previous_extra_end = Some(range.end_block_id);
                    canonical.push(range);
                }

                Ok(Self::Blocks {
                    completed_watermark,
                    extra_completed: canonical,
                })
            }
            ack @ Self::MettleStream { .. } => {
                ack.validate()?;
                Ok(ack)
            }
        }
    }

    /// Produce the deterministic wire snapshot: canonical form followed by
    /// lowest-block-id truncation to the protocol range capacity.
    pub fn for_wire(self, total_blocks: u64) -> Result<Self, LosslessSessionValidationError> {
        let mut canonical = self.canonicalized(total_blocks)?;
        match &mut canonical {
            Self::Blocks {
                extra_completed, ..
            } => extra_completed.truncate(MAX_BLOCK_ACK_RANGES),
            Self::MettleStream { stalled, .. } => {
                if let Some(stalled) = stalled {
                    stalled
                        .missing_bin_ranges
                        .truncate(MAX_METTLE_MISSING_BIN_RANGES);
                }
            }
        }
        Ok(canonical)
    }

    pub fn validate(&self) -> Result<(), LosslessSessionValidationError> {
        match self {
            Self::Blocks {
                extra_completed, ..
            } => {
                if extra_completed.len() > MAX_BLOCK_ACK_RANGES {
                    return Err(LosslessSessionValidationError::TooManyBlockAckRanges {
                        configured: extra_completed.len(),
                        max: MAX_BLOCK_ACK_RANGES,
                    });
                }
                self.clone().canonicalized(u64::MAX).map(|_| ())
            }
            Self::MettleStream { stalled, .. } => {
                if let Some(stalled) = stalled {
                    stalled.validate()?;
                }
                Ok(())
            }
        }
    }

    pub fn validate_against_total_blocks(
        &self,
        total_blocks: u64,
    ) -> Result<(), LosslessSessionValidationError> {
        self.validate()?;
        match self {
            Self::Blocks { .. } => self.clone().canonicalized(total_blocks).map(|_| ()),
            Self::MettleStream { .. } => Ok(()),
        }
    }
}

impl LosslessSessionManifest {
    pub fn total_blocks_for(
        total_bytes: u64,
        block_size: u32,
    ) -> Result<u64, LosslessSessionValidationError> {
        if block_size == 0 {
            return Err(LosslessSessionValidationError::ZeroBlockSize);
        }
        let block_size = u64::from(block_size);
        if total_bytes == 0 {
            Ok(0)
        } else {
            Ok(total_bytes.div_ceil(block_size))
        }
    }

    pub fn validate(&self) -> Result<(), LosslessSessionValidationError> {
        let expected = Self::total_blocks_for(self.total_bytes, self.block_size)?;
        if self.total_blocks != expected {
            return Err(LosslessSessionValidationError::InconsistentTotalBlocks {
                expected,
                actual: self.total_blocks,
            });
        }

        if let LosslessSessionMode::Fec(fec) = &self.mode {
            if fec.scheme_kind().is_none() {
                return Err(LosslessSessionValidationError::UnknownFecScheme {
                    scheme: fec.scheme,
                });
            }
            let wire_geometry = WireFecGeometry::new(self.block_size, fec.symbols_per_block)
                .map_err(|err| match err {
                    WireFecGeometryError::ZeroBlockSize => {
                        LosslessSessionValidationError::ZeroBlockSize
                    }
                    WireFecGeometryError::ZeroSourceSymbols => {
                        LosslessSessionValidationError::ZeroSymbolsPerBlock
                    }
                    WireFecGeometryError::SymbolPayloadTooLarge { symbol_size, max } => {
                        LosslessSessionValidationError::FecSymbolPayloadTooLarge {
                            symbol_size,
                            max,
                        }
                    }
                    WireFecGeometryError::PaddedBlockSizeOverflow {
                        source_symbols,
                        symbol_size,
                    } => LosslessSessionValidationError::FecPaddedBlockSizeOverflow {
                        source_symbols,
                        symbol_size,
                    },
                    WireFecGeometryError::SymbolPayloadCeilingUnrepresentable => {
                        LosslessSessionValidationError::FecSymbolPayloadCeilingUnrepresentable
                    }
                })?;
            if !fec.coded_rate_is_valid() {
                return Err(LosslessSessionValidationError::InvalidFecCodedRate {
                    numerator: fec.coded_rate_num,
                    denominator: fec.coded_rate_den,
                });
            }
            if fec.tree_ids.is_empty() {
                return Err(LosslessSessionValidationError::FecTreeIdsEmpty);
            }
            if fec.tree_ids.len() > MAX_MANIFEST_TREE_IDS {
                return Err(LosslessSessionValidationError::TooManyTreeIds {
                    configured: fec.tree_ids.len(),
                    max: MAX_MANIFEST_TREE_IDS,
                });
            }
            if !fec.tree_ids.windows(2).all(|pair| pair[0] < pair[1]) {
                return Err(LosslessSessionValidationError::TreeIdsMustBeSortedUnique);
            }
            match (
                fec.scheme_kind(),
                fec.feedback_mode,
                fec.mettle_object_stream,
            ) {
                (Some(FecScheme::Mettle), FecFeedbackMode::Carousel, Some(geometry)) => {
                    geometry.validate(self.total_bytes, wire_geometry.symbol_size())?;
                }
                (Some(FecScheme::Mettle), FecFeedbackMode::Carousel, None) => {
                    return Err(LosslessSessionValidationError::MettleObjectStreamGeometryRequired);
                }
                (_, _, Some(_)) => {
                    return Err(
                        LosslessSessionValidationError::MettleObjectStreamGeometryUnexpected,
                    );
                }
                (_, _, None) => {}
            }
        }

        Ok(())
    }

    pub fn validate_block_id(&self, block_id: u64) -> Result<(), LosslessSessionValidationError> {
        self.validate()?;
        if block_id >= self.total_blocks {
            return Err(LosslessSessionValidationError::BlockIdOutOfRange {
                block_id,
                total_blocks: self.total_blocks,
            });
        }
        Ok(())
    }

    pub fn expected_block_payload_len(
        &self,
        block_id: u64,
    ) -> Result<u32, LosslessSessionValidationError> {
        self.validate_block_id(block_id)?;
        if self.total_blocks == 0 {
            return Ok(0);
        }
        if block_id + 1 == self.total_blocks {
            let rem = (self.total_bytes % u64::from(self.block_size)) as u32;
            if rem == 0 {
                Ok(self.block_size)
            } else {
                Ok(rem)
            }
        } else {
            Ok(self.block_size)
        }
    }

    pub fn validate_block_data(
        &self,
        data: &LosslessSessionBlockData,
        payload_len: usize,
    ) -> Result<(), LosslessSessionValidationError> {
        if self.mode.is_fec() {
            return Err(LosslessSessionValidationError::BlockDataRequiresPlainMode);
        }
        let expected = self.expected_block_payload_len(data.block_id)?;
        let actual = u32::try_from(payload_len).unwrap_or(u32::MAX);
        if actual != expected {
            return Err(LosslessSessionValidationError::BlockDataLenMismatch {
                block_id: data.block_id,
                expected,
                actual,
            });
        }
        Ok(())
    }

    pub fn validate_block_symbol(
        &self,
        symbol: &LosslessSessionBlockSymbol,
    ) -> Result<(), LosslessSessionValidationError> {
        let LosslessSessionMode::Fec(fec) = &self.mode else {
            return Err(LosslessSessionValidationError::BlockSymbolRequiresFecMode);
        };
        if let Some(geometry) = fec.mettle_object_stream {
            if symbol.block_id >= geometry.stream_count {
                return Err(LosslessSessionValidationError::BlockIdOutOfRange {
                    block_id: symbol.block_id,
                    total_blocks: geometry.stream_count,
                });
            }
        } else {
            self.validate_block_id(symbol.block_id)?;
        }
        if !fec.tree_ids.contains(&symbol.tree_id) {
            return Err(
                LosslessSessionValidationError::BlockSymbolTreeIdNotAdvertised {
                    tree_id: symbol.tree_id,
                },
            );
        }
        Ok(())
    }

    pub fn validate_need_report(
        &self,
        report: &NeedReport,
    ) -> Result<(), LosslessSessionValidationError> {
        self.validate()?;
        match report {
            NeedReport::Complete => Ok(()),
            NeedReport::Plain { ranges } => {
                if self.mode.is_fec() && !ranges.is_empty() {
                    return Err(LosslessSessionValidationError::NeedRequiresPlainMode);
                }
                NeedReport::Plain {
                    ranges: ranges.clone(),
                }
                .validate_against_total_blocks(self.total_blocks)
            }
            NeedReport::Fec { blocks } => {
                if !self.mode.is_fec() && !blocks.is_empty() {
                    return Err(LosslessSessionValidationError::NeedRequiresFecMode);
                }
                NeedReport::Fec {
                    blocks: blocks.clone(),
                }
                .validate_against_total_blocks(self.total_blocks)
            }
        }
    }

    pub fn validate_control(
        &self,
        control: &LosslessSessionControl,
    ) -> Result<(), LosslessSessionValidationError> {
        self.validate()?;
        control.validate()?;
        match control {
            LosslessSessionControl::Manifest { manifest } => manifest.validate(),
            LosslessSessionControl::Ready => Ok(()),
            LosslessSessionControl::SourceDone { .. } => {
                if matches!(
                    &self.mode,
                    LosslessSessionMode::Fec(fec)
                        if fec.feedback_mode == FecFeedbackMode::Carousel
                ) {
                    Err(LosslessSessionValidationError::RoundsControlRequiresRoundsMode)
                } else {
                    Ok(())
                }
            }
            LosslessSessionControl::Need { report, .. } => {
                if matches!(
                    &self.mode,
                    LosslessSessionMode::Fec(fec)
                        if fec.feedback_mode == FecFeedbackMode::Carousel
                ) {
                    Err(LosslessSessionValidationError::RoundsControlRequiresRoundsMode)
                } else {
                    self.validate_need_report(report)
                }
            }
            LosslessSessionControl::BlockAck { ack } => {
                if !matches!(
                    &self.mode,
                    LosslessSessionMode::Fec(fec)
                        if fec.feedback_mode == FecFeedbackMode::Carousel
                ) {
                    return Err(
                        LosslessSessionValidationError::CarouselControlRequiresCarouselMode,
                    );
                }
                match (&self.mode, ack) {
                    (LosslessSessionMode::Fec(fec), BlockAck::Blocks { .. })
                        if fec.scheme_kind() == Some(FecScheme::RaptorQ) =>
                    {
                        ack.validate_against_total_blocks(self.total_blocks)
                    }
                    (
                        LosslessSessionMode::Fec(fec),
                        BlockAck::MettleStream {
                            stream_id,
                            decoded_source_watermark,
                            ..
                        },
                    ) if fec.scheme_kind() == Some(FecScheme::Mettle) => {
                        let geometry = fec.mettle_object_stream.ok_or(
                            LosslessSessionValidationError::MettleObjectStreamGeometryRequired,
                        )?;
                        let stream_source_count = geometry.stream_source_count(*stream_id).ok_or(
                            LosslessSessionValidationError::MettleAckStreamOutOfRange {
                                stream_id: *stream_id,
                                stream_count: geometry.stream_count,
                            },
                        )?;
                        if *decoded_source_watermark > stream_source_count {
                            return Err(
                                LosslessSessionValidationError::MettleAckWatermarkOutOfRange {
                                    decoded_source_watermark: *decoded_source_watermark,
                                    stream_source_count,
                                },
                            );
                        }
                        ack.validate()
                    }
                    (LosslessSessionMode::Fec(_), BlockAck::Blocks { .. }) => {
                        Err(LosslessSessionValidationError::BlockAckVariantRequiresRaptorQ)
                    }
                    (LosslessSessionMode::Fec(_), BlockAck::MettleStream { .. }) => {
                        Err(LosslessSessionValidationError::MettleBlockAckRequiresMettle)
                    }
                    _ => Err(LosslessSessionValidationError::CarouselControlRequiresCarouselMode),
                }
            }
            LosslessSessionControl::AckProbe { .. } | LosslessSessionControl::SessionComplete => {
                if matches!(
                    &self.mode,
                    LosslessSessionMode::Fec(fec)
                        if fec.feedback_mode == FecFeedbackMode::Carousel
                ) {
                    Ok(())
                } else {
                    Err(LosslessSessionValidationError::CarouselControlRequiresCarouselMode)
                }
            }
        }
    }
}

impl LosslessSessionControl {
    pub fn validate(&self) -> Result<(), LosslessSessionValidationError> {
        match self {
            Self::Manifest { manifest } => manifest.validate(),
            Self::Ready
            | Self::SourceDone { .. }
            | Self::AckProbe { .. }
            | Self::SessionComplete => Ok(()),
            Self::Need { report, .. } => report.validate(),
            Self::BlockAck { ack } => ack.validate(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lossless_session::test_support::{
        carousel_manifest, fec_manifest, fec_need, plain_manifest, plain_need,
    };
    use crate::lossless_session::{
        CompletedBlockRange, LosslessSessionBlockData, LosslessSessionBlockSymbol,
        LosslessSessionFecMode, LosslessSessionManifest, LosslessSessionMode, MissingBlockRange,
        NeedBlock,
    };

    #[test]
    fn manifest_validation_rejects_bad_shapes() {
        let zero_block = LosslessSessionManifest {
            block_size: 0,
            total_bytes: 1,
            total_blocks: 1,
            mode: LosslessSessionMode::Plain,
        };
        assert_eq!(
            zero_block.validate(),
            Err(LosslessSessionValidationError::ZeroBlockSize)
        );

        let bad_total_blocks = LosslessSessionManifest {
            block_size: 1024,
            total_bytes: 2049,
            total_blocks: 2,
            mode: LosslessSessionMode::Plain,
        };
        assert_eq!(
            bad_total_blocks.validate(),
            Err(LosslessSessionValidationError::InconsistentTotalBlocks {
                expected: 3,
                actual: 2,
            })
        );

        let bad_fec = LosslessSessionManifest {
            block_size: 1024,
            total_bytes: 1024,
            total_blocks: 1,
            mode: LosslessSessionMode::Fec(LosslessSessionFecMode {
                scheme: 99,
                symbols_per_block: 0,
                coded_rate_num: 1,
                coded_rate_den: 1,
                feedback_mode: Default::default(),
                mettle_object_stream: None,
                tree_ids: vec![],
            }),
        };
        assert_eq!(
            bad_fec.validate(),
            Err(LosslessSessionValidationError::UnknownFecScheme { scheme: 99 })
        );
    }

    #[test]
    fn manifest_validation_accepts_known_fec_schemes_and_rejects_unknown() {
        let mettle = LosslessSessionManifest {
            block_size: 1024,
            total_bytes: 1024,
            total_blocks: 1,
            mode: LosslessSessionMode::Fec(LosslessSessionFecMode::new_mettle(8, vec![1, 3])),
        };
        mettle
            .validate()
            .expect("METTLE should be a known FEC scheme");

        let unknown = LosslessSessionManifest {
            block_size: 1024,
            total_bytes: 1024,
            total_blocks: 1,
            mode: LosslessSessionMode::Fec(LosslessSessionFecMode {
                scheme: 99,
                symbols_per_block: 8,
                coded_rate_num: 1,
                coded_rate_den: 1,
                feedback_mode: Default::default(),
                mettle_object_stream: None,
                tree_ids: vec![1, 3],
            }),
        };
        assert_eq!(
            unknown.validate(),
            Err(LosslessSessionValidationError::UnknownFecScheme { scheme: 99 })
        );
    }

    #[test]
    fn mettle_carousel_requires_exact_checked_object_stream_geometry() {
        let without_geometry = LosslessSessionManifest {
            block_size: 1024,
            total_bytes: 2048,
            total_blocks: 2,
            mode: LosslessSessionMode::Fec(
                LosslessSessionFecMode::new_mettle(8, vec![1])
                    .with_feedback_mode(FecFeedbackMode::Carousel),
            ),
        };
        assert_eq!(
            without_geometry.validate(),
            Err(LosslessSessionValidationError::MettleObjectStreamGeometryRequired)
        );

        let valid = LosslessSessionManifest {
            mode: LosslessSessionMode::Fec(
                LosslessSessionFecMode::new_mettle(8, vec![1])
                    .with_feedback_mode(FecFeedbackMode::Carousel)
                    .with_mettle_object_stream(MettleObjectStreamGeometry::new(128, 8, 2, 8)),
            ),
            ..without_geometry.clone()
        };
        valid.validate().expect("checked object stream geometry");

        let wrong_count = LosslessSessionManifest {
            mode: LosslessSessionMode::Fec(
                LosslessSessionFecMode::new_mettle(8, vec![1])
                    .with_feedback_mode(FecFeedbackMode::Carousel)
                    .with_mettle_object_stream(MettleObjectStreamGeometry::new(128, 8, 3, 8)),
            ),
            ..valid.clone()
        };
        assert_eq!(
            wrong_count.validate(),
            Err(LosslessSessionValidationError::MettleStreamCountMismatch {
                expected: 2,
                actual: 3,
            })
        );

        let wrong_final = LosslessSessionManifest {
            mode: LosslessSessionMode::Fec(
                LosslessSessionFecMode::new_mettle(8, vec![1])
                    .with_feedback_mode(FecFeedbackMode::Carousel)
                    .with_mettle_object_stream(MettleObjectStreamGeometry::new(128, 8, 2, 7)),
            ),
            ..valid.clone()
        };
        assert_eq!(
            wrong_final.validate(),
            Err(
                LosslessSessionValidationError::MettleFinalStreamSourceCountMismatch {
                    expected: 8,
                    actual: 7,
                }
            )
        );

        let rounds_with_geometry = LosslessSessionManifest {
            mode: LosslessSessionMode::Fec(
                LosslessSessionFecMode::new_mettle(8, vec![1])
                    .with_mettle_object_stream(MettleObjectStreamGeometry::new(128, 8, 2, 8)),
            ),
            ..valid
        };
        assert_eq!(
            rounds_with_geometry.validate(),
            Err(LosslessSessionValidationError::MettleObjectStreamGeometryUnexpected)
        );
    }

    #[test]
    fn mettle_stream_geometry_enforces_source_and_payload_caps() {
        let source_cap = MettleObjectStreamGeometry::new(
            1,
            METTLE_STREAM_SOURCE_CAP + 1,
            1,
            METTLE_STREAM_SOURCE_CAP + 1,
        );
        assert_eq!(
            source_cap.validate(u64::from(METTLE_STREAM_SOURCE_CAP) + 1, 1),
            Err(
                LosslessSessionValidationError::MettleStreamSourceLimitOutOfRange {
                    configured: METTLE_STREAM_SOURCE_CAP + 1,
                    max: METTLE_STREAM_SOURCE_CAP,
                }
            )
        );

        let payload_cap = MettleObjectStreamGeometry::new(65_000, 1_549, 1, 1_549);
        assert_eq!(
            payload_cap.validate(65_000 * 1_549, 65_000),
            Err(
                LosslessSessionValidationError::MettleStreamPayloadTooLarge {
                    configured: 100_685_000,
                    max: METTLE_STREAM_PAYLOAD_CAP_BYTES,
                }
            )
        );
    }

    #[test]
    fn mettle_progress_ack_is_validated_against_its_stream() {
        let manifest = LosslessSessionManifest {
            block_size: 1024,
            total_bytes: 2176,
            total_blocks: 3,
            mode: LosslessSessionMode::Fec(
                LosslessSessionFecMode::new_mettle(8, vec![1])
                    .with_feedback_mode(FecFeedbackMode::Carousel)
                    .with_mettle_object_stream(MettleObjectStreamGeometry::new(128, 8, 3, 1)),
            ),
        };
        let valid = LosslessSessionControl::BlockAck {
            ack: BlockAck::MettleStream {
                stream_id: 2,
                decoded_source_watermark: 1,
                stalled: Some(MettleStallEvidence {
                    repair_epoch: 4,
                    missing_bin_ranges: vec![super::super::MissingMettleBinRange {
                        start_bin_id: 2,
                        end_bin_id: 5,
                    }],
                }),
            },
        };
        manifest
            .validate_control(&valid)
            .expect("final stream watermark is in range");

        let too_far = LosslessSessionControl::BlockAck {
            ack: BlockAck::MettleStream {
                stream_id: 2,
                decoded_source_watermark: 2,
                stalled: None,
            },
        };
        assert_eq!(
            manifest.validate_control(&too_far),
            Err(
                LosslessSessionValidationError::MettleAckWatermarkOutOfRange {
                    decoded_source_watermark: 2,
                    stream_source_count: 1,
                }
            )
        );
    }

    #[test]
    fn manifest_validation_rejects_invalid_fec_coded_rate() {
        let bad = LosslessSessionManifest {
            block_size: 1024,
            total_bytes: 1024,
            total_blocks: 1,
            mode: LosslessSessionMode::Fec(LosslessSessionFecMode::new_mettle_with_coded_rate(
                8,
                vec![1, 3],
                19,
                20,
            )),
        };

        assert_eq!(
            bad.validate(),
            Err(LosslessSessionValidationError::InvalidFecCodedRate {
                numerator: 19,
                denominator: 20,
            })
        );
    }

    #[test]
    fn manifest_validation_rejects_audit_geometry_before_codec_construction() {
        let manifest = LosslessSessionManifest {
            block_size: 2_097_152,
            total_bytes: 2_097_152,
            total_blocks: 1,
            mode: LosslessSessionMode::Fec(LosslessSessionFecMode::new_raptorq(32, vec![1])),
        };

        assert_eq!(
            manifest.validate(),
            Err(LosslessSessionValidationError::FecSymbolPayloadTooLarge {
                symbol_size: 65_536,
                max: 65_443,
            })
        );
    }

    #[test]
    fn plain_mode_rejects_fec_only_frames() {
        let manifest = plain_manifest();
        let symbol = LosslessSessionBlockSymbol {
            block_id: 0,
            symbol_id: 0,
            tree_id: 1,
        };
        assert_eq!(
            manifest.validate_block_symbol(&symbol),
            Err(LosslessSessionValidationError::BlockSymbolRequiresFecMode)
        );
        assert_eq!(
            manifest.validate_control(&fec_need(
                0,
                vec![NeedBlock {
                    block_id: 0,
                    deficit_symbols: 1,
                }],
            )),
            Err(LosslessSessionValidationError::NeedRequiresFecMode)
        );
        manifest
            .validate_control(&plain_need(0, vec![]))
            .expect("plain manifests should accept canonical and empty need reports");
    }

    #[test]
    fn fec_mode_rejects_plain_only_frames_and_unknown_tree_ids() {
        let manifest = fec_manifest();
        let data = LosslessSessionBlockData { block_id: 0 };
        assert_eq!(
            manifest.validate_block_data(&data, 1024),
            Err(LosslessSessionValidationError::BlockDataRequiresPlainMode)
        );

        let bad_symbol = LosslessSessionBlockSymbol {
            block_id: 0,
            symbol_id: 5,
            tree_id: 99,
        };
        assert_eq!(
            manifest.validate_block_symbol(&bad_symbol),
            Err(LosslessSessionValidationError::BlockSymbolTreeIdNotAdvertised { tree_id: 99 })
        );
        assert_eq!(
            manifest.validate_control(&plain_need(
                0,
                vec![MissingBlockRange {
                    start_block_id: 0,
                    end_block_id: 1,
                }],
            )),
            Err(LosslessSessionValidationError::NeedRequiresPlainMode)
        );
        manifest
            .validate_control(&fec_need(0, vec![]))
            .expect("fec manifests should accept canonical and empty need reports");
    }

    #[test]
    fn plain_manifest_validates_block_lengths() {
        let manifest = plain_manifest();
        let full_block = LosslessSessionBlockData { block_id: 0 };
        manifest
            .validate_block_data(&full_block, 1024)
            .expect("first block should use full block size");

        let tail_block = LosslessSessionBlockData { block_id: 2 };
        manifest
            .validate_block_data(&tail_block, 452)
            .expect("tail block should use the remainder");

        let bad_tail = LosslessSessionBlockData { block_id: 2 };
        assert_eq!(
            manifest.validate_block_data(&bad_tail, 1024),
            Err(LosslessSessionValidationError::BlockDataLenMismatch {
                block_id: 2,
                expected: 452,
                actual: 1024,
            })
        );
    }

    #[test]
    fn zero_deficit_need_block_is_rejected() {
        let control = fec_need(
            18,
            vec![NeedBlock {
                block_id: 1,
                deficit_symbols: 0,
            }],
        );
        assert_eq!(
            control.validate(),
            Err(LosslessSessionValidationError::ZeroDeficitSymbols)
        );
    }

    #[test]
    fn block_ack_validation_enforces_manifest_bounds_and_canonical_ranges() {
        let manifest = carousel_manifest();
        let watermark_past_end = BlockAck::Blocks {
            completed_watermark: manifest.total_blocks + 1,
            extra_completed: vec![],
        };
        assert_eq!(
            watermark_past_end.validate_against_total_blocks(manifest.total_blocks),
            Err(
                LosslessSessionValidationError::BlockAckWatermarkOutOfRange {
                    completed_watermark: manifest.total_blocks + 1,
                    total_blocks: manifest.total_blocks,
                }
            )
        );

        let touching_ranges = BlockAck::Blocks {
            completed_watermark: 0,
            extra_completed: vec![
                CompletedBlockRange {
                    start_block_id: 1,
                    end_block_id: 2,
                },
                CompletedBlockRange {
                    start_block_id: 2,
                    end_block_id: 3,
                },
            ],
        };
        assert_eq!(
            touching_ranges.validate_against_total_blocks(manifest.total_blocks),
            Err(LosslessSessionValidationError::BlockAckRangesMustBeSortedMerged)
        );

        let range_past_end = BlockAck::Blocks {
            completed_watermark: 1,
            extra_completed: vec![CompletedBlockRange {
                start_block_id: 2,
                end_block_id: manifest.total_blocks + 1,
            }],
        };
        assert_eq!(
            range_past_end.validate_against_total_blocks(manifest.total_blocks),
            Err(LosslessSessionValidationError::BlockAckRangeOutOfRange {
                end_block_id: manifest.total_blocks + 1,
                total_blocks: manifest.total_blocks,
            })
        );
    }

    #[test]
    fn block_ack_canonicalization_folds_only_the_contiguous_watermark_prefix() {
        let ack = BlockAck::Blocks {
            completed_watermark: 1,
            extra_completed: vec![
                CompletedBlockRange {
                    start_block_id: 1,
                    end_block_id: 2,
                },
                CompletedBlockRange {
                    start_block_id: 2,
                    end_block_id: 4,
                },
                CompletedBlockRange {
                    start_block_id: 6,
                    end_block_id: 7,
                },
            ],
        };

        assert_eq!(
            ack.canonicalized(8).expect("canonical ack"),
            BlockAck::Blocks {
                completed_watermark: 4,
                extra_completed: vec![CompletedBlockRange {
                    start_block_id: 6,
                    end_block_id: 7,
                }],
            }
        );
    }

    #[test]
    fn need_validation_rejects_bad_blocks() {
        let manifest = fec_manifest();

        manifest
            .validate_need_report(&NeedReport::Complete)
            .expect("complete need should validate");
        manifest
            .validate_need_report(&NeedReport::Fec {
                blocks: vec![
                    NeedBlock {
                        block_id: 0,
                        deficit_symbols: 2,
                    },
                    NeedBlock {
                        block_id: 2,
                        deficit_symbols: 1,
                    },
                ],
            })
            .expect("sorted in-range need blocks should validate");

        assert_eq!(NeedReport::Fec { blocks: vec![] }.validate(), Ok(()));
        assert_eq!(
            manifest.validate_need_report(&NeedReport::Fec {
                blocks: vec![NeedBlock {
                    block_id: 0,
                    deficit_symbols: 0,
                }],
            }),
            Err(LosslessSessionValidationError::ZeroDeficitSymbols)
        );
        assert_eq!(
            NeedReport::Fec {
                blocks: vec![
                    NeedBlock {
                        block_id: 1,
                        deficit_symbols: 2,
                    },
                    NeedBlock {
                        block_id: 1,
                        deficit_symbols: 3,
                    },
                ],
            }
            .validate(),
            Err(LosslessSessionValidationError::NeedBlocksMustBeSortedUnique)
        );
        assert_eq!(
            manifest.validate_need_report(&NeedReport::Fec {
                blocks: vec![NeedBlock {
                    block_id: 3,
                    deficit_symbols: 1,
                }],
            }),
            Err(LosslessSessionValidationError::BlockIdOutOfRange {
                block_id: 3,
                total_blocks: 3,
            })
        );

        assert_eq!(
            NeedReport::Fec {
                blocks: (0..(MAX_NEED_BLOCKS as u64 + 1))
                    .map(|block_id| NeedBlock {
                        block_id,
                        deficit_symbols: 1,
                    })
                    .collect(),
            }
            .validate(),
            Err(LosslessSessionValidationError::TooManyNeedBlocks {
                configured: MAX_NEED_BLOCKS + 1,
                max: MAX_NEED_BLOCKS,
            })
        );
    }

    #[test]
    fn need_validation_rejects_bad_ranges() {
        let manifest = plain_manifest();

        manifest
            .validate_need_report(&NeedReport::Complete)
            .expect("complete need should validate");
        manifest
            .validate_need_report(&NeedReport::Plain {
                ranges: vec![
                    MissingBlockRange {
                        start_block_id: 0,
                        end_block_id: 1,
                    },
                    MissingBlockRange {
                        start_block_id: 2,
                        end_block_id: 3,
                    },
                ],
            })
            .expect("sorted disjoint missing ranges should validate");

        assert_eq!(NeedReport::Plain { ranges: vec![] }.validate(), Ok(()));
        assert_eq!(
            manifest.validate_need_report(&NeedReport::Plain {
                ranges: vec![MissingBlockRange {
                    start_block_id: 2,
                    end_block_id: 2,
                }],
            }),
            Err(LosslessSessionValidationError::MissingBlockRangeInvalid {
                start_block_id: 2,
                end_block_id: 2,
            })
        );
        assert_eq!(
            manifest.validate_need_report(&NeedReport::Plain {
                ranges: vec![MissingBlockRange {
                    start_block_id: 2,
                    end_block_id: 4,
                }],
            }),
            Err(LosslessSessionValidationError::NeedRangeOutOfRange {
                end_block_id: 4,
                total_blocks: 3,
            })
        );
        assert_eq!(
            manifest.validate_need_report(&NeedReport::Plain {
                ranges: vec![
                    MissingBlockRange {
                        start_block_id: 1,
                        end_block_id: 3,
                    },
                    MissingBlockRange {
                        start_block_id: 2,
                        end_block_id: 3,
                    },
                ],
            }),
            Err(LosslessSessionValidationError::NeedRangesMustBeSortedMerged)
        );
    }
}
