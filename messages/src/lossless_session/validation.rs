use super::{
    FecScheme, LosslessSessionBlockData, LosslessSessionBlockSymbol, LosslessSessionControl,
    LosslessSessionManifest, LosslessSessionMettleSymbol, LosslessSessionMode,
    LosslessSessionValidationError, MAX_MANIFEST_TREE_IDS, MAX_NEED_BLOCKS, MAX_NEED_RANGES,
    NeedReport,
};

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
            Self::Mettle { window } => {
                if window.replay_start_bin_id >= window.replay_end_bin_id {
                    return Err(LosslessSessionValidationError::MettleReplayWindowInvalid {
                        replay_start_bin_id: window.replay_start_bin_id,
                        replay_end_bin_id: window.replay_end_bin_id,
                    });
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
            Self::Mettle { window } => {
                if window.stalled_source_id >= total_blocks {
                    return Err(
                        LosslessSessionValidationError::MettleReplaySourceOutOfRange {
                            stalled_source_id: window.stalled_source_id,
                            total_blocks,
                        },
                    );
                }
                Ok(())
            }
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
            let Some(scheme) = fec.scheme_kind() else {
                return Err(LosslessSessionValidationError::UnknownFecScheme {
                    scheme: fec.scheme,
                });
            };
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
            match scheme {
                FecScheme::RaptorQ => {
                    if fec.symbols_per_block == 0 {
                        return Err(LosslessSessionValidationError::ZeroSymbolsPerBlock);
                    }
                }
                FecScheme::MettleV1 => {
                    if fec.symbols_per_block != 0 {
                        return Err(
                            LosslessSessionValidationError::MettleSymbolsPerBlockMustBeZero {
                                value: fec.symbols_per_block,
                            },
                        );
                    }
                    if fec.coded_rate_denominator == 0 {
                        return Err(LosslessSessionValidationError::ZeroMettleRateDenominator);
                    }
                    if fec.coded_rate_numerator < fec.coded_rate_denominator {
                        return Err(LosslessSessionValidationError::SubUnitMettleCodedRate {
                            numerator: fec.coded_rate_numerator,
                            denominator: fec.coded_rate_denominator,
                        });
                    }
                }
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
        if fec.scheme_kind() != Some(FecScheme::RaptorQ) {
            return Err(LosslessSessionValidationError::BlockSymbolRequiresRaptorQMode);
        }
        self.validate_block_id(symbol.block_id)?;
        if !fec.tree_ids.contains(&symbol.tree_id) {
            return Err(
                LosslessSessionValidationError::BlockSymbolTreeIdNotAdvertised {
                    tree_id: symbol.tree_id,
                },
            );
        }
        Ok(())
    }

    pub fn validate_mettle_symbol(
        &self,
        symbol: &LosslessSessionMettleSymbol,
    ) -> Result<(), LosslessSessionValidationError> {
        let LosslessSessionMode::Fec(fec) = &self.mode else {
            return Err(LosslessSessionValidationError::MettleSymbolRequiresMettleMode);
        };
        if fec.scheme_kind() != Some(FecScheme::MettleV1) {
            return Err(LosslessSessionValidationError::MettleSymbolRequiresMettleMode);
        }
        if !fec.tree_ids.contains(&symbol.tree_id) {
            return Err(
                LosslessSessionValidationError::MettleSymbolTreeIdNotAdvertised {
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
                let LosslessSessionMode::Fec(fec) = &self.mode else {
                    return Err(LosslessSessionValidationError::NeedRequiresFecMode);
                };
                if fec.scheme_kind() != Some(FecScheme::RaptorQ) && !blocks.is_empty() {
                    return Err(LosslessSessionValidationError::NeedRequiresFecMode);
                }
                NeedReport::Fec {
                    blocks: blocks.clone(),
                }
                .validate_against_total_blocks(self.total_blocks)
            }
            NeedReport::Mettle { window } => {
                let LosslessSessionMode::Fec(fec) = &self.mode else {
                    return Err(LosslessSessionValidationError::NeedRequiresMettleMode);
                };
                if fec.scheme_kind() != Some(FecScheme::MettleV1) {
                    return Err(LosslessSessionValidationError::NeedRequiresMettleMode);
                }
                NeedReport::Mettle { window: *window }
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
            LosslessSessionControl::Ready | LosslessSessionControl::SourceDone { .. } => Ok(()),
            LosslessSessionControl::Need { report, .. } => self.validate_need_report(report),
        }
    }
}

impl LosslessSessionControl {
    pub fn validate(&self) -> Result<(), LosslessSessionValidationError> {
        match self {
            Self::Manifest { manifest } => manifest.validate(),
            Self::Ready | Self::SourceDone { .. } => Ok(()),
            Self::Need { report, .. } => report.validate(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lossless_session::test_support::{
        fec_manifest, fec_need, plain_manifest, plain_need,
    };
    use crate::lossless_session::{
        LosslessSessionBlockData, LosslessSessionBlockSymbol, LosslessSessionFecMode,
        LosslessSessionManifest, LosslessSessionMode, MissingBlockRange, NeedBlock,
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
                tree_ids: vec![],
            }),
        };
        assert_eq!(
            bad_fec.validate(),
            Err(LosslessSessionValidationError::UnknownFecScheme { scheme: 99 })
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
