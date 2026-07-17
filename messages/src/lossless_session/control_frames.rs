use super::{
    BLOCK_ACK_BLOCKS_VARIANT, BLOCK_ACK_METTLE_STREAM_VARIANT, BlockAck, CompletedBlockRange,
    FecFeedbackMode, LosslessSessionControl, LosslessSessionCtrlKind, LosslessSessionFecMode,
    LosslessSessionHeader, LosslessSessionKind, LosslessSessionManifest, LosslessSessionMode,
    LosslessSessionModeKind, MAX_BLOCK_ACK_RANGES, MAX_MANIFEST_TREE_IDS,
    MAX_METTLE_MISSING_BIN_RANGES, MAX_NEED_BLOCKS, MAX_NEED_RANGES, MettleObjectStreamGeometry,
    MettleStallEvidence, MissingBlockRange, MissingMettleBinRange, NeedBlock, NeedReport,
};

const MANIFEST_FIXED_BODY_LEN: usize = 1 + 1 + 1 + 1 + 4 + 8 + 8 + 4 + 4 + 4 + 4 + 4 + 8 + 4;
const NEED_FIXED_BODY_LEN: usize = 4 + 1 + 2 + 1;
const NEED_RANGE_LEN: usize = 8 + 8;
const NEED_BLOCK_LEN: usize = 8 + 2;
const BLOCK_ACK_FIXED_BODY_LEN: usize = 1 + 1 + 2 + 8;
const BLOCK_ACK_RANGE_LEN: usize = 8 + 8;
const METTLE_ACK_FIXED_BODY_LEN: usize = 1 + 1 + 2 + 8 + 4 + 4;
const METTLE_ACK_RANGE_LEN: usize = 4 + 4;

/// Stack-friendly scratch size for common control frames.
///
/// Large `Need::Fec` reports can exceed this bound; callers that need to encode
/// arbitrarily large FEC feedback payloads should use [`encode_control`], which
/// allocates an exact-size `Vec<u8>`.
#[cfg(test)]
const MAX_CONTROL_FRAME_SIZE: usize = LosslessSessionHeader::LEN
    + max_control_body_len(
        MANIFEST_FIXED_BODY_LEN + (MAX_MANIFEST_TREE_IDS * 2),
        NEED_FIXED_BODY_LEN + (MAX_NEED_RANGES * NEED_RANGE_LEN),
        NEED_FIXED_BODY_LEN + (MAX_NEED_BLOCKS * NEED_BLOCK_LEN),
        BLOCK_ACK_FIXED_BODY_LEN + (MAX_BLOCK_ACK_RANGES * BLOCK_ACK_RANGE_LEN),
    );

#[cfg(test)]
const fn max_control_body_len(lhs: usize, mid: usize, rhs: usize, fourth: usize) -> usize {
    let first = if lhs > mid { lhs } else { mid };
    let second = if first > rhs { first } else { rhs };
    if second > fourth { second } else { fourth }
}

fn manifest_tree_ids(mode: &LosslessSessionMode) -> &[u16] {
    match mode {
        LosslessSessionMode::Plain => &[],
        LosslessSessionMode::Fec(fec) => &fec.tree_ids,
    }
}

fn control_body_len(control: &LosslessSessionControl) -> usize {
    match control {
        LosslessSessionControl::Manifest { manifest } => {
            MANIFEST_FIXED_BODY_LEN + (manifest_tree_ids(&manifest.mode).len() * 2)
        }
        LosslessSessionControl::Ready => 0,
        LosslessSessionControl::SourceDone { .. } => 4,
        LosslessSessionControl::Need { report, .. } => match report {
            NeedReport::Complete => NEED_FIXED_BODY_LEN,
            NeedReport::Plain { ranges } => {
                if ranges.is_empty() {
                    NEED_FIXED_BODY_LEN
                } else {
                    NEED_FIXED_BODY_LEN + (ranges.len() * NEED_RANGE_LEN)
                }
            }
            NeedReport::Fec { blocks } => {
                if blocks.is_empty() {
                    NEED_FIXED_BODY_LEN
                } else {
                    NEED_FIXED_BODY_LEN + (blocks.len() * NEED_BLOCK_LEN)
                }
            }
        },
        LosslessSessionControl::BlockAck { ack } => match ack {
            BlockAck::Blocks {
                extra_completed, ..
            } => BLOCK_ACK_FIXED_BODY_LEN + (extra_completed.len() * BLOCK_ACK_RANGE_LEN),
            BlockAck::MettleStream { stalled, .. } => {
                METTLE_ACK_FIXED_BODY_LEN
                    + stalled
                        .as_ref()
                        .map_or(0, |evidence| evidence.missing_bin_ranges.len())
                        * METTLE_ACK_RANGE_LEN
            }
        },
        LosslessSessionControl::AckProbe { .. } => 8,
        LosslessSessionControl::SessionComplete => 0,
        LosslessSessionControl::DepartureCheckpoint { .. } => 8 + 4 + 4,
    }
}

/// Encode a CONTROL frame into the provided buffer.
///
/// The buffer must be at least `LosslessSessionHeader::LEN + control_body_len(control)` bytes.
fn encode_control_into<'a>(
    buf: &'a mut [u8],
    session_id: u64,
    control: &LosslessSessionControl,
) -> &'a [u8] {
    let canonical_control = match control {
        LosslessSessionControl::BlockAck { ack } => Some(LosslessSessionControl::BlockAck {
            ack: ack
                .clone()
                .canonicalized(u64::MAX)
                .expect("lossless BlockAck must canonicalize before encoding"),
        }),
        _ => None,
    };
    let control = canonical_control.as_ref().unwrap_or(control);
    control
        .validate()
        .expect("lossless control must validate before encoding");
    let body_len = control_body_len(control);
    assert!(
        buf.len() >= LosslessSessionHeader::LEN + body_len,
        "buffer too small for encoded control frame"
    );

    let ctrl_kind = match control {
        LosslessSessionControl::Manifest { manifest } => {
            let body_start = LosslessSessionHeader::LEN;
            let (
                scheme,
                symbols_per_block,
                coded_rate_num,
                coded_rate_den,
                feedback_mode,
                mettle_object_stream,
                tree_ids,
            ) = match &manifest.mode {
                LosslessSessionMode::Plain => (0u8, 0u32, 0u32, 0u32, 0u8, None, &[][..]),
                LosslessSessionMode::Fec(fec) => (
                    fec.scheme,
                    fec.symbols_per_block,
                    fec.coded_rate_num,
                    fec.coded_rate_den,
                    fec.feedback_mode.to_wire(),
                    fec.mettle_object_stream,
                    fec.tree_ids.as_slice(),
                ),
            };
            assert!(
                tree_ids.len() <= MAX_MANIFEST_TREE_IDS,
                "manifest tree set exceeds wire capacity"
            );

            buf[body_start] = manifest.mode.kind() as u8;
            buf[body_start + 1] = scheme;
            buf[body_start + 2] = tree_ids.len() as u8;
            buf[body_start + 3] = feedback_mode;
            buf[body_start + 4..body_start + 8].copy_from_slice(&manifest.block_size.to_be_bytes());
            buf[body_start + 8..body_start + 16]
                .copy_from_slice(&manifest.total_bytes.to_be_bytes());
            buf[body_start + 16..body_start + 24]
                .copy_from_slice(&manifest.total_blocks.to_be_bytes());
            buf[body_start + 24..body_start + 28].copy_from_slice(&symbols_per_block.to_be_bytes());
            buf[body_start + 28..body_start + 32].copy_from_slice(&coded_rate_num.to_be_bytes());
            buf[body_start + 32..body_start + 36].copy_from_slice(&coded_rate_den.to_be_bytes());
            let geometry =
                mettle_object_stream.unwrap_or(MettleObjectStreamGeometry::new(0, 0, 0, 0));
            buf[body_start + 36..body_start + 40]
                .copy_from_slice(&geometry.source_symbol_bytes.to_be_bytes());
            buf[body_start + 40..body_start + 44]
                .copy_from_slice(&geometry.source_symbols_per_stream.to_be_bytes());
            buf[body_start + 44..body_start + 52]
                .copy_from_slice(&geometry.stream_count.to_be_bytes());
            buf[body_start + 52..body_start + 56]
                .copy_from_slice(&geometry.final_stream_source_symbols.to_be_bytes());

            let mut pos = body_start + MANIFEST_FIXED_BODY_LEN;
            for tree_id in tree_ids {
                buf[pos..pos + 2].copy_from_slice(&tree_id.to_be_bytes());
                pos += 2;
            }
            LosslessSessionCtrlKind::Manifest as u8
        }
        LosslessSessionControl::Ready => LosslessSessionCtrlKind::Ready as u8,
        LosslessSessionControl::SourceDone { round_id } => {
            let body_start = LosslessSessionHeader::LEN;
            buf[body_start..body_start + 4].copy_from_slice(&round_id.to_be_bytes());
            LosslessSessionCtrlKind::SourceDone as u8
        }
        LosslessSessionControl::Need { round_id, report } => {
            let body_start = LosslessSessionHeader::LEN;
            buf[body_start..body_start + 4].copy_from_slice(&round_id.to_be_bytes());
            match report {
                NeedReport::Complete => {
                    buf[body_start + 4] = 0;
                    buf[body_start + 5..body_start + 8].copy_from_slice(&0u32.to_be_bytes()[1..]);
                }
                NeedReport::Plain { ranges } if ranges.is_empty() => {
                    buf[body_start + 4] = 0;
                    buf[body_start + 5..body_start + 8].copy_from_slice(&0u32.to_be_bytes()[1..]);
                }
                NeedReport::Plain { ranges } => {
                    assert!(
                        ranges.len() <= MAX_NEED_RANGES,
                        "missing block ranges exceed wire capacity"
                    );
                    buf[body_start + 4] = 1;
                    let count = ranges.len() as u16;
                    buf[body_start + 5..body_start + 7].copy_from_slice(&count.to_be_bytes());
                    buf[body_start + 7] = 0;
                    let mut pos = body_start + NEED_FIXED_BODY_LEN;
                    for range in ranges {
                        buf[pos..pos + 8].copy_from_slice(&range.start_block_id.to_be_bytes());
                        buf[pos + 8..pos + 16].copy_from_slice(&range.end_block_id.to_be_bytes());
                        pos += NEED_RANGE_LEN;
                    }
                }
                NeedReport::Fec { blocks } if blocks.is_empty() => {
                    buf[body_start + 4] = 0;
                    buf[body_start + 5..body_start + 8].copy_from_slice(&0u32.to_be_bytes()[1..]);
                }
                NeedReport::Fec { blocks } => {
                    assert!(
                        blocks.len() <= MAX_NEED_BLOCKS,
                        "fec need blocks exceed wire capacity"
                    );
                    buf[body_start + 4] = 2;
                    let count = blocks.len() as u16;
                    buf[body_start + 5..body_start + 7].copy_from_slice(&count.to_be_bytes());
                    buf[body_start + 7] = 0;
                    let mut pos = body_start + NEED_FIXED_BODY_LEN;
                    for block in blocks {
                        buf[pos..pos + 8].copy_from_slice(&block.block_id.to_be_bytes());
                        buf[pos + 8..pos + 10]
                            .copy_from_slice(&block.deficit_symbols.to_be_bytes());
                        pos += NEED_BLOCK_LEN;
                    }
                }
            }
            LosslessSessionCtrlKind::Need as u8
        }
        LosslessSessionControl::BlockAck { ack } => {
            let body_start = LosslessSessionHeader::LEN;
            match ack {
                BlockAck::Blocks {
                    completed_watermark,
                    extra_completed,
                } => {
                    assert!(
                        extra_completed.len() <= MAX_BLOCK_ACK_RANGES,
                        "block-ack ranges exceed wire capacity"
                    );
                    buf[body_start] = BLOCK_ACK_BLOCKS_VARIANT;
                    buf[body_start + 1] = 0;
                    let count = extra_completed.len() as u16;
                    buf[body_start + 2..body_start + 4].copy_from_slice(&count.to_be_bytes());
                    buf[body_start + 4..body_start + 12]
                        .copy_from_slice(&completed_watermark.to_be_bytes());
                    let mut pos = body_start + BLOCK_ACK_FIXED_BODY_LEN;
                    for range in extra_completed {
                        buf[pos..pos + 8].copy_from_slice(&range.start_block_id.to_be_bytes());
                        buf[pos + 8..pos + 16].copy_from_slice(&range.end_block_id.to_be_bytes());
                        pos += BLOCK_ACK_RANGE_LEN;
                    }
                }
                BlockAck::MettleStream {
                    stream_id,
                    decoded_source_watermark,
                    stalled,
                } => {
                    let missing_ranges = stalled
                        .as_ref()
                        .map_or(&[][..], |evidence| evidence.missing_bin_ranges.as_slice());
                    assert!(
                        missing_ranges.len() <= MAX_METTLE_MISSING_BIN_RANGES,
                        "METTLE missing-bin ranges exceed wire capacity"
                    );
                    buf[body_start] = BLOCK_ACK_METTLE_STREAM_VARIANT;
                    buf[body_start + 1] = u8::from(stalled.is_some());
                    let count = u16::try_from(missing_ranges.len())
                        .expect("validated METTLE range count fits u16");
                    buf[body_start + 2..body_start + 4].copy_from_slice(&count.to_be_bytes());
                    buf[body_start + 4..body_start + 12].copy_from_slice(&stream_id.to_be_bytes());
                    buf[body_start + 12..body_start + 16]
                        .copy_from_slice(&decoded_source_watermark.to_be_bytes());
                    let repair_epoch = stalled.as_ref().map_or(0, |evidence| evidence.repair_epoch);
                    buf[body_start + 16..body_start + 20]
                        .copy_from_slice(&repair_epoch.to_be_bytes());
                    let mut pos = body_start + METTLE_ACK_FIXED_BODY_LEN;
                    for range in missing_ranges {
                        buf[pos..pos + 4].copy_from_slice(&range.start_bin_id.to_be_bytes());
                        buf[pos + 4..pos + 8].copy_from_slice(&range.end_bin_id.to_be_bytes());
                        pos += METTLE_ACK_RANGE_LEN;
                    }
                }
            }
            LosslessSessionCtrlKind::BlockAck as u8
        }
        LosslessSessionControl::AckProbe { target_peer_id } => {
            let body_start = LosslessSessionHeader::LEN;
            buf[body_start..body_start + 8].copy_from_slice(&target_peer_id.to_be_bytes());
            LosslessSessionCtrlKind::AckProbe as u8
        }
        LosslessSessionControl::SessionComplete => LosslessSessionCtrlKind::SessionComplete as u8,
        LosslessSessionControl::DepartureCheckpoint {
            stream_id,
            repair_epoch,
            departure_bin_exclusive,
        } => {
            let body_start = LosslessSessionHeader::LEN;
            buf[body_start..body_start + 8].copy_from_slice(&stream_id.to_be_bytes());
            buf[body_start + 8..body_start + 12].copy_from_slice(&repair_epoch.to_be_bytes());
            buf[body_start + 12..body_start + 16]
                .copy_from_slice(&departure_bin_exclusive.to_be_bytes());
            LosslessSessionCtrlKind::DepartureCheckpoint as u8
        }
    };

    LosslessSessionHeader {
        magic: super::LOSSLESS_SESSION_MAGIC,
        version: super::LOSSLESS_SESSION_VERSION,
        kind: LosslessSessionKind::Control,
        ctrl_kind,
        session_id,
        body_len: body_len as u32,
    }
    .encode_into(&mut buf[..LosslessSessionHeader::LEN]);

    &buf[..LosslessSessionHeader::LEN + body_len]
}

/// Encode a CONTROL frame (header + control body) into a fresh `Vec<u8>`.
pub fn encode_control(session_id: u64, control: &LosslessSessionControl) -> Vec<u8> {
    let mut buf = vec![0u8; LosslessSessionHeader::LEN + control_body_len(control)];
    let encoded_len = encode_control_into(&mut buf, session_id, control).len();
    buf.truncate(encoded_len);
    buf
}

/// Try to decode a CONTROL frame; returns (header, parsed control).
pub fn decode_control(buf: &[u8]) -> Option<(LosslessSessionHeader, LosslessSessionControl)> {
    let (hdr, off) = LosslessSessionHeader::decode_from(buf)?;
    if hdr.kind != LosslessSessionKind::Control {
        return None;
    }
    let body_end = off.checked_add(usize::try_from(hdr.body_len).ok()?)?;
    if buf.len() < body_end {
        return None;
    }
    let body = &buf[off..body_end];
    let ctrl = match hdr.ctrl_kind {
        x if x == LosslessSessionCtrlKind::Manifest as u8 => {
            if body.len() < MANIFEST_FIXED_BODY_LEN {
                return None;
            }
            let mode_kind = LosslessSessionModeKind::from_wire(body[0])?;
            let scheme = body[1];
            let tree_count = body[2] as usize;
            let feedback_mode = body[3];
            let block_size = u32::from_be_bytes(body[4..8].try_into().ok()?);
            let total_bytes = u64::from_be_bytes(body[8..16].try_into().ok()?);
            let total_blocks = u64::from_be_bytes(body[16..24].try_into().ok()?);
            let symbols_per_block = u32::from_be_bytes(body[24..28].try_into().ok()?);
            let coded_rate_num = u32::from_be_bytes(body[28..32].try_into().ok()?);
            let coded_rate_den = u32::from_be_bytes(body[32..36].try_into().ok()?);
            let object_source_symbol_bytes = u32::from_be_bytes(body[36..40].try_into().ok()?);
            let object_sources_per_stream = u32::from_be_bytes(body[40..44].try_into().ok()?);
            let object_stream_count = u64::from_be_bytes(body[44..52].try_into().ok()?);
            let object_final_stream_sources = u32::from_be_bytes(body[52..56].try_into().ok()?);
            let mettle_object_stream = if object_source_symbol_bytes == 0
                && object_sources_per_stream == 0
                && object_stream_count == 0
                && object_final_stream_sources == 0
            {
                None
            } else {
                Some(MettleObjectStreamGeometry::new(
                    object_source_symbol_bytes,
                    object_sources_per_stream,
                    object_stream_count,
                    object_final_stream_sources,
                ))
            };

            if body.len() != MANIFEST_FIXED_BODY_LEN + (tree_count * 2) {
                return None;
            }

            let mut tree_ids = Vec::with_capacity(tree_count);
            let mut pos = MANIFEST_FIXED_BODY_LEN;
            for _ in 0..tree_count {
                tree_ids.push(u16::from_be_bytes(body[pos..pos + 2].try_into().ok()?));
                pos += 2;
            }

            let mode = match mode_kind {
                LosslessSessionModeKind::Plain => {
                    if scheme != 0
                        || feedback_mode != 0
                        || symbols_per_block != 0
                        || coded_rate_num != 0
                        || coded_rate_den != 0
                        || mettle_object_stream.is_some()
                        || !tree_ids.is_empty()
                    {
                        return None;
                    }
                    LosslessSessionMode::Plain
                }
                LosslessSessionModeKind::Fec => LosslessSessionMode::Fec(LosslessSessionFecMode {
                    scheme,
                    symbols_per_block,
                    coded_rate_num,
                    coded_rate_den,
                    feedback_mode: FecFeedbackMode::from_wire(feedback_mode)?,
                    mettle_object_stream,
                    tree_ids,
                }),
            };
            let manifest = LosslessSessionManifest {
                block_size,
                total_bytes,
                total_blocks,
                mode,
            };
            manifest.validate().ok()?;
            LosslessSessionControl::Manifest { manifest }
        }
        x if x == LosslessSessionCtrlKind::Ready as u8 => {
            if !body.is_empty() {
                return None;
            }
            LosslessSessionControl::Ready
        }
        x if x == LosslessSessionCtrlKind::SourceDone as u8 => {
            if body.len() != 4 {
                return None;
            }
            let round_id = u32::from_be_bytes(body[0..4].try_into().ok()?);
            LosslessSessionControl::SourceDone { round_id }
        }
        x if x == LosslessSessionCtrlKind::Need as u8 => {
            if body.len() < NEED_FIXED_BODY_LEN {
                return None;
            }

            let round_id = u32::from_be_bytes(body[0..4].try_into().ok()?);
            let report_kind = body[4];
            let report_count = usize::from(u16::from_be_bytes(body[5..7].try_into().ok()?));
            if body[7] != 0 {
                return None;
            }

            let report = match report_kind {
                0 => {
                    if report_count != 0 || body.len() != NEED_FIXED_BODY_LEN {
                        return None;
                    }
                    NeedReport::Complete
                }
                1 => {
                    if report_count == 0 || report_count > MAX_NEED_RANGES {
                        return None;
                    }
                    if body.len() != NEED_FIXED_BODY_LEN + (report_count * NEED_RANGE_LEN) {
                        return None;
                    }
                    let mut ranges = Vec::with_capacity(report_count);
                    let mut pos = NEED_FIXED_BODY_LEN;
                    for _ in 0..report_count {
                        ranges.push(MissingBlockRange {
                            start_block_id: u64::from_be_bytes(body[pos..pos + 8].try_into().ok()?),
                            end_block_id: u64::from_be_bytes(
                                body[pos + 8..pos + 16].try_into().ok()?,
                            ),
                        });
                        pos += NEED_RANGE_LEN;
                    }
                    NeedReport::Plain { ranges }
                }
                2 => {
                    if report_count == 0 || report_count > MAX_NEED_BLOCKS {
                        return None;
                    }
                    if body.len() != NEED_FIXED_BODY_LEN + (report_count * NEED_BLOCK_LEN) {
                        return None;
                    }
                    let mut blocks = Vec::with_capacity(report_count);
                    let mut pos = NEED_FIXED_BODY_LEN;
                    for _ in 0..report_count {
                        blocks.push(NeedBlock {
                            block_id: u64::from_be_bytes(body[pos..pos + 8].try_into().ok()?),
                            deficit_symbols: u16::from_be_bytes(
                                body[pos + 8..pos + 10].try_into().ok()?,
                            ),
                        });
                        pos += NEED_BLOCK_LEN;
                    }
                    NeedReport::Fec { blocks }
                }
                _ => return None,
            };
            report.validate().ok()?;
            LosslessSessionControl::Need { round_id, report }
        }
        x if x == LosslessSessionCtrlKind::BlockAck as u8 => {
            if body.len() < BLOCK_ACK_FIXED_BODY_LEN {
                return None;
            }
            let ack = match body[0] {
                BLOCK_ACK_BLOCKS_VARIANT => {
                    if body[1] != 0 {
                        return None;
                    }
                    let range_count = usize::from(u16::from_be_bytes(body[2..4].try_into().ok()?));
                    if range_count > MAX_BLOCK_ACK_RANGES
                        || body.len()
                            != BLOCK_ACK_FIXED_BODY_LEN + (range_count * BLOCK_ACK_RANGE_LEN)
                    {
                        return None;
                    }
                    let completed_watermark = u64::from_be_bytes(body[4..12].try_into().ok()?);
                    let mut extra_completed = Vec::with_capacity(range_count);
                    let mut pos = BLOCK_ACK_FIXED_BODY_LEN;
                    for _ in 0..range_count {
                        extra_completed.push(CompletedBlockRange {
                            start_block_id: u64::from_be_bytes(body[pos..pos + 8].try_into().ok()?),
                            end_block_id: u64::from_be_bytes(
                                body[pos + 8..pos + 16].try_into().ok()?,
                            ),
                        });
                        pos += BLOCK_ACK_RANGE_LEN;
                    }
                    BlockAck::Blocks {
                        completed_watermark,
                        extra_completed,
                    }
                    .canonicalized(u64::MAX)
                    .ok()?
                }
                BLOCK_ACK_METTLE_STREAM_VARIANT => {
                    if body.len() < METTLE_ACK_FIXED_BODY_LEN || body[1] > 1 {
                        return None;
                    }
                    let has_stall_evidence = body[1] == 1;
                    let range_count = usize::from(u16::from_be_bytes(body[2..4].try_into().ok()?));
                    if range_count > MAX_METTLE_MISSING_BIN_RANGES
                        || (!has_stall_evidence && range_count != 0)
                        || body.len()
                            != METTLE_ACK_FIXED_BODY_LEN + (range_count * METTLE_ACK_RANGE_LEN)
                    {
                        return None;
                    }
                    let stream_id = u64::from_be_bytes(body[4..12].try_into().ok()?);
                    let decoded_source_watermark =
                        u32::from_be_bytes(body[12..16].try_into().ok()?);
                    let repair_epoch = u32::from_be_bytes(body[16..20].try_into().ok()?);
                    if !has_stall_evidence && repair_epoch != 0 {
                        return None;
                    }
                    let mut missing_bin_ranges = Vec::with_capacity(range_count);
                    let mut pos = METTLE_ACK_FIXED_BODY_LEN;
                    for _ in 0..range_count {
                        missing_bin_ranges.push(MissingMettleBinRange {
                            start_bin_id: u32::from_be_bytes(body[pos..pos + 4].try_into().ok()?),
                            end_bin_id: u32::from_be_bytes(body[pos + 4..pos + 8].try_into().ok()?),
                        });
                        pos += METTLE_ACK_RANGE_LEN;
                    }
                    BlockAck::MettleStream {
                        stream_id,
                        decoded_source_watermark,
                        stalled: has_stall_evidence.then_some(MettleStallEvidence {
                            repair_epoch,
                            missing_bin_ranges,
                        }),
                    }
                }
                _ => return None,
            };
            LosslessSessionControl::BlockAck { ack }
        }
        x if x == LosslessSessionCtrlKind::AckProbe as u8 => {
            if body.len() != 8 {
                return None;
            }
            LosslessSessionControl::AckProbe {
                target_peer_id: u64::from_be_bytes(body.try_into().ok()?),
            }
        }
        x if x == LosslessSessionCtrlKind::SessionComplete as u8 => {
            if !body.is_empty() {
                return None;
            }
            LosslessSessionControl::SessionComplete
        }
        x if x == LosslessSessionCtrlKind::DepartureCheckpoint as u8 => {
            if body.len() != 16 {
                return None;
            }
            LosslessSessionControl::DepartureCheckpoint {
                stream_id: u64::from_be_bytes(body[0..8].try_into().ok()?),
                repair_epoch: u32::from_be_bytes(body[8..12].try_into().ok()?),
                departure_bin_exclusive: u32::from_be_bytes(body[12..16].try_into().ok()?),
            }
        }
        _ => return None,
    };
    ctrl.validate().ok()?;
    Some((hdr, ctrl))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lossless_session::test_support::{
        carousel_manifest, fec_manifest, fec_need, plain_manifest, plain_need,
    };
    use crate::lossless_session::{
        BLOCK_ACK_METTLE_STREAM_VARIANT, BlockAck, CompletedBlockRange, FecScheme,
        LosslessSessionControl, LosslessSessionFecMode, LosslessSessionHeader,
        LosslessSessionManifest, LosslessSessionMode, MettleStallEvidence, MissingMettleBinRange,
        NeedBlock, NeedReport, encode_block_data,
    };

    #[test]
    fn roundtrip_controls() {
        let manifest_plain = LosslessSessionControl::Manifest {
            manifest: plain_manifest(),
        };
        let manifest_fec = LosslessSessionControl::Manifest {
            manifest: fec_manifest(),
        };
        let ctrls = vec![
            manifest_plain,
            manifest_fec,
            LosslessSessionControl::Ready,
            LosslessSessionControl::SourceDone { round_id: 7 },
            LosslessSessionControl::Need {
                round_id: 8,
                report: NeedReport::Complete,
            },
            plain_need(
                9,
                vec![
                    MissingBlockRange {
                        start_block_id: 0,
                        end_block_id: 1,
                    },
                    MissingBlockRange {
                        start_block_id: 2,
                        end_block_id: 3,
                    },
                ],
            ),
            fec_need(
                10,
                vec![
                    NeedBlock {
                        block_id: 0,
                        deficit_symbols: 2,
                    },
                    NeedBlock {
                        block_id: 2,
                        deficit_symbols: 1,
                    },
                ],
            ),
            LosslessSessionControl::BlockAck {
                ack: BlockAck::Blocks {
                    completed_watermark: 2,
                    extra_completed: vec![CompletedBlockRange {
                        start_block_id: 4,
                        end_block_id: 6,
                    }],
                },
            },
            LosslessSessionControl::BlockAck {
                ack: BlockAck::MettleStream {
                    stream_id: 3,
                    decoded_source_watermark: 17,
                    stalled: None,
                },
            },
            LosslessSessionControl::BlockAck {
                ack: BlockAck::MettleStream {
                    stream_id: 4,
                    decoded_source_watermark: 9,
                    stalled: Some(MettleStallEvidence {
                        repair_epoch: 7,
                        missing_bin_ranges: vec![
                            MissingMettleBinRange {
                                start_bin_id: 2,
                                end_bin_id: 4,
                            },
                            MissingMettleBinRange {
                                start_bin_id: 9,
                                end_bin_id: 10,
                            },
                        ],
                    }),
                },
            },
            LosslessSessionControl::BlockAck {
                ack: BlockAck::MettleStream {
                    stream_id: 4,
                    decoded_source_watermark: 9,
                    stalled: Some(MettleStallEvidence {
                        repair_epoch: 8,
                        missing_bin_ranges: vec![],
                    }),
                },
            },
            LosslessSessionControl::AckProbe { target_peer_id: 22 },
            LosslessSessionControl::SessionComplete,
            LosslessSessionControl::DepartureCheckpoint {
                stream_id: 3,
                repair_epoch: 7,
                departure_bin_exclusive: 4096,
            },
        ];

        for ctrl in ctrls {
            let buf = encode_control(77, &ctrl);
            let (hdr, decoded) = decode_control(&buf).expect("decode control");
            assert_eq!(hdr.session_id, 77);
            assert_eq!(hdr.version, super::super::LOSSLESS_SESSION_VERSION);
            assert_eq!(decoded, ctrl);
        }
    }

    #[test]
    fn ready_control_uses_empty_body() {
        let ctrl = LosslessSessionControl::Ready;
        let buf = encode_control(77, &ctrl);
        let (hdr, decoded) = decode_control(&buf).expect("decode ready");
        assert_eq!(hdr.body_len, 0);
        assert_eq!(buf.len(), LosslessSessionHeader::LEN);
        assert_eq!(decoded, ctrl);
    }

    #[test]
    fn carousel_signal_control_bodies_are_canonical() {
        let probe = LosslessSessionControl::AckProbe { target_peer_id: 22 };
        let encoded = encode_control(77, &probe);
        let (header, decoded) = decode_control(&encoded).expect("decode AckProbe");
        assert_eq!(header.body_len, 8);
        assert_eq!(encoded.len(), LosslessSessionHeader::LEN + 8);
        assert_eq!(decoded, probe);

        let encoded = encode_control(77, &LosslessSessionControl::SessionComplete);
        let (header, decoded) = decode_control(&encoded).expect("decode SessionComplete");
        assert_eq!(header.body_len, 0);
        assert_eq!(encoded.len(), LosslessSessionHeader::LEN);
        assert_eq!(decoded, LosslessSessionControl::SessionComplete);

        let checkpoint = LosslessSessionControl::DepartureCheckpoint {
            stream_id: 3,
            repair_epoch: 7,
            departure_bin_exclusive: 4096,
        };
        let encoded = encode_control(77, &checkpoint);
        let (header, decoded) = decode_control(&encoded).expect("decode DepartureCheckpoint");
        assert_eq!(header.body_len, 16);
        assert_eq!(encoded.len(), LosslessSessionHeader::LEN + 16);
        assert_eq!(decoded, checkpoint);

        let mut truncated_checkpoint = encoded;
        truncated_checkpoint.pop();
        assert!(decode_control(&truncated_checkpoint).is_none());

        let mut empty_probe = encode_control(77, &LosslessSessionControl::SessionComplete);
        empty_probe[6] = LosslessSessionCtrlKind::AckProbe as u8;
        assert!(
            decode_control(&empty_probe).is_none(),
            "AckProbe must carry its target peer id"
        );
    }

    #[test]
    fn block_ack_decode_folds_ranges_touching_watermark() {
        let encoded = encode_control(
            77,
            &LosslessSessionControl::BlockAck {
                ack: BlockAck::Blocks {
                    completed_watermark: 2,
                    extra_completed: vec![
                        CompletedBlockRange {
                            start_block_id: 2,
                            end_block_id: 4,
                        },
                        CompletedBlockRange {
                            start_block_id: 4,
                            end_block_id: 7,
                        },
                        CompletedBlockRange {
                            start_block_id: 9,
                            end_block_id: 10,
                        },
                    ],
                },
            },
        );

        let (_, decoded) = decode_control(&encoded).expect("decode foldable block ack");
        assert_eq!(
            decoded,
            LosslessSessionControl::BlockAck {
                ack: BlockAck::Blocks {
                    completed_watermark: 7,
                    extra_completed: vec![CompletedBlockRange {
                        start_block_id: 9,
                        end_block_id: 10,
                    }],
                },
            }
        );
    }

    #[test]
    fn block_ack_encode_uses_canonical_wire_bytes() {
        let unfolded = LosslessSessionControl::BlockAck {
            ack: BlockAck::Blocks {
                completed_watermark: 2,
                extra_completed: vec![
                    CompletedBlockRange {
                        start_block_id: 2,
                        end_block_id: 4,
                    },
                    CompletedBlockRange {
                        start_block_id: 4,
                        end_block_id: 7,
                    },
                    CompletedBlockRange {
                        start_block_id: 9,
                        end_block_id: 10,
                    },
                ],
            },
        };
        let canonical = LosslessSessionControl::BlockAck {
            ack: BlockAck::Blocks {
                completed_watermark: 7,
                extra_completed: vec![CompletedBlockRange {
                    start_block_id: 9,
                    end_block_id: 10,
                }],
            },
        };

        assert_eq!(
            encode_control(77, &unfolded),
            encode_control(77, &canonical)
        );
    }

    #[test]
    fn mettle_progress_ack_uses_reserved_variant_layout() {
        let control = LosslessSessionControl::BlockAck {
            ack: BlockAck::MettleStream {
                stream_id: 0x0102_0304_0506_0708,
                decoded_source_watermark: 37,
                stalled: Some(MettleStallEvidence {
                    repair_epoch: 11,
                    missing_bin_ranges: vec![MissingMettleBinRange {
                        start_bin_id: 19,
                        end_bin_id: 23,
                    }],
                }),
            },
        };

        let encoded = encode_control(77, &control);
        let body = &encoded[LosslessSessionHeader::LEN..];
        assert_eq!(body.len(), METTLE_ACK_FIXED_BODY_LEN + METTLE_ACK_RANGE_LEN);
        assert_eq!(body[0], BLOCK_ACK_METTLE_STREAM_VARIANT);
        assert_eq!(body[1], 1);
        assert_eq!(&body[2..4], 1u16.to_be_bytes().as_slice());
        assert_eq!(&body[4..12], 0x0102_0304_0506_0708u64.to_be_bytes());
        assert_eq!(&body[12..16], 37u32.to_be_bytes());
        assert_eq!(&body[16..20], 11u32.to_be_bytes());
        assert_eq!(
            decode_control(&encoded).map(|(_, value)| value),
            Some(control)
        );
    }

    #[test]
    fn mettle_progress_ack_rejects_ranges_without_stall_evidence() {
        let mut encoded = encode_control(
            77,
            &LosslessSessionControl::BlockAck {
                ack: BlockAck::MettleStream {
                    stream_id: 3,
                    decoded_source_watermark: 7,
                    stalled: Some(MettleStallEvidence {
                        repair_epoch: 9,
                        missing_bin_ranges: vec![MissingMettleBinRange {
                            start_bin_id: 11,
                            end_bin_id: 12,
                        }],
                    }),
                },
            },
        );
        let body = LosslessSessionHeader::LEN;
        encoded[body + 1] = 0;
        encoded[body + 16..body + 20].copy_from_slice(&0u32.to_be_bytes());

        assert!(
            decode_control(&encoded).is_none(),
            "ranges without the stall-evidence flag are non-canonical"
        );
    }

    #[test]
    fn block_ack_truncates_to_lowest_wire_ranges() {
        let ranges: Vec<_> = (0..MAX_BLOCK_ACK_RANGES + 40)
            .map(|index| CompletedBlockRange {
                start_block_id: 1 + (index as u64 * 2),
                end_block_id: 2 + (index as u64 * 2),
            })
            .collect();
        let oversized = BlockAck::Blocks {
            completed_watermark: 0,
            extra_completed: ranges.clone(),
        };
        assert_eq!(
            oversized.validate(),
            Err(
                super::super::LosslessSessionValidationError::TooManyBlockAckRanges {
                    configured: ranges.len(),
                    max: MAX_BLOCK_ACK_RANGES,
                }
            )
        );

        let wire = oversized.for_wire(10_000).expect("truncate valid ranges");
        let BlockAck::Blocks {
            extra_completed, ..
        } = &wire
        else {
            panic!("expected block acknowledgement")
        };
        assert_eq!(extra_completed.len(), MAX_BLOCK_ACK_RANGES);
        assert_eq!(extra_completed.as_slice(), &ranges[..MAX_BLOCK_ACK_RANGES]);

        let encoded = encode_control(91, &LosslessSessionControl::BlockAck { ack: wire });
        assert!(decode_control(&encoded).is_some());
    }

    #[test]
    fn block_ack_rejects_malformed_variant_shapes() {
        assert_eq!(BLOCK_ACK_METTLE_STREAM_VARIANT, 2);
        let good = encode_control(
            92,
            &LosslessSessionControl::BlockAck {
                ack: BlockAck::Blocks {
                    completed_watermark: 1,
                    extra_completed: vec![],
                },
            },
        );

        let mut wrong_variant_shape = good.clone();
        wrong_variant_shape[LosslessSessionHeader::LEN] = BLOCK_ACK_METTLE_STREAM_VARIANT;
        assert!(decode_control(&wrong_variant_shape).is_none());

        let mut reserved_byte = good.clone();
        reserved_byte[LosslessSessionHeader::LEN + 1] = 1;
        assert!(decode_control(&reserved_byte).is_none());

        let mut bad_count = good;
        bad_count[LosslessSessionHeader::LEN + 2..LosslessSessionHeader::LEN + 4]
            .copy_from_slice(&1u16.to_be_bytes());
        assert!(decode_control(&bad_count).is_none());
    }

    #[test]
    fn mettle_manifest_roundtrips_as_known_fec_scheme() {
        let ctrl =
            LosslessSessionControl::Manifest {
                manifest: LosslessSessionManifest {
                    block_size: 1024,
                    total_bytes: 2048,
                    total_blocks: 2,
                    mode: LosslessSessionMode::Fec(
                        LosslessSessionFecMode::new_mettle_with_coded_rate(8, vec![2, 4], 21, 20),
                    ),
                },
            };

        let encoded = encode_control(88, &ctrl);
        let body_start = LosslessSessionHeader::LEN;
        assert_eq!(encoded[body_start + 1], FecScheme::Mettle as u8);
        assert_eq!(
            &encoded[body_start + 28..body_start + 32],
            21u32.to_be_bytes().as_slice()
        );
        assert_eq!(
            &encoded[body_start + 32..body_start + 36],
            20u32.to_be_bytes().as_slice()
        );
        let (_, decoded) = decode_control(&encoded).expect("decode METTLE manifest");
        assert_eq!(decoded, ctrl);
    }

    #[test]
    fn carousel_feedback_mode_roundtrips_in_manifest_feedback_byte() {
        let ctrl = LosslessSessionControl::Manifest {
            manifest: LosslessSessionManifest {
                block_size: 1024,
                total_bytes: 2048,
                total_blocks: 2,
                mode: LosslessSessionMode::Fec(
                    LosslessSessionFecMode::new_raptorq(8, vec![2, 4])
                        .with_feedback_mode(FecFeedbackMode::Carousel),
                ),
            },
        };

        let encoded = encode_control(88, &ctrl);
        assert_eq!(
            encoded[LosslessSessionHeader::LEN + 3],
            FecFeedbackMode::Carousel.to_wire()
        );
        let (_, decoded) = decode_control(&encoded).expect("decode carousel manifest");
        assert_eq!(decoded, ctrl);
    }

    #[test]
    fn mettle_object_stream_geometry_roundtrips_in_manifest() {
        let geometry = MettleObjectStreamGeometry::new(128, 8, 2, 8);
        let ctrl = LosslessSessionControl::Manifest {
            manifest: LosslessSessionManifest {
                block_size: 1024,
                total_bytes: 2048,
                total_blocks: 2,
                mode: LosslessSessionMode::Fec(
                    LosslessSessionFecMode::new_mettle(8, vec![2, 4])
                        .with_feedback_mode(FecFeedbackMode::Carousel)
                        .with_mettle_object_stream(geometry),
                ),
            },
        };

        let encoded = encode_control(88, &ctrl);
        let body_start = LosslessSessionHeader::LEN;
        assert_eq!(
            &encoded[body_start + 36..body_start + 40],
            128u32.to_be_bytes()
        );
        assert_eq!(
            &encoded[body_start + 40..body_start + 44],
            8u32.to_be_bytes()
        );
        assert_eq!(
            &encoded[body_start + 44..body_start + 52],
            2u64.to_be_bytes()
        );
        assert_eq!(
            &encoded[body_start + 52..body_start + 56],
            8u32.to_be_bytes()
        );
        assert_eq!(decode_control(&encoded).map(|(_, value)| value), Some(ctrl));
    }

    #[test]
    fn manifest_decode_rejects_unknown_or_plain_feedback_mode() {
        let mut fec = encode_control(
            88,
            &LosslessSessionControl::Manifest {
                manifest: fec_manifest(),
            },
        );
        fec[LosslessSessionHeader::LEN + 3] = 99;
        assert!(decode_control(&fec).is_none());

        let mut plain = encode_control(
            88,
            &LosslessSessionControl::Manifest {
                manifest: plain_manifest(),
            },
        );
        plain[LosslessSessionHeader::LEN + 3] = FecFeedbackMode::Rounds.to_wire();
        assert!(decode_control(&plain).is_none());
    }

    #[test]
    fn fec_manifest_roundtrips_large_symbols_per_block() {
        let symbols_per_block = 131_072u32;
        let ctrl = LosslessSessionControl::Manifest {
            manifest: LosslessSessionManifest {
                block_size: 1_073_741_824,
                total_bytes: 1_073_741_824,
                total_blocks: 1,
                mode: LosslessSessionMode::Fec(LosslessSessionFecMode::new_mettle(
                    symbols_per_block,
                    vec![2, 4],
                )),
            },
        };

        let encoded = encode_control(89, &ctrl);
        let body_start = LosslessSessionHeader::LEN;
        assert_eq!(
            &encoded[body_start + 24..body_start + 28],
            symbols_per_block.to_be_bytes().as_slice()
        );
        let (_, decoded) = decode_control(&encoded).expect("decode large-K METTLE manifest");
        assert_eq!(decoded, ctrl);
    }

    #[test]
    fn encode_control_into_matches_encode_control() {
        let ctrls = vec![
            LosslessSessionControl::Manifest {
                manifest: plain_manifest(),
            },
            LosslessSessionControl::Manifest {
                manifest: fec_manifest(),
            },
            LosslessSessionControl::Ready,
            LosslessSessionControl::SourceDone { round_id: 11 },
            LosslessSessionControl::BlockAck {
                ack: BlockAck::Blocks {
                    completed_watermark: 3,
                    extra_completed: vec![],
                },
            },
            LosslessSessionControl::AckProbe { target_peer_id: 22 },
            LosslessSessionControl::SessionComplete,
            plain_need(
                12,
                vec![MissingBlockRange {
                    start_block_id: 1,
                    end_block_id: 2,
                }],
            ),
            fec_need(
                13,
                vec![NeedBlock {
                    block_id: 1,
                    deficit_symbols: 4,
                }],
            ),
        ];

        for ctrl in ctrls {
            let heap_encoded = encode_control(42, &ctrl);
            let mut buf = [0u8; MAX_CONTROL_FRAME_SIZE];
            let stack_encoded = encode_control_into(&mut buf, 42, &ctrl);
            assert_eq!(heap_encoded.as_slice(), stack_encoded);
            let (_, decoded_heap) = decode_control(&heap_encoded).expect("decode heap");
            let (_, decoded_stack) = decode_control(stack_encoded).expect("decode stack");
            assert_eq!(decoded_heap, ctrl);
            assert_eq!(decoded_stack, ctrl);
        }
    }

    #[test]
    fn need_empty_payloads_canonicalize_to_complete() {
        let complete = encode_control(
            7,
            &LosslessSessionControl::Need {
                round_id: 16,
                report: NeedReport::Complete,
            },
        );
        let empty_plain = encode_control(7, &plain_need(16, vec![]));
        let empty_fec = encode_control(7, &fec_need(16, vec![]));

        assert_eq!(complete, empty_plain);
        assert_eq!(complete, empty_fec);

        let (_, decoded) = decode_control(&complete).expect("decode complete need");
        assert_eq!(
            decoded,
            LosslessSessionControl::Need {
                round_id: 16,
                report: NeedReport::Complete,
            }
        );
    }

    #[test]
    fn need_encoding_is_byte_stable_for_equal_semantics() {
        let ctrl = plain_need(
            19,
            vec![MissingBlockRange {
                start_block_id: 2,
                end_block_id: 4,
            }],
        );
        let first = encode_control(55, &ctrl);
        let second = encode_control(55, &ctrl);
        assert_eq!(first, second);
    }

    #[test]
    fn decode_control_rejects_invalid_manifest_and_short_bodies() {
        let good = encode_control(
            9,
            &LosslessSessionControl::Manifest {
                manifest: plain_manifest(),
            },
        );
        let mut truncated = good.clone();
        truncated.truncate(LosslessSessionHeader::LEN);
        assert!(decode_control(&truncated).is_none());

        let ready = encode_control(1, &LosslessSessionControl::Ready);
        let mut bad_ready = ready.clone();
        bad_ready[16..20].copy_from_slice(&4u32.to_be_bytes());
        assert!(decode_control(&bad_ready).is_none());

        let need = encode_control(
            1,
            &LosslessSessionControl::Need {
                round_id: 5,
                report: NeedReport::Complete,
            },
        );
        let mut bad_need = need.clone();
        bad_need.truncate(LosslessSessionHeader::LEN + 1);
        assert!(decode_control(&bad_need).is_none());

        let mut bad_mode = good.clone();
        bad_mode[LosslessSessionHeader::LEN] = 9;
        assert!(decode_control(&bad_mode).is_none());

        let mut bad_tree_count = encode_control(
            10,
            &LosslessSessionControl::Manifest {
                manifest: fec_manifest(),
            },
        );
        bad_tree_count[LosslessSessionHeader::LEN + 2] = 7;
        assert!(decode_control(&bad_tree_count).is_none());
    }

    #[test]
    fn carousel_controls_validate_against_carousel_manifest() {
        let manifest = carousel_manifest();
        for control in [
            LosslessSessionControl::BlockAck {
                ack: BlockAck::Blocks {
                    completed_watermark: manifest.total_blocks,
                    extra_completed: vec![],
                },
            },
            LosslessSessionControl::AckProbe { target_peer_id: 22 },
            LosslessSessionControl::SessionComplete,
        ] {
            manifest
                .validate_control(&control)
                .expect("carousel control should match manifest");
        }

        assert!(
            fec_manifest()
                .validate_control(&LosslessSessionControl::AckProbe { target_peer_id: 22 })
                .is_err()
        );
        let block_ack = LosslessSessionControl::BlockAck {
            ack: BlockAck::Blocks {
                completed_watermark: 0,
                extra_completed: Vec::new(),
            },
        };
        for non_carousel in [plain_manifest(), fec_manifest()] {
            assert_eq!(
                non_carousel.validate_control(&block_ack),
                Err(super::super::LosslessSessionValidationError::CarouselControlRequiresCarouselMode)
            );
        }
        assert!(
            manifest
                .validate_control(&LosslessSessionControl::SourceDone { round_id: 0 })
                .is_err()
        );
    }

    #[test]
    fn decode_control_rejects_removed_legacy_control_ids() {
        let mut legacy_source_done =
            encode_control(16, &LosslessSessionControl::SourceDone { round_id: 16 });
        legacy_source_done[6] = 3;
        assert!(
            decode_control(&legacy_source_done).is_none(),
            "removed ctrl_kind=3 must not be reinterpreted as a live control"
        );

        let mut legacy_need = encode_control(
            17,
            &LosslessSessionControl::Need {
                round_id: 17,
                report: NeedReport::Complete,
            },
        );
        legacy_need[6] = 4;
        assert!(
            decode_control(&legacy_need).is_none(),
            "removed ctrl_kind=4 must not be reinterpreted as a live control"
        );
    }

    #[test]
    fn decode_control_rejects_invalid_need_payload_shapes() {
        let mut bad_kind = encode_control(
            12,
            &LosslessSessionControl::Need {
                round_id: 12,
                report: NeedReport::Complete,
            },
        );
        bad_kind[LosslessSessionHeader::LEN + 4] = 9;
        assert!(
            decode_control(&bad_kind).is_none(),
            "need decode must reject unsupported report kinds"
        );

        let mut malformed_ranges = encode_control(
            13,
            &plain_need(
                13,
                vec![MissingBlockRange {
                    start_block_id: 1,
                    end_block_id: 2,
                }],
            ),
        );
        let body_start = LosslessSessionHeader::LEN;
        malformed_ranges
            [body_start + NEED_FIXED_BODY_LEN + 8..body_start + NEED_FIXED_BODY_LEN + 16]
            .copy_from_slice(&1u64.to_be_bytes());
        assert!(
            decode_control(&malformed_ranges).is_none(),
            "need decode must reject malformed missing ranges"
        );

        let mut malformed_complete = encode_control(
            14,
            &LosslessSessionControl::Need {
                round_id: 14,
                report: NeedReport::Complete,
            },
        );
        malformed_complete[LosslessSessionHeader::LEN + 5] = 1;
        assert!(
            decode_control(&malformed_complete).is_none(),
            "complete need reports must stay canonical"
        );

        let mut malformed_fec = encode_control(
            15,
            &fec_need(
                15,
                vec![NeedBlock {
                    block_id: 1,
                    deficit_symbols: 2,
                }],
            ),
        );
        malformed_fec[LosslessSessionHeader::LEN + 5] = 0;
        malformed_fec[LosslessSessionHeader::LEN + 6] = 0;
        assert!(
            decode_control(&malformed_fec).is_none(),
            "need decode must reject zero-length fec payloads"
        );
    }

    #[test]
    fn need_roundtrips_at_max_block_limit() {
        let control = fec_need(
            16,
            (0..MAX_NEED_BLOCKS as u64)
                .map(|block_id| NeedBlock {
                    block_id,
                    deficit_symbols: 4,
                })
                .collect(),
        );

        let encoded = encode_control(16, &control);
        let (_, decoded) = decode_control(&encoded).expect("decode max-size need report");
        assert_eq!(decoded, control);
    }

    #[test]
    fn decode_control_rejects_plain_manifest_with_fec_fields() {
        let mut encoded = encode_control(
            11,
            &LosslessSessionControl::Manifest {
                manifest: plain_manifest(),
            },
        );
        let body_start = LosslessSessionHeader::LEN;

        encoded[body_start + 1] = FecScheme::RaptorQ as u8;
        encoded[body_start + 2] = 1;
        encoded[body_start + 24..body_start + 28].copy_from_slice(&4u32.to_be_bytes());
        encoded.extend_from_slice(&7u16.to_be_bytes());

        let body_len = MANIFEST_FIXED_BODY_LEN + 2;
        encoded[16..20].copy_from_slice(&(body_len as u32).to_be_bytes());

        assert!(
            decode_control(&encoded).is_none(),
            "plain manifests must not carry FEC scheme, symbol, or tree-id fields"
        );
    }

    #[test]
    fn decode_control_rejects_noncanonical_need_shapes() {
        let mut bad_plain = encode_control(
            21,
            &plain_need(
                21,
                vec![MissingBlockRange {
                    start_block_id: 0,
                    end_block_id: 1,
                }],
            ),
        );
        bad_plain[LosslessSessionHeader::LEN + 4] = 0;
        assert!(decode_control(&bad_plain).is_none());

        let mut bad_fec = encode_control(
            22,
            &fec_need(
                22,
                vec![NeedBlock {
                    block_id: 0,
                    deficit_symbols: 1,
                }],
            ),
        );
        bad_fec[LosslessSessionHeader::LEN + 4] = 0;
        assert!(decode_control(&bad_fec).is_none());
    }

    #[test]
    fn control_decoding_stays_separate_from_block_frames() {
        let payload = encode_block_data(1, 1, b"x");
        assert!(decode_control(&payload).is_none());
    }

    #[test]
    fn control_decoder_is_total_over_peer_body_lengths_and_range_counts() {
        let range_frame = encode_control(
            23,
            &plain_need(
                23,
                vec![MissingBlockRange {
                    start_block_id: 1,
                    end_block_id: 2,
                }],
            ),
        );

        for body_len in (0..=96).chain([u32::MAX]) {
            let mut candidate = range_frame.clone();
            candidate[16..20].copy_from_slice(&body_len.to_be_bytes());
            let _ = decode_control(&candidate);
            for truncated_len in 0..candidate.len() {
                let _ = decode_control(&candidate[..truncated_len]);
            }
        }

        for report_count in 0..=u16::MAX {
            let mut candidate = range_frame.clone();
            candidate[LosslessSessionHeader::LEN + 5..LosslessSessionHeader::LEN + 7]
                .copy_from_slice(&report_count.to_be_bytes());
            assert_eq!(
                decode_control(&candidate).is_some(),
                report_count == 1,
                "one encoded range is canonical only when the peer count is one"
            );
        }

        let ack_frame = encode_control(
            24,
            &LosslessSessionControl::BlockAck {
                ack: BlockAck::Blocks {
                    completed_watermark: 0,
                    extra_completed: vec![CompletedBlockRange {
                        start_block_id: 1,
                        end_block_id: 2,
                    }],
                },
            },
        );
        for body_len in (0..=96).chain([u32::MAX]) {
            let mut candidate = ack_frame.clone();
            candidate[16..20].copy_from_slice(&body_len.to_be_bytes());
            let _ = decode_control(&candidate);
            for truncated_len in 0..candidate.len() {
                let _ = decode_control(&candidate[..truncated_len]);
            }
        }
        for range_count in 0..=u16::MAX {
            let mut candidate = ack_frame.clone();
            candidate[LosslessSessionHeader::LEN + 2..LosslessSessionHeader::LEN + 4]
                .copy_from_slice(&range_count.to_be_bytes());
            assert_eq!(
                decode_control(&candidate).is_some(),
                range_count == 1,
                "one encoded completion range is valid only when the peer count is one"
            );
        }
    }
}
