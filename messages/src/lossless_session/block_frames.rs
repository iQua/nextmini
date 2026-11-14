use super::{
    LOSSLESS_SESSION_MAGIC, LOSSLESS_SESSION_VERSION, LosslessSessionBlockData,
    LosslessSessionBlockSymbol, LosslessSessionHeader, LosslessSessionKind,
};

/// Encode a `BlockData` frame into a fresh `Vec<u8>`.
pub fn encode_block_data(session_id: u64, block_id: u64, payload: &[u8]) -> Vec<u8> {
    let body_len = 8 + payload.len() as u32;
    let mut out = vec![0u8; LosslessSessionHeader::LEN + body_len as usize];
    LosslessSessionHeader {
        magic: LOSSLESS_SESSION_MAGIC,
        version: LOSSLESS_SESSION_VERSION,
        kind: LosslessSessionKind::BlockData,
        ctrl_kind: 0,
        session_id,
        body_len,
    }
    .encode_into(&mut out[..LosslessSessionHeader::LEN]);
    let mut pos = LosslessSessionHeader::LEN;
    out[pos..pos + 8].copy_from_slice(&block_id.to_be_bytes());
    pos += 8;
    out[pos..pos + payload.len()].copy_from_slice(payload);
    out
}

const BLOCK_SYMBOL_FIXED_BODY_LEN: usize = 8 + 4 + 2 + 2;
#[cfg(test)]
const BLOCK_SYMBOL_TREE_ID_OFFSET: usize = LosslessSessionHeader::LEN + 8 + 4;

/// Return the framed packet size for one BlockSymbol payload.
pub fn block_symbol_packet_len(payload_len: usize, transport_overhead: usize) -> Option<usize> {
    transport_overhead
        .checked_add(LosslessSessionHeader::LEN)?
        .checked_add(BLOCK_SYMBOL_FIXED_BODY_LEN)?
        .checked_add(payload_len)
}

/// Encode a `BlockSymbol` frame into a fresh `Vec<u8>`.
pub fn encode_block_symbol(
    session_id: u64,
    block_id: u64,
    symbol_id: u32,
    tree_id: u16,
    payload: &[u8],
) -> Vec<u8> {
    let mut out = Vec::new();
    encode_block_symbol_into(&mut out, session_id, block_id, symbol_id, tree_id, payload);
    out
}

/// Encode a `BlockSymbol` frame into the provided reusable buffer.
fn encode_block_symbol_into<'a>(
    buf: &'a mut Vec<u8>,
    session_id: u64,
    block_id: u64,
    symbol_id: u32,
    tree_id: u16,
    payload: &[u8],
) -> &'a [u8] {
    let body_len = BLOCK_SYMBOL_FIXED_BODY_LEN + payload.len();
    let frame_len = LosslessSessionHeader::LEN + body_len;
    buf.resize(frame_len, 0);
    LosslessSessionHeader {
        magic: LOSSLESS_SESSION_MAGIC,
        version: LOSSLESS_SESSION_VERSION,
        kind: LosslessSessionKind::BlockSymbol,
        ctrl_kind: 0,
        session_id,
        body_len: body_len as u32,
    }
    .encode_into(&mut buf[..LosslessSessionHeader::LEN]);

    let mut pos = LosslessSessionHeader::LEN;
    buf[pos..pos + 8].copy_from_slice(&block_id.to_be_bytes());
    pos += 8;
    buf[pos..pos + 4].copy_from_slice(&symbol_id.to_be_bytes());
    pos += 4;
    buf[pos..pos + 2].copy_from_slice(&tree_id.to_be_bytes());
    pos += 2;
    buf[pos..pos + 2].copy_from_slice(&0u16.to_be_bytes());
    pos += 2;
    buf[pos..pos + payload.len()].copy_from_slice(payload);
    &buf[..frame_len]
}

/// Update the tree id for an already-encoded `BlockSymbol` frame.
#[cfg(test)]
fn set_block_symbol_tree_id(buf: &mut [u8], tree_id: u16) -> Option<()> {
    let (hdr, off) = LosslessSessionHeader::decode_from(buf)?;
    if hdr.kind != LosslessSessionKind::BlockSymbol || hdr.ctrl_kind != 0 {
        return None;
    }
    if hdr.body_len < BLOCK_SYMBOL_FIXED_BODY_LEN as u32 || buf.len() < off + hdr.body_len as usize
    {
        return None;
    }

    let pos = off + (BLOCK_SYMBOL_TREE_ID_OFFSET - LosslessSessionHeader::LEN);
    buf[pos..pos + 2].copy_from_slice(&tree_id.to_be_bytes());
    Some(())
}

/// Try to decode a `BlockData` frame; returns (header, block metadata, payload slice).
pub fn decode_block_data(
    buf: &[u8],
) -> Option<(LosslessSessionHeader, LosslessSessionBlockData, &[u8])> {
    let (hdr, off) = LosslessSessionHeader::decode_from(buf)?;
    if hdr.kind != LosslessSessionKind::BlockData || hdr.ctrl_kind != 0 {
        return None;
    }
    if hdr.body_len < 8 {
        return None;
    }
    let payload_end = off + hdr.body_len as usize;
    if buf.len() != payload_end {
        return None;
    }
    let mut pos = off;
    let block_id = u64::from_be_bytes(buf[pos..pos + 8].try_into().ok()?);
    pos += 8;
    Some((
        hdr,
        LosslessSessionBlockData { block_id },
        &buf[pos..payload_end],
    ))
}

/// Try to decode a `BlockSymbol` frame; returns (header, block metadata, payload slice).
pub fn decode_block_symbol(
    buf: &[u8],
) -> Option<(LosslessSessionHeader, LosslessSessionBlockSymbol, &[u8])> {
    let (hdr, off) = LosslessSessionHeader::decode_from(buf)?;
    if hdr.kind != LosslessSessionKind::BlockSymbol || hdr.ctrl_kind != 0 {
        return None;
    }
    if hdr.body_len < BLOCK_SYMBOL_FIXED_BODY_LEN as u32 {
        return None;
    }
    let payload_end = off + hdr.body_len as usize;
    if buf.len() != payload_end {
        return None;
    }

    let mut pos = off;
    let block_id = u64::from_be_bytes(buf[pos..pos + 8].try_into().ok()?);
    pos += 8;
    let symbol_id = u32::from_be_bytes(buf[pos..pos + 4].try_into().ok()?);
    pos += 4;
    let tree_id = u16::from_be_bytes(buf[pos..pos + 2].try_into().ok()?);
    pos += 2;
    pos += 2;

    Some((
        hdr,
        LosslessSessionBlockSymbol {
            block_id,
            symbol_id,
            tree_id,
        },
        &buf[pos..payload_end],
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_block_data() {
        let payload = b"plain block";
        let buf = encode_block_data(42, 7, payload);
        let (hdr, data, body) = decode_block_data(&buf).expect("decode block data");
        assert_eq!(hdr.magic, LOSSLESS_SESSION_MAGIC);
        assert_eq!(hdr.version, LOSSLESS_SESSION_VERSION);
        assert_eq!(hdr.kind, LosslessSessionKind::BlockData);
        assert_eq!(hdr.session_id, 42);
        assert_eq!(data.block_id, 7);
        assert_eq!(body, payload);
    }

    #[test]
    fn block_data_body_len_tracks_payload_without_inner_length_field() {
        let payload = b"plain block";
        let buf = encode_block_data(42, 7, payload);
        let (hdr, data, body) = decode_block_data(&buf).expect("decode block data");
        assert_eq!(hdr.body_len as usize, 8 + payload.len());
        assert_eq!(buf.len(), LosslessSessionHeader::LEN + 8 + payload.len());
        assert_eq!(data.block_id, 7);
        assert_eq!(body, payload);
    }

    #[test]
    fn roundtrip_block_symbol() {
        let payload = b"fec symbol";
        let mut buf = Vec::new();
        encode_block_symbol_into(&mut buf, 42, 9, 3, 5, payload);
        let (hdr, data, body) = decode_block_symbol(&buf).expect("decode block symbol");
        assert_eq!(hdr.session_id, 42);
        assert_eq!(hdr.kind, LosslessSessionKind::BlockSymbol);
        assert_eq!(data.block_id, 9);
        assert_eq!(data.symbol_id, 3);
        assert_eq!(data.tree_id, 5);
        assert_eq!(body, payload);
        assert!(
            decode_block_data(&buf).is_none(),
            "wrong decoder must reject block symbol"
        );
    }

    #[test]
    fn block_symbol_body_len_tracks_payload_without_inner_length_field() {
        let payload = b"fec symbol";
        let mut buf = Vec::new();
        encode_block_symbol_into(&mut buf, 42, 9, 3, 5, payload);
        let (hdr, data, body) = decode_block_symbol(&buf).expect("decode block symbol");
        assert_eq!(hdr.body_len as usize, 8 + 4 + 2 + 2 + payload.len());
        assert_eq!(
            buf.len(),
            LosslessSessionHeader::LEN + 8 + 4 + 2 + 2 + payload.len()
        );
        assert_eq!(data.block_id, 9);
        assert_eq!(data.symbol_id, 3);
        assert_eq!(data.tree_id, 5);
        assert_eq!(body, payload);
    }

    #[test]
    fn encode_block_symbol_into_supports_tree_id_patch() {
        let mut frame = Vec::new();
        let encoded = encode_block_symbol_into(&mut frame, 42, 7, 3, 5, b"payload");
        let (_, symbol, body) = decode_block_symbol(encoded).expect("decode symbol");
        assert_eq!(symbol.block_id, 7);
        assert_eq!(symbol.symbol_id, 3);
        assert_eq!(symbol.tree_id, 5);
        assert_eq!(body, b"payload");

        set_block_symbol_tree_id(&mut frame, 9).expect("patch tree id");
        let (_, patched, patched_body) = decode_block_symbol(&frame).expect("decode patched");
        assert_eq!(patched.tree_id, 9);
        assert_eq!(patched_body, b"payload");
    }

    #[test]
    fn decode_block_frames_reject_malformed_body_len_without_inner_length_field() {
        let mut block_data = encode_block_data(21, 4, b"payload");
        block_data[16..20].copy_from_slice(&7u32.to_be_bytes());
        assert!(
            decode_block_data(&block_data).is_none(),
            "shorter block-data body_len must be rejected"
        );

        let mut block_data = encode_block_data(21, 4, b"payload");
        block_data[16..20].copy_from_slice(&32u32.to_be_bytes());
        assert!(
            decode_block_data(&block_data).is_none(),
            "larger block-data body_len must be rejected"
        );

        let mut block_symbol = encode_block_symbol(21, 4, 2, 7, b"symbol");
        block_symbol[16..20].copy_from_slice(&15u32.to_be_bytes());
        assert!(
            decode_block_symbol(&block_symbol).is_none(),
            "shorter block-symbol body_len must be rejected"
        );

        let mut block_symbol = encode_block_symbol(21, 4, 2, 7, b"symbol");
        block_symbol[16..20].copy_from_slice(&64u32.to_be_bytes());
        assert!(
            decode_block_symbol(&block_symbol).is_none(),
            "larger block-symbol body_len must be rejected"
        );
    }

    #[test]
    fn decode_rejects_bad_magic_and_wrong_kinds() {
        let mut buf = encode_block_data(1, 1, b"x");
        buf[0] = 0;
        assert!(decode_block_data(&buf).is_none());

        let mut symbol = Vec::new();
        encode_block_symbol_into(&mut symbol, 1, 0, 0, 1, b"y");
        assert!(decode_block_data(&symbol).is_none());
    }
}
