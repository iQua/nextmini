use nextmini_messages::lossless_session::{
    LOSSLESS_SESSION_MAGIC, LOSSLESS_SESSION_VERSION, LosslessSessionHeader, LosslessSessionKind,
};

const BLOCK_SYMBOL_FIXED_BODY_LEN: usize = 8 + 4 + 2 + 2;
const BLOCK_SYMBOL_TREE_ID_OFFSET: usize = LosslessSessionHeader::LEN + 8 + 4;

pub(super) const fn framing_len() -> usize {
    LosslessSessionHeader::LEN + BLOCK_SYMBOL_FIXED_BODY_LEN
}

pub(super) fn encode_into(
    buf: &mut Vec<u8>,
    session_id: u64,
    block_id: u64,
    symbol_id: u32,
    tree_id: u16,
    payload: &[u8],
) {
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
}

pub(super) fn patch_tree_id(buf: &mut [u8], tree_id: u16) -> Option<()> {
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

#[cfg(test)]
mod tests {
    use super::*;
    use nextmini_messages::lossless_session;

    #[test]
    fn scratch_encoder_matches_public_block_symbol_encoder() {
        let mut scratch = Vec::new();
        encode_into(&mut scratch, 42, 7, 3, 5, b"payload");
        assert_eq!(
            scratch,
            lossless_session::encode_block_symbol(42, 7, 3, 5, b"payload")
        );

        patch_tree_id(&mut scratch, 9).expect("tree-id patch should succeed");
        assert_eq!(
            scratch,
            lossless_session::encode_block_symbol(42, 7, 3, 9, b"payload")
        );
    }
}
