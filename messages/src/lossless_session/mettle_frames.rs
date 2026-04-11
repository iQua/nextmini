use super::{
    LOSSLESS_SESSION_MAGIC, LOSSLESS_SESSION_VERSION, LosslessSessionHeader, LosslessSessionKind,
    LosslessSessionMettleSymbol,
};

const METTLE_SYMBOL_FIXED_BODY_LEN: usize = 8 + 4 + 8 + 8 + 2 + 2;

/// Encode a `MettleSymbol` frame into a fresh `Vec<u8>`.
pub fn encode_mettle_symbol(
    session_id: u64,
    symbol: LosslessSessionMettleSymbol,
    payload: &[u8],
) -> Vec<u8> {
    let body_len = METTLE_SYMBOL_FIXED_BODY_LEN + payload.len();
    let mut out = vec![0u8; LosslessSessionHeader::LEN + body_len];
    LosslessSessionHeader {
        magic: LOSSLESS_SESSION_MAGIC,
        version: LOSSLESS_SESSION_VERSION,
        kind: LosslessSessionKind::MettleSymbol,
        ctrl_kind: 0,
        session_id,
        body_len: body_len as u32,
    }
    .encode_into(&mut out[..LosslessSessionHeader::LEN]);

    let mut pos = LosslessSessionHeader::LEN;
    out[pos..pos + 8].copy_from_slice(&symbol.bin_id.to_be_bytes());
    pos += 8;
    out[pos..pos + 4].copy_from_slice(&symbol.degree.to_be_bytes());
    pos += 4;
    out[pos..pos + 8].copy_from_slice(&symbol.xor_source_id.to_be_bytes());
    pos += 8;
    out[pos..pos + 8].copy_from_slice(&symbol.xor_source_sig.to_be_bytes());
    pos += 8;
    out[pos..pos + 2].copy_from_slice(&symbol.tree_id.to_be_bytes());
    pos += 2;
    out[pos..pos + 2].copy_from_slice(&0u16.to_be_bytes());
    pos += 2;
    out[pos..pos + payload.len()].copy_from_slice(payload);
    out
}

/// Try to decode a `MettleSymbol` frame; returns (header, symbol metadata, payload slice).
pub fn decode_mettle_symbol(
    buf: &[u8],
) -> Option<(LosslessSessionHeader, LosslessSessionMettleSymbol, &[u8])> {
    let (hdr, off) = LosslessSessionHeader::decode_from(buf)?;
    if hdr.kind != LosslessSessionKind::MettleSymbol || hdr.ctrl_kind != 0 {
        return None;
    }
    if hdr.body_len < METTLE_SYMBOL_FIXED_BODY_LEN as u32 {
        return None;
    }
    let payload_end = off + hdr.body_len as usize;
    if buf.len() != payload_end {
        return None;
    }

    let mut pos = off;
    let bin_id = u64::from_be_bytes(buf[pos..pos + 8].try_into().ok()?);
    pos += 8;
    let degree = u32::from_be_bytes(buf[pos..pos + 4].try_into().ok()?);
    pos += 4;
    let xor_source_id = u64::from_be_bytes(buf[pos..pos + 8].try_into().ok()?);
    pos += 8;
    let xor_source_sig = u64::from_be_bytes(buf[pos..pos + 8].try_into().ok()?);
    pos += 8;
    let tree_id = u16::from_be_bytes(buf[pos..pos + 2].try_into().ok()?);
    pos += 2;
    pos += 2;

    Some((
        hdr,
        LosslessSessionMettleSymbol {
            bin_id,
            degree,
            xor_source_id,
            xor_source_sig,
            tree_id,
        },
        &buf[pos..payload_end],
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_mettle_symbol() {
        let payload = b"mettle symbol";
        let symbol = LosslessSessionMettleSymbol {
            bin_id: 17,
            degree: 3,
            xor_source_id: 9,
            xor_source_sig: 99,
            tree_id: 5,
        };
        let buf = encode_mettle_symbol(42, symbol, payload);
        let (hdr, decoded, body) = decode_mettle_symbol(&buf).expect("decode mettle symbol");
        assert_eq!(hdr.session_id, 42);
        assert_eq!(hdr.kind, LosslessSessionKind::MettleSymbol);
        assert_eq!(decoded, symbol);
        assert_eq!(body, payload);
    }
}
