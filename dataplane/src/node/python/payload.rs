//! Shared helpers for py-payload segmentation so Python bindings and in-process
//! components can agree on header formats and MTU budgeting.

use std::fmt;

use crate::node::packet::{PY_PAYLOAD_SEGMENT_HEADER_LEN, PyPayloadSegHeader};

const IPV4_HEADER_LEN: usize = 20;
const TCP_HEADER_LEN: usize = 20;

/// Minimum MTU that leaves at least one byte of payload after IPv4/TCP + header.
pub const PY_MIN_FRAGMENTATION_MTU: usize =
    IPV4_HEADER_LEN + TCP_HEADER_LEN + PY_PAYLOAD_SEGMENT_HEADER_LEN + 1;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PayloadFragmentError {
    InvalidMtu(i32),
    BelowMinimum { mtu: i32, required: usize },
    PayloadTooLarge { len: usize, max: usize },
    PayloadExceedsLimit(usize),
    TooManyFragments { count: usize, max: usize },
    Encoding(String),
}

impl fmt::Display for PayloadFragmentError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PayloadFragmentError::InvalidMtu(mtu) => {
                write!(
                    f,
                    "configured MTU {mtu} is invalid; expected positive value."
                )
            }
            PayloadFragmentError::BelowMinimum { mtu, required } => write!(
                f,
                "configured MTU {mtu} is below the minimum {required} required for Python fragmentation \
                 (must exceed {} bytes of IPv4/TCP/PyPayloadSeg headers).",
                PY_MIN_FRAGMENTATION_MTU - 1
            ),
            PayloadFragmentError::PayloadTooLarge { len, max } => write!(
                f,
                "python buffer length {len} exceeds configured limit of {max} bytes."
            ),
            PayloadFragmentError::PayloadExceedsLimit(len) => write!(
                f,
                "python buffer length {len} exceeds 4 GiB limit for a single message."
            ),
            PayloadFragmentError::TooManyFragments { count, max } => write!(
                f,
                "python buffer requires {count} fragments which exceeds the limit of {max}; \
                 increase MTU or reduce payload size."
            ),
            PayloadFragmentError::Encoding(msg) => {
                write!(f, "failed to encode fragment header: {msg}")
            }
        }
    }
}

pub fn python_payload_budget(mtu: i32) -> Result<usize, PayloadFragmentError> {
    let mtu_value = usize::try_from(mtu).map_err(|_| PayloadFragmentError::InvalidMtu(mtu))?;
    let header_overhead = IPV4_HEADER_LEN + TCP_HEADER_LEN + PY_PAYLOAD_SEGMENT_HEADER_LEN;
    match mtu_value.checked_sub(header_overhead) {
        Some(0) | None => Err(PayloadFragmentError::BelowMinimum {
            mtu,
            required: PY_MIN_FRAGMENTATION_MTU,
        }),
        Some(budget) => Ok(budget),
    }
}

pub fn build_py_payload_segments(
    body: &[u8],
    chunk_budget: usize,
    max_message_bytes: usize,
    message_id: u64,
) -> Result<Vec<Vec<u8>>, PayloadFragmentError> {
    if chunk_budget == 0 {
        return Err(PayloadFragmentError::BelowMinimum {
            mtu: chunk_budget as i32,
            required: PY_MIN_FRAGMENTATION_MTU,
        });
    }

    if body.len() > max_message_bytes {
        return Err(PayloadFragmentError::PayloadTooLarge {
            len: body.len(),
            max: max_message_bytes,
        });
    }

    if body.len() > u32::MAX as usize {
        return Err(PayloadFragmentError::PayloadExceedsLimit(body.len()));
    }

    let fragment_count = if body.is_empty() {
        1
    } else {
        body.len().div_ceil(chunk_budget)
    };

    if fragment_count > u16::MAX as usize {
        return Err(PayloadFragmentError::TooManyFragments {
            count: fragment_count,
            max: u16::MAX as usize,
        });
    }

    let total_len = body.len() as u32;
    let fragment_count_u16 = fragment_count as u16;
    let mut fragments = Vec::with_capacity(fragment_count);
    let is_fragmented = fragment_count > 1;

    for idx in 0..fragment_count {
        let (chunk_start, chunk_end) = if body.is_empty() {
            (0, 0)
        } else {
            let start = idx * chunk_budget;
            let end = std::cmp::min(start + chunk_budget, body.len());
            (start, end)
        };
        let chunk = &body[chunk_start..chunk_end];
        let header = PyPayloadSegHeader {
            fragmented: is_fragmented,
            last_fragment: is_fragmented && idx + 1 == fragment_count,
            message_id,
            total_len,
            fragment_index: idx as u16,
            fragment_count: fragment_count_u16,
            fragment_payload_len: chunk.len() as u32,
        };

        let mut payload = vec![0u8; PY_PAYLOAD_SEGMENT_HEADER_LEN + chunk.len()];
        header
            .encode_into(&mut payload[..PY_PAYLOAD_SEGMENT_HEADER_LEN])
            .map_err(|err| PayloadFragmentError::Encoding(err.to_string()))?;
        payload[PY_PAYLOAD_SEGMENT_HEADER_LEN..].copy_from_slice(chunk);

        fragments.push(payload);
    }

    Ok(fragments)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node::packet::PyPayloadSegHeader;

    #[test]
    fn python_payload_budget_accounts_for_headers() {
        assert_eq!(python_payload_budget(1400).unwrap(), 1336);
    }

    #[test]
    fn python_payload_budget_minimum_valid_mtu() {
        assert_eq!(python_payload_budget(65).unwrap(), 1);
    }

    #[test]
    fn python_payload_budget_exactly_at_header_boundary_errors() {
        let err = python_payload_budget(64).unwrap_err();
        assert!(matches!(err, PayloadFragmentError::BelowMinimum { .. }));
    }

    #[test]
    fn python_payload_budget_negative_mtu_errors() {
        let err = python_payload_budget(-1).unwrap_err();
        assert!(matches!(err, PayloadFragmentError::InvalidMtu(_)));
    }

    #[test]
    fn python_payload_budget_standard_ethernet_mtu() {
        assert_eq!(python_payload_budget(1500).unwrap(), 1436);
    }

    #[test]
    fn python_payload_budget_jumbo_frame_mtu() {
        assert_eq!(python_payload_budget(9000).unwrap(), 8936);
    }

    #[test]
    fn build_segments_single_fragment_sets_header() {
        let payload = vec![0xAA; 512];
        let segments = build_py_payload_segments(&payload, 1024, 4096, 42).expect("segments");
        assert_eq!(segments.len(), 1);
        let fragment = &segments[0];
        let (header, body) = PyPayloadSegHeader::decode_from(fragment).expect("header");
        assert!(!header.is_fragmented());
        assert!(!header.is_last_fragment());
        assert_eq!(header.message_id, 42);
        assert_eq!(header.total_len as usize, payload.len());
        assert_eq!(header.fragment_index, 0);
        assert_eq!(header.fragment_count, 1);
        assert_eq!(header.fragment_payload_len as usize, payload.len());
        assert_eq!(body, payload.as_slice());
    }

    #[test]
    fn build_segments_multi_fragment_sets_flags_and_counts() {
        let payload: Vec<u8> = (0..3500).map(|i| (i % 256) as u8).collect();
        let message_id = 7;
        let segments =
            build_py_payload_segments(&payload, 1000, 4096, message_id).expect("segments");
        assert_eq!(segments.len(), 4);

        for (idx, fragment) in segments.iter().enumerate() {
            let (header, body) =
                PyPayloadSegHeader::decode_from(fragment).expect("fragment header");
            assert!(header.is_fragmented());
            if idx == segments.len() - 1 {
                assert!(header.is_last_fragment());
            } else {
                assert!(!header.is_last_fragment());
            }
            assert_eq!(header.fragment_index, idx as u16);
            assert_eq!(header.fragment_count, segments.len() as u16);
            let start = idx * 1000;
            let end = std::cmp::min(start + 1000, payload.len());
            assert_eq!(body, &payload[start..end]);
        }
    }

    #[test]
    fn build_segments_errors_when_payload_exceeds_limit() {
        let payload = vec![0xCC; 10];
        let err = build_py_payload_segments(&payload, 8, 5, 1).unwrap_err();
        assert!(matches!(err, PayloadFragmentError::PayloadTooLarge { .. }));
    }

    #[test]
    fn build_segments_empty_payload_creates_single_fragment() {
        let payload = vec![];
        let segments = build_py_payload_segments(&payload, 1024, 4096, 99).expect("segments");
        assert_eq!(segments.len(), 1);
        let (header, body) = PyPayloadSegHeader::decode_from(&segments[0]).expect("header");
        assert!(!header.is_fragmented());
        assert!(!header.is_last_fragment());
        assert_eq!(header.message_id, 99);
        assert_eq!(header.total_len, 0);
        assert_eq!(header.fragment_count, 1);
        assert_eq!(header.fragment_payload_len, 0);
        assert_eq!(body.len(), 0);
    }

    #[test]
    fn build_segments_exactly_at_chunk_boundary() {
        let payload = vec![0xDD; 2000];
        let segments = build_py_payload_segments(&payload, 1000, 4096, 15).expect("segments");
        assert_eq!(segments.len(), 2);
        let (first_header, first_body) =
            PyPayloadSegHeader::decode_from(&segments[0]).expect("first");
        assert!(first_header.is_fragmented());
        assert!(!first_header.is_last_fragment());
        assert_eq!(first_header.fragment_index, 0);
        assert_eq!(first_body.len(), 1000);

        let (second_header, second_body) =
            PyPayloadSegHeader::decode_from(&segments[1]).expect("second");
        assert!(second_header.is_fragmented());
        assert!(second_header.is_last_fragment());
        assert_eq!(second_header.fragment_index, 1);
        assert_eq!(second_body.len(), 1000);
    }

    #[test]
    fn build_segments_errors_when_chunk_budget_is_zero() {
        let payload = vec![0xEE; 100];
        let err = build_py_payload_segments(&payload, 0, 4096, 1).unwrap_err();
        assert!(matches!(err, PayloadFragmentError::BelowMinimum { .. }));
    }

    #[test]
    fn build_segments_errors_when_payload_exceeds_u32_max() {
        let payload_size = (u32::MAX as usize) + 1;
        let payload = vec![0xFF; payload_size];
        let err = build_py_payload_segments(&payload, 1024, usize::MAX, 1).unwrap_err();
        assert!(matches!(err, PayloadFragmentError::PayloadExceedsLimit(_)));
    }

    #[test]
    fn build_segments_errors_when_fragment_count_exceeds_u16_max() {
        let chunk_budget = 1;
        let payload_size = (u16::MAX as usize) + 1;
        let payload = vec![0xAB; payload_size];
        let err = build_py_payload_segments(&payload, chunk_budget, usize::MAX, 1).unwrap_err();
        assert!(matches!(err, PayloadFragmentError::TooManyFragments { .. }));
    }

    #[test]
    fn build_segments_last_fragment_can_be_partial() {
        let payload = vec![0x11; 2100];
        let segments = build_py_payload_segments(&payload, 1000, 4096, 8).expect("segments");
        assert_eq!(segments.len(), 3);

        let (last_header, last_body) = PyPayloadSegHeader::decode_from(&segments[2]).expect("last");
        assert!(last_header.is_last_fragment());
        assert_eq!(last_body.len(), 100);
        assert_eq!(last_header.fragment_payload_len, 100);
    }

    #[test]
    fn build_segments_reconstructed_payload_matches_original() {
        let payload = (0..3500).map(|i| (i % 256) as u8).collect::<Vec<u8>>();
        let segments = build_py_payload_segments(&payload, 1000, 5000, 50).expect("segments");

        let mut reconstructed = Vec::new();
        for fragment in segments {
            let (_, body) = PyPayloadSegHeader::decode_from(&fragment).expect("fragment");
            reconstructed.extend_from_slice(body);
        }

        assert_eq!(reconstructed, payload);
    }
}
