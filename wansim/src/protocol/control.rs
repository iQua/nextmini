use super::BlockAck;

const LOSSLESS_HEADER_BYTES: usize = 20;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ControlFrame {
    BlockAck(BlockAck),
    AckProbe { target_peer_id: u64 },
    SessionComplete,
    SourceDone { round_id: u32 },
    Need { round_id: u32, deficit: usize },
    StripeAck { stripe_id: usize },
}

impl ControlFrame {
    pub fn payload_bytes(&self) -> usize {
        let body = match self {
            Self::BlockAck(ack) => 12 + (16 * ack.extra_completed.len()),
            Self::AckProbe { .. } => 8,
            Self::SessionComplete => 0,
            Self::SourceDone { .. } => 4,
            Self::Need { deficit, .. } => {
                if *deficit == 0 {
                    8
                } else {
                    18
                }
            }
            Self::StripeAck { .. } => 4,
        };
        LOSSLESS_HEADER_BYTES + body
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn section_p_control_sizes_match_v10_layout() {
        assert_eq!(
            ControlFrame::BlockAck(BlockAck::empty()).payload_bytes(),
            32
        );
        assert_eq!(
            ControlFrame::AckProbe { target_peer_id: 9 }.payload_bytes(),
            28
        );
        assert_eq!(ControlFrame::SessionComplete.payload_bytes(), 20);
        assert_eq!(ControlFrame::SourceDone { round_id: 1 }.payload_bytes(), 24);
        assert_eq!(
            ControlFrame::Need {
                round_id: 1,
                deficit: 1
            }
            .payload_bytes(),
            38
        );
    }
}
