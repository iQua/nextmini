mod ack;
mod carousel;
mod control;
mod rounds;
mod striped;

pub use ack::{BlockAck, CompletedRange, PeerBlockCompletion};
pub use carousel::{
    CarouselConfigError, CarouselPeerDiagnostics, CarouselReceiver, CarouselReceiverState,
    CarouselSender, CarouselSenderState, CarouselTiming, LivenessViolation, OutboundControl,
};
pub use control::ControlFrame;
pub use rounds::{RoundsReceiver, RoundsSender, RoundsSenderState};
pub use striped::{
    StripeReceiver, StripeSender, StripeSenderMode, equal_quotas, proportional_quotas,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProtocolKind {
    EqualSplitStriping,
    RateProportionalStriping,
    PerStripeFec,
    PooledRounds,
    PooledCarousel,
}

impl ProtocolKind {
    pub const ALL: [Self; 5] = [
        Self::EqualSplitStriping,
        Self::RateProportionalStriping,
        Self::PerStripeFec,
        Self::PooledRounds,
        Self::PooledCarousel,
    ];

    pub const fn name(self) -> &'static str {
        match self {
            Self::EqualSplitStriping => "equal_split_striping",
            Self::RateProportionalStriping => "rate_proportional_striping",
            Self::PerStripeFec => "per_stripe_fec",
            Self::PooledRounds => "pooled_rounds",
            Self::PooledCarousel => "pooled_carousel",
        }
    }
}
