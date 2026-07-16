use super::{
    FecFeedbackMode, LosslessSessionControl, LosslessSessionFecMode, LosslessSessionManifest,
    LosslessSessionMode, MissingBlockRange, NeedBlock, NeedReport,
};

pub(super) fn plain_manifest() -> LosslessSessionManifest {
    LosslessSessionManifest {
        block_size: 1024,
        total_bytes: 2500,
        total_blocks: 3,
        mode: LosslessSessionMode::Plain,
    }
}

pub(super) fn fec_manifest() -> LosslessSessionManifest {
    LosslessSessionManifest {
        block_size: 1024,
        total_bytes: 2500,
        total_blocks: 3,
        mode: LosslessSessionMode::Fec(LosslessSessionFecMode::new_raptorq(8, vec![1, 3, 5])),
    }
}

pub(super) fn carousel_manifest() -> LosslessSessionManifest {
    LosslessSessionManifest {
        mode: LosslessSessionMode::Fec(
            LosslessSessionFecMode::new_raptorq(8, vec![1, 3, 5])
                .with_feedback_mode(FecFeedbackMode::Carousel),
        ),
        ..fec_manifest()
    }
}

pub(super) fn plain_need(round_id: u32, ranges: Vec<MissingBlockRange>) -> LosslessSessionControl {
    LosslessSessionControl::Need {
        round_id,
        report: NeedReport::Plain { ranges },
    }
}

pub(super) fn fec_need(round_id: u32, blocks: Vec<NeedBlock>) -> LosslessSessionControl {
    LosslessSessionControl::Need {
        round_id,
        report: NeedReport::Fec { blocks },
    }
}
