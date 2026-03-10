//! Receiver task for block-first lossless sessions.
//!
//! The receiver accepts a manifest, acknowledges completed plain blocks
//! directly, and optionally accumulates FEC symbols until a block can be
//! decoded. After `Eot`, incomplete FEC blocks trigger deficit feedback so the
//! sender can emit additional fountain symbols.

mod fec;
mod plain;

use std::collections::BTreeSet;

use tokio::sync::mpsc;
use tracing::{debug, info, warn};

use nextmini_messages::lossless_session::{
    self, LosslessSessionControl, LosslessSessionManifest, LosslessSessionMode,
};

use crate::node::processor::ProcessorHandle;
use crate::node::session::api::InboundFrame;
use crate::node::session::api::SessionId;
use crate::node::session::control;
use crate::node::session::plan::BlockPlan;
use crate::node::session::runtime::{ReceiverConfig, TransportRoute};

use self::fec::FecReceiver;
use self::plain::PlainReceiver;

/// Run one receiver session until the transfer is complete or the channel closes.
pub async fn run(
    cfg: ReceiverConfig,
    mut rx: mpsc::Receiver<InboundFrame>,
    processors: ProcessorHandle,
) {
    let mut receiver = SessionReceiver::new(cfg, processors);
    receiver.run(&mut rx).await;
}

/// Stateful receiver loop shared by plain and FEC transfer modes.
struct SessionReceiver {
    shared: ReceiverShared,
    mode: Option<ReceiverMode>,
}

/// Receiver state that is truly common across plain and FEC modes.
pub(super) struct ReceiverShared {
    pub(super) session_id: SessionId,
    pub(super) route: TransportRoute,
    pub(super) local_node_id: usize,
    pub(super) cfg: ReceiverConfig,
    pub(super) processors: ProcessorHandle,
    pub(super) manifest: Option<LosslessSessionManifest>,
    pub(super) plan: Option<BlockPlan>,
    pub(super) complete_blocks: BTreeSet<u64>,
}

/// Concrete receiver mode selected after the manifest is installed.
enum ReceiverMode {
    Plain(PlainReceiver),
    Fec(FecReceiver),
}

impl SessionReceiver {
    /// Build receiver state for one lossless session.
    fn new(cfg: ReceiverConfig, processors: ProcessorHandle) -> Self {
        Self {
            shared: ReceiverShared {
                session_id: cfg.session_id,
                route: cfg.route,
                local_node_id: cfg.local_node_id,
                cfg,
                processors,
                manifest: None,
                plan: None,
                complete_blocks: BTreeSet::new(),
            },
            mode: None,
        }
    }

    /// Execute the receiver loop until the object is complete.
    async fn run(&mut self, rx: &mut mpsc::Receiver<InboundFrame>) {
        info!(
            session_id = self.shared.session_id,
            "Lossless receiver started"
        );

        while let Some(frame) = rx.recv().await {
            if lossless_session::decode_control(&frame.bytes).is_some() {
                self.handle_control_frame(frame).await;
            } else if lossless_session::decode_block_data(&frame.bytes).is_some() {
                self.handle_block_data_frame(frame).await;
            } else if lossless_session::decode_block_symbol(&frame.bytes).is_some() {
                self.handle_block_symbol_frame(frame).await;
            }

            if self.is_complete() {
                break;
            }
        }

        debug!(
            session_id = self.shared.session_id,
            complete = self.is_complete(),
            "Lossless receiver finished"
        );
    }

    /// Return whether the receiver has completed every planned block.
    fn is_complete(&self) -> bool {
        self.shared.is_complete()
    }

    /// Handle one inbound control frame.
    async fn handle_control_frame(&mut self, frame: InboundFrame) {
        let Some((_, control)) = lossless_session::decode_control(&frame.bytes) else {
            return;
        };

        match control {
            LosslessSessionControl::Manifest { manifest } => {
                self.install_manifest(manifest).await;
            }
            LosslessSessionControl::Ready { .. }
            | LosslessSessionControl::BlockAck { .. }
            | LosslessSessionControl::BlockStatus { .. } => {}
            LosslessSessionControl::Eot => {
                self.shared.reemit_completed_acks().await;
                if let Some(ReceiverMode::Fec(mode)) = self.mode.as_mut() {
                    mode.eot_seen = true;
                    mode.send_status_for_incomplete_blocks(&self.shared).await;
                }
            }
        }
    }

    /// Dispatch one plain data frame when the installed manifest is plain.
    async fn handle_block_data_frame(&mut self, frame: InboundFrame) {
        let Some(ReceiverMode::Plain(mode)) = self.mode.as_mut() else {
            return;
        };
        mode.handle_block_data_frame(&mut self.shared, frame).await;
    }

    /// Dispatch one FEC symbol frame when the installed manifest is FEC.
    async fn handle_block_symbol_frame(&mut self, frame: InboundFrame) {
        let Some(ReceiverMode::Fec(mode)) = self.mode.as_mut() else {
            return;
        };
        mode.handle_block_symbol_frame(&mut self.shared, frame)
            .await;
    }

    /// Install the first valid manifest and send READY.
    async fn install_manifest(&mut self, manifest: LosslessSessionManifest) {
        if let Some(existing) = &self.shared.manifest {
            if existing == &manifest {
                self.shared.send_ready().await;
            } else {
                warn!(
                    session_id = self.shared.session_id,
                    installed_total_bytes = existing.total_bytes,
                    installed_block_size = existing.block_size,
                    received_total_bytes = manifest.total_bytes,
                    received_block_size = manifest.block_size,
                    "Lossless receiver ignored conflicting manifest after install"
                );
            }
            return;
        }

        if manifest.mode.is_fec() && !self.shared.cfg.fec_enabled {
            warn!(
                session_id = self.shared.session_id,
                "Lossless receiver rejected FEC manifest because local runtime disabled FEC"
            );
            return;
        }

        let Ok(block_size) = usize::try_from(manifest.block_size) else {
            return;
        };
        let Ok(plan) = BlockPlan::new(manifest.total_bytes, block_size) else {
            return;
        };
        let mode = match &manifest.mode {
            LosslessSessionMode::Plain => ReceiverMode::Plain(PlainReceiver),
            LosslessSessionMode::Fec(fec) => {
                let Some(geometry) = plan.symbol_geometry(fec.symbols_per_block).ok() else {
                    return;
                };
                ReceiverMode::Fec(FecReceiver::new(geometry))
            }
        };

        let Some(object_len) = plan.total_bytes_usize() else {
            warn!(
                session_id = self.shared.session_id,
                manifest_total_bytes = manifest.total_bytes,
                "Lossless receiver rejected manifest that did not fit local address space"
            );
            return;
        };
        if !self.shared.ensure_sink_buffer_len(object_len).await {
            return;
        }
        self.shared.plan = Some(plan);
        self.shared.manifest = Some(manifest);
        self.mode = Some(mode);
        self.shared.send_ready().await;
    }
}

impl ReceiverShared {
    /// Return whether the receiver has completed every planned block.
    fn is_complete(&self) -> bool {
        let Some(plan) = self.plan else {
            return false;
        };
        self.complete_blocks.len() as u64 == plan.total_blocks()
    }

    /// Copy one completed block payload into the optional sink buffer.
    pub(super) async fn write_block(&self, block_id: u64, payload: &[u8]) {
        let Some(plan) = self.plan else {
            return;
        };
        let Some(span) = plan.block_span(block_id) else {
            return;
        };
        let Some(sink) = &self.cfg.sink_buffer else {
            return;
        };

        let mut guard = sink.lock().await;
        let Some(object_len) = plan.total_bytes_usize() else {
            return;
        };
        if guard.len() < object_len {
            guard.resize(object_len, 0);
        }

        let start = usize::try_from(span.offset()).unwrap_or(0);
        let end = start + payload.len().min(span.len());
        if end <= guard.len() {
            guard[start..end].copy_from_slice(&payload[..end - start]);
        }
    }

    /// Ensure the optional sink buffer is large enough for the full object.
    async fn ensure_sink_buffer_len(&self, object_len: usize) -> bool {
        let Some(sink) = &self.cfg.sink_buffer else {
            return true;
        };
        let mut guard = sink.lock().await;
        if guard.len() < object_len {
            let additional = object_len - guard.len();
            if guard.try_reserve_exact(additional).is_err() {
                warn!(
                    session_id = self.session_id,
                    object_len, "Lossless receiver failed to reserve sink buffer for manifest"
                );
                return false;
            }
            guard.resize(object_len, 0);
        }
        true
    }

    /// Send a READY control frame back to the sender.
    async fn send_ready(&self) {
        control::send_control(
            &self.processors,
            control::FrameRoute {
                session_id: self.session_id,
                tree_id: None,
                src_ip: self.route.src_ip,
                src_port: self.route.src_port,
                dst_ip: self.route.dst_ip,
                dst_port: self.route.dst_port,
            },
            &LosslessSessionControl::Ready {
                node_id: self.local_node_id as u64,
            },
        )
        .await;
    }

    /// Acknowledge completion of one logical block.
    pub(super) async fn send_block_ack(&self, block_id: u64) {
        control::send_control(
            &self.processors,
            control::FrameRoute {
                session_id: self.session_id,
                tree_id: None,
                src_ip: self.route.src_ip,
                src_port: self.route.src_port,
                dst_ip: self.route.dst_ip,
                dst_port: self.route.dst_port,
            },
            &LosslessSessionControl::BlockAck { block_id },
        )
        .await;
    }

    /// Re-send block acknowledgements once `Eot` arrives.
    async fn reemit_completed_acks(&self) {
        for &block_id in &self.complete_blocks {
            self.send_block_ack(block_id).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use super::*;
    use crate::node::session::receiver::fec::{FecBlockState, FecReceiver};

    #[tokio::test]
    async fn block_deficit_requests_missing_source_symbols_first() {
        let shared = ReceiverShared {
            session_id: 7,
            route: crate::node::session::runtime::TransportRoute {
                src_ip: std::net::Ipv4Addr::new(10, 0, 0, 1),
                dst_ip: std::net::Ipv4Addr::new(10, 0, 0, 2),
                src_port: 1,
                dst_port: 2,
            },
            local_node_id: 1,
            cfg: ReceiverConfig {
                session_id: 7,
                route: crate::node::session::runtime::TransportRoute {
                    src_ip: std::net::Ipv4Addr::new(10, 0, 0, 1),
                    dst_ip: std::net::Ipv4Addr::new(10, 0, 0, 2),
                    src_port: 1,
                    dst_port: 2,
                },
                local_node_id: 1,
                sink_buffer: None,
                fec_enabled: true,
            },
            processors: crate::node::processor::ProcessorHandle::new(Default::default()),
            manifest: Some(LosslessSessionManifest {
                block_size: 8,
                total_bytes: 16,
                total_blocks: 2,
                mode: LosslessSessionMode::Fec(
                    nextmini_messages::lossless_session::LosslessSessionFecMode::new_raptorq(
                        4,
                        vec![0, 1],
                    ),
                ),
            }),
            plan: BlockPlan::new(16, 8).ok(),
            complete_blocks: BTreeSet::new(),
        };
        let receiver = FecReceiver {
            geometry: BlockPlan::new(16, 8)
                .ok()
                .and_then(|plan| plan.symbol_geometry(4).ok())
                .expect("valid geometry"),
            blocks: BTreeMap::from([(
                0,
                FecBlockState {
                    symbols: BTreeMap::from([(0, vec![1, 2])]),
                },
            )]),
            eot_seen: true,
        };

        assert_eq!(receiver.block_deficit(&shared, 0), 3);
    }

    #[tokio::test]
    async fn receiver_completion_does_not_require_eot() {
        let receiver = SessionReceiver {
            shared: ReceiverShared {
                session_id: 8,
                route: crate::node::session::runtime::TransportRoute {
                    src_ip: std::net::Ipv4Addr::new(10, 0, 0, 1),
                    dst_ip: std::net::Ipv4Addr::new(10, 0, 0, 2),
                    src_port: 1,
                    dst_port: 2,
                },
                local_node_id: 1,
                cfg: ReceiverConfig {
                    session_id: 8,
                    route: crate::node::session::runtime::TransportRoute {
                        src_ip: std::net::Ipv4Addr::new(10, 0, 0, 1),
                        dst_ip: std::net::Ipv4Addr::new(10, 0, 0, 2),
                        src_port: 1,
                        dst_port: 2,
                    },
                    local_node_id: 1,
                    sink_buffer: None,
                    fec_enabled: false,
                },
                processors: crate::node::processor::ProcessorHandle::new(Default::default()),
                manifest: Some(LosslessSessionManifest {
                    block_size: 8,
                    total_bytes: 16,
                    total_blocks: 2,
                    mode: LosslessSessionMode::Plain,
                }),
                plan: BlockPlan::new(16, 8).ok(),
                complete_blocks: BTreeSet::from([0, 1]),
            },
            mode: Some(ReceiverMode::Plain(PlainReceiver::default())),
        };

        assert!(receiver.is_complete());
    }
}
