//! Receiver task for block-first lossless sessions.
//!
//! The receiver accepts a manifest, records completed plain blocks locally, and
//! optionally accumulates FEC symbols until a block can be decoded. After
//! `Eot`, plain mode emits end-of-round status feedback while FEC mode emits
//! one aggregate round status describing either completion or the remaining
//! per-block deficits for the next retransmit round.

mod fec;
mod plain;

use std::collections::BTreeSet;
use std::ops::Bound::{Excluded, Unbounded};

use tokio::sync::{mpsc, oneshot};
use tracing::{debug, info, warn};

use nextmini_messages::lossless_session::{
    self, FecStatus, LosslessSessionControl, LosslessSessionManifest, LosslessSessionMode,
    MissingBlockRange, PlainStatus,
};

use crate::node::processor::ProcessorHandle;
use crate::node::session::api::SessionId;
use crate::node::session::api::{CompletedReceiverReplay, InboundFrame, LosslessRuntimeMessage};
use crate::node::session::control;
use crate::node::session::plan::BlockPlan;
use crate::node::session::runtime::{ReceiverConfig, TransportRoute};

use self::fec::FecReceiver;
use self::plain::PlainReceiver;

/// Run one receiver session until the transfer is complete or the channel closes.
pub async fn run(
    cfg: ReceiverConfig,
    rx: mpsc::Receiver<InboundFrame>,
    processors: ProcessorHandle,
) {
    run_with_runtime(cfg, rx, processors, None).await;
}

pub(super) async fn run_with_runtime(
    cfg: ReceiverConfig,
    mut rx: mpsc::Receiver<InboundFrame>,
    processors: ProcessorHandle,
    runtime_sender: Option<mpsc::UnboundedSender<LosslessRuntimeMessage>>,
) {
    let mut receiver = SessionReceiver::new(cfg, processors);
    receiver.run(&mut rx, runtime_sender).await;
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
    async fn run(
        &mut self,
        rx: &mut mpsc::Receiver<InboundFrame>,
        runtime_sender: Option<mpsc::UnboundedSender<LosslessRuntimeMessage>>,
    ) {
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

        if self.is_complete() {
            self.register_completed_replay(runtime_sender).await;
        }

        debug!(
            session_id = self.shared.session_id,
            complete = self.is_complete(),
            "Lossless receiver finished"
        );
    }

    /// Return whether the receiver has completed every planned block.
    fn is_complete(&self) -> bool {
        match self.mode.as_ref() {
            Some(ReceiverMode::Plain(mode)) => mode.is_complete(),
            Some(ReceiverMode::Fec(mode)) => mode.is_complete(),
            None => false,
        }
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
            | LosslessSessionControl::PlainStatus { .. }
            | LosslessSessionControl::BlockStatus { .. }
            | LosslessSessionControl::FecStatus { .. } => {}
            LosslessSessionControl::Eot => {
                if let Some(ReceiverMode::Plain(mode)) = self.mode.as_mut() {
                    mode.handle_eot(&self.shared).await;
                }
                if let Some(ReceiverMode::Fec(mode)) = self.mode.as_mut() {
                    mode.handle_eot(&self.shared).await;
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
            LosslessSessionMode::Plain => ReceiverMode::Plain(PlainReceiver::default()),
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

    async fn register_completed_replay(
        &self,
        runtime_sender: Option<mpsc::UnboundedSender<LosslessRuntimeMessage>>,
    ) {
        let Some(runtime_sender) = runtime_sender else {
            return;
        };
        let Some(replay) = self.completed_replay() else {
            return;
        };

        let (ack_tx, ack_rx) = oneshot::channel();
        if runtime_sender
            .send(LosslessRuntimeMessage::ReceiverCompleted {
                session_id: self.shared.session_id,
                replay,
                ack: ack_tx,
            })
            .is_ok()
        {
            let _ = ack_rx.await;
        }
    }

    fn completed_replay(&self) -> Option<CompletedReceiverReplay> {
        match self.mode.as_ref() {
            Some(ReceiverMode::Plain(_)) if self.is_complete() => {
                Some(CompletedReceiverReplay::Plain {
                    route: self.shared.route,
                    status: PlainStatus::Complete,
                })
            }
            _ => None,
        }
    }
}

impl ReceiverShared {
    /// Return whether the receiver has completed every planned block.
    fn has_all_blocks(&self) -> bool {
        let Some(plan) = self.plan else {
            return false;
        };
        self.complete_blocks.len() as u64 == plan.total_blocks()
    }

    /// Copy one completed block payload into the optional sink buffer.
    pub(super) async fn write_block(&self, block_id: u64, payload: &[u8]) {
        if let Some(progress) = &self.cfg.progress {
            progress.mark_first_completed_block();
        }

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

    async fn send_fec_status(&self, status: &FecStatus) {
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
            &LosslessSessionControl::FecStatus {
                status: status.clone(),
            },
        )
        .await;
    }

    fn plain_status(&self) -> Option<PlainStatus> {
        let total_blocks = self.plan?.total_blocks();
        if total_blocks == 0 {
            return Some(PlainStatus::Complete);
        }
        if self.complete_blocks.len() as u64 == total_blocks {
            return Some(PlainStatus::Complete);
        }

        let mut ranges = Vec::new();
        let mut next_missing = 0u64;
        while next_missing < total_blocks {
            if self.complete_blocks.contains(&next_missing) {
                next_missing += 1;
                continue;
            }

            let start_block_id = next_missing;
            let end_block_id = self
                .complete_blocks
                .range((Excluded(start_block_id), Unbounded))
                .next()
                .copied()
                .unwrap_or(total_blocks);
            ranges.push(MissingBlockRange {
                start_block_id,
                end_block_id,
            });
            next_missing = end_block_id;
        }

        Some(PlainStatus::MissingBlocks { ranges })
    }

    async fn send_plain_status(&self, status: &PlainStatus) {
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
            &LosslessSessionControl::PlainStatus {
                status: status.clone(),
            },
        )
        .await;
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};
    use std::net::Ipv4Addr;
    use std::sync::Arc;
    use std::time::Duration;

    use tokio::sync::mpsc;
    use tokio::time::timeout;

    use nextmini_messages::{RouteForwardingMode, RoutingTableEntry};

    use super::*;
    use crate::node::NodeIdExt;
    use crate::node::config::LocalConfig;
    use crate::node::packet::Packet;
    use crate::node::processor::ProcessorHandle;
    use crate::node::session::receiver::fec::{FecBlockState, FecReceiver};

    const SOURCE_NODE_ID: usize = 51;
    const RECEIVER_NODE_ID: usize = 52;

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
                progress: None,
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
        let mut receiver = FecReceiver::new(
            BlockPlan::new(16, 8)
                .ok()
                .and_then(|plan| plan.symbol_geometry(4).ok())
                .expect("valid geometry"),
        );
        receiver.blocks = BTreeMap::from([(
            0,
            FecBlockState {
                symbols: BTreeMap::from([(0, vec![1, 2])]),
            },
        )]);
        receiver.eot_seen = true;

        assert_eq!(receiver.block_deficit(&shared, 0), 3);
    }

    #[tokio::test]
    async fn plain_receiver_only_completes_after_reporting_complete_on_eot() {
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
                    progress: None,
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

        assert!(!receiver.is_complete());
    }

    #[tokio::test]
    async fn plain_receiver_reports_complete_on_eot() {
        let (mut receiver, mut packet_rx) = plain_test_receiver(2, BTreeSet::from([0, 1])).await;

        receiver
            .handle_control_frame(InboundFrame {
                bytes: lossless_session::encode_control(
                    receiver.shared.session_id,
                    &LosslessSessionControl::Eot,
                ),
                peer_id: Some(SOURCE_NODE_ID),
            })
            .await;

        assert_eq!(
            recv_plain_status(&mut packet_rx).await,
            PlainStatus::Complete
        );
        assert!(receiver.is_complete());
        assert_eq!(receiver.shared.plain_status(), Some(PlainStatus::Complete));
    }

    #[tokio::test]
    async fn plain_receiver_reports_sparse_missing_ranges() {
        let (receiver, _packet_rx) = plain_test_receiver(4, BTreeSet::from([0, 2])).await;

        assert_eq!(
            receiver.shared.plain_status(),
            Some(PlainStatus::MissingBlocks {
                ranges: vec![
                    MissingBlockRange {
                        start_block_id: 1,
                        end_block_id: 2,
                    },
                    MissingBlockRange {
                        start_block_id: 3,
                        end_block_id: 4,
                    },
                ],
            })
        );
    }

    #[tokio::test]
    async fn plain_receiver_emits_sparse_missing_ranges_on_eot() {
        let (mut receiver, mut packet_rx) = plain_test_receiver(4, BTreeSet::from([0, 2])).await;
        let expected = PlainStatus::MissingBlocks {
            ranges: vec![
                MissingBlockRange {
                    start_block_id: 1,
                    end_block_id: 2,
                },
                MissingBlockRange {
                    start_block_id: 3,
                    end_block_id: 4,
                },
            ],
        };

        receiver
            .handle_control_frame(InboundFrame {
                bytes: lossless_session::encode_control(
                    receiver.shared.session_id,
                    &LosslessSessionControl::Eot,
                ),
                peer_id: Some(SOURCE_NODE_ID),
            })
            .await;

        assert_eq!(recv_plain_status(&mut packet_rx).await, expected);
        assert!(!receiver.is_complete());
    }

    #[tokio::test]
    async fn plain_receiver_keeps_missing_status_stable_across_repeated_eot() {
        let (mut receiver, mut packet_rx) = plain_test_receiver(2, BTreeSet::from([0])).await;

        let eot = InboundFrame {
            bytes: lossless_session::encode_control(
                receiver.shared.session_id,
                &LosslessSessionControl::Eot,
            ),
            peer_id: Some(SOURCE_NODE_ID),
        };
        let expected = PlainStatus::MissingBlocks {
            ranges: vec![MissingBlockRange {
                start_block_id: 1,
                end_block_id: 2,
            }],
        };

        receiver.handle_control_frame(eot.clone()).await;
        assert_eq!(recv_plain_status(&mut packet_rx).await, expected.clone());
        assert!(!receiver.is_complete());
        assert_eq!(receiver.shared.plain_status(), Some(expected.clone()));

        receiver.handle_control_frame(eot).await;
        assert_eq!(recv_plain_status(&mut packet_rx).await, expected.clone());
        assert!(!receiver.is_complete());
        assert_eq!(receiver.shared.plain_status(), Some(expected));
    }

    #[tokio::test]
    async fn plain_receiver_ignores_duplicate_data_before_eot() {
        let (mut receiver, mut packet_rx) = plain_test_receiver(2, BTreeSet::from([0])).await;
        let frame = InboundFrame {
            bytes: lossless_session::encode_block_data(receiver.shared.session_id, 0, b"abcdefgh"),
            peer_id: Some(SOURCE_NODE_ID),
        };

        receiver.handle_block_data_frame(frame.clone()).await;
        receiver.handle_block_data_frame(frame).await;

        assert!(
            timeout(Duration::from_millis(100), packet_rx.recv())
                .await
                .is_err(),
            "plain receiver should not emit per-block feedback before Eot"
        );
        assert!(!receiver.is_complete());
        assert_eq!(
            receiver.shared.plain_status(),
            Some(PlainStatus::MissingBlocks {
                ranges: vec![MissingBlockRange {
                    start_block_id: 1,
                    end_block_id: 2,
                }],
            })
        );
    }

    #[tokio::test]
    async fn write_block_marks_first_completed_block_progress() {
        let progress = Arc::new(crate::node::session::runtime::ReceiverProgress::default());
        let shared = ReceiverShared {
            session_id: 9,
            route: crate::node::session::runtime::TransportRoute {
                src_ip: std::net::Ipv4Addr::new(10, 0, 0, 1),
                dst_ip: std::net::Ipv4Addr::new(10, 0, 0, 2),
                src_port: 1,
                dst_port: 2,
            },
            local_node_id: 1,
            cfg: ReceiverConfig {
                session_id: 9,
                route: crate::node::session::runtime::TransportRoute {
                    src_ip: std::net::Ipv4Addr::new(10, 0, 0, 1),
                    dst_ip: std::net::Ipv4Addr::new(10, 0, 0, 2),
                    src_port: 1,
                    dst_port: 2,
                },
                local_node_id: 1,
                sink_buffer: None,
                progress: Some(progress.clone()),
                fec_enabled: false,
            },
            processors: crate::node::processor::ProcessorHandle::new(Default::default()),
            manifest: Some(LosslessSessionManifest {
                block_size: 8,
                total_bytes: 8,
                total_blocks: 1,
                mode: LosslessSessionMode::Plain,
            }),
            plan: BlockPlan::new(8, 8).ok(),
            complete_blocks: BTreeSet::new(),
        };

        shared.write_block(0, b"abcdefgh").await;

        assert!(progress.first_completed_block_at().is_some());
    }

    async fn plain_test_receiver(
        total_blocks: u64,
        complete_blocks: BTreeSet<u64>,
    ) -> (SessionReceiver, mpsc::Receiver<Packet>) {
        let cfg = LocalConfig {
            node_id: RECEIVER_NODE_ID,
            n_nodes: SOURCE_NODE_ID.max(RECEIVER_NODE_ID) + 1,
            num_packet_processors: 1,
            channel_capacity: 2048,
            user_space_base_addr: Ipv4Addr::new(10, 0, 0, 0),
            local_netmask: Ipv4Addr::new(255, 255, 255, 0),
            ..Default::default()
        };
        let processors = ProcessorHandle::new(cfg.clone());
        processors
            .update_routing_table(vec![RoutingTableEntry {
                route_id: 1,
                next_hops: vec![cfg.node_id],
                src_node_id: cfg.node_id,
                dst_node_id: SOURCE_NODE_ID,
                forward_mode: RouteForwardingMode::Unicast,
            }])
            .await;

        let route = crate::node::session::runtime::TransportRoute {
            src_ip: RECEIVER_NODE_ID.ip_addr(cfg.user_space_base_addr, cfg.local_netmask),
            dst_ip: SOURCE_NODE_ID.ip_addr(cfg.user_space_base_addr, cfg.local_netmask),
            src_port: 4750,
            dst_port: 5750,
        };
        let flow_id =
            Packet::flow_id_from_parts(route.src_ip, route.src_port, route.dst_ip, route.dst_port);
        let (packet_tx, packet_rx) = mpsc::channel(8);
        processors.connect_user_space_sender(flow_id, packet_tx);
        tokio::time::sleep(Duration::from_millis(50)).await;

        (
            SessionReceiver {
                shared: ReceiverShared {
                    session_id: 8,
                    route,
                    local_node_id: RECEIVER_NODE_ID,
                    cfg: ReceiverConfig {
                        session_id: 8,
                        route,
                        local_node_id: RECEIVER_NODE_ID,
                        sink_buffer: None,
                        progress: None,
                        fec_enabled: false,
                    },
                    processors,
                    manifest: Some(LosslessSessionManifest {
                        block_size: 8,
                        total_bytes: total_blocks * 8,
                        total_blocks,
                        mode: LosslessSessionMode::Plain,
                    }),
                    plan: BlockPlan::new(total_blocks * 8, 8).ok(),
                    complete_blocks,
                },
                mode: Some(ReceiverMode::Plain(PlainReceiver::default())),
            },
            packet_rx,
        )
    }

    async fn recv_plain_status(packet_rx: &mut mpsc::Receiver<Packet>) -> PlainStatus {
        let packet = timeout(Duration::from_secs(2), packet_rx.recv())
            .await
            .expect("timed out waiting for plain status")
            .expect("packet capture closed unexpectedly");
        let payload = packet
            .tcp_payload()
            .expect("plain status packet should include payload");
        let (_, control) =
            lossless_session::decode_control(payload).expect("plain status should decode");
        let LosslessSessionControl::PlainStatus { status } = control else {
            panic!("unexpected control frame: {control:?}");
        };
        status
    }
}
