//! Sender task for block-first lossless sessions.
//!
//! Plain mode sends complete blocks on the default tree. FEC mode sends source
//! symbols first, emits `Eot` after the source sweep, and only then responds to
//! per-block deficit feedback with extra fountain symbols.

mod fec;
mod plain;

use bytes::Bytes;
use std::collections::BTreeSet;
use tokio::sync::{mpsc, watch};
use tokio::time::{Duration, Instant};
use tracing::{info, warn};

use nextmini_messages::lossless_session::{
    self, BlockStatus, LosslessSessionControl, LosslessSessionManifest, LosslessSessionMode,
};

use crate::node::processor::ProcessorHandle;
use crate::node::scheduler::token_bucket::TokenBucket;
use crate::node::session::api::InboundFrame;
use crate::node::session::control;
use crate::node::session::ledger::SessionLedger;
use crate::node::session::plan::{BlockPlan, BlockSpan, SymbolGeometry};
use crate::node::session::runtime::{SenderConfig, SessionConfig, TransportRoute};

use self::fec::FecSender;
use self::plain::PlainSender;

const MANIFEST_RETRY_INTERVAL: Duration = Duration::from_millis(250);
const IDLE_WAIT: Duration = Duration::from_millis(10);

/// Run one sender session until completion or channel shutdown.
pub async fn run(
    cfg: SenderConfig,
    mut ctrl_rx: mpsc::Receiver<InboundFrame>,
    processors: ProcessorHandle,
) {
    let mut sender = match SessionSender::new(cfg, processors) {
        Ok(sender) => sender,
        Err(reason) => {
            warn!(reason, "Lossless sender aborted before start");
            return;
        }
    };

    sender.run(&mut ctrl_rx).await;
}

/// Mode-specific sender hooks invoked by the shared control path.
pub(super) trait ModeHooks {
    /// Observe a newly completed block after a peer ACK updates the ledger.
    fn on_block_completed(&mut self, _block_id: u64) {}

    /// Observe per-block FEC deficit feedback from a receiver.
    fn on_block_status(&mut self, _shared: &SenderShared, _peer_id: usize, _status: BlockStatus) {}
}

/// Shared sender shell that owns session-level transport and ledger state.
struct SessionSender {
    shared: SenderShared,
    mode: SenderMode,
}

/// Sender state that is truly common across plain and FEC modes.
pub(super) struct SenderShared {
    pub(super) session: SessionConfig,
    pub(super) route: TransportRoute,
    pub(super) processors: ProcessorHandle,
    pub(super) manifest: LosslessSessionManifest,
    pub(super) receiver_ids: Vec<usize>,
    pub(super) receiver_set: BTreeSet<usize>,
    pub(super) ready_peers: BTreeSet<usize>,
    pub(super) plan: BlockPlan,
    pub(super) source: BlockSource,
    pub(super) ledger: SessionLedger,
    pub(super) ready_grace: Duration,
    pub(super) topology_ready: Option<watch::Receiver<bool>>,
    pub(super) pacer: Option<TokenBucket>,
}

/// Concrete sender mode selected from the manifest.
enum SenderMode {
    Plain(PlainSender),
    Fec(FecSender),
}

/// Source object wrapper used to derive block payloads and source symbols.
#[derive(Clone)]
pub(super) struct BlockSource {
    bytes: Bytes,
}

impl BlockSource {
    /// Wrap the transfer bytes used by the sender.
    fn new(bytes: Bytes) -> Self {
        Self { bytes }
    }

    /// Materialize one logical block payload for the requested span.
    fn block_payload(&self, span: BlockSpan) -> Vec<u8> {
        let len = span.len();
        let Some(offset) = usize::try_from(span.offset()).ok() else {
            return vec![0u8; len];
        };
        if offset + len <= self.bytes.len() {
            return self.bytes.slice(offset..offset + len).to_vec();
        }

        let mut out = vec![0u8; len];
        if offset < self.bytes.len() {
            let available = len.min(self.bytes.len() - offset);
            out[..available].copy_from_slice(&self.bytes[offset..offset + available]);
        }
        out
    }

    /// Partition one logical block into fixed-size source symbols.
    fn source_symbols(&self, span: BlockSpan, geometry: SymbolGeometry) -> Vec<Vec<u8>> {
        let block = self.block_payload(span);
        let total_symbol_bytes = geometry.source_symbols() * geometry.symbol_size();
        let mut padded = vec![0u8; total_symbol_bytes];
        let copy_len = block.len().min(total_symbol_bytes);
        padded[..copy_len].copy_from_slice(&block[..copy_len]);
        padded
            .chunks(geometry.symbol_size())
            .map(|chunk| chunk.to_vec())
            .collect()
    }
}

impl SessionSender {
    /// Build sender state from the validated runtime configuration.
    fn new(cfg: SenderConfig, processors: ProcessorHandle) -> Result<Self, &'static str> {
        let manifest = cfg.manifest.clone();
        let block_size =
            usize::try_from(manifest.block_size).map_err(|_| "invalid block size in manifest")?;
        let plan = BlockPlan::new(manifest.total_bytes, block_size)
            .map_err(|_| "invalid block plan for sender")?;
        let ledger = SessionLedger::new(plan.total_blocks(), cfg.receiver_ids.iter().copied())
            .map_err(|_| "unable to allocate sender ledger")?;
        let pacer = cfg.pacing.clone().map(TokenBucket::new);
        let ready_grace = Duration::from_millis(cfg.ready_grace_ms);
        let source = BlockSource::new(cfg.source_buffer.clone());
        let receiver_set = cfg.receiver_ids.iter().copied().collect::<BTreeSet<_>>();
        let mode = match &manifest.mode {
            LosslessSessionMode::Plain => SenderMode::Plain(PlainSender::default()),
            LosslessSessionMode::Fec(_) => SenderMode::Fec(FecSender::new(&manifest, plan)?),
        };

        Ok(Self {
            shared: SenderShared {
                session: cfg.session,
                route: cfg.route,
                processors,
                manifest,
                receiver_ids: cfg.receiver_ids,
                receiver_set,
                ready_peers: BTreeSet::new(),
                plan,
                source,
                ledger,
                ready_grace,
                topology_ready: cfg.topology_ready,
                pacer,
            },
            mode,
        })
    }

    /// Execute the sender state machine for the negotiated transfer mode.
    async fn run(&mut self, ctrl_rx: &mut mpsc::Receiver<InboundFrame>) {
        info!(
            session_id = self.shared.session.session_id,
            total_bytes = self.shared.manifest.total_bytes,
            total_blocks = self.shared.manifest.total_blocks,
            receivers = self.shared.receiver_ids.len(),
            fec = self.shared.manifest.mode.is_fec(),
            "Lossless sender started"
        );

        self.shared.wait_topology_ready().await;
        let ready = match &mut self.mode {
            SenderMode::Plain(mode) => self.shared.negotiate_ready(ctrl_rx, mode).await,
            SenderMode::Fec(mode) => self.shared.negotiate_ready(ctrl_rx, mode).await,
        };
        if !ready {
            return;
        }

        match &mut self.mode {
            SenderMode::Plain(mode) => mode.run(&mut self.shared, ctrl_rx).await,
            SenderMode::Fec(mode) => mode.run(&mut self.shared, ctrl_rx).await,
        }

        info!(
            session_id = self.shared.session.session_id,
            complete = self.shared.ledger.is_complete(),
            "Lossless sender finished"
        );
    }
}

impl SenderShared {
    /// Wait for the runtime-level topology gate before beginning the handshake.
    async fn wait_topology_ready(&mut self) {
        let Some(rx) = self.topology_ready.as_mut() else {
            return;
        };
        if *rx.borrow() {
            return;
        }

        while rx.changed().await.is_ok() {
            if *rx.borrow() {
                break;
            }
        }
    }

    /// Repeatedly advertise the manifest until the READY gate opens.
    async fn negotiate_ready<M: ModeHooks>(
        &mut self,
        ctrl_rx: &mut mpsc::Receiver<InboundFrame>,
        mode: &mut M,
    ) -> bool {
        if self.receiver_set.is_empty() {
            return true;
        }

        let deadline = Instant::now() + self.ready_grace;
        let mut next_manifest_at = Instant::now();

        while self.ready_peers.len() < self.receiver_set.len() {
            let now = Instant::now();
            if now >= next_manifest_at {
                self.send_manifest().await;
                next_manifest_at = now + MANIFEST_RETRY_INTERVAL;
            }
            if now >= deadline {
                break;
            }

            let wake_at = next_manifest_at.min(deadline);
            tokio::select! {
                maybe_frame = ctrl_rx.recv() => {
                    let Some(frame) = maybe_frame else {
                        return false;
                    };
                    self.handle_control(frame, mode);
                }
                _ = tokio::time::sleep_until(wake_at) => {}
            }
        }

        if self.ready_peers.len() < self.receiver_set.len() {
            let missing = self
                .receiver_set
                .difference(&self.ready_peers)
                .copied()
                .collect::<Vec<_>>();
            warn!(
                session_id = self.session.session_id,
                ?missing,
                "Lossless sender opening data gate before all receivers sent Ready"
            );
        }

        true
    }

    /// Drain any queued control frames without blocking the send loop.
    pub(super) fn drain_controls<M: ModeHooks>(
        &mut self,
        ctrl_rx: &mut mpsc::Receiver<InboundFrame>,
        mode: &mut M,
    ) {
        while let Ok(frame) = ctrl_rx.try_recv() {
            self.handle_control(frame, mode);
        }
    }

    /// Wait for either new control input or a short idle retry interval.
    pub(super) async fn wait_for_signal<M: ModeHooks>(
        &mut self,
        ctrl_rx: &mut mpsc::Receiver<InboundFrame>,
        mode: &mut M,
    ) -> bool {
        tokio::select! {
            maybe_frame = ctrl_rx.recv() => {
                let Some(frame) = maybe_frame else {
                    return false;
                };
                self.handle_control(frame, mode);
                true
            }
            _ = tokio::time::sleep(IDLE_WAIT) => true,
        }
    }

    /// Apply one inbound control frame to the sender state machine.
    fn handle_control<M: ModeHooks>(&mut self, frame: InboundFrame, mode: &mut M) {
        let Some((_, control)) = lossless_session::decode_control(&frame.bytes) else {
            return;
        };

        match control {
            LosslessSessionControl::Manifest { .. } | LosslessSessionControl::Eot => {}
            LosslessSessionControl::Ready { node_id } => {
                if let Ok(node_id) = usize::try_from(node_id)
                    && self.receiver_set.contains(&node_id)
                {
                    self.ready_peers.insert(node_id);
                }
            }
            LosslessSessionControl::BlockAck { block_id } => {
                let Some(peer_id) = frame.peer_id else {
                    return;
                };
                if !self.receiver_set.contains(&peer_id) {
                    return;
                }
                if let Ok(update) = self.ledger.ack_block(peer_id, block_id)
                    && update.block_completed_now
                {
                    mode.on_block_completed(block_id);
                }
            }
            LosslessSessionControl::BlockStatus { status } => {
                let Some(peer_id) = frame.peer_id else {
                    return;
                };
                mode.on_block_status(self, peer_id, status);
            }
        }
    }

    /// Send the negotiated manifest to every receiver.
    async fn send_manifest(&mut self) {
        control::send_control(
            &self.processors,
            control::FrameRoute {
                session_id: self.session.session_id,
                tree_id: None,
                src_ip: self.route.src_ip,
                src_port: self.route.src_port,
                dst_ip: self.route.dst_ip,
                dst_port: self.route.dst_port,
            },
            &LosslessSessionControl::Manifest {
                manifest: self.manifest.clone(),
            },
        )
        .await;
    }

    /// Emit the end-of-transmission marker for the current send round.
    pub(super) async fn send_eot(&mut self) {
        control::send_control(
            &self.processors,
            control::FrameRoute {
                session_id: self.session.session_id,
                tree_id: None,
                src_ip: self.route.src_ip,
                src_port: self.route.src_port,
                dst_ip: self.route.dst_ip,
                dst_port: self.route.dst_port,
            },
            &LosslessSessionControl::Eot,
        )
        .await;
    }

    /// Apply optional pacing before sending `bytes` bytes of payload.
    pub(super) async fn pace(&mut self, bytes: usize) {
        if let Some(bucket) = self.pacer.as_mut() {
            bucket.wait_for_bytes(bytes).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn block_source_zero_fills_when_buffer_is_short() {
        let source = BlockSource::new(Bytes::from_static(b"ab"));
        let plan = BlockPlan::new(8, 4).expect("valid plan");
        let payload = source.block_payload(plan.block_span(1).expect("second block"));
        assert_eq!(payload, b"\0\0\0\0");
    }

    #[test]
    fn block_source_builds_fixed_size_source_symbols() {
        let source = BlockSource::new(Bytes::from_static(b"abcdef"));
        let plan = BlockPlan::new(6, 6).expect("valid plan");
        let geometry = plan.symbol_geometry(4).expect("valid geometry");
        let symbols = source.source_symbols(plan.block_span(0).expect("first block"), geometry);

        assert_eq!(symbols.len(), 4);
        assert_eq!(symbols[0], b"ab");
        assert_eq!(symbols[1], b"cd");
        assert_eq!(symbols[2], b"ef");
        assert_eq!(symbols[3], b"\0\0");
    }
}
