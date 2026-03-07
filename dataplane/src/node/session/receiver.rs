use std::collections::{BTreeMap, BTreeSet};

use tokio::sync::mpsc;
use tracing::{debug, info, warn};

use nextmini_messages::lossless_session::{
    self, BlockStatus, LosslessSessionControl, LosslessSessionManifest, LosslessSessionMode,
};

use crate::node::processor::ProcessorHandle;
use crate::node::session::api::InboundFrame;
use crate::node::session::control;
use crate::node::session::fec::{self, BlockParams, Decoder};
use crate::node::session::plan::{BlockPlan, SymbolGeometry};
use crate::node::session::runtime::ReceiverConfig;
use crate::node::{NodeId, NodeIdExt};

pub async fn run(
    cfg: ReceiverConfig,
    mut rx: mpsc::Receiver<InboundFrame>,
    processors: ProcessorHandle,
) {
    let mut receiver = SessionReceiver::new(cfg, processors);
    receiver.run(&mut rx).await;
}

struct SessionReceiver {
    cfg: ReceiverConfig,
    processors: ProcessorHandle,
    src_ip: std::net::Ipv4Addr,
    dst_ip: std::net::Ipv4Addr,
    manifest: Option<LosslessSessionManifest>,
    plan: Option<BlockPlan>,
    geometry: Option<SymbolGeometry>,
    complete_blocks: BTreeSet<u64>,
    fec_blocks: BTreeMap<u64, FecBlockState>,
    eot_seen: bool,
}

#[derive(Default)]
struct FecBlockState {
    symbols: BTreeMap<u32, Vec<u8>>,
}

impl SessionReceiver {
    fn new(cfg: ReceiverConfig, processors: ProcessorHandle) -> Self {
        let src_ip =
            (cfg.common.local_node_id as NodeId).ip_addr(cfg.common.user_space_base_addr, cfg.common.local_netmask);
        let dst_ip =
            (cfg.source_node_id as NodeId).ip_addr(cfg.common.user_space_base_addr, cfg.common.local_netmask);

        Self {
            cfg,
            processors,
            src_ip,
            dst_ip,
            manifest: None,
            plan: None,
            geometry: None,
            complete_blocks: BTreeSet::new(),
            fec_blocks: BTreeMap::new(),
            eot_seen: false,
        }
    }

    async fn run(&mut self, rx: &mut mpsc::Receiver<InboundFrame>) {
        info!(
            session_id = self.cfg.common.session_id,
            expected_bytes = self.cfg.expected_bytes,
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
            session_id = self.cfg.common.session_id,
            complete = self.is_complete(),
            "Lossless receiver finished"
        );
    }

    fn is_complete(&self) -> bool {
        let Some(plan) = self.plan else {
            return false;
        };
        self.eot_seen && self.complete_blocks.len() as u64 == plan.total_blocks()
    }

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
                self.eot_seen = true;
                self.reemit_completed_acks().await;
                if matches!(self.manifest.as_ref().map(|m| &m.mode), Some(LosslessSessionMode::Fec(_)))
                {
                    self.send_status_for_incomplete_blocks().await;
                }
            }
        }
    }

    async fn install_manifest(&mut self, manifest: LosslessSessionManifest) {
        if let Some(existing) = &self.manifest {
            if existing == &manifest {
                self.send_ready().await;
            }
            return;
        }

        if manifest.total_bytes != self.cfg.expected_bytes
            || usize::try_from(manifest.block_size).ok() != Some(self.cfg.common.block_size)
        {
            warn!(
                session_id = self.cfg.common.session_id,
                expected_bytes = self.cfg.expected_bytes,
                manifest_total_bytes = manifest.total_bytes,
                expected_block_size = self.cfg.common.block_size,
                manifest_block_size = manifest.block_size,
                "Lossless receiver rejected manifest with mismatched geometry"
            );
            return;
        }
        if manifest.mode.is_fec() && !self.cfg.fec_enabled {
            warn!(
                session_id = self.cfg.common.session_id,
                "Lossless receiver rejected FEC manifest because local runtime disabled FEC"
            );
            return;
        }

        let Ok(plan) = BlockPlan::new(manifest.total_bytes, self.cfg.common.block_size) else {
            return;
        };
        let geometry = match &manifest.mode {
            LosslessSessionMode::Plain => None,
            LosslessSessionMode::Fec(fec) => plan.symbol_geometry(fec.symbols_per_block).ok(),
        };

        self.ensure_sink_buffer().await;
        self.plan = Some(plan);
        self.geometry = geometry;
        self.manifest = Some(manifest);
        self.send_ready().await;
    }

    async fn handle_block_data_frame(&mut self, frame: InboundFrame) {
        let Some(manifest) = self.manifest.as_ref() else {
            return;
        };
        if !matches!(manifest.mode, LosslessSessionMode::Plain) {
            return;
        }

        let Some((_, data, payload)) = lossless_session::decode_block_data(&frame.bytes) else {
            return;
        };
        if manifest.validate_block_data(&data).is_err() {
            return;
        }
        if self.complete_blocks.contains(&data.block_id) {
            self.send_block_ack(data.block_id).await;
            return;
        }

        self.write_block(data.block_id, payload).await;
        self.complete_blocks.insert(data.block_id);
        self.send_block_ack(data.block_id).await;
    }

    async fn handle_block_symbol_frame(&mut self, frame: InboundFrame) {
        let Some(manifest) = self.manifest.as_ref() else {
            return;
        };
        let LosslessSessionMode::Fec(fec_mode) = manifest.mode.clone() else {
            return;
        };
        let Some((_, symbol, payload)) = lossless_session::decode_block_symbol(&frame.bytes) else {
            return;
        };
        if manifest.validate_block_symbol(&symbol).is_err() {
            return;
        }
        if self.complete_blocks.contains(&symbol.block_id) {
            self.send_block_ack(symbol.block_id).await;
            return;
        }

        let state = self.fec_blocks.entry(symbol.block_id).or_default();
        let inserted = state
            .symbols
            .insert(symbol.symbol_id, payload.to_vec())
            .is_none();

        if self.try_decode_fec_block(symbol.block_id, &fec_mode).await {
            self.send_block_ack(symbol.block_id).await;
            return;
        }

        if self.eot_seen && (inserted || !self.complete_blocks.contains(&symbol.block_id)) {
            self.send_block_status(symbol.block_id).await;
        }
    }

    async fn try_decode_fec_block(
        &mut self,
        block_id: u64,
        fec_mode: &nextmini_messages::lossless_session::LosslessSessionFecMode,
    ) -> bool {
        let Some(plan) = self.plan else {
            return false;
        };
        let Some(geometry) = self.geometry else {
            return false;
        };
        let Some(block_state) = self.fec_blocks.get(&block_id) else {
            return false;
        };
        if block_state.symbols.len() < usize::from(fec_mode.symbols_per_block) {
            return false;
        }

        let params = BlockParams::new(
            usize::from(fec_mode.symbols_per_block),
            geometry.symbol_size(),
            fec::block_seed(self.cfg.common.session_id, block_id),
        );
        let decoder = Decoder::from_block(params);
        let mut received = Vec::with_capacity(block_state.symbols.len());

        for (&symbol_id, payload) in &block_state.symbols {
            let mut padded = payload.clone();
            padded.resize(geometry.symbol_size(), 0);
            if symbol_id < u32::from(fec_mode.symbols_per_block) {
                received.push(decoder.source_symbol(symbol_id, padded));
            } else {
                received.push(decoder.coded_symbol(symbol_id, padded));
            }
        }

        let Ok(output) = decoder.decode(&received) else {
            return false;
        };
        let Some(block_len) = plan.block_len(block_id) else {
            return false;
        };

        let mut block = Vec::with_capacity(output.source_symbols.len() * geometry.symbol_size());
        for symbol in output.source_symbols {
            block.extend_from_slice(&symbol);
        }
        block.truncate(block_len);

        self.write_block(block_id, &block).await;
        self.complete_blocks.insert(block_id);
        self.fec_blocks.remove(&block_id);
        true
    }

    async fn write_block(&self, block_id: u64, payload: &[u8]) {
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
        let expected_len = usize::try_from(self.cfg.expected_bytes).unwrap_or(0);
        if guard.len() < expected_len {
            guard.resize(expected_len, 0);
        }

        let start = usize::try_from(span.offset()).unwrap_or(0);
        let end = start + payload.len().min(span.len());
        if end <= guard.len() {
            guard[start..end].copy_from_slice(&payload[..end - start]);
        }
    }

    async fn ensure_sink_buffer(&self) {
        let Some(sink) = &self.cfg.sink_buffer else {
            return;
        };
        let mut guard = sink.lock().await;
        let expected_len = usize::try_from(self.cfg.expected_bytes).unwrap_or(0);
        if guard.len() < expected_len {
            guard.resize(expected_len, 0);
        }
    }

    async fn send_ready(&self) {
        control::send_control(
            &self.processors,
            self.cfg.common.session_id,
            self.src_ip,
            self.cfg.common.src_port,
            self.dst_ip,
            self.cfg.common.dst_port,
            &LosslessSessionControl::Ready {
                node_id: self.cfg.common.local_node_id as u64,
            },
        )
        .await;
    }

    async fn send_block_ack(&self, block_id: u64) {
        control::send_control(
            &self.processors,
            self.cfg.common.session_id,
            self.src_ip,
            self.cfg.common.src_port,
            self.dst_ip,
            self.cfg.common.dst_port,
            &LosslessSessionControl::BlockAck { block_id },
        )
        .await;
    }

    async fn send_block_status(&self, block_id: u64) {
        let deficit = self.block_deficit(block_id);
        control::send_control(
            &self.processors,
            self.cfg.common.session_id,
            self.src_ip,
            self.cfg.common.src_port,
            self.dst_ip,
            self.cfg.common.dst_port,
            &LosslessSessionControl::BlockStatus {
                status: BlockStatus {
                    block_id,
                    deficit_symbols: deficit,
                },
            },
        )
        .await;
    }

    fn block_deficit(&self, block_id: u64) -> u16 {
        let Some(manifest) = self.manifest.as_ref() else {
            return 1;
        };
        let LosslessSessionMode::Fec(fec_mode) = &manifest.mode else {
            return 1;
        };
        let present = self
            .fec_blocks
            .get(&block_id)
            .map(|state| state.symbols.len())
            .unwrap_or(0);
        let total = usize::from(fec_mode.symbols_per_block);
        if present >= total {
            1
        } else {
            u16::try_from(total - present).unwrap_or(u16::MAX).max(1)
        }
    }

    async fn reemit_completed_acks(&self) {
        for &block_id in &self.complete_blocks {
            self.send_block_ack(block_id).await;
        }
    }

    async fn send_status_for_incomplete_blocks(&self) {
        let Some(plan) = self.plan else {
            return;
        };
        for block_id in 0..plan.total_blocks() {
            if self.complete_blocks.contains(&block_id) {
                continue;
            }
            self.send_block_status(block_id).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn block_deficit_requests_missing_source_symbols_first() {
        let receiver = SessionReceiver {
            cfg: ReceiverConfig {
                common: crate::node::session::runtime::CommonConfig {
                    session_id: 7,
                    dest_ip: std::net::Ipv4Addr::new(10, 0, 0, 2),
                    block_size: 8,
                    src_port: 1,
                    dst_port: 2,
                    data_bucket: None,
                    local_node_id: 1,
                    user_space_base_addr: std::net::Ipv4Addr::new(10, 0, 0, 0),
                    local_netmask: std::net::Ipv4Addr::new(255, 255, 255, 0),
                },
                source_node_id: 2,
                expected_bytes: 16,
                sink_buffer: None,
                fec_enabled: true,
            },
            processors: crate::node::processor::ProcessorHandle::new(Default::default()),
            src_ip: std::net::Ipv4Addr::new(10, 0, 0, 1),
            dst_ip: std::net::Ipv4Addr::new(10, 0, 0, 2),
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
            geometry: BlockPlan::new(16, 8).ok().and_then(|plan| plan.symbol_geometry(4).ok()),
            complete_blocks: BTreeSet::new(),
            fec_blocks: BTreeMap::from([(
                0,
                FecBlockState {
                    symbols: BTreeMap::from([(0, vec![1, 2])]),
                },
            )]),
            eot_seen: true,
        };

        assert_eq!(receiver.block_deficit(0), 3);
    }
}
