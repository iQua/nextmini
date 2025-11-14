use std::sync::Arc;

use tokio::sync::broadcast;
use tracing::{error, info, warn};
use tun_rs::AsyncDevice;

use crate::node::RECEIVE_BUF_SIZE;
use crate::node::controller::flowstats::FlowStatsReporterHandle;
use crate::node::flow;
use crate::node::local::interface::ShutdownMessage;
use crate::node::packet::{Packet, PacketBuf};
use crate::node::processor::ProcessorHandle;

/// Reads packets asynchronously from a TUN device, and sends them out to the Processor for processing.
pub struct LocalReader {
    pub device: Arc<AsyncDevice>, // each device is shared by both LocalReader and LocalWriter actors
    pub shutdown_receiver: broadcast::Receiver<ShutdownMessage>,
    pub processor: ProcessorHandle,
    pub flowstats_reporter: FlowStatsReporterHandle,
}

impl LocalReader {
    pub fn new(
        device: Arc<AsyncDevice>,
        shutdown_receiver: broadcast::Receiver<ShutdownMessage>,
        processor: ProcessorHandle,
        flowstats_reporter: FlowStatsReporterHandle,
    ) -> Self {
        Self {
            device,
            shutdown_receiver,
            processor,
            flowstats_reporter,
        }
    }

    pub async fn run(&mut self) {
        loop {
            let mut packet_buf = PacketBuf::new();
            let recv_slice = packet_buf.prepare_uninit(RECEIVE_BUF_SIZE);

            tokio::select! {
                msg = self.shutdown_receiver.recv() => {
                    if let Ok(ShutdownMessage::Shutdown) = msg {
                            info!("LocalReader received shutdown signal, stopping...");
                            break;
                    }
                }
                // reads from the local TUN device
                result = self.device.recv(recv_slice) => {
                    let n = match result {
                        Ok(n) => n,
                        Err(e) => {
                            error!(
                                "Failed to read from TUN device: {:?}: interface may be down. Retrying...",
                                e
                            );
                            continue;
                        }
                    };
                    // skips empty packets
                    if n == 0 {
                        warn!("LocalReader received an empty packet.");
                        continue;
                    }

                    packet_buf.truncate(n);
                    let packet = Packet::new(n, packet_buf);

                    // checks if packet creation was successful
                    if packet.flow_id == flow::INVALID_FLOW_ID {
                        continue;
                    }

                    // reports packet flow stats (app flow start and flow finish if FIN/RST)
                    self.flowstats_reporter.report_packet(&packet);

                    // sends to the processor for routing and forwarding
                    self.processor.process_packet(packet).await;
                }
            }
        }
    }
}
