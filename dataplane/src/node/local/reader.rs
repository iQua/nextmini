use std::sync::Arc;

use tokio::sync::broadcast;
use tracing::{error, info, warn};
use tun_rs::AsyncDevice;

use crate::node::RECEIVE_BUF_SIZE;
use crate::node::controller::flowstats::FlowStatsReporterHandle;
use crate::node::flow;
use crate::node::local::interface::ShutdownMessage;
use crate::node::packet::Packet;
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
        let mut buf = [0; RECEIVE_BUF_SIZE];

        loop {
            tokio::select! {
                msg = self.shutdown_receiver.recv() => {
                    if let Ok(ShutdownMessage::Shutdown) = msg {
                            info!("LocalReader received shutdown signal, stopping...");
                            break;
                    }
                }
                // reads from the local TUN device
                result = self.device.recv(&mut buf) => {
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

                    let packet = Packet::new(n, buf.to_vec());

                    // checks if packet creation was successful
                    if packet.flow_id == flow::INVALID_FLOW_ID {
                        continue;
                    }

                    // reports new app flows to the controller
                    self.flowstats_reporter.report_app_flow(packet.flow_id);

                    // checks and reports if flow finished (FIN/RST)
                    self.flowstats_reporter.check_and_report_finished(&packet);

                    // sends to the processor for routing and forwarding
                    self.processor.process_packet(packet);
                }
            }
        }
    }
}
