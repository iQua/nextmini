use std::sync::Arc;

use tokio::sync::broadcast;
use tracing::{error, info, warn};
use tun_rs::{AsyncDevice, IDEAL_BATCH_SIZE, VIRTIO_NET_HDR_LEN};

use crate::node::controller::flowstats::FlowStatsReporterHandle;
use crate::node::flow;
use crate::node::local::interface::ShutdownMessage;
use crate::node::packet::Packet;
use crate::node::processor::ProcessorHandle;

/// Reads packets asynchronously from a TUN device, and sends them out to the Processor for processing.
pub struct LocalReader {
    device: Arc<AsyncDevice>, // each device is shared by both LocalReader and LocalWriter actors
    shutdown_receiver: broadcast::Receiver<ShutdownMessage>,
    processor: ProcessorHandle,
    flowstats_reporter: FlowStatsReporterHandle,
    // for TSO support on Linux
    original_buffer: Vec<u8>,
    packet_buffers: Vec<Vec<u8>>,
    packet_sizes: Vec<usize>,
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
            original_buffer: vec![0; VIRTIO_NET_HDR_LEN + 65535],
            packet_buffers: vec![vec![0u8; 1500]; IDEAL_BATCH_SIZE],
            packet_sizes: vec![0; IDEAL_BATCH_SIZE],
        }
    }

    pub async fn run(&mut self) {
        loop {
            tokio::select! {
                msg = self.shutdown_receiver.recv() => {
                    if let Ok(ShutdownMessage::Shutdown) = msg {
                            info!("LocalReader received shutdown signal, stopping...");
                            break;
                    }
                }
                // receives packets with TSO support
                result = self.device.recv_multiple(
                    &mut self.original_buffer,
                    &mut self.packet_buffers,
                    &mut self.packet_sizes,
                    0
                ) => {
                    let num_packets = match result {
                        Ok(num) => num,
                        Err(e) => {
                            error!("Failed to read from TUN device: {:?}", e);
                            continue;
                        }
                    };

                    // processes each packet
                    for i in 0..num_packets {
                        let packet_size = self.packet_sizes[i];
                        // skips empty packets
                        if packet_size == 0 {
                            warn!("LocalReader received an empty packet.");
                            continue;
                        }

                        // uses slice to avoid copying
                        let packet = Packet::from_slice(packet_size, &self.packet_buffers[i]);

                        // checks if packet creation was successful
                        if packet.flow_id == flow::INVALID_FLOW_ID {
                            continue;
                        }

                        // reports new app flows to the controller
                        self.flowstats_reporter.report_app_flow(packet.flow_id);

                        // checks and reports if flow finished (receivedFIN/RST)
                        self.flowstats_reporter.check_and_report_finished(&packet);

                        // sends to the processor for routing and forwarding
                        self.processor.process_packet(packet);
                    }
                }
            }
        }
    }
}
