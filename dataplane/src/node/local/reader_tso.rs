use std::sync::Arc;

use tokio::sync::broadcast;
use tracing::{error, info, warn};
use tun_rs::{AsyncDevice, IDEAL_BATCH_SIZE, VIRTIO_NET_HDR_LEN};

use nextmini_messages::OperatingMode;

use crate::node::config::LocalConfig;
use crate::node::local::interface::ShutdownMessage;
use crate::node::packet::Packet;
use crate::node::processor::ProcessorHandle;
use crate::node::splice::connector::ConnectorHandle;

/// Reads packets asynchronously from a TUN device, and sends them out to the Processor for processing.
pub struct LocalReader {
    device: Arc<AsyncDevice>, // each device is shared by both LocalReader and LocalWriter actors
    shutdown_receiver: broadcast::Receiver<ShutdownMessage>,
    processor: ProcessorHandle,
    connector: ConnectorHandle,
    config: LocalConfig,
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
        connector: ConnectorHandle,
        config: LocalConfig,
    ) -> Self {
        Self {
            device,
            shutdown_receiver,
            processor,
            connector,
            config,
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
                            warn!("LocalReader received an empty packet."); //keeps the original design
                            continue;
                        }

                        // uses slice to avoid copying
                        let packet = Packet::from_slice(packet_size, &self.packet_buffers[i]);

                        // checks if packet creation was successful (non-zero flow_id indicates valid packet)
                        if packet.flow_id == 0 {
                            continue;
                        }

                        // decides which processor to use based on the node's operating mode
                        match self.config.operating_mode {
                            OperatingMode::Normal => {
                                self.processor.process_packet(packet);
                            }
                            OperatingMode::Max => {
                                self.connector.process_packet(packet);
                            }
                        }
                    }
                }
            }
        }
    }
}
