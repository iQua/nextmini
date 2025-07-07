use std::sync::Arc;

use tokio::sync::broadcast;
use tracing::{error, info, warn};
use tun_rs::AsyncDevice;

use nextmini_messages::OperatingMode;

use crate::node::RECEIVE_BUF_SIZE;
use crate::node::config::LocalConfig;
use crate::node::local::interface::ShutdownMessage;
use crate::node::packet::Packet;
use crate::node::processor::ProcessorHandle;
use crate::node::splice::connector::ConnectorHandle;

/// Reads packets asynchronously from a TUN device, and sends them out to the Processor for processing.
pub struct LocalReader {
    pub device: Arc<AsyncDevice>, // each device is shared by both LocalReader and LocalWriter actors
    pub shutdown_receiver: broadcast::Receiver<ShutdownMessage>,
    pub processor: ProcessorHandle,
    pub connector: ConnectorHandle,
    pub config: LocalConfig,
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

                    // checks if packet creation was successful (non-zero flow_id indicates valid packet)
                    if packet.flow_id == 0 {
                        continue;
                    }

                    // decides which processor to use based on the node's operating mode
                    match self.config.operating_mode {
                        OperatingMode::Normal => {
                            // local flows use Normal mode processing
                            self.processor.process_packet(packet);
                        }
                        OperatingMode::Max => {
                            // local flows use Max mode processing
                            self.connector.process_packet(packet);
                        }
                    }
                }
            }
        }
    }
}
