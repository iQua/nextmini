use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap, HashSet};
use std::net::Ipv4Addr;
use std::sync::Arc;

use tokio::sync::{Mutex, Notify, broadcast, mpsc};
use tracing::{error, info, warn};
use tun_rs::{AsyncDevice, DeviceBuilder, GROTable, IDEAL_BATCH_SIZE, VIRTIO_NET_HDR_LEN};

use crate::node::RECEIVE_BUF_SIZE;
use crate::node::config::{Feature, LocalConfig};
use crate::node::packet::Packet;
use crate::node::processor::ProcessorHandle;
use crate::node::{FlowId, FlowIdExt};

/// Reads packets asynchronously from a TUN device, and sends them out to the Processor for processing.
pub struct LocalReader {
    device: Arc<AsyncDevice>, // each device is shared by both LocalReader and LocalWriter actors
    shutdown_receiver: broadcast::Receiver<ShutdownMessage>,
    processor: ProcessorHandle,
    // for TSO support on Linux
    original_buffer: Vec<u8>,
    packet_buffers: Vec<Vec<u8>>,
    packet_sizes: Vec<usize>,
}

impl LocalReader {
    async fn run(&mut self) {
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

                        // sends to the processor for routing and forwarding
                        self.processor.process_packet(packet);
                    }
                }
            }
        }
    }
}
