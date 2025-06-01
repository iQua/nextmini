use std::net::Ipv4Addr;
use std::sync::Arc;

use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};
use tun_rs::{AsyncDevice, DeviceBuilder};

use crate::node::RECEIVE_BUF_SIZE;
use crate::node::config::LocalConfig;
use crate::node::packet::Packet;
use crate::node::processor::ProcessorHandle;

/// Converts a netmask tuple to prefix length. Used in 'create_tun_device()'.
fn mask_to_prefix(mask: (u8, u8, u8, u8)) -> u8 {
    let mask_u32 = u32::from_be_bytes([mask.0, mask.1, mask.2, mask.3]);
    mask_u32.count_ones() as u8
}

/// Creates TUN devices for sending data to the application.
pub async fn create_tun_device(config: LocalConfig) -> Vec<Arc<AsyncDevice>> {
    #[cfg(target_os = "linux")]
    {
        let num_queues = config.num_packet_processors;

        let if_name = config.tun_interface_name.clone();
        let ipv4_addr = config.local_address;
        let ipv4_prefix = mask_to_prefix(config.local_netmask);

        let dev = DeviceBuilder::new()
            .name(&if_name)
            .ipv4(
                Ipv4Addr::new(ipv4_addr.0, ipv4_addr.1, ipv4_addr.2, ipv4_addr.3),
                ipv4_prefix,
                None,
            )
            .mtu(config.mtu as u16)
            .multi_queue(true)
            .build_async()
            .expect("Failed to create tun device");

        let mut queues = Vec::with_capacity(num_queues);

        // creates multiple TUN queues with error handling
        info!("Creating {num_queues} TUN queues.");
        for _ in 0..num_queues - 1 {
            match dev.try_clone() {
                Ok(cloned_dev) => {
                    queues.push(Arc::new(cloned_dev));
                }
                Err(e) => {
                    // if we are unable to create all the queues, use what we have
                    warn!(
                        "Could not create all TUN queues ({}), continuing with {} queues",
                        e,
                        queues.len()
                    );
                    break;
                }
            }
        }

        queues.push(Arc::new(dev));

        // ensures that we have at least one queue
        if queues.is_empty() {
            panic!("Failed to create even a single TUN queue. Terminating.");
        }

        queues
    }

    #[cfg(not(target_os = "linux"))]
    {
        let ipv4_addr = config.local_address;
        let ipv4_prefix = mask_to_prefix(config.local_netmask);

        let dev = DeviceBuilder::new()
            .ipv4(
                Ipv4Addr::new(ipv4_addr.0, ipv4_addr.1, ipv4_addr.2, ipv4_addr.3),
                ipv4_prefix,
                None,
            )
            .mtu(config.mtu as u16)
            .build_async()
            .expect("Failed to create tun device");

        // creates a single TUN queue on non-Linux platforms without multi-queue support
        info!("Creating one TUN queue on non-Linux platforms without multi-queue support.");
        let queues = vec![Arc::new(dev)];

        queues
    }
}

/// Message types for LocalReader
enum LocalReaderMessage {
    Shutdown,
}

/// Reads packets asynchronously from a TUN device in a Tokio task, and sends them out
/// via a ProcessorHandle
pub struct LocalReader {
    dev: Arc<AsyncDevice>, // a shared reference to the device that can be cloned
    processor_handle: ProcessorHandle,
    receiver: mpsc::Receiver<LocalReaderMessage>,
}

impl LocalReader {
    async fn run(&mut self) {
        let mut buf = [0; RECEIVE_BUF_SIZE];

        loop {
            tokio::select! {
                msg = self.receiver.recv() => {
                    if let Some(LocalReaderMessage::Shutdown) = msg {
                            info!("LocalReader received shutdown signal, stopping...");
                            break;
                    }
                }

                // Read from TUN device
                result = self.dev.recv(&mut buf) => {
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

                    // Skip empty packets
                    if n == 0 {
                        warn!("LocalReader received an empty packet.");
                        continue;
                    }

                    let packet = Packet::new(n, buf);

                    // Check if packet creation was successful (non-zero flow_id indicates valid packet)
                    if packet.flow_id == 0 {
                        debug!("LocalReader: Invalid packet received, dropping (size: {})", n);
                        continue;
                    }

                    // Use ProcessorHandle
                    self.processor_handle.process_packet(packet).await;
                }
            }
        }
    }
}

#[derive(Clone)]
pub struct LocalReaderHandle {
    sender: mpsc::Sender<LocalReaderMessage>,
}

impl LocalReaderHandle {
    pub fn new(dev: Arc<AsyncDevice>, processor_handle: ProcessorHandle) -> Self {
        let (sender, receiver) = mpsc::channel(10);
        let mut actor = LocalReader {
            dev,
            processor_handle,
            receiver,
        };

        tokio::spawn(async move {
            actor.run().await;
        });

        Self { sender }
    }

    pub async fn shutdown(&self) {
        if let Err(e) = self.sender.send(LocalReaderMessage::Shutdown).await {
            error!("Failed to send shutdown signal to LocalReader: {:?}", e);
        }
    }
}

/// Message types
enum LocalWriterMessage {
    WritePacket(Packet),
}

/// Writes one packet to a TUN device.
struct LocalWriter {
    receiver: mpsc::Receiver<LocalWriterMessage>,
    dev: Arc<AsyncDevice>,
}

impl LocalWriter {
    async fn run(&mut self) {
        while let Some(msg) = self.receiver.recv().await {
            match msg {
                LocalWriterMessage::WritePacket(packet) => {
                    let buf = &packet.buf[0..packet.packet_size];

                    if let Err(e) = self.dev.send(buf).await {
                        error!("Failed to write to TUN device: {:?}", e);
                    }
                }
            }
        }
    }
}

/// LocalWriterHandle
#[derive(Clone)]
pub struct LocalWriterHandle {
    sender: mpsc::Sender<LocalWriterMessage>,
}

impl LocalWriterHandle {
    pub fn new(dev: Arc<AsyncDevice>) -> Self {
        let (sender, receiver) = mpsc::channel(100);
        let mut actor = LocalWriter { receiver, dev };

        tokio::spawn(async move {
            actor.run().await;
        });

        Self { sender }
    }

    pub async fn write_packet(&self, packet: Packet) {
        if let Err(e) = self
            .sender
            .send(LocalWriterMessage::WritePacket(packet))
            .await
        {
            error!("Failed to send packet to LocalWriter actor: {:?}", e);
        }
    }
}

/// For Processor to call LocalInterfaceHandle
#[derive(Clone)]
pub struct LocalInterfaceHandle {
    local_writer_handles: Vec<LocalWriterHandle>,
    _local_reader_handles: Vec<LocalReaderHandle>, // Not used
}

impl LocalInterfaceHandle {
    pub fn new(tun_queues: Vec<Arc<AsyncDevice>>, processor_handle: ProcessorHandle) -> Self {
        // Create one LocalWriter for each TUN queue
        let mut local_writer_handles = Vec::new();
        let mut local_reader_handles = Vec::new();
        for queue in &tun_queues {
            let writer_handle = LocalWriterHandle::new(queue.clone());
            local_writer_handles.push(writer_handle);

            let reader_handle = LocalReaderHandle::new(queue.clone(), processor_handle.clone());
            local_reader_handles.push(reader_handle);
        }

        Self {
            local_writer_handles,
            _local_reader_handles: local_reader_handles,
        }
    }
    // The i-th processor uses the writer at index i
    // like what previously was done in old design
    // each processor to write to its own TUN queue(LocalWriter)
    pub async fn write_packet(&self, packet: Packet, writer_index: usize) {
        if let Some(writer) = self.local_writer_handles.get(writer_index) {
            writer.write_packet(packet).await; // Get the writer at the specified index from the vector
        } else {
            error!(
                "Invalid writer index: {}, available writers: {}",
                writer_index,
                self.local_writer_handles.len()
            );
        }
    }
}
