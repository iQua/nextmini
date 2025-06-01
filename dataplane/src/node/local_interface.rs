use std::net::Ipv4Addr;
use std::sync::Arc;

use tokio::sync::mpsc;
use tokio::sync::mpsc::error::SendError;
use tracing::{debug, error, info, warn};
use tun_rs::{AsyncDevice, DeviceBuilder};

use crate::node::RECEIVE_BUF_SIZE;
use crate::node::config::LocalConfig;
use crate::node::packet::Packet;
use crate::node::processor::ProcessorHandle;

/// Message types for the LocalInterface, which manages the LocalReader and LocalWriter actors.
pub enum LocalInterfaceMessage {
    // the processor writes a packet to the local interface
    WritePacket(Packet),
    Shutdown,
}

/// Handle for Processor to interact with LocalInterface
#[derive(Clone)]
pub struct LocalInterfaceHandle {
    read_sender: mpsc::Sender<LocalReaderMessage>,
    write_sender: mpsc::Sender<LocalInterfaceMessage>,
}

impl LocalInterfaceHandle {
    pub fn new(config: LocalConfig) -> Self {
        let (read_sender, read_receiver) = mpsc::channel(config.channel_capacity);
        let (write_sender, write_receiver) = mpsc::channel(config.channel_capacity);

        let writer = LocalWriter::new(config.clone(), write_receiver);
        let reader = LocalReader::new(config, read_receiver);

        tokio::spawn(async move {
            reader.run().await;
        });

        tokio::spawn(async move {
            writer.run().await;
        });

        Self {
            read_sender,
            write_sender,
        }
    }

    pub async fn write_packet(
        &self,
        packet: Packet,
    ) -> Result<(), SendError<LocalInterfaceMessage>> {
        self.write_sender
            .send(LocalInterfaceMessage::WritePacket(packet))
            .await
    }

    pub async fn shutdown(&self) -> Result<(), SendError<LocalInterfaceMessage>> {
        // Handle the first send operation, but ignore its specific error type
        if let Err(_) = self.read_sender.send(LocalReaderMessage::Shutdown).await {
            // if it fails, we continue with shutting down the local interface writer
            error!("Failed to send a shutdown message to the local interface reader.");
        }

        // Return the result of the write_sender operation
        self.write_sender
            .send(LocalInterfaceMessage::Shutdown)
            .await
    }
}

/// Converts a netmask tuple to prefix length. Used in 'create_tun_devices()'.
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
