use std::net::Ipv4Addr;
use std::sync::Arc;

use flume;
use tokio::sync::broadcast;
use tracing::{debug, error, info, warn};
use tun_rs::{AsyncDevice, DeviceBuilder};

use crate::node::RECEIVE_BUF_SIZE;
use crate::node::config::LocalConfig;
use crate::node::packet::Packet;
use crate::node::processor::ProcessorHandle;

/// Message types for LocalInterface, which manages the LocalReader and LocalWriter actors.
#[derive(Clone)]
pub enum LocalInterfaceMessage {
    WritePacket(Packet), // the processor sends a packet to the application via the local interface
    Shutdown, // shuts down LocalInterface gracefully, stopping all LocalReader and LocalWriter actors
}

/// Handle for Processors to interact with LocalInterface
#[derive(Clone)]
pub struct LocalInterfaceHandle {
    shutdown_sender: broadcast::Sender<LocalInterfaceMessage>,
    write_sender: flume::Sender<LocalInterfaceMessage>,
}

impl LocalInterfaceHandle {
    pub fn new(config: LocalConfig, processor: ProcessorHandle) -> Self {
        // creates local TUN devices. On Linux, this creates multiple queues for parallel processing,
        // each queue corresponding to its own device. On non-Linux platforms, it creates one device only.
        let tun_devices = Self::create_tun_devices(config.clone());

        // a broadcast channel for sending the shutdown signal to both local interface readers and writers
        let (shutdown_sender, _) = broadcast::channel(config.channel_capacity);

        // an MPMC channel for the processors to send packets to the local interface writers
        let (write_sender, write_receiver) = flume::bounded(config.channel_capacity);

        for dev in tun_devices.iter() {
            let mut reader = LocalReader {
                shutdown_receiver: shutdown_sender.subscribe(),
                processor: processor.clone(),
                device: dev.clone(),
            };

            let mut writer = LocalWriter {
                shutdown_receiver: shutdown_sender.subscribe(),
                packet_receiver: write_receiver.clone(),
                device: dev.clone(),
            };

            tokio::spawn(async move {
                reader.run().await;
            });

            tokio::spawn(async move {
                writer.run().await;
            });
        }

        Self {
            shutdown_sender,
            write_sender,
        }
    }

    pub async fn write_packet(
        &self,
        packet: Packet,
    ) -> Result<(), flume::SendError<LocalInterfaceMessage>> {
        let _ = self
            .write_sender
            .send(LocalInterfaceMessage::WritePacket(packet))?;

        Ok(())
    }

    pub async fn shutdown(&self) {
        if let Err(e) = self.shutdown_sender.send(LocalInterfaceMessage::Shutdown) {
            error!("Error shutting down all the actors: {}", e);
        };
    }

    /// Converts a netmask tuple to prefix length. Used in 'LocalInterfaceHandle::create_tun_device()'.
    fn mask_to_prefix(mask: (u8, u8, u8, u8)) -> u8 {
        let mask_u32 = u32::from_be_bytes([mask.0, mask.1, mask.2, mask.3]);
        mask_u32.count_ones() as u8
    }

    /// Creates local TUN devices for communicating with the application.
    pub fn create_tun_devices(config: LocalConfig) -> Vec<Arc<AsyncDevice>> {
        #[cfg(target_os = "linux")]
        {
            let num_queues = config.num_tun_queues;

            let if_name = config.tun_interface_name.clone();
            let ipv4_addr = config.local_address;
            let ipv4_prefix = Self::mask_to_prefix(config.local_netmask);

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
            let ipv4_prefix = Self::mask_to_prefix(config.local_netmask);

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
}

/// Reads packets asynchronously from a TUN device, and sends them out to the Processor for processing.
pub struct LocalReader {
    device: Arc<AsyncDevice>, // each device is shared by both LocalReader and LocalWriter actors
    shutdown_receiver: broadcast::Receiver<LocalInterfaceMessage>,
    processor: ProcessorHandle,
}

impl LocalReader {
    async fn run(&mut self) {
        let mut buf = [0; RECEIVE_BUF_SIZE];

        loop {
            tokio::select! {
                msg = self.shutdown_receiver.recv() => {
                    if let Ok(LocalInterfaceMessage::Shutdown) = msg {
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

                    let packet = Packet::new(n, buf);

                    // checks if packet creation was successful (non-zero flow_id indicates valid packet)
                    if packet.flow_id == 0 {
                        debug!("LocalReader: Invalid packet received, dropping (size: {})", n);
                        continue;
                    }

                    // sends to the processor for routing and forwarding
                    self.processor.process_packet(packet).await;
                }
            }
        }
    }
}

/// Writes one packet to a TUN device.
struct LocalWriter {
    device: Arc<AsyncDevice>, // each device is shared by both LocalReader and LocalWriter actors
    shutdown_receiver: broadcast::Receiver<LocalInterfaceMessage>,
    packet_receiver: flume::Receiver<LocalInterfaceMessage>,
}

impl LocalWriter {
    async fn run(&mut self) {
        loop {
            tokio::select! {
                msg = self.shutdown_receiver.recv() => {
                    if let Ok(LocalInterfaceMessage::Shutdown) = msg {
                            info!("LocalWriter received shutdown signal, stopping...");
                            break;
                    }
                }
                msg = self.packet_receiver.recv_async() => {
                    if let Ok(LocalInterfaceMessage::WritePacket(packet)) = msg {
                        let buf = &packet.buf[0..packet.packet_size];

                        let _ = self.device.send(buf).await;
                    }
                }
            }
        }
    }
}
