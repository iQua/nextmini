use std::net::Ipv4Addr;
use std::sync::Arc;

use tokio::sync::{broadcast, mpsc};
use tracing::{error, info};
use tun_rs::{AsyncDevice, DeviceBuilder};

use crate::node::FlowIdExt;
use crate::node::config::LocalConfig;
use crate::node::packet::Packet;
use crate::node::processor::ProcessorHandle;

#[cfg(target_os = "linux")]
use crate::node::local_reader_tso::LocalReader;
#[cfg(target_os = "linux")]
use crate::node::local_writer_tso::LocalWriter;

#[cfg(not(target_os = "linux"))]
use crate::node::local_reader::LocalReader;
#[cfg(not(target_os = "linux"))]
use crate::node::local_writer::LocalWriter;

/// Message types for LocalInterface, which manages the LocalReader and LocalWriter actors.
#[derive(Clone)]
pub enum ShutdownMessage {
    Shutdown, // shuts down LocalInterface gracefully, stopping all LocalReader and LocalWriter actors
}

pub enum LocalInterfaceMessage {
    WritePacket(Packet), // the processor sends a packet to the application via the local interface
}

/// Handle for Processors to interact with LocalInterface
#[derive(Clone)]
pub struct LocalInterfaceHandle {
    shutdown_sender: broadcast::Sender<ShutdownMessage>,
    write_senders: Vec<mpsc::Sender<LocalInterfaceMessage>>,
}

impl LocalInterfaceHandle {
    pub fn new(config: LocalConfig, processor: ProcessorHandle) -> Self {
        // creates local TUN devices. On Linux, this creates multiple queues for parallel processing,
        // each queue corresponding to its own device. On non-Linux platforms, it creates one device only.
        let tun_devices = Self::create_tun_devices(config.clone());

        // a broadcast channel for sending the shutdown signal to both local interface readers and writers
        let (shutdown_sender, _) = broadcast::channel(config.channel_capacity);

        let mut write_senders = Vec::with_capacity(tun_devices.len());

        for dev in tun_devices.iter() {
            // for each LocalWriter, creates its MPSC channel
            let (write_sender, write_receiver) = mpsc::channel(config.channel_capacity);
            write_senders.push(write_sender);

            let mut reader =
                LocalReader::new(dev.clone(), shutdown_sender.subscribe(), processor.clone());

            tokio::spawn(async move {
                reader.run().await;
            });

            let mut writer = LocalWriter::new(
                config.clone(),
                dev.clone(),
                shutdown_sender.subscribe(),
                write_receiver,
            );

            tokio::spawn(async move {
                writer.run().await;
            });
        }

        Self {
            shutdown_sender,
            write_senders,
        }
    }

    pub fn write_packet(&self, packet: Packet) {
        let idx = packet.flow_id.hash(self.write_senders.len());
        let sender = &self.write_senders[idx];

        if let Err(e) = sender.try_send(LocalInterfaceMessage::WritePacket(packet)) {
            error!(
                "Error sending a packet to the local interface writer: {}. Dropped.",
                e
            );
        }
    }

    pub async fn shutdown(&self) {
        if let Err(e) = self.shutdown_sender.send(ShutdownMessage::Shutdown) {
            error!("Error shutting down all the actors: {}", e);
        };
    }

    /// Converts a netmask tuple to prefix length. Used in 'LocalInterfaceHandle::create_tun_device()'.
    fn mask_to_prefix(mask: (u8, u8, u8, u8)) -> u8 {
        let mask_u32 = u32::from_be_bytes([mask.0, mask.1, mask.2, mask.3]);
        mask_u32.count_ones() as u8
    }

    /// Creates local TUN devices for communicating with the application.
    #[cfg(not(target_os = "linux"))]
    pub fn create_tun_devices(config: LocalConfig) -> Vec<Arc<AsyncDevice>> {
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

    /// Creates local TUN devices for communicating with the application.
    #[cfg(target_os = "linux")]
    pub fn create_tun_devices(config: LocalConfig) -> Vec<Arc<AsyncDevice>> {
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
            .multi_queue(true) // enables multi-queue support
            .offload(true) // enables TSO support
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
}
