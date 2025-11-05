use std::net::Ipv4Addr;
use std::sync::Arc;

use tokio::sync::{broadcast, mpsc};
use tracing::{debug, error, info};
use tun_rs::{AsyncDevice, DeviceBuilder};

use crate::node::FlowIdExt;
use crate::node::config::LocalConfig;
use crate::node::controller::flowstats::FlowStatsReporterHandle;
use crate::node::packet::Packet;
use crate::node::processor::ProcessorHandle;

#[cfg(target_os = "linux")]
use crate::node::local::reader_tso::LocalReader;
#[cfg(target_os = "linux")]
use crate::node::local::writer_tso::LocalWriter;

#[cfg(not(target_os = "linux"))]
use crate::node::local::reader::LocalReader;
#[cfg(not(target_os = "linux"))]
use crate::node::local::writer::LocalWriter;

/// Message types for LocalInterface, which manages the LocalReader and LocalWriter actors.
#[derive(Clone)]
pub enum ShutdownMessage {
    Shutdown, // shuts down LocalInterface gracefully, stopping all LocalReader and LocalWriter actors
}

pub enum LocalInterfaceMessage {
    WritePacket(Packet), // the processor sends a packet to the application via the local interface
}

/// Handle for Processors to interact with LocalInterface.
#[derive(Clone, Debug)]
pub struct LocalInterfaceHandle {
    shutdown_sender: broadcast::Sender<ShutdownMessage>,
    write_senders: Vec<mpsc::Sender<LocalInterfaceMessage>>,
}

impl LocalInterfaceHandle {
    pub fn new(
        config: LocalConfig,
        processor: ProcessorHandle,
        flowstats_reporter: FlowStatsReporterHandle,
    ) -> Self {
        let (shutdown_sender, _) = broadcast::channel(config.channel_capacity);

        if !config.enable_local_interface {
            debug!(
                "Local interface disabled via configuration; skipping TUN interface initialization."
            );
            let _ = processor;
            let _ = flowstats_reporter;
            return Self {
                shutdown_sender,
                write_senders: Vec::new(),
            };
        }

        // creates local TUN devices. On Linux, this creates multiple queues for parallel processing,
        // each queue corresponding to its own device. On non-Linux platforms, it creates one device only.
        let tun_devices = Self::create_tun_devices(config.clone());

        let mut write_senders = Vec::with_capacity(tun_devices.len());

        for dev in tun_devices.iter() {
            // for each LocalWriter, creates its MPSC channel
            let (write_sender, write_receiver) = mpsc::channel(config.channel_capacity);
            write_senders.push(write_sender);

            let mut reader = LocalReader::new(
                dev.clone(),
                shutdown_sender.subscribe(),
                processor.clone(),
                flowstats_reporter.clone(),
            );

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
        if self.write_senders.is_empty() {
            debug!(
                "Local interface disabled; dropping packet with flow {}.",
                packet.flow_id
            );
            return;
        }

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
    fn mask_to_prefix(mask: Ipv4Addr) -> u8 {
        let mask_u32 = u32::from_be_bytes(mask.octets());
        mask_u32.count_ones() as u8
    }

    /// Creates local TUN devices for communicating with the application.
    #[cfg(not(target_os = "linux"))]
    pub fn create_tun_devices(config: LocalConfig) -> Vec<Arc<AsyncDevice>> {
        let ipv4_prefix = Self::mask_to_prefix(config.local_netmask);

        let dev = DeviceBuilder::new()
            .ipv4(config.local_address, ipv4_prefix, None)
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
        let ipv4_prefix = Self::mask_to_prefix(config.local_netmask);

        let dev = DeviceBuilder::new()
            .name(&if_name)
            .ipv4(config.local_address, ipv4_prefix, None)
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
                    error!(
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node::FlowId;
    use crate::node::FlowIdExt;
    use tokio::time::{Duration, timeout};

    fn flow_id_for_bucket(bucket: usize, capacity: usize) -> FlowId {
        for candidate in 0u128..50_000u128 {
            if FlowIdExt::hash(&candidate, capacity) == bucket {
                return candidate;
            }
        }
        panic!(
            "unable to find flow id for bucket {} within search space",
            bucket
        );
    }

    fn packet_with_flow_id(flow_id: FlowId) -> Packet {
        let mut packet = Packet::from_vec(Vec::new());
        packet.flow_id = flow_id;
        packet
    }

    #[tokio::test(flavor = "current_thread")]
    async fn mask_to_prefix_handles_common_netmasks() {
        assert_eq!(
            LocalInterfaceHandle::mask_to_prefix(Ipv4Addr::new(255, 255, 255, 0)),
            24
        );
        assert_eq!(
            LocalInterfaceHandle::mask_to_prefix(Ipv4Addr::new(255, 255, 0, 0)),
            16
        );
        assert_eq!(
            LocalInterfaceHandle::mask_to_prefix(Ipv4Addr::new(255, 255, 255, 192)),
            26
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn write_packet_routes_based_on_flow_hash() {
        let (shutdown_sender, _) = broadcast::channel(8);
        let (sender_a, mut receiver_a) = mpsc::channel(4);
        let (sender_b, mut receiver_b) = mpsc::channel(4);

        let handle = LocalInterfaceHandle {
            shutdown_sender,
            write_senders: vec![sender_a, sender_b],
        };

        let flow_id_a = flow_id_for_bucket(0, 2);
        let flow_id_b = flow_id_for_bucket(1, 2);

        handle.write_packet(packet_with_flow_id(flow_id_a));
        handle.write_packet(packet_with_flow_id(flow_id_b));

        let msg_a = receiver_a
            .try_recv()
            .expect("flow mapped to bucket 0 should reach first sender");
        let msg_b = receiver_b
            .try_recv()
            .expect("flow mapped to bucket 1 should reach second sender");

        let LocalInterfaceMessage::WritePacket(packet_a) = msg_a;
        assert_eq!(packet_a.flow_id, flow_id_a);

        let LocalInterfaceMessage::WritePacket(packet_b) = msg_b;
        assert_eq!(packet_b.flow_id, flow_id_b);

        assert!(
            receiver_a.try_recv().is_err() && receiver_b.try_recv().is_err(),
            "each sender should receive only one packet in this scenario"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn shutdown_broadcasts_signal_to_listeners() {
        let (shutdown_sender, _) = broadcast::channel(4);
        let handle = LocalInterfaceHandle {
            shutdown_sender: shutdown_sender.clone(),
            write_senders: Vec::new(),
        };
        let mut receiver = shutdown_sender.subscribe();

        handle.shutdown().await;

        let msg = timeout(Duration::from_millis(50), receiver.recv())
            .await
            .expect("shutdown signal should be delivered promptly")
            .expect("broadcast channel should remain open");
        assert!(matches!(msg, ShutdownMessage::Shutdown));
    }
}
