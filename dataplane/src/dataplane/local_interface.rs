use core::panic;
use std::net::Ipv4Addr;
use std::sync::Arc;

use tokio::sync::mpsc::Sender;
use tun_rs::{AsyncDevice, DeviceBuilder};

use crate::dataplane::RECEIVE_BUF_SIZE;
use crate::dataplane::configs::{ControllerConfigs, LocalConfigs};
use crate::dataplane::packet::Packet;
use crate::dataplane::processor::SenderLoadBalancer;

/// Converts a netmask tuple to prefix length. Used in 'create_tun_devices()'.
fn mask_to_prefix(mask: (u8, u8, u8, u8)) -> u8 {
    let mask_u32 = u32::from_be_bytes([mask.0, mask.1, mask.2, mask.3]);
    mask_u32.count_ones() as u8
}

/// Creates TUN devices for sending data to the application.
pub async fn create_tun_device(
    configs: LocalConfigs,
    controller_configs: ControllerConfigs,
) -> Vec<Arc<AsyncDevice>> {
    #[cfg(target_os = "linux")]
    {
        let num_queues = configs.num_packet_processors;

        let if_name = configs.tun_interface_name.clone();
        let ipv4_addr = controller_configs.strato_address;
        let ipv4_prefix = mask_to_prefix(controller_configs.strato_mask);

        let dev = DeviceBuilder::new()
            .name(&if_name)
            .ipv4(
                Ipv4Addr::new(ipv4_addr.0, ipv4_addr.1, ipv4_addr.2, ipv4_addr.3),
                ipv4_prefix,
                None,
            )
            .mtu(configs.mtu as u16)
            .multi_queue(true)
            .build_async()
            .expect("Failed to create tun device");

        let mut queues = Vec::with_capacity(num_queues);

        // creates multiple TUN queues with error handling
        eprintln!("Creating {num_queues} TUN queues.");
        for _ in 0..num_queues - 1 {
            match dev.try_clone() {
                Ok(cloned_dev) => {
                    queues.push(Arc::new(cloned_dev));
                }
                Err(e) => {
                    // if we are unable to create all the queues, use what we have
                    eprintln!(
                        "Warning: Could not create all TUN queues ({}), continuing with {} queues",
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
        let ipv4_addr = controller_configs.strato_address;
        let ipv4_prefix = mask_to_prefix(controller_configs.strato_mask);

        let dev = DeviceBuilder::new()
            .ipv4(
                Ipv4Addr::new(ipv4_addr.0, ipv4_addr.1, ipv4_addr.2, ipv4_addr.3),
                ipv4_prefix,
                None,
            )
            .mtu(configs.mtu as u16)
            .build_async()
            .expect("Failed to create tun device");

        // creates a single TUN queue on non-Linux platforms without multi-queue support
        eprintln!("Creating one TUN queue on non-Linux platforms without multi-queue support.");
        let queues = vec![Arc::new(dev)];

        queues
    }
}

/// Reads packets asynchronously from a TUN device in a Tokio task, and sends them out
/// via a SenderLoadBalancer.
pub struct TunReader {
    dev: Arc<AsyncDevice>, // a shared reference to the device that can be cloned
    senders: SenderLoadBalancer,
}

impl TunReader {
    pub fn new(dev: Arc<AsyncDevice>, senders_to_proc: Vec<Sender<Packet>>) -> TunReader {
        TunReader {
            dev,
            senders: SenderLoadBalancer::new(senders_to_proc),
        }
    }

    pub async fn start_reading(&mut self) {
        let mut buf = [0; RECEIVE_BUF_SIZE];

        loop {
            let n = match self.dev.recv(&mut buf).await {
                Ok(n) => n,
                Err(e) => {
                    panic!("Error reading from the TUN device: {:?}", e);
                }
            };

            let packet = Packet::new(n, buf);
            self.senders.try_send(packet);
        }
    }
}

/// Writes one packet to a TUN device using either the stream or interface multi-path method.
#[derive(Clone)]
pub struct TunWriter {
    dev: Arc<AsyncDevice>,
}

impl TunWriter {
    pub fn new(dev: Arc<AsyncDevice>) -> TunWriter {
        TunWriter { dev }
    }

    pub async fn write_packet(&self, packet: Packet) {
        let buf = &packet.buf[0..packet.packet_size];

        self.dev
            .send(buf)
            .await
            .expect("Failed to write to TUN device");
    }
}
