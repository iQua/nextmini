use core::panic;
use std::net::Ipv4Addr;
use std::sync::Arc;

use tokio::sync::mpsc::Sender;
use tun_rs::{AsyncDevice, DeviceBuilder};

use nextmini_messages::MultiPathMethod;

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
pub async fn create_tun_devices(
    configs: LocalConfigs,
    controller_configs: ControllerConfigs,
) -> Vec<Vec<Arc<AsyncDevice>>> {
    let num_queues = configs.num_packet_processors;
    let num_interfaces = controller_configs.num_interfaces;
    let mut queues_per_interface = Vec::with_capacity(num_interfaces);

    #[cfg(target_os = "linux")]
    for i in 0..num_interfaces {
        let mut if_name = configs.tun_interface_name.clone();
        if_name.push_str(i.to_string().as_str());
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

        queues_per_interface.push(queues);
    }

    #[cfg(not(target_os = "linux"))]
    for _ in 0..num_interfaces {
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
        queues_per_interface.push(queues);
    }

    // transposes to a vector of queues, each queue corresponds to multiple interfaces
    let mut queues_by_queue_id = vec![Vec::with_capacity(num_interfaces); num_queues];

    for interface_queues in queues_per_interface {
        for (queue_id, dev) in interface_queues.into_iter().enumerate() {
            queues_by_queue_id[queue_id].push(dev);
        }
    }

    queues_by_queue_id
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

    pub async fn start_reading(&mut self, method: MultiPathMethod) {
        let mut buf = [0; RECEIVE_BUF_SIZE];

        loop {
            let n = match self.dev.recv(&mut buf).await {
                Ok(n) => n,
                Err(e) => {
                    panic!("Error reading from the TUN device: {:?}", e);
                }
            };

            let mut packet = Packet::new(n, buf);

            if method == MultiPathMethod::Stream {
                packet.try_set_stream_id();
            }

            self.senders.try_send(packet);
        }
    }
}

/// Writes one packet to a TUN device using either the stream or interface multi-path method.
#[derive(Clone)]
pub struct TunWriter {
    dev: Arc<AsyncDevice>,
    method: MultiPathMethod,
}

impl TunWriter {
    pub fn new(dev: Arc<AsyncDevice>, method: MultiPathMethod) -> TunWriter {
        TunWriter { dev, method }
    }

    pub async fn write_packet(&self, packet: Packet) {
        let buf = &packet.buf[0..packet.packet_size];

        match self.method {
            MultiPathMethod::Stream => {
                let mut modified_buf = buf.to_vec();

                // resets prior modifications to the IP address
                modified_buf[14] = 0;

                self.dev
                    .send(&modified_buf)
                    .await
                    .expect("Failed to write to TUN device");
            }
            MultiPathMethod::Interface => {
                self.dev
                    .send(buf)
                    .await
                    .expect("Failed to write to TUN device");
            }
        };
    }
}
