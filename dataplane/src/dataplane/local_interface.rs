use core::panic;
use std::net::Ipv4Addr;
use std::sync::Arc;

use tokio::sync::mpsc::Sender;
use tun_rs::{AsyncDevice, DeviceBuilder};

use tracing::debug;

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
) -> Arc<AsyncDevice> {
    let if_name = configs.tun_interface_name.clone();
    let ipv4_addr = controller_configs.strato_address;
    let ipv4_prefix = mask_to_prefix(controller_configs.strato_mask);

    #[cfg(target_os = "linux")]
    let dev_builder = DeviceBuilder::new()
        .name(&if_name)
        .ipv4(
            Ipv4Addr::new(ipv4_addr.0, ipv4_addr.1, ipv4_addr.2, ipv4_addr.3),
            ipv4_prefix,
            None,
        )
        .mtu(configs.mtu as u16)
        .multi_queue(false);

    #[cfg(not(target_os = "linux"))]
    let dev_builder = DeviceBuilder::new()
        .ipv4(
            Ipv4Addr::new(ipv4_addr.0, ipv4_addr.1, ipv4_addr.2, ipv4_addr.3),
            ipv4_prefix,
            None,
        )
        .mtu(configs.mtu as u16);

    let dev = dev_builder.build_async()
        .expect("Failed to create tun device");
    
    eprintln!("Successfully created TUN device {if_name}.");

    Arc::new(dev)
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

            // packet received from the TUN device is already IPv6
            // .len() can be removed to see the entire packet
            debug!("TunReader: Received packet of {:?} bytes", packet.buf.len());
            
            // Always try to set stream ID as Stream mode is default
            // packet.try_set_stream_id(); // Commented out since method is not available

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

        // Defaulting to Stream mode behavior
        let mut modified_buf = buf.to_vec();

        // resets prior modifications to the IP address
        modified_buf[14] = 0;

        self.dev
            .send(&modified_buf)
            .await
            .expect("Failed to write to TUN device");
    }
}
