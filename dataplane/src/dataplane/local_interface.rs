use core::panic;
use std::net::Ipv4Addr;
use std::sync::Arc;
use std::time::Instant;

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

    let dev = dev_builder
        .build_async()
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

        if cfg!(debug_assertions) {
            debug!("[PERF] TunReader started");
        }

        loop {
            let read_start = if cfg!(debug_assertions) {
                Some(Instant::now())
            } else {
                None
            };

            let n = match self.dev.recv(&mut buf).await {
                Ok(n) => {
                    if cfg!(debug_assertions) {
                        if let Some(start) = read_start {
                            let read_duration = start.elapsed();
                            if read_duration.as_micros() > 100 {
                                debug!(
                                    "[PERF] TunReader recv() took {}μs to read {} bytes",
                                    read_duration.as_micros(),
                                    n
                                );
                            }
                        }
                        debug!("TunReader: Successfully read {} bytes from TUN device", n);
                    }
                    n
                }
                Err(e) => {
                    panic!("Error reading from the TUN device: {:?}", e);
                }
            };

            // Skip empty or invalid packets to prevent downstream errors
            if n == 0 {
                debug!("TunReader: Received empty packet from TUN device, skipping");
                continue;
            }

            let packet_create_start = if cfg!(debug_assertions) {
                Some(Instant::now())
            } else {
                None
            };
            let packet = Packet::new(n, buf);
            debug!("TunReader: Received packet of {:?} bytes", packet.buf.len());

            if cfg!(debug_assertions) {
                if let Some(start) = packet_create_start {
                    let create_duration = start.elapsed();
                    if create_duration.as_micros() > 10 {
                        debug!(
                            "[PERF] Packet::new() took {}μs for {} bytes",
                            create_duration.as_micros(),
                            n
                        );
                    }
                }
                // packet received from the TUN device is already IPv6
                // .len() can be removed to see the entire packet
                debug!(
                    "TunReader: Created packet with flow_id: {}, size: {} bytes, first 16 bytes: {:?}",
                    packet.flow_id,
                    packet.packet_size,
                    &packet.buf[0..std::cmp::min(16, packet.packet_size)]
                );
            }

            // Always try to set stream ID as Stream mode is default
            // packet.try_set_stream_id(); // Commented out since method is not available

            let send_start = if cfg!(debug_assertions) {
                Some(Instant::now())
            } else {
                None
            };
            self.senders.try_send(packet);

            if cfg!(debug_assertions) {
                if let Some(start) = send_start {
                    let send_duration = start.elapsed();
                    if send_duration.as_micros() > 50 {
                        debug!(
                            "[PERF] LoadBalancer.try_send() took {}μs",
                            send_duration.as_micros()
                        );
                    }
                }
            }
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
        let validation_start = if cfg!(debug_assertions) {
            Some(Instant::now())
        } else {
            None
        };
        let buf = &packet.buf[0..packet.packet_size];

        // Skip empty or invalid packets
        if buf.len() < 20 {
            if cfg!(debug_assertions) {
                debug!(
                    "TunWriter: Skipping packet with insufficient size: {} bytes (need at least 20 for IP header)",
                    buf.len()
                );
            }
            return;
        }

        // Validate this is an IPv4 packet
        if buf[0] >> 4 != 4 {
            if cfg!(debug_assertions) {
                debug!(
                    "TunWriter: Skipping non-IPv4 packet (version={})",
                    buf[0] >> 4
                );
            }
            return;
        }

        // Validate packet structure
        let ihl = (buf[0] & 0x0F) as usize;
        let header_length = ihl * 4;
        if header_length > buf.len() || header_length < 20 {
            if cfg!(debug_assertions) {
                debug!(
                    "TunWriter: Invalid IP header length: IHL={}, header_len={}, packet_len={}",
                    ihl,
                    header_length,
                    buf.len()
                );
            }
            return;
        }

        if cfg!(debug_assertions) {
            if let Some(start) = validation_start {
                let validation_duration = start.elapsed();
                if validation_duration.as_micros() > 10 {
                    debug!(
                        "[PERF] TunWriter validation took {}μs",
                        validation_duration.as_micros()
                    );
                }
            }
            // Send the original packet without modification
            // Note: Removed the problematic modification of buf[14] which was corrupting destination IP
            debug!(
                "TunWriter: Sending packet of {} bytes to TUN device",
                buf.len()
            );
        }

        let write_start = if cfg!(debug_assertions) {
            Some(Instant::now())
        } else {
            None
        };
        self.dev
            .send(buf)
            .await
            .expect("Failed to write to TUN device");

        if cfg!(debug_assertions) {
            if let Some(start) = write_start {
                let write_duration = start.elapsed();
                if write_duration.as_micros() > 100 {
                    debug!(
                        "[PERF] TUN device send() took {}μs for {} bytes",
                        write_duration.as_micros(),
                        buf.len()
                    );
                }
            }
        }
    }
}
