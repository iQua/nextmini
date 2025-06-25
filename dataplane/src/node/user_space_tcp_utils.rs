use std::time::Instant as StdInstant;

use smoltcp::phy::{Device, DeviceCapabilities, Medium, RxToken, TxToken};
use smoltcp::time::Instant;

use tracing::info;

use crate::node::config::LocalConfig;
use crate::node::packet::Packet;
use crate::node::processor::ProcessorHandle;

#[derive(Debug, Clone)]
pub struct ConnectionState {
    pub connected: bool,
    pub start_time: StdInstant,
    pub time_last_updated: StdInstant,
    pub bytes_last_updated: u64,
    pub bytes_total: u64,
}

// Implements test_throughput for both client and server side.
impl ConnectionState {
    // node_type: node as client or server
    // bytes_added: bytes received or sent
    pub fn test_throughput(&mut self, node_type: &str, id: usize, bytes_added: u64) {
        if self.bytes_total == 0 {
            self.start_time = StdInstant::now();
            info!("{} {} started transferring data", node_type, id);
        }

        self.bytes_total += bytes_added;
        self.bytes_last_updated += bytes_added;

        let now = StdInstant::now();
        let elapsed_time = now.duration_since(self.time_last_updated).as_secs_f64();

        if elapsed_time > 1.0 {
            let throughput =
                (self.bytes_last_updated as f64 * 8.0) / (elapsed_time * 1_000_000_000.0);

            println!(
                "{} {} throughput: {:.3} Gbps ({} bytes in {:.3}s)",
                node_type, id, throughput, self.bytes_last_updated, elapsed_time
            );

            self.bytes_last_updated = 0;
            self.time_last_updated = now;
        }
    }
}

#[derive(Clone)]
pub struct VirtualDevice {
    pub config: LocalConfig,
    pub receiver: flume::Receiver<Packet>,
    pub sender: ProcessorHandle,
}

impl Device for VirtualDevice {
    type RxToken<'a> = PacketRxToken;
    type TxToken<'a> = PacketTxToken;

    fn receive(&mut self, _timestamp: Instant) -> Option<(Self::RxToken<'_>, Self::TxToken<'_>)> {
        self.receiver
            .try_recv()
            .ok()
            .map(|packet| (PacketRxToken(packet), PacketTxToken(self.sender.clone())))
    }

    fn transmit(&mut self, _timestamp: Instant) -> Option<Self::TxToken<'_>> {
        Some(PacketTxToken(self.sender.clone()))
    }

    fn capabilities(&self) -> DeviceCapabilities {
        let mut caps = DeviceCapabilities::default();
        caps.medium = Medium::Ip; // needs IP packet format
        caps.max_transmission_unit = self.config.mtu as usize;
        caps
    }
}

pub struct PacketRxToken(pub Packet);

impl RxToken for PacketRxToken {
    fn consume<R, F>(self, f: F) -> R
    where
        F: FnOnce(&[u8]) -> R,
    {
        f(&self.0.buf[0..self.0.packet_size])
    }
}

pub struct PacketTxToken(pub ProcessorHandle);

impl TxToken for PacketTxToken {
    fn consume<R, F>(self, len: usize, f: F) -> R
    where
        F: FnOnce(&mut [u8]) -> R,
    {
        let mut buf = vec![0; len];
        let result = f(&mut buf);
        let packet = Packet::new(len, buf);

        // uses non-blocking send() to send the outbound packet
        self.0.process_packet(packet);

        result
    }
}
