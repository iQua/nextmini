/// Implements a virtual device for SmolTcp to send and receive packets through the rest of Nextmini.
/// Packets are received using Tokio mpsc channels, one for each local user-space destination, from the
/// processors, and sent via a sequential or concurrent processor handle.
use smoltcp::phy::{Device, DeviceCapabilities, Medium, RxToken, TxToken};
use smoltcp::time::Instant;
use tokio::sync::mpsc;
use flume;

use crate::node::config::LocalConfig;
use crate::node::packet::Packet;
use crate::node::processor::ProcessorHandle;

pub enum Receiver {
    Mpsc(mpsc::Receiver<Packet>),
    Flume(flume::Receiver<Packet>),
}

pub struct VirtualDevice {
    pub config: LocalConfig,
    pub receiver: Receiver,
    pub sender: ProcessorHandle,
}

impl Device for VirtualDevice {
    type RxToken<'a> = PacketRxToken;
    type TxToken<'a> = PacketTxToken;

    fn receive(&mut self, _timestamp: Instant) -> Option<(Self::RxToken<'_>, Self::TxToken<'_>)> {
        let result = match &mut self.receiver {
            Receiver::Mpsc(r) => r.try_recv().map_err(|e| e.to_string()),
            Receiver::Flume(r) => r.try_recv().map_err(|e| e.to_string()),
        };

        match result {
            Ok(packet) => Some((PacketRxToken(packet), PacketTxToken(self.sender.clone()))),
            Err(_) => None,
        }
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
