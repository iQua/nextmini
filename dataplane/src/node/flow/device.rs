/// Implements a virtual device for SmolTcp to send and receive packets through the rest of Nextmini.
/// Packets are received using Flume mpmc channels, one for each local user-space destination, from the
/// processors, and sent via a sequential or concurrent processor handle.
use smoltcp::phy::{Device, DeviceCapabilities, Medium, RxToken, TxToken};
use smoltcp::time::Instant;
use tokio::sync::mpsc;

use nextmini_messages::OperatingMode;

use crate::node::config::LocalConfig;
use crate::node::packet::Packet;
use crate::node::processor::ProcessorHandle;
use crate::node::splice::connector::ConnectorHandle;

pub struct VirtualDevice {
    pub config: LocalConfig,
    pub receiver: mpsc::Receiver<Packet>,
    pub processors: ProcessorHandle,
    pub connector: ConnectorHandle,
}

impl Device for VirtualDevice {
    type RxToken<'a> = PacketRxToken;
    type TxToken<'a> = PacketTxToken;

    fn receive(&mut self, _timestamp: Instant) -> Option<(Self::RxToken<'_>, Self::TxToken<'_>)> {
        match self.receiver.try_recv() {
            Ok(packet) => Some((
                PacketRxToken(packet),
                PacketTxToken {
                    processors: self.processors.clone(),
                    connector: self.connector.clone(),
                    config: self.config.clone(),
                },
            )),
            Err(mpsc::error::TryRecvError::Empty) => None,
            Err(mpsc::error::TryRecvError::Disconnected) => None,
        }
    }

    fn transmit(&mut self, _timestamp: Instant) -> Option<Self::TxToken<'_>> {
        Some(PacketTxToken {
            processors: self.processors.clone(),
            connector: self.connector.clone(),
            config: self.config.clone(),
        })
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

pub struct PacketTxToken {
    pub processors: ProcessorHandle,
    pub connector: ConnectorHandle,
    pub config: LocalConfig,
}

impl TxToken for PacketTxToken {
    fn consume<R, F>(self, len: usize, f: F) -> R
    where
        F: FnOnce(&mut [u8]) -> R,
    {
        let mut buf = vec![0; len];
        let result = f(&mut buf);
        let packet = Packet::new(len, buf);

        // decides which processor to use based on the node's operating mode
        match self.config.operating_mode {
            OperatingMode::Normal => {
                self.processors.process_packet(packet);
            }
            OperatingMode::Max => {
                self.connector.process_packet(packet);
            }
        }

        result
    }
}
