/// Implements a virtual device for SmolTcp to send and receive packets through the rest of Nextmini.
/// Packets are received using Flume mpmc channels, one for each local user-space destination, from the
/// processors, and sent via a sequential or concurrent processor handle.
use smoltcp::phy::{Device, DeviceCapabilities, Medium, RxToken, TxToken};
use smoltcp::time::Instant;
use tokio::sync::mpsc;

use crate::node::config::LocalConfig;
use crate::node::packet::Packet;
use crate::node::processor::ProcessorHandle;

pub struct VirtualDevice {
    pub config: LocalConfig,
    pub receiver: mpsc::Receiver<Packet>,
    pub sender: ProcessorHandle,
}

impl Device for VirtualDevice {
    type RxToken<'a> = PacketRxToken;
    type TxToken<'a> = PacketTxToken;

    fn receive(&mut self, _timestamp: Instant) -> Option<(Self::RxToken<'_>, Self::TxToken<'_>)> {
        match self.receiver.try_recv() {
            Ok(packet) => Some((PacketRxToken(packet), PacketTxToken(self.sender.clone()))),
            Err(mpsc::error::TryRecvError::Empty) => None,
            Err(mpsc::error::TryRecvError::Disconnected) => None,
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

#[cfg(test)]
mod tests {
    use super::*;
    use byteorder::{BigEndian, ByteOrder};
    use smoltcp::phy::Device;
    use std::net::Ipv4Addr;
    use std::time::Duration;

    use crate::node::NodeIdExt;

    fn make_test_config() -> LocalConfig {
        let mut config = LocalConfig::default();
        config.node_id = 1;
        config.num_packet_processors = 1;
        config.channel_capacity = 16;
        config
    }

    fn build_virtual_device(config: LocalConfig) -> (VirtualDevice, mpsc::Sender<Packet>) {
        let (sender, receiver) = mpsc::channel(config.channel_capacity);
        let processors = ProcessorHandle::new(config.clone());

        (
            VirtualDevice {
                config,
                receiver,
                sender: processors,
            },
            sender,
        )
    }

    fn make_test_packet(
        src_ip: Ipv4Addr,
        dst_ip: Ipv4Addr,
        src_port: u16,
        dst_port: u16,
    ) -> Packet {
        let ip_header_len = 20;
        let tcp_header_len = 20;
        let payload_len = 0;
        let total_len = ip_header_len + tcp_header_len + payload_len;

        let mut buf = vec![0u8; total_len];
        buf[0] = 0x45; // IPv4, 5 * 4 byte header
        buf[1] = 0;
        BigEndian::write_u16(&mut buf[2..4], total_len as u16);
        buf[8] = 64;
        buf[9] = 6; // TCP
        buf[12..16].copy_from_slice(&src_ip.octets());
        buf[16..20].copy_from_slice(&dst_ip.octets());
        BigEndian::write_u16(&mut buf[20..22], src_port);
        BigEndian::write_u16(&mut buf[22..24], dst_port);

        // TCP data offset (header length = 5 * 4 = 20 bytes)
        buf[32] = 0x50;

        Packet::new(total_len, buf)
    }

    #[tokio::test(flavor = "current_thread")]
    async fn receive_returns_none_when_channel_empty() {
        let config = make_test_config();
        let (mut device, _) = build_virtual_device(config);

        assert!(
            device.receive(Instant::now()).is_none(),
            "receive should return None when no packets are buffered"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn receive_returns_tokens_with_packet() {
        let config = make_test_config();
        let (mut device, sender) = build_virtual_device(config);
        let packet = make_test_packet(
            Ipv4Addr::new(10, 0, 0, 1),
            Ipv4Addr::new(10, 0, 0, 2),
            4001,
            5000,
        );
        let expected = packet.buf[0..packet.packet_size].to_vec();
        sender
            .try_send(packet)
            .expect("packet should be enqueued successfully");

        let (rx_token, _tx_token) = device
            .receive(Instant::now())
            .expect("receive should yield packet tokens");

        let mut received = Vec::new();
        rx_token.consume(|buf| {
            received.extend_from_slice(buf);
        });

        assert_eq!(received, expected);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn transmit_executes_closure_and_returns_result() {
        let config = make_test_config();
        let dst_ip = config
            .node_id
            .ip_addr(config.user_space_base_addr, config.local_netmask);
        let src_ip =
            (config.node_id + 1).ip_addr(config.user_space_base_addr, config.local_netmask);
        let dst_port = config.user_space_server_port;
        let src_port = 4100;
        let (mut device, _) = build_virtual_device(config);

        // ensure we can send through the virtual device
        let tx_token = device
            .transmit(Instant::now())
            .expect("transmit should always yield a token");
        let result = tx_token.consume(40, |buf| {
            assert_eq!(buf.len(), 40);
            buf.fill(0);
            buf[0] = 0x45;
            BigEndian::write_u16(&mut buf[2..4], 40);
            buf[8] = 64;
            buf[9] = 6;
            buf[12..16].copy_from_slice(&src_ip.octets());
            buf[16..20].copy_from_slice(&dst_ip.octets());
            BigEndian::write_u16(&mut buf[20..22], src_port);
            BigEndian::write_u16(&mut buf[22..24], dst_port);
            buf[32] = 0x50;
            123usize
        });

        assert_eq!(result, 123);

        // allow processor to consume the generated packet
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn capabilities_match_configured_mtu() {
        let mut config = make_test_config();
        config.mtu = 2048;
        let (device, _) = build_virtual_device(config.clone());

        let caps = device.capabilities();
        assert_eq!(caps.max_transmission_unit, 2048);
        assert_eq!(caps.medium, Medium::Ip);
    }
}
