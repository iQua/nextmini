use std::sync::Arc;

use tokio::io::Result;
use tokio::net::UdpSocket;

use crate::node::RECEIVE_BUF_SIZE;
use crate::node::config::LocalConfig;
use crate::node::packet::Packet;
use crate::node::processor::ProcessorHandle;

pub struct UdpServer {
    socket: Arc<UdpSocket>,
    processors: ProcessorHandle,
}

impl UdpServer {
    pub fn new(config: LocalConfig, processors: ProcessorHandle) -> Self {
        let socket = config.udp_socket.clone().unwrap();

        Self { socket, processors }
    }

    /// starts listening for incoming packets
    pub async fn start_listening(&mut self, _addr: &str) {
        loop {
            if let Ok(packet) = self.read_packet().await {
                self.processors.process_packet(packet);
            }
        }
    }

    /// Reads a packet from a UDP socket.
    async fn read_packet(&mut self) -> Result<Packet> {
        let mut buf = vec![0; RECEIVE_BUF_SIZE];
        let len = self.socket.recv(&mut buf).await?;

        Ok(Packet::new(len, buf))
    }
}

pub struct UdpWriter {
    socket: Arc<UdpSocket>,
    remote_addr: String,
}

impl UdpWriter {
    pub fn new(config: LocalConfig, remote_addr: String) -> Self {
        let socket = config.udp_socket.clone().unwrap();

        Self {
            socket,
            remote_addr,
        }
    }

    /// Writes a packet to the UDP socket.
    pub async fn write_packet(&self, packet: &Packet) -> Result<()> {
        // UdpSocket.send_to() returns the number of bytes sent
        self.socket
            .send_to(&packet.buf[0..packet.packet_size], &self.remote_addr)
            .await?;

        Ok(())
    }

    /// Writes a batch of packets to the UDP socket.
    pub async fn write_packets(&self, packets: Vec<Packet>) -> Result<()> {
        for packet in packets {
            self.socket
                .send_to(&packet.buf[0..packet.packet_size], &self.remote_addr)
                .await?;
        }
        Ok(())
    }
}
