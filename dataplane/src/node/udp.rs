use std::sync::Arc;

use crate::node::RECEIVE_BUF_SIZE;
use crate::node::config::LocalConfig;
use crate::node::network_interface::NetworkInterfaceMessage;
use crate::node::packet::Packet;
use crate::node::processor::ProcessorHandle;
use tokio::io::Result;
use tokio::net::UdpSocket;
use tokio::sync::mpsc;
use tracing::error;

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
                self.processors.process_packet(packet).await;
            }
        }
    }

    /// Reads a packet from a UDP socket.
    async fn read_packet(&mut self) -> Result<Packet> {
        let mut buf = [0; RECEIVE_BUF_SIZE];
        let len = self.socket.recv(&mut buf).await?;

        Ok(Packet::new(len, buf))
    }
}

pub struct UdpRelay {
    receiver: mpsc::Receiver<NetworkInterfaceMessage>,
    socket: Arc<UdpSocket>,
    remote_addr: String,
}

impl UdpRelay {
    pub async fn new(
        config: LocalConfig,
        receiver: mpsc::Receiver<NetworkInterfaceMessage>,
        remote_addr: String,
    ) -> Self {
        let socket = config.udp_socket.clone().unwrap();

        Self {
            receiver,
            socket,
            remote_addr,
        }
    }

    pub async fn run(mut self) {
        while let Some(msg) = self.receiver.recv().await {
            match msg {
                NetworkInterfaceMessage::SendPacket(packet) => {
                    if let Err(e) = self.write_packet(&packet).await {
                        error!("Failed to send a packet via UDP: {}", e);
                    }
                }
                NetworkInterfaceMessage::Shutdown => break,
            }
        }
    }

    /// Writes a packet to the UDP socket.
    async fn write_packet(&self, packet: &Packet) -> Result<()> {
        // UdpSocket.send_to() returns the number of bytes sent
        self.socket
            .send_to(&packet.buf[0..packet.packet_size], &self.remote_addr)
            .await?;

        Ok(())
    }
}
