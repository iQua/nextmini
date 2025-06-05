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
    config: LocalConfig,
    processors: ProcessorHandle,
    socket: Option<Arc<UdpSocket>>,
}

impl UdpServer {
    pub fn new(config: LocalConfig, processors: ProcessorHandle) -> Self {
        Self {
            config,
            processors,
            socket: None,
        }
    }

    /// Binds the UDP socket, updates the configuration, and starts listening for packets.
    pub async fn start_listening(&mut self, addr: &str) {
        if let Ok(mut guard) = self.config.udp_socket.try_write() {
            if guard.is_none() {
                let socket = Arc::new(
                    UdpSocket::bind(addr)
                        .await
                        .unwrap_or_else(|_| panic!("Failed to bind the UDP socket")),
                );
                *guard = Some(socket.clone());
            }
        }

        self.socket = self.config.udp_socket.read().await.clone();

        loop {
            if let Ok(packet) = self.read_packet().await {
                self.processors.process_packet(packet).await;
            }
        }
    }

    /// Reads a packet from a UDP socket.
    async fn read_packet(&mut self) -> Result<Packet> {
        let mut buf = [0; RECEIVE_BUF_SIZE];
        let len = self.socket.as_ref().unwrap().recv(&mut buf).await?;

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
        if let Ok(mut guard) = config.udp_socket.try_write() {
            if guard.is_none() {
                let addr = format!("{}:{}", "0.0.0.0", config.public_network_port);
                let socket = Arc::new(
                    UdpSocket::bind(addr)
                        .await
                        .unwrap_or_else(|_| panic!("Failed to bind the UDP socket")),
                );
                *guard = Some(socket.clone());
            }
        }
        let guard = config.udp_socket.read().await;
        let socket = guard.as_ref().unwrap().clone();
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
