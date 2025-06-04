use std::sync::Arc;

use tokio::io::Result;
use tokio::net::UdpSocket;
use tokio::sync::mpsc;
use tracing::error;

use crate::node::RECEIVE_BUF_SIZE;
use crate::node::config::LocalConfig;
use crate::node::network_interface::NetworkInterfaceMessage;
use crate::node::packet::Packet;
use crate::node::processor::ProcessorHandle;

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
    pub async fn start_listening(&mut self, addr: &String) {
        let socket = Arc::new(
            UdpSocket::bind(addr)
                .await
                .unwrap_or_else(|_| panic!("Failed to bind the UDP socket.")),
        );

        self.socket = Some(socket.clone());
        self.config.udp_socket = Arc::new(Some(socket.clone()));

        loop {
            if let Ok(packet) = self.read_packet().await {
                self.processors.process_packet(packet).await;
            }
        }
    }

    /// Reads a packet from a UDP socket.
    async fn read_packet(&mut self) -> Result<Packet> {
        let mut buf = [0; RECEIVE_BUF_SIZE];
        let len = self.socket.as_ref().unwrap().recv(&mut buf[..]).await?;

        Ok(Packet::new(len, buf))
    }
}

pub struct UdpRelay {
    receiver: mpsc::Receiver<NetworkInterfaceMessage>,
    socket: Arc<UdpSocket>,
    remote_addr: String,
}

impl UdpRelay {
    pub fn new(
        config: LocalConfig,
        receiver: mpsc::Receiver<NetworkInterfaceMessage>,
        remote_addr: String,
    ) -> Self {
        let socket = config.udp_socket.as_ref().clone().unwrap();
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
        let _ = self
            .socket
            .send_to(&packet.buf[..packet.packet_size], self.remote_addr.as_str())
            .await?;

        Ok(())
    }
}
