use tokio::net::UdpSocket;
use tokio::sync::mpsc;
use std::sync::Arc;
use tracing::error;

use crate::node::config::LocalConfig;
use crate::node::processor::ProcessorHandle;
use crate::node::packet::Packet;
use crate::node::network_interface::NetworkInterfaceMessage;
use crate::node::RECEIVE_BUF_SIZE;
pub struct UdpReader {
    config: LocalConfig,
    processors: ProcessorHandle,
    socket: Option<Arc<UdpSocket>>,
}

impl UdpReader {
    pub fn new(config: LocalConfig, processors: ProcessorHandle) -> Self {
        Self { config, processors, socket: None }
    }

    /// Binds the UDP socket, updates the config, and starts listening for packets on the background
    pub async fn start_listening(&mut self, addr: &String) {
        let socket = Arc::new(UdpSocket::bind(addr)
            .await
            .unwrap_or_else(|_| panic!("Failed to bind UDP socket.")));
        self.socket = Some(socket.clone());
        self.config.udp_socket = Arc::new(Some(socket.clone()));
        loop {
            if let Ok(packet) = self.read_packet().await {
                self.processors.process_packet(packet).await;
            }
        }
    }

    pub async fn read_packet(&mut self) ->Result<Packet, std::io::Error> {
        let mut buf = [0; RECEIVE_BUF_SIZE];
        match self.socket.as_ref().unwrap().recv(&mut buf[..]).await {
            Ok(msg_len  ) => Ok(Packet::new( msg_len, buf)),
            Err(e) => panic!("{e}"),
        }
    }
}

pub struct UdpWriter {
    receiver: mpsc::Receiver<NetworkInterfaceMessage>,
    socket: Arc<UdpSocket>,
    addr: String,
}


impl UdpWriter {
    pub fn new(config: LocalConfig, receiver: mpsc::Receiver<NetworkInterfaceMessage>, addr: String) -> Self {
        let socket = config
            .udp_socket
            .as_ref()
            .clone()
            .unwrap();
        Self { receiver, socket: socket, addr: addr.clone()}
    }

    pub async fn run(mut self) {
        while let Some(msg) = self.receiver.recv().await {
            match msg {
                NetworkInterfaceMessage::SendPacket(packet) => {
                    let result = self.write_packet(&packet).await;
                    if let Err(e) = result {
                        error!("Failed to write packet: {}", e);
                    }
                }
                NetworkInterfaceMessage::Shutdown => break,
            }
        }
    }
    pub async fn write_packet(&self, packet: &Packet) -> Result<(), std::io::Error> {
        self.socket
            .send_to(&packet.buf[..packet.packet_size], self.addr.as_str())
            .await?;
        Ok(())
    }
}