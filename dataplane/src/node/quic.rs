use crate::node::config::CongestionControl;
use crate::node::config::LocalConfig;
use crate::node::processor::ProcessorHandle;
use crate::node::packet::Packet;
use crate::node::RECEIVE_BUF_SIZE;
use crate::node::network_interface::NetworkInterfaceHandle;
use crate::node::scheduler::SchedulerHandle;

use s2n_quic::Server;
use s2n_quic::provider::congestion_controller;
use s2n_quic::stream::{ReceiveStream, SendStream};
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use tokio::sync::{Mutex, mpsc};
use crate::node::network_interface::NetworkInterfaceMessage;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tracing::{info, error};

pub struct QuicServer {
    config: LocalConfig,
    processor_handle: ProcessorHandle,
}

impl QuicServer {
    pub fn new(config: LocalConfig, processor_handle: ProcessorHandle) -> Self {
        Self { config, processor_handle }
    }

    pub async fn start_listening(&mut self, addr: &str) {
        let server_addr: SocketAddr = addr.parse().unwrap();

        let mut server = match self.config.quic_congestion_control {
            CongestionControl::Cubic => Server::builder()
                .with_tls((Path::new("server_cert.pem"), Path::new("server_key.pem")))
                .expect("Failed to set TLS config")
                .with_congestion_controller(congestion_controller::Cubic::default())
                .expect("Failed to set congestion controller")
                .with_io(server_addr)
                .expect("Failed to bind to address")
                .start()
                .expect("Failed to start server"),
            CongestionControl::Bbr => Server::builder()
                .with_tls((Path::new("server_cert.pem"), Path::new("server_key.pem")))
                .expect("Failed to set TLS config")
                .with_congestion_controller(congestion_controller::Bbr::default())
                .expect("Failed to set congestion controller")
                .with_io(server_addr)
                .expect("Failed to bind to address")
                .start()
                .expect("Failed to start server"),
        };

        while let Some(mut connection) = server.accept().await {
            let processor_handle = self.processor_handle.clone();
            let config = self.config.clone();

            tokio::spawn(async move {
                info!("Connection accepted from {:?}.", connection.remote_addr());

                if let Ok(Some(mut stream)) = connection.accept_bidirectional_stream().await {
                    let mut node_id_buf: [u8; 8] = [0; 8];

                    if let Err(e) = stream.read_exact(&mut node_id_buf).await {
                        info!("Failed to read node ID: {}", e);
                        connection.close(0u32.into());
                        return;
                    }

                    let node_id = u64::from_be_bytes(node_id_buf) as usize;
                    info!("Incoming connection from node {}...", node_id);

                    // Consider redesign network interface to avoid creation here
                    let (receive_stream, send_stream) = stream.split();
                    let (sender, receiver) = mpsc::channel::<NetworkInterfaceMessage>(100);
                    let mut quic_reader = QuicReader::new(processor_handle.clone(), receive_stream);
                    let mut quic_writer = QuicWriter::new(Arc::new(Mutex::new(send_stream)), receiver);
                    tokio::spawn(async move {
                        quic_reader.run().await;
                    });
                    tokio::spawn(async move {
                        quic_writer.run().await;
                    });
                    let network_interface = NetworkInterfaceHandle { sender };
                    let scheduler = SchedulerHandle::new(config.clone(), node_id, network_interface);
                    processor_handle.add_node(node_id, scheduler).await;    

                    info!("Connected.");
                } else {
                    connection.close(0u32.into());
                }
            });
        }
    }
}

pub struct QuicReader {
    processor_handle: ProcessorHandle,
    stream: ReceiveStream,
}

impl QuicReader {
    pub fn new(processor_handle: ProcessorHandle, stream: ReceiveStream) -> Self {
        Self { processor_handle, stream }
    }

    pub async fn run(&mut self) {
        loop {
            let packet = self.read().await;
            match packet {
                Ok(packet) => {
                    self.processor_handle.process_packet(packet);
                }
                Err(e) => {
                    error!("Failed to read packet: {}", e);
                }
            }
        }
    }
    pub async fn read(&mut self) -> Result<Packet, std::io::Error> {
        let mut buf = [0u8; RECEIVE_BUF_SIZE];

        match self.stream.read_exact(&mut buf[0..4]).await {
            Ok(_) => (),
            Err(_) => {
                return Err(std::io::Error::new(std::io::ErrorKind::Other, "Failed to read packet length"));
            }
        }

        let msg_len = buf[2] as usize * 256 + buf[3] as usize;

        match self.stream.read_exact(&mut buf[4..msg_len]).await {
            Ok(_) => (),
            Err(_) => {
                return Err(std::io::Error::new(std::io::ErrorKind::Other, "Failed to read packet"));
            }
        }

        Ok(Packet::new(msg_len, buf))
    }
}

pub struct QuicWriter {
    stream: Arc<Mutex<SendStream>>,
    receiver: mpsc::Receiver<NetworkInterfaceMessage>,
}

impl QuicWriter {
    pub fn new(stream: Arc<Mutex<SendStream>>, receiver: mpsc::Receiver<NetworkInterfaceMessage>) -> Self {
        Self {
            stream: stream.clone(),
            receiver,
        }
    }

    pub async fn run(&mut self) {
        while let Some(msg) = self.receiver.recv().await {
            match msg {
                NetworkInterfaceMessage::SendPacket(packet) => {
                    let result = self.write_packet(&packet).await;
                    if let Err(e) = result {
                        error!("Failed to write packet: {}", e);
                    }
                }
                NetworkInterfaceMessage::Shutdown => {
                    break;
                }   
            }
        }
    }

    pub async fn write_packet(&mut self, packet: &Packet) -> Result<(), std::io::Error> {
        let mut stream_guard = self.stream.lock().await;

        match stream_guard.write_all(&packet.buf).await {
            Ok(_) => Ok(()),
            Err(e) => Err(e),
        }
    }
}
