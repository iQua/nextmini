use std::io::{self, Cursor, Error, ErrorKind};
use std::net::IpAddr;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tracing::{error, info};

use crate::node::config::LocalConfig;
use crate::node::controller::reporter::ControllerReporterHandle;
use crate::node::network::interface::{NetworkInterfaceHandle, NetworkStream};
use crate::node::processor::ProcessorHandle;
use crate::node::scheduler::scheduler::SchedulerHandle;
use crate::node::{FlowId, NodeId};

pub struct TcpMaxServer {
    #[allow(dead_code)]
    config: LocalConfig,
    processors: ProcessorHandle,
}

impl TcpMaxServer {
    pub fn new(config: LocalConfig, processors: ProcessorHandle) -> Self {
        Self { config, processors }
    }

    /// accepts incoming tcp connections.
    pub async fn start_listening(&mut self, addr: &String) {
        let listener = match TcpListener::bind(addr).await {
            Ok(listener) => listener,
            Err(e) => {
                error!("Failed to bind to address {}: {}", addr, e);
                return;
            }
        };

        let mut first_byte = [0u8; 1];

        loop {
            let (mut stream, socket_addr) = match listener.accept().await {
                Ok((stream, socket_addr)) => {
                    info!("Connection accepted from {:?}.", socket_addr);
                    (stream, socket_addr)
                }
                Err(_) => {
                    error!("Failed to accept TCP connection");
                    continue;
                }
            };

            // reads the first byte to determine the protocol.
            if let Err(e) = stream.read_exact(&mut first_byte).await {
                error!("Failed to read first byte: {}", e);
                continue;
            }

            // handles the connection based on protocol
            let flow_id = match first_byte[0] {
                0x05 => self.handle_socks5_request(&mut stream).await,
                0x06 => self.handle_tcp_max_request(&mut stream).await,
                _ => {
                    error!("Unsupported protocol");
                    continue;
                }
            };

            // gets flow_id from socks5 or tcp max request
            match flow_id {
                Ok(flow_id) => {
                    // Tell the processor to splice the upstream
                    self.processors.inbound_max_request(flow_id, stream).await;

                    info!("Connected to {:?}.", socket_addr);
                }
                Err(e) => {
                    error!("Failed to handle request: {}", e);
                    continue;
                }
            }
        }
    }

    /// handles connection request from an external client using socks5 protocol.
    async fn handle_socks5_request(&self, stream: &mut TcpStream) -> io::Result<FlowId> {
        // reads the number of verfication methods supported.
        let mut nmethods = [0u8; 1];
        stream.read_exact(&mut nmethods).await?;

        // only supports no authentication for now
        let methods = vec![0u8; nmethods[0] as usize];
        stream.read_exact(&mut methods).await?;

        if !methods.contains(&0) {
            stream.write_all(&[0x05, 0xff]).await?;
            return Err(Error::new(
                ErrorKind::NotFound,
                "No supported authentication method",
            ));
        }
        stream.write_all(&[0x05, 0x00]).await?;

        // reads the request header
        let mut request_header = [0u8; 4];
        stream.read_exact(&mut request_header).await?;

        if request_header[0] != 0x05 {
            stream.write_all(&[0x05, 0xff]).await?;
            return Err(Error::new(ErrorKind::NotFound, "Not a socks5 request"));
        }

        // only supports connect requests
        if request_header[1] != 0x01 {
            let response = [0x05, 0x07, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00];
            stream.write_all(&response).await?;
            return Err(Error::new(ErrorKind::NotFound, "Unsupported request type"));
        }

        // only supports tcp requests
        match request_header[3] {
            // ipv4 protocol
            0x01 => {
                // read the server address and port
                let mut server_address_buf = [0u8; 4];
                stream.read_exact(&mut server_address_buf).await?;
                let mut server_port_buf = [0u8; 2];
                stream.read_exact(&mut server_port_buf).await?;

                let server_ip = u32::from_be_bytes(server_address_buf);
                let server_port = u16::from_be_bytes(server_port_buf);

                // get the client address and port
                let client_addr = stream.peer_addr()?;
                let client_port = client_addr.port();
                let client_ip = match client_addr.ip() {
                    IpAddr::V4(ipv4) => u32::from(ipv4),
                    IpAddr::V6(_) => {
                        return Err(Error::new(ErrorKind::Unsupported, "IPv6 not supported"));
                    }
                };

                // gets flow_id from the client address and port
                let flow_id = ((client_ip as u128) << 96)
                    | ((server_ip as u128) << 64)
                    | ((client_port as u128) << 48)
                    | ((server_port as u128) << 32);

                // send the success response
                let response = [0x05, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00];
                stream.write_all(&response).await?;

                Ok(flow_id)
            }

            _ => {
                stream.write_all(&[0x05, 0xff]).await?;
                return Err(Error::new(ErrorKind::NotFound, "Unsupported request type"));
            }
        }
    }

    /// handles connection request from a tcp max client.
    async fn handle_tcp_max_request(&self, stream: &mut TcpStream) -> io::Result<FlowId> {
        let mut flow_id_buf = [0u8; 16];

        if let Err(e) = stream.read_exact(&mut flow_id_buf).await {
            return Err(Error::new(
                ErrorKind::InvalidData,
                format!("Failed to read flow ID: {}", e),
            ));
        }

        let mut cursor = Cursor::new(&flow_id_buf);
        match cursor.read_u128().await {
            Ok(id) => Ok(id),
            Err(e) => Err(Error::new(
                ErrorKind::InvalidData,
                format!("Failed to parse flow ID: {}", e),
            )),
        }
    }
}

#[derive(Debug, Clone)]
pub struct TcpMaxClient {
    config: LocalConfig,
    processor: ProcessorHandle,
    reporter: ControllerReporterHandle,
}

impl TcpMaxClient {
    pub fn new(
        config: LocalConfig,
        processor: ProcessorHandle,
        reporter: ControllerReporterHandle,
    ) -> Self {
        Self {
            config,
            processor,
            reporter,
        }
    }

    /// Requests a tcp connection and writes the first packet.
    pub async fn request_remote(&self, flow_id: FlowId, remote_addr: &str) -> TcpStream {
        let mut retry_count = 0;
        const MAX_RETRY: usize = 10;
        let mut delay = Duration::from_secs(1);

        loop {
            match TcpStream::connect(remote_addr).await {
                Ok(mut stream) => {
                    // represents the tcp max client
                    stream
                        .write_all(&[0x06])
                        .await
                        .expect("Failed to send max client identifier to the node");

                    // sends the flow_id to the node
                    stream
                        .write_all(&flow_id.to_be_bytes())
                        .await
                        .expect("Failed to send local node id to the node");

                    info!("Connected to {} with TCP MAX.", remote_addr);

                    return stream;
                }
                Err(e) => {
                    error!(
                        "Failed to connect to node address {} with error: {}, retrying in {}s.",
                        remote_addr,
                        e,
                        delay.as_secs()
                    );
                    tokio::time::sleep(delay).await;
                    retry_count += 1;

                    if retry_count >= MAX_RETRY {
                        panic!("Maximum retry reached for TCP MAX connection to {remote_addr}");
                    }

                    delay = delay.mul_f32(1.5); // Exponential backoff
                }
            }
        }
    }

    /// Initializes a scheduler with a network interface for src node and dst node
    /// Used by dst node only when dst node is in max mode.
    pub async fn initialize_scheduler(
        &self,
        stream: TcpStream,
        remote_node_id: NodeId,
    ) -> SchedulerHandle {
        let network_interface = NetworkInterfaceHandle::new(
            self.config.clone(),
            NetworkStream::Tcp(stream),
            self.processor.clone(),
            self.reporter.clone(),
            remote_node_id,
        )
        .await;

        SchedulerHandle::new(self.config.clone(), network_interface)
    }
}
