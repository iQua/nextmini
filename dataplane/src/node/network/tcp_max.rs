use std::io::{self, Cursor, Error, ErrorKind};
use std::net::IpAddr;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tracing::{error, info, warn};

use crate::node::config::LocalConfig;
use crate::node::controller::reporter::ControllerReporterHandle;
use crate::node::network::interface::{NetworkInterfaceHandle, NetworkStream};
use crate::node::processor::ProcessorHandle;
use crate::node::scheduler::sched::SchedulerHandle;
use crate::node::{FlowId, NodeId};

pub struct TcpMaxServer {
    // Retained for runtime scheduler initialization.
    processors: ProcessorHandle,
}

/// TcpMaxServer supports both SOCKS5 proxy requests and direct TCP max connections.
impl TcpMaxServer {
    pub fn new(processors: ProcessorHandle) -> Self {
        Self { processors }
    }

    /// Accepts incoming TCP connections.
    pub async fn start_listening(&mut self, addr: &str) {
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

            // disables Nagle's algorithm to reduce extra latency in the outer TCP
            if let Err(e) = stream.set_nodelay(true) {
                warn!("Failed to set TCP_NODELAY on accepted MAX stream: {}", e);
            }

            // reads the first byte to determine the protocol
            if let Err(e) = stream.read_exact(&mut first_byte).await {
                error!("Failed to read first byte: {}", e);
                continue;
            }

            // handles an inbound connection based on its protocol
            let flow_id = match first_byte[0] {
                // indicates a SOCKS5 protocol request, typically from an external client
                0x05 => self.handle_socks5_request(&mut stream).await,
                // indicates a TCP max protocol request from within Nextmini nodes
                0x06 => self.handle_tcp_max_request(&mut stream).await,
                _ => {
                    error!("Unsupported protocol");
                    continue;
                }
            };

            // extracts the flow ID from the SOCKS5 protocol or TCP max request
            match flow_id {
                Ok(flow_id) => {
                    // asks the processor to splice the upstream
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

    /// Handles connection request from an external client using the SOCKS5 protocol.
    async fn handle_socks5_request(&self, stream: &mut TcpStream) -> io::Result<FlowId> {
        // reads the number of verfication methods supported
        let mut nmethods = [0u8; 1];
        stream.read_exact(&mut nmethods).await?;

        // only supports no authentication for now
        let mut methods = vec![0u8; nmethods[0] as usize];
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
            return Err(Error::new(ErrorKind::NotFound, "Not a SOCKS5 request"));
        }

        // only supports connect requests
        if request_header[1] != 0x01 {
            let response = [0x05, 0x07, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00];
            stream.write_all(&response).await?;
            return Err(Error::new(ErrorKind::NotFound, "Unsupported request type"));
        }

        // only supports TCP requests
        match request_header[3] {
            // ipv4 protocol
            0x01 => {
                // reads the server address and port number
                let mut server_address_buf = [0u8; 4];
                stream.read_exact(&mut server_address_buf).await?;
                let mut server_port_buf = [0u8; 2];
                stream.read_exact(&mut server_port_buf).await?;

                let server_ip = u32::from_be_bytes(server_address_buf);
                let server_port = u16::from_be_bytes(server_port_buf);

                // obtains the client address and port number
                let client_addr = stream.peer_addr()?;
                let client_port = client_addr.port();

                let client_ip = match client_addr.ip() {
                    IpAddr::V4(ipv4) => u32::from(ipv4),
                    IpAddr::V6(_) => {
                        return Err(Error::new(ErrorKind::Unsupported, "IPv6 not supported"));
                    }
                };

                // obtains the flow ID from the client address and port number
                let flow_id = ((client_ip as u128) << 96)
                    | ((server_ip as u128) << 64)
                    | ((client_port as u128) << 48)
                    | ((server_port as u128) << 32);

                // sends a response indicating success
                let response = [0x05, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00];
                stream.write_all(&response).await?;

                Ok(flow_id)
            }

            _ => {
                stream.write_all(&[0x05, 0xff]).await?;
                Err(Error::new(ErrorKind::NotFound, "Unsupported request type"))
            }
        }
    }

    /// Handles connection request from a TCP max client.
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

    /// Connects to a TCP max server and sends the first packet.
    pub async fn connect(&self, flow_id: FlowId, remote_addr: &str) -> TcpStream {
        let mut retry_count = 0;
        const MAX_RETRY: usize = 10;
        let mut delay = Duration::from_secs(1);

        loop {
            match TcpStream::connect(remote_addr).await {
                Ok(mut stream) => {
                    // disables Nagle's algorithm to reduce extra latency in the outer TCP
                    if let Err(e) = stream.set_nodelay(true) {
                        warn!("Failed to set TCP_NODELAY on MAX client stream: {}", e);
                    }

                    // after requesting a remote connection, it writes the first byte 0x06 into the stream,
                    // which indicates that it is a TCP max connection
                    stream
                        .write_all(&[0x06])
                        .await
                        .expect("Failed to send the max client identifier to the node.");

                    // sends the flow ID to the node
                    stream
                        .write_all(&flow_id.to_be_bytes()) // writes the flow_id as a 16-byte big-endian integer
                        .await
                        .expect("Failed to send the flow ID to the node.");

                    info!("Connected to {} with TCP max.", remote_addr);

                    return stream;
                }
                Err(e) => {
                    warn!(
                        "Failed to connect to node address {} with error: {}, retrying in {} seconds.",
                        remote_addr,
                        e,
                        delay.as_secs()
                    );
                    tokio::time::sleep(delay).await;
                    retry_count += 1;

                    if retry_count >= MAX_RETRY {
                        error!(
                            "Maximum retry reached for establishing a TCP max connection to {}.",
                            remote_addr
                        );
                    }

                    delay = delay.mul_f32(1.5); // Exponential backoff
                }
            }
        }
    }

    /// Initializes a scheduler with a network interface for the source and destination node.
    /// Used by the destination node only when it is operating in max mode.
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

    pub async fn connect_without_header(&self, remote_addr: &str) -> TcpStream {
        let mut retry_count = 0;
        const MAX_RETRY: usize = 10;
        let mut delay = Duration::from_secs(1);

        loop {
            match TcpStream::connect(remote_addr).await {
                Ok(stream) => {
                    // disables Nagle's algorithm to reduce extra latency in the outer TCP
                    if let Err(e) = stream.set_nodelay(true) {
                        warn!(
                            "Failed to set TCP_NODELAY on MAX client stream (no header): {}",
                            e
                        );
                    }

                    info!("Connected to {} without max header.", remote_addr);

                    return stream;
                }
                Err(e) => {
                    warn!(
                        "Failed to connect to node address {} with error: {}, retrying in {} seconds.",
                        remote_addr,
                        e,
                        delay.as_secs()
                    );

                    tokio::time::sleep(delay).await;
                    retry_count += 1;

                    if retry_count >= MAX_RETRY {
                        error!(
                            "Maximum retry reached for establishing a TCP connection to {}.",
                            remote_addr
                        );
                    }

                    delay = delay.mul_f32(1.5); // Exponential backoff
                }
            }
        }
    }
}
