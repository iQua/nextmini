use std::io::Cursor;
use std::time::Duration;

use std::io::IoSlice;
use tokio::io::Result;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::io::{ReadHalf, WriteHalf};
use tokio::net::{TcpListener, TcpStream};
use tracing::{error, info, warn};

use crate::node::RECEIVE_BUF_SIZE;
use crate::node::config::LocalConfig;
use crate::node::controller::reporter::ControllerReporterHandle;
use crate::node::network::interface::{NetworkInterfaceHandle, NetworkStream};
use crate::node::packet::Packet;
use crate::node::processor::ProcessorHandle;
use crate::node::scheduler::sched::SchedulerHandle;

pub struct TcpServer {
    config: LocalConfig,
    processors: ProcessorHandle,
    reporter: ControllerReporterHandle,
}

impl TcpServer {
    pub fn new(
        config: LocalConfig,
        processors: ProcessorHandle,
        reporter: ControllerReporterHandle,
    ) -> Self {
        Self {
            config,
            processors,
            reporter,
        }
    }

    pub async fn start_listening(&mut self, addr: &String) {
        let listener = match TcpListener::bind(addr).await {
            Ok(listener) => listener,
            Err(e) => {
                error!("Failed to bind to address {}: {}", addr, e);
                return;
            }
        };

        let mut node_id_buf: [u8; 8] = [0; 8];

        loop {
            let mut stream = match listener.accept().await {
                Ok((stream, socket_addr)) => {
                    info!("Connection accepted from {:?}.", socket_addr);
                    stream
                }
                Err(e) => {
                    error!("Failed to accept TCP connection: {}", e);
                    continue;
                }
            };

            // Reduce latency on the outer TCP tunnel.
            if let Err(e) = stream.set_nodelay(true) {
                warn!("Failed to set TCP_NODELAY on accepted stream: {}", e);
            }

            if let Err(e) = stream.read_exact(&mut node_id_buf).await {
                error!("Failed to read node ID: {}", e);
                continue;
            }

            let mut cursor = Cursor::new(&node_id_buf);

            let remote_node_id = match cursor.read_u64().await {
                Ok(id) => id as usize,
                Err(e) => {
                    error!("Failed to parse node ID: {}", e);
                    continue;
                }
            };

            info!("Incoming connection from node {}...", remote_node_id);

            // handles an inbound connection from a new client
            let network_interface = NetworkInterfaceHandle::new(
                self.config.clone(),
                NetworkStream::Tcp(stream),
                self.processors.clone(),
                self.reporter.clone(),
                remote_node_id,
            )
            .await;

            // creates the scheduler handle
            let scheduler = SchedulerHandle::new(self.config.clone(), network_interface);

            // adds the scheduler to send packets to the new node
            if let Err(e) = self.processors.add_node(remote_node_id, scheduler) {
                error!(
                    "Failed to add node {} with address {}: {}",
                    remote_node_id,
                    listener.local_addr().unwrap(),
                    e
                );
                continue;
            }

            info!("Connected to node {}.", remote_node_id);
        }
    }
}

pub struct TcpClient {
    pub config: LocalConfig,
}

impl TcpClient {
    pub async fn connect(&self, remote_node_id: usize, remote_addr: &str) -> TcpStream {
        let mut retry_count = 0;
        const MAX_RETRY: usize = 10;
        let mut delay = Duration::from_secs(1);

        loop {
            match TcpStream::connect(remote_addr).await {
                Ok(mut stream) => {
                    // Reduce latency on the outer TCP tunnel.
                    if let Err(e) = stream.set_nodelay(true) {
                        warn!("Failed to set TCP_NODELAY on client stream: {}", e);
                    }

                    let local_node_id = self.config.node_id; // gets the updated local node_id

                    stream
                        .write_all(&local_node_id.to_be_bytes())
                        .await
                        .expect("Failed to send local node id to the node.");

                    info!("Connected to node {} with TCP.", remote_node_id);
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

pub struct TcpReader {
    stream: ReadHalf<TcpStream>,
    processors: ProcessorHandle,
}

impl TcpReader {
    pub fn new(stream: ReadHalf<TcpStream>, processors: ProcessorHandle) -> Self {
        Self { stream, processors }
    }

    pub async fn run(mut self) {
        loop {
            // reads a packet from the TCP connection
            if let Ok(packet) = self.read_packet().await {
                // forwards the packet to the processor
                self.processors.process_packet(packet);
            }
        }
    }

    /// Reads a single packet from the TCP connection.
    async fn read_packet(&mut self) -> Result<Packet> {
        let mut buf = vec![0; RECEIVE_BUF_SIZE];
        self.stream.read_exact(&mut buf[0..4]).await?;

        let msg_len = buf[2] as usize * 256 + buf[3] as usize;
        self.stream.read_exact(&mut buf[4..msg_len]).await?;

        Ok(Packet::new(msg_len, buf))
    }
}

pub struct TcpWriter {
    stream: WriteHalf<TcpStream>,
}

impl TcpWriter {
    pub fn new(stream: WriteHalf<TcpStream>) -> Self {
        Self { stream }
    }

    /// Writes multiple packets to the TCP network stream.
    pub async fn write_packets(&mut self, packets: Vec<Packet>) -> Result<()> {
        if packets.is_empty() {
            return Ok(());
        }

        // first creates IoSlice objects from packet buffers
        let mut io_slices: Vec<IoSlice> = packets
            .iter()
            .map(|packet| IoSlice::new(&packet.buf[0..packet.packet_size]))
            .collect();

        let mut slices = io_slices.as_mut_slice();

        while !slices.is_empty() {
            let written_this_call = self.stream.write_vectored(slices).await?;

            if written_this_call == 0 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::WriteZero,
                    "write_vectored returned 0",
                ));
            }

            // advances the slices to skip the written data
            IoSlice::advance_slices(&mut slices, written_this_call);
        }

        Ok(())
    }
}
