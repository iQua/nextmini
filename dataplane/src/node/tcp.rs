use crate::node::packet::Packet;
use crate::node::RECEIVE_BUF_SIZE;
use crate::node::processor::ProcessorHandle;

use std::io::Cursor;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::io::{ReadHalf, WriteHalf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Mutex, mpsc};
use tracing::{error, info};


pub struct TcpServer {
    processor_handle: ProcessorHandle,
}

impl TcpServer {

    pub fn new(processor_handle: ProcessorHandle) -> Self {
        Self {processor_handle}
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

            if let Err(e) = stream.read_exact(&mut node_id_buf).await {
                error!("Failed to read node ID: {}", e);
                continue;
            }

            let mut cursor = Cursor::new(&node_id_buf);

            let node_id = match cursor.read_u64().await {
                Ok(id) => id as usize,
                Err(e) => {
                    error!("Failed to parse node ID: {}", e);
                    continue;
                }
            };

            info!("Incoming connection from node {}...", node_id);

            // Add new node : There are three potential ways
            // 1. Change processor's 'scheduler' hashmap directly
            // 2. Add a new message type for processor handle
            // 3. Redesign in compatiable with the current ControllerToDataplane::AddNode message

            info!("Connected to node {}.", node_id);
        }
    }
}

pub struct TcpReader {
    stream: ReadHalf<TcpStream>,
    processor_handle: ProcessorHandle,
}

impl TcpReader {
    pub fn new(
        stream: ReadHalf<TcpStream>,
        processor_handle: ProcessorHandle,
    ) -> Self {
        Self {
            stream,
            processor_handle,
        }
    }

    pub async fn run(mut self) {
        loop {

            // TODO : Shutdown logic
            // 1. Send shutdown message to scheduler and break the loop when error
            // 2. Receive shutdown message from scheduler and break the loop

            // Read from network
            match self.read_packet().await {
                Ok(packet) => {
                    // Send packet via ProcessorHandleforReader
                    self.processor_handle.process_packet(packet).await;
                }
                Err(e) => {
                    error!("Failed to read TCP packet: {}", e);
                    break;
                }
            }
        }
    }

    async fn read_packet(&mut self) -> Result<Packet, std::io::Error> {
        let mut buf = [0u8; RECEIVE_BUF_SIZE];
        // Read header
        self.stream.read_exact(&mut buf[0..4]).await?;

        let msg_len = buf[2] as usize * 256 + buf[3] as usize;
        self.stream.read_exact(&mut buf[4..msg_len]).await?;

        // New packet
        let packet = Packet::new(msg_len, buf);
        Ok(packet)
    }
}

pub struct TcpWriter {
    stream: Arc<Mutex<WriteHalf<TcpStream>>>,
    receiver: mpsc::Receiver<Packet>,
}

impl TcpWriter {
    pub fn new(
        stream: Arc<Mutex<WriteHalf<TcpStream>>>,
        receiver: mpsc::Receiver<Packet>,
    ) -> Self {
        Self { stream, receiver }
    }

    pub async fn run(mut self) {
        while let Some(packet) = self.receiver.recv().await {

            // TODO : Shutdown logic
            // 1. Send shutdown message to scheduler and break the loop when error
            // 2. Receive shutdown message from scheduler and break the loop

            let mut stream_guard = self.stream.lock().await;
            if let Err(e) = stream_guard.write_all(&packet.buf).await {
                error!("Failed to write TCP data: {}", e);
            }
        }
    }
}

