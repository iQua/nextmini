use crate::node::packet::Packet;
use crate::node::RECEIVE_BUF_SIZE;
use std::io::Cursor;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::io::{ReadHalf, WriteHalf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Mutex, mpsc};
use tracing::{error, info};

use super::processor::ProcessorHandleforReader;

pub struct TcpServer {
    // context: Context,
}

impl TcpServer {
    // pub fn new(context: Context) -> Self {
    //     Self { context }
    // }

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

            // self.context.add_tcp_node(node_id, stream).await;
            // self.processor_manager
            //     .write()
            //     .await
            //     .update_processors()
            //     .await;

            info!("Connected to node {}.", node_id);
        }
    }
}

pub enum TcpReaderMessage {
    Shutdown,
}

pub struct TcpReader {
    stream: ReadHalf<TcpStream>,
    processor_handle: ProcessorHandleforReader,
    receiver: mpsc::Receiver<TcpReaderMessage>,
}

impl TcpReader {
    pub fn new(
        stream: ReadHalf<TcpStream>,
        processor_handle: ProcessorHandleforReader,
        receiver: mpsc::Receiver<TcpReaderMessage>,
    ) -> Self {
        Self {
            stream,
            processor_handle,
            receiver,
        }
    }

    pub async fn run(mut self) {
        loop {
            if let Ok(msg) = self.receiver.try_recv() {
                match msg {
                    TcpReaderMessage::Shutdown => {
                        info!("TcpReader received shutdown signal");
                        break;
                    }
                }
            }

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

#[derive(Clone)]
pub struct TcpReaderHandle {
    sender: mpsc::Sender<TcpReaderMessage>,
}

impl TcpReaderHandle {
    pub fn new(
        reader: ReadHalf<TcpStream>,
        processor_handle: ProcessorHandleforReader,
    ) -> Self {
        let (sender, receiver) = mpsc::channel(100);
        let actor = TcpReader::new(reader, processor_handle, receiver);

        tokio::spawn(async move {
            actor.run().await;
        });

        Self { sender }
    }

    pub async fn shutdown(&mut self) {
        if let Err(e) = self.sender.send(TcpReaderMessage::Shutdown).await {
            error!("Failed to send shutdown message to TcpReader: {}", e);
        }
    }
}


pub enum TcpWriterMessage {
    WritePacket(Vec<u8>),
}

pub struct TcpWriter {
    stream: Arc<Mutex<WriteHalf<TcpStream>>>,
    receiver: mpsc::Receiver<TcpWriterMessage>,
}

impl TcpWriter {
    pub fn new(
        stream: Arc<Mutex<WriteHalf<TcpStream>>>,
        receiver: mpsc::Receiver<TcpWriterMessage>,
    ) -> Self {
        Self { stream, receiver }
    }

    pub async fn run(mut self) {
        while let Some(msg) = self.receiver.recv().await {
            match msg {
                TcpWriterMessage::WritePacket(data) => {
                    let mut stream_guard = self.stream.lock().await;
                    if let Err(e) = stream_guard.write_all(&data).await {
                        error!("Failed to write TCP data: {}", e);
                    }
                }
            }
        }
    }
}

#[derive(Clone)]
pub struct TcpWriterHandle {
    sender: mpsc::Sender<TcpWriterMessage>,
}

impl TcpWriterHandle {
    pub fn new(stream: Arc<Mutex<WriteHalf<TcpStream>>>) -> Self {
        let (sender, receiver) = mpsc::channel(100);
        let actor = TcpWriter::new(stream, receiver);

        tokio::spawn(async move {
            actor.run().await;
        });

        Self { sender }
    }

    pub async fn write(&mut self, data: &[u8]) {
        if let Err(e) = self
            .sender
            .send(TcpWriterMessage::WritePacket(data.to_vec()))
            .await
        {
            error!("Failed to send packet to TcpWriter actor: {:?}", e);
        }
    }
}
