use crate::node::PacketBuf;
use crate::node::quic::{QuicReader, QuicWriter};
use crate::node::tcp::{TcpReader, TcpWriter};
use crate::node::udp::{UdpReader, UdpWriter};
use tokio::sync::mpsc;
use tracing::error;

pub enum ProtocolReader {
    Tcp(TcpReader),
    Udp(UdpReader),
    Quic(QuicReader),
}

impl ProtocolReader {
    pub async fn recv(&mut self, buf: &mut PacketBuf) -> usize {
        match self {
            Self::Tcp(reader) => reader.recv(buf).await,
            Self::Udp(reader) => reader.recv(buf).await,
            Self::Quic(reader) => reader.recv(buf).await,
        }
    }
}

enum ProtocolWriterMessage {
    Send(Vec<u8>),
    Shutdown,
}

struct TcpProtocolWriter {
    receiver: mpsc::Receiver<ProtocolWriterMessage>,
    writer: TcpWriter,
}

impl TcpProtocolWriter {
    async fn run(&mut self) {
        while let Some(message) = self.receiver.recv().await {
            match message {
                ProtocolWriterMessage::Send(data) => {
                    self.writer.send(&data).await;
                }
                ProtocolWriterMessage::Shutdown => break,
            }
        }
    }
}

struct UdpProtocolWriter {
    receiver: mpsc::Receiver<ProtocolWriterMessage>,
    writer: UdpWriter,
}

impl UdpProtocolWriter {
    async fn run(&mut self) {
        while let Some(message) = self.receiver.recv().await {
            match message {
                ProtocolWriterMessage::Send(data) => {
                    self.writer.send(&data).await;
                }
                ProtocolWriterMessage::Shutdown => break,
            }
        }
    }
}

struct QuicProtocolWriter {
    receiver: mpsc::Receiver<ProtocolWriterMessage>,
    writer: QuicWriter,
}

impl QuicProtocolWriter {
    async fn run(&mut self) {
        while let Some(message) = self.receiver.recv().await {
            match message {
                ProtocolWriterMessage::Send(data) => {
                    self.writer.send(&data).await;
                }
                ProtocolWriterMessage::Shutdown => break,
            }
        }
    }
}

#[derive(Clone)]
pub struct ProtocolWriterHandle {
    sender: mpsc::Sender<ProtocolWriterMessage>,
}

impl ProtocolWriterHandle {
    pub fn new_tcp(tcp_writer: TcpWriter) -> Self {
        let (sender, receiver) = mpsc::channel(100);

        let mut actor = TcpProtocolWriter {
            receiver,
            writer: tcp_writer,
        };

        tokio::spawn(async move {
            actor.run().await;
        });

        Self { sender }
    }

    pub fn new_udp(udp_writer: UdpWriter) -> Self {
        let (sender, receiver) = mpsc::channel(100);

        let mut actor = UdpProtocolWriter {
            receiver,
            writer: udp_writer,
        };

        tokio::spawn(async move {
            actor.run().await;
        });

        Self { sender }
    }

    pub fn new_quic(quic_writer: QuicWriter) -> Self {
        let (sender, receiver) = mpsc::channel(100);

        let mut actor = QuicProtocolWriter {
            receiver,
            writer: quic_writer,
        };

        tokio::spawn(async move {
            actor.run().await;
        });

        Self { sender }
    }

    pub async fn send(&self, data: &[u8]) {
        if let Err(_) = self
            .sender
            .send(ProtocolWriterMessage::Send(data.to_vec()))
            .await
        {
            error!("Failed to send data through protocol writer channel");
        }
    }

    pub fn reproduce(&self) -> Self {
        self.clone()
    }
}
