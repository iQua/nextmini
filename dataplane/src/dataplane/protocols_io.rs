use crate::dataplane::PacketBuf;
use s2n_quic::stream::{ReceiveStream, SendStream};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::io::{ReadHalf, WriteHalf};
use tokio::net::TcpStream;
use tokio::net::UdpSocket;
use tokio::sync::Mutex;

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

pub enum ProtocolWriter {
    Tcp(TcpWriter),
    Udp(UdpWriter),
    Quic(QuicWriter),
}

impl ProtocolWriter {
    pub async fn send(&mut self, data: &[u8]) {
        match self {
            Self::Tcp(writer) => writer.send(data).await,
            Self::Udp(writer) => writer.send(data).await,
            Self::Quic(writer) => writer.send(data).await,
        }
    }

    pub fn reproduce(&self) -> Self {
        match self {
            Self::Tcp(writer) => Self::Tcp(writer.reproduce()),
            Self::Udp(writer) => Self::Udp(writer.reproduce()),
            Self::Quic(writer) => Self::Quic(writer.reproduce()),
        }
    }
}

pub struct TcpReader {
    stream: ReadHalf<TcpStream>,
}

impl TcpReader {
    pub fn new(stream: ReadHalf<TcpStream>) -> Self {
        Self { stream }
    }

    pub async fn recv(&mut self, buf: &mut PacketBuf) -> usize {
        // Read raw IP packet data directly
        match self.stream.read(&mut buf[..]).await {
            Ok(0) => 0, // Connection closed
            Ok(n) => n,
            Err(e) => {
                eprintln!("Failed to read TCP data: {}", e);
                0
            }
        }
    }
}

#[derive(Clone)]
pub struct TcpWriter {
    stream: Arc<Mutex<WriteHalf<TcpStream>>>,
}

impl TcpWriter {
    pub fn new(stream: Arc<Mutex<WriteHalf<TcpStream>>>) -> Self {
        Self { stream }
    }

    pub async fn send(&mut self, data: &[u8]) {
        let mut stream_guard = self.stream.lock().await;
        match stream_guard.write_all(data).await {
            Ok(_) => (),
            Err(e) => {
                eprintln!("Failed to write TCP data: {}", e);
            }
        }
    }

    pub fn reproduce(&self) -> Self {
        Self {
            stream: self.stream.clone(),
        }
    }
}

#[derive(Clone)]
pub struct UdpReader {
    sock: Arc<UdpSocket>,
}

impl UdpReader {
    pub fn new(sock: Arc<UdpSocket>) -> Self {
        Self { sock }
    }

    pub async fn recv(&self, buf: &mut PacketBuf) -> usize {
        match self.sock.recv(&mut buf[..]).await {
            Ok(n) => n,
            Err(e) => {
                eprintln!("Failed to receive UDP data: {}", e);
                0
            }
        }
    }
}

#[derive(Clone)]
pub struct UdpWriter {
    sock: Arc<UdpSocket>,
    addr: String,
}

impl UdpWriter {
    pub fn new(sock: Arc<UdpSocket>, addr: String) -> Self {
        Self { sock, addr }
    }

    pub async fn send(&self, data: &[u8]) {
        match self.sock.send_to(data, self.addr.as_str()).await {
            Ok(_) => (),
            Err(e) => {
                eprintln!("Failed to send UDP data to {}: {}", self.addr, e);
            }
        }
    }

    pub fn reproduce(&self) -> Self {
        let sock = self.sock.clone();
        let addr = self.addr.clone();
        Self { sock, addr }
    }
}

pub struct QuicReader {
    stream: ReceiveStream,
}

impl QuicReader {
    pub fn new(stream: ReceiveStream) -> Self {
        Self { stream }
    }

    pub async fn recv(&mut self, buf: &mut PacketBuf) -> usize {
        // Read raw IP packet data directly
        match self.stream.read(&mut buf[..]).await {
            Ok(0) => 0, // Stream closed
            Ok(n) => n,
            Err(e) => {
                eprintln!("Failed to read QUIC data: {}", e);
                0
            }
        }
    }
}

pub struct QuicWriter {
    stream: Arc<Mutex<SendStream>>,
}

impl QuicWriter {
    pub fn new(stream: Arc<Mutex<SendStream>>) -> Self {
        Self {
            stream: stream.clone(),
        }
    }

    pub async fn send(&mut self, buf: &[u8]) {
        let mut stream_guard = self.stream.lock().await;

        match stream_guard.write_all(buf).await {
            Ok(_) => (),
            Err(e) => {
                eprintln!("Failed to write QUIC data: {}", e);
            }
        }
    }

    pub fn reproduce(&self) -> Self {
        Self {
            stream: self.stream.clone(),
        }
    }
}