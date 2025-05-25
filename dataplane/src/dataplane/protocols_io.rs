use crate::dataplane::PacketBuf;
use s2n_quic::stream::{ReceiveStream, SendStream};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::io::{ReadHalf, WriteHalf};
use tokio::net::TcpStream;
use tokio::net::UdpSocket;
use tokio::sync::Mutex;
use tracing::{error, info, warn};

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
    pub async fn send(&mut self, data: &[u8]) -> Result<(), String> {
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
            Ok(0) => {
                info!("WARNING: TCP connection closed by peer");
                0 // Connection closed
            }
            Ok(n) => n,
            Err(e) => {
                error!("Failed to read TCP data: {} - connection may be broken", e);
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

    pub async fn send(&mut self, data: &[u8]) -> Result<(), String> {
        let mut stream_guard = self.stream.lock().await;
        match stream_guard.write_all(data).await {
            Ok(_) => {
                // Ensure data is flushed to the network
                if let Err(flush_err) = stream_guard.flush().await {
                    let error = format!("Failed to flush TCP data: {} - packets may be buffered", flush_err);
                    error!("{}", error);
                    return Err(error);
                }
                Ok(())
            }
            Err(e) => {
                let error = format!(
                    "Failed to write TCP data (size: {} bytes): {} - connection may be broken",
                    data.len(),
                    e
                );
                error!("{}", error);
                Err(error)
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
            Ok(n) => {
                if n == 0 {
                    warn!("Received empty UDP packet");
                }
                n
            }
            Err(e) => {
                error!(
                    "Failed to receive UDP data: {} - socket may be closed or network unreachable",
                    e
                );
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

    pub async fn send(&self, data: &[u8]) -> Result<(), String> {
        match self.sock.send_to(data, self.addr.as_str()).await {
            Ok(bytes_sent) => {
                if bytes_sent != data.len() {
                    let error = format!(
                        "UDP partial send - expected {} bytes, sent {} bytes to {}",
                        data.len(),
                        bytes_sent,
                        self.addr
                    );
                    warn!("{}", error);
                    return Err(error);
                }
                Ok(())
            }
            Err(e) => {
                let error = format!(
                    "Failed to send UDP data to {} (size: {} bytes): {} - destination may be unreachable",
                    self.addr,
                    data.len(),
                    e
                );
                error!("{}", error);
                Err(error)
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
            Ok(0) => {
                warn!("QUIC stream closed by peer");
                0 // Stream closed
            }
            Ok(n) => n,
            Err(e) => {
                error!(
                    "Failed to read QUIC data: {} - stream may be broken or connection lost",
                    e
                );
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

    pub async fn send(&mut self, buf: &[u8]) -> Result<(), String> {
        let mut stream_guard = self.stream.lock().await;

        match stream_guard.write_all(buf).await {
            Ok(_) => {
                // Ensure data is flushed to the network
                if let Err(flush_err) = stream_guard.flush().await {
                    let error = format!("Failed to flush QUIC data: {} - packets may be buffered", flush_err);
                    error!("{}", error);
                    return Err(error);
                }
                Ok(())
            }
            Err(e) => {
                let error = format!(
                    "Failed to write QUIC data (size: {} bytes): {} - stream may be broken",
                    buf.len(),
                    e
                );
                error!("{}", error);
                Err(error)
            }
        }
    }

    pub fn reproduce(&self) -> Self {
        Self {
            stream: self.stream.clone(),
        }
    }
}
