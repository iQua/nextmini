use crate::dataplane::PacketBuf;
use s2n_quic::stream::{ReceiveStream, SendStream};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::io::{ReadHalf, WriteHalf};
use tokio::net::TcpStream;
use tokio::net::UdpSocket;
use tokio::sync::Mutex;
use tracing::{debug, error};

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
        debug!("TcpReader: Attempting to read 4-byte header");
        if let Err(e) = self.stream.read_exact(&mut buf[0..4]).await {
            error!("TcpReader: Failed to read 4-byte header: {}", e);
            return 0;
        }
        debug!(header = ?&buf[0..4], "TcpReader: Successfully read 4-byte header");

        let msg_len = buf[2] as usize * 256 + buf[3] as usize;

        if msg_len == 0 {
            error!("TcpReader: Calculated message length is 0. No payload to read after header.");
            return 0; 
        }

        msg_len
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
        debug!(data_len = data.len(), first_bytes = ?&data[0..std::cmp::min(data.len(), 16)], "TcpWriter: Sending data");
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
            Err(e) => panic!("{e}"),
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
        debug!(data_len = data.len(), first_bytes = ?&data[0..std::cmp::min(data.len(), 16)], addr = %self.addr, "UdpWriter: Sending data");
        match self.sock.send_to(data, self.addr.as_str()).await {
            Ok(_) => (),
            Err(e) => panic!("{e}"),
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
        debug!("QuicReader: Attempting to read 4-byte header");
        match self.stream.read_exact(&mut buf[0..4]).await {
            Ok(_) => {
                debug!(header = ?&buf[0..4], "QuicReader: Successfully read 4-byte header");
            }
            Err(e) => {
                debug!("QuicReader: Failed to read 4-byte header: {}", e);
                return 0;
            }
        }

        let msg_len = buf[2] as usize * 256 + buf[3] as usize;

        match self.stream.read_exact(&mut buf[4..msg_len]).await {
            Ok(_) => (),
            Err(_) => {
                return 0;
            }
        }

        msg_len
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
        debug!(data_len = buf.len(), first_bytes = ?&buf[0..std::cmp::min(buf.len(), 16)], "QuicWriter: Sending data");
        let mut stream_guard = self.stream.lock().await;

        match stream_guard.write_all(buf).await {
            Ok(_) => (),
            Err(e) => panic!("{e}"),
        };
    }

    pub fn reproduce(&self) -> Self {
        Self {
            stream: self.stream.clone(),
        }
    }
}
