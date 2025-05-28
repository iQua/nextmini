use crate::node::udp::{UdpReader, UdpWriter};
use crate::node::tcp::{TcpReader, TcpWriter};
use crate::node::quic::{QuicReader, QuicWriter};
use crate::node::PacketBuf;

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

#[derive(Clone)]
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
