use crate::node::PacketBuf;
use crate::node::quic::{QuicReader, QuicWriter};
use crate::node::tcp::{TcpReader, TcpWriter};
use crate::node::udp::{UdpReader, UdpWriter};

pub enum ProtocolReader {
    Tcp(TcpReader),
    Udp(UdpReader),
    Quic(QuicReader),
}

impl ProtocolReader {
    pub async fn recv(&mut self, buf: &mut PacketBuf) -> usize {
        match self {
            Self::Tcp(reader) => reader.read(buf).await,
            Self::Udp(reader) => reader.read(buf).await,
            Self::Quic(reader) => reader.read(buf).await,
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
            Self::Tcp(writer) => writer.write(data).await,
            Self::Udp(writer) => writer.write(data).await,
            Self::Quic(writer) => writer.write(data).await,
        }
    }
}
