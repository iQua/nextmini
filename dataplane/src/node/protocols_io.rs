use crate::node::quic::{QuicWriterHandle, QuicWriter};
use crate::node::tcp::{TcpWriterHandle, TcpWriter};
use crate::node::udp::{UdpWriterHandle, UdpWriter};

pub enum ProtocolWriterMessage {
    Send(Vec<u8>),
    Shutdown,
}


// Should be changed after all writers are fixed
#[derive(Clone)]
pub enum ProtocolWriterHandle {
    Tcp(TcpWriterHandle),
    Udp(UdpWriterHandle),
    Quic(QuicWriterHandle),
}

impl ProtocolWriterHandle {
    pub fn new(&self) -> Self {
        match self {
            Self::Tcp(writer) => writer.new_tcp(),
            Self::Udp(writer) => writer.new_udp(),
            Self::Quic(writer) => writer.new_quic(),
        }
    }
    pub async fn send(&mut self, buf: &[u8]) {
        match self {
            Self::Tcp(writer) => writer.send(buf).await,
            Self::Udp(writer) => writer.send(buf).await,
            Self::Quic(writer) => writer.send(buf).await,
        }
    }
}
