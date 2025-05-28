use crate::node::tcp::TcpProtocolWriterHandle;
use crate::node::udp::UdpProtocolWriterHandle;
use crate::node::quic::QuicProtocolWriterHandle;



pub enum ProtocolWriterMessage {
    Send(Vec<u8>),
    Shutdown,
}

#[derive(Clone)]
pub enum ProtocolWriterHandle {
    Tcp(TcpProtocolWriterHandle),
    Udp(UdpProtocolWriterHandle),
    Quic(QuicProtocolWriterHandle),
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
