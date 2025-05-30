use crate::node::PacketBuf;
use crate::node::processor::ProcessorHandleforReader;
use crate::node::protocols_client::connect_tcp_node;
use crate::node::quic::{QuicReader, QuicWriter};
use crate::node::tcp::{TcpReaderHandle, TcpWriterHandle};
use crate::node::udp::{UdpReader, UdpWriter};
use std::sync::Arc;
use tokio::sync::Mutex;

pub enum ProtocolReader {
    Tcp(TcpReaderHandle),
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
    Tcp(TcpWriterHandle),
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

pub async fn create_tcp_node(
    node_id: usize,
    addr: &str,
    local_id: usize,
    processor_handle: ProcessorHandleforReader,
) -> (ProtocolReader, ProtocolWriter) {
    let stream = connect_tcp_node(local_id, addr, node_id).await;
    let (reader, writer) = tokio::io::split(stream);

    let tcp_reader = TcpReaderHandle::new(reader, processor_handle);
    let tcp_writer = TcpWriterHandle::new(Arc::new(Mutex::new(writer)));

    (
        ProtocolReader::Tcp(tcp_reader),
        ProtocolWriter::Tcp(tcp_writer),
    )
}
