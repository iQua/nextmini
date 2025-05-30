use crate::node::PacketBuf;
use crate::node::processor::ProcessorHandleforReader;
use crate::node::protocols_client::connect_tcp_node;
use crate::node::quic::{QuicReaderHandle, QuicWriterHandle};
use crate::node::tcp::{TcpReaderHandle, TcpWriterHandle};
use crate::node::udp::{UdpReaderHandle, UdpWriterHandle};
use std::sync::Arc;
use tokio::sync::Mutex;

pub enum ProtocolReader {
    Tcp(TcpReaderHandle),
    // Udp(UdpReaderHandle),
    Quic(QuicReaderHandle),
}

impl ProtocolReader {
    pub async fn shutdown(&mut self) {
        match self {
            Self::Tcp(reader) => reader.shutdown().await,
            // Self::Udp(reader) => reader.shutdown().await,
            Self::Quic(reader) => reader.shutdown().await,
        }
    }
}

#[derive(Clone)]
pub enum ProtocolWriter {
    Tcp(TcpWriterHandle),
    Udp(UdpWriterHandle),
    Quic(QuicWriterHandle),
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

pub async fn create_node_connection(
    node_id: usize,
    addr: &str,
    local_id: usize,
    processor_handle: ProcessorHandleforReader,
    protocol: Protocol,
) -> (ProtocolReader, ProtocolWriter) {
    match protocol {
        Protocol::Tcp => {
            let stream = connect_tcp_node(local_id, addr, node_id).await;
            let (reader, writer) = tokio::io::split(stream);

            let tcp_reader = TcpReaderHandle::new(reader, processor_handle);
            let tcp_writer = TcpWriterHandle::new(Arc::new(Mutex::new(writer)));

            (
                ProtocolReader::Tcp(tcp_reader),
                ProtocolWriter::Tcp(tcp_writer),
            )
        }

        // TDDO: Implement UDP connection
        Protocol::Udp => {}
        // TODO: Implement QUIC connection
        Protocol::Quic => {}
    }
}
