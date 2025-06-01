use nextmini_messages::Protocol;
use crate::node::packet::Packet;
use crate::node::protocols_client::connect_tcp_node;
use crate::node::processor::ProcessorHandle;
use crate::node::tcp::{TcpReader, TcpWriter};

use std::sync::Arc;
use tokio::sync::{Mutex, mpsc};

// used in scheduler to send packet to network interface
// network interface need processor handle to send packet to processor
// created in controller interface
pub struct NetworkInterfaceHandle {
    sender: mpsc::Sender<Packet>,
}

impl NetworkInterfaceHandle {
    pub async fn new(processor_handle: ProcessorHandle, addr: &str, local_node_id: usize, remote_node_id: usize, protocol: Protocol) -> Self {
        let (sender, receiver) = mpsc::channel(100);
        let network_interface = Self { sender };
        
        network_interface.create_connection(addr, local_node_id, remote_node_id, protocol, processor_handle, receiver).await;

        network_interface
    }

    pub async fn create_connection(
        &self, 
        addr: &str, 
        local_node_id: usize, 
        remote_node_id: usize, 
        protocol: Protocol, 
        processor_handle: ProcessorHandle,
        receiver: mpsc::Receiver<Packet>
    ){
        match protocol {
            Protocol::Tcp => {
                // Request connection to remote node server
                let stream = connect_tcp_node(local_node_id, addr, remote_node_id).await;
                let (reader, writer) = tokio::io::split(stream);

                // Create TCP reader handle
                let tcp_reader = TcpReader::new(reader, processor_handle);
                
                // Create TCP writer handle
                let tcp_writer = TcpWriter::new(Arc::new(Mutex::new(writer)), receiver);

                tokio::spawn(async move {
                    tcp_reader.run().await;
                });
                tokio::spawn(async move {
                    tcp_writer.run().await;
                });
            }
            Protocol::Quic => {
                return;
            }
            Protocol::Udp => {
                return;
            }
        }
    }

    pub async fn send(&self, packet: Packet){
        self.sender.send(packet).await.unwrap();
    }
}


