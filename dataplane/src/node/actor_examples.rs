use crate::dataplane::local_interface::TunWriter;
use crate::dataplane::packet::Packet;
use crate::dataplane::protocols_io::{ProtocolReader, ProtocolWriter};
use crate::dataplane::routes::RoutingTable;
use crate::dataplane::{FlowId, NodeId, RECEIVE_BUF_SIZE};
use std::collections::HashMap;

use tokio::sync::mpsc;
use flume::bounded;

enum ProcessorMessage {
    ProcessPacket(Packet),
    UpdateRoutingTable(RoutingTable),
}

enum LocalWriterMessage {
    WritePacket(Packet),
}

enum ProtocolSenderMessage {
    SendPacket(Packet),
}

// LocalWriter: Handles writing packets to the TUN interface
struct LocalWriter {
    receiver: mpsc::Receiver<LocalWriterMessage>,
    tun_writer: TunWriter,
}

impl LocalWriter {
    async fn run(&mut self) {
        while let Some(msg) = self.receiver.recv().await {
            match msg {
                LocalWriterMessage::WritePacket(packet) => {
                    println!("Wrote packet to TUN");
                    self.tun_writer.write_packet(packet).await;
                }
            }
        }
    }
}

#[derive(Clone)]
struct LocalWriterHandle {
    sender: mpsc::Sender<LocalWriterMessage>,
}

impl LocalWriterHandle {
    pub fn new(tun_writer: TunWriter) -> Self {
        let (sender, receiver) = mpsc::channel(100);
        let mut actor = LocalWriter {
            receiver,
            tun_writer,
        };
        tokio::spawn(async move { actor.run().await });
        Self { sender }
    }

    pub async fn send(&self, packet: Packet) {
        self.sender
            .send(LocalWriterMessage::WritePacket(packet))
            .await
            .expect("Failed to send to local writer");
    }
}

// ProtocolSender: Handles sending packets to remote nodes
struct ProtocolSender {
    receiver: mpsc::Receiver<ProtocolSenderMessage>,
    protocol_writer: ProtocolWriter,
}

impl ProtocolSender {
    async fn run(&mut self) {
        while let Some(msg) = self.receiver.recv().await {
            match msg {
                ProtocolSenderMessage::SendPacket(packet) => {
                    self.protocol_writer
                        .send(&packet.buf[0..packet.packet_size])
                        .await;
                }
            }
        }
    }

}

#[derive(Clone)]
struct ProtocolSenderHandle {
    sender: mpsc::Sender<ProtocolSenderMessage>,
}

impl ProtocolSenderHandle {
    pub fn new(protocol_writer: ProtocolWriter) -> Self {
        let (sender, receiver) = mpsc::channel(100);
        let mut actor = ProtocolSender {
            receiver,
            protocol_writer,
        };
        tokio::spawn(async move { actor.run().await });
        Self { sender }
    }
    pub async fn send(&self, packet: Packet) {
        self.sender
            .send(ProtocolSenderMessage::SendPacket(packet))
            .await
            .expect("Failed to send to protocol sender");
    }
}

// Processor: Processes packets and forwards them to the next hop
struct Processor {
    receiver: flume::Receiver<ProcessorMessage>,
    routing_table: RoutingTable,
    local_id: NodeId,
    local_writer_handle: LocalWriterHandle,
    senders: HashMap<NodeId, ProtocolSenderHandle>,
}

impl Processor {
    async fn run(&mut self) {
        while let Ok(msg) = self.receiver.recv_async().await {
            match msg {
                ProcessorMessage::ProcessPacket(packet) => {
                    if let Some(next_hop) = self.routing_table.next_hop(&packet.flow_id) {
                        if *next_hop == self.local_id {
                            self.local_writer_handle
                                .send(packet)
                                .await
                                .expect("Failed to send to local writer");
                        } else if let Some(sender) = self.senders.get(next_hop) {
                            sender.send(packet).await.expect("Failed to send to sender");
                        } else {
                            println!("No sender for node {}", next_hop);
                        }
                    } else {
                        println!("No route for flow {}", packet.flow_id);
                    }
                }
                ProcessorMessage::UpdateRoutingTable(new_table) => {
                    self.routing_table = new_table;
                }
            }
        }
    }
}

#[derive(Clone)]
struct ProcessorHandle {
    sender: flume::Sender<ProcessorMessage>,
}

impl ProcessorHandle {
    
    pub fn new(
        // The size of the mpmc channel
        mpmc_channel_size: usize,

        // The number of processors
        num_processors: usize,
        // A copy of the routing table
        routing_table: RoutingTable,

        // The local id
        local_id: NodeId,
        // The local writer handle
        local_writer: LocalWriterHandle,
        // The protocol senders
        protocol_senders: HashMap<NodeId, ProtocolSenderHandle>,
        
    ) -> Self {
        let (processor_sender, processor_receiver) = bounded(mpmc_channel_size);
        for _ in 0..num_processors {
            let mut actor = Processor {
                receiver: processor_receiver.clone(),
                routing_table: routing_table.clone(),
                local_id,
                local_writer_handle: local_writer.clone(),
                senders: protocol_senders.clone(),
            };
            tokio::spawn(async move { actor.run().await });
        }
        Self { sender: processor_sender }
    }

    pub async fn update_routing_table(&self, new_table: RoutingTable) {
        self.sender
            .send_async(ProcessorMessage::UpdateRoutingTable(new_table))
            .await
            .expect("Failed to send update to processor");
    }
    pub async fn process_packet(&self, packet: Packet) {
        self.sender
            .send_async(ProcessorMessage::ProcessPacket(packet))
            .await
            .expect("Failed to send packet to processor");
    }
}

// Setup function to initialize all actors
async fn setup_actors(
    routing_table: RoutingTable,
    local_id: NodeId,
    tun_writer: TunWriter,
    protocol_writers: HashMap<NodeId, ProtocolWriter>,
    mpmc_channel_size: usize,
) -> ProcessorHandle {
    let local_writer_handle = LocalWriterHandle::new(tun_writer);

    let mut senders = HashMap::new();

    for (node_id, protocol_writer) in protocol_writers {
        let sender_handle = ProtocolSenderHandle::new(protocol_writer);
        senders.insert(node_id, sender_handle);
    }

    ProcessorHandle::new(mpmc_channel_size, routing_table, local_id, local_writer_handle, senders)
}

// Reader task to forward packets to the processor
// TODO :  Add ProtocolReader and ProtocolReaderHandle
async fn run_reader(processor_handle: ProcessorHandle) {
    let mut cnt = 0; 
    loop {
        let mut buf = [0; RECEIVE_BUF_SIZE];
        let packet = Packet::new(RECEIVE_BUF_SIZE, buf);
        processor_handle
            .process_packet(packet)
            .await;

        cnt += 1;
        println!("Packets {} sent to processor", cnt);
        if cnt > 10 {
            break;
        }
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn test_connectivity() {
        let local_id = NodeId::new();
        let routing_table = RoutingTable::new(local_id);
        let tun_writer = TunWriter::new();
        let protocol_writers = HashMap::new(); // Populate with ProtocolWriters
        let processor_handle = setup_actors(routing_table, local_id, tun_writer, protocol_writers, 100).await;
        tokio::spawn(run_reader(processor_handle.clone()));
    }
}
