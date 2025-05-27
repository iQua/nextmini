use crate::dataplane::local_interface::TunWriter;
use crate::dataplane::packet::Packet;
use crate::dataplane::protocols_io::{ProtocolReader, ProtocolWriter};
use crate::dataplane::routes::RoutingTable;
use crate::dataplane::{FlowId, NodeId, RECEIVE_BUF_SIZE};
use std::collections::HashMap;
use tokio::sync::mpsc;

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

    pub async fn send(&self, packet: Packet) {
        self.sender
            .send(ProtocolSenderMessage::SendPacket(packet))
            .await
            .expect("Failed to send to protocol sender");
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
}

// Processor: Processes packets and forwards them to the next hop
struct Processor {
    receiver: mpsc::Receiver<ProcessorMessage>,
    routing_table: RoutingTable,
    local_id: NodeId,
    local_writer_handle: LocalWriterHandle,
    senders: HashMap<NodeId, ProtocolSenderHandle>,
}

impl Processor {
    async fn run(&mut self) {
        while let Some(msg) = self.receiver.recv().await {
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
    sender: mpsc::Sender<ProcessorMessage>,
}

impl ProcessorHandle {
    pub fn new(
        routing_table: RoutingTable,
        local_id: NodeId,
        local_writer: LocalWriterHandle,
        protocol_senders: HashMap<NodeId, ProtocolSenderHandle>,
    ) -> Self {
        let (sender, receiver) = mpsc::channel(100);
        let mut actor = Processor {
            receiver,
            routing_table,
            local_id,
            local_writer_sender,
            senders,
        };
        tokio::spawn(async move { actor.run().await });
        Self { sender }
    }

    pub async fn update_routing_table(&self, new_table: RoutingTable) {
        self.sender
            .send(ProcessorMessage::UpdateRoutingTable(new_table))
            .await
            .expect("Failed to send update to processor");
    }
}

// Setup function to initialize all actors
async fn setup_actors(
    routing_table: RoutingTable,
    local_id: NodeId,
    tun_writer: TunWriter,
    protocol_writers: HashMap<NodeId, ProtocolWriter>,
) -> ProcessorHandle {
    let local_writer_handle = LocalWriterHandle::new(tun_writer);

    let mut senders = HashMap::new();

    for (node_id, protocol_writer) in protocol_writers {
        let sender_handle = SenderHandle::new(protocol_writer);
        senders.insert(node_id, sender_handle);
    }

    ProcessorHandle::new(routing_table, local_id, local_writer_handle, sender_handle)
}

// Reader task to forward packets to the processor
async fn run_reader(mut reader: ProtocolReader, processor_handle: ProcessorHandle) {
    loop {
        let mut buf = [0; RECEIVE_BUF_SIZE];
        let n = reader.recv(&mut buf).await;
        let packet = Packet::new(n, buf);
        processor_handle
            .sender
            .send(ProcessorMessage::ProcessPacket(packet))
            .await
            .expect("Failed to send packet to processor");
    }
}

// Example usage in main
/*
#[tokio::main]
async fn main() {
    let local_id = NodeId::new();
    let routing_table = RoutingTable::new(local_id);
    let tun_writer = TunWriter::new();
    let protocol_writers = HashMap::new(); // Populate with ProtocolWriters
    let processor_handle = setup_actors(routing_table, local_id, tun_writer, protocol_writers).await;
    let reader = ProtocolReader::new();
    tokio::spawn(run_reader(reader, processor_handle.clone()));
    // Additional logic here
}
*/
