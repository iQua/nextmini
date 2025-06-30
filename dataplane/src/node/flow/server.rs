// A TCP server for user-space flows, implemented using SmolTcp.
use ahash::AHashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::thread;
use std::time::Duration;

use smoltcp::iface::{Config, Interface, SocketSet};
use smoltcp::socket::tcp;
use smoltcp::time::Instant;
use smoltcp::wire::{HardwareAddress, IpAddress, IpCidr};
use tokio::sync::mpsc;
use tracing::{error, info};

use crate::node::config::LocalConfig;
use crate::node::flow::device::VirtualDevice;
use crate::node::flow::{SOCKET_BUFFER_SIZE, UserSpaceSender};
use crate::node::packet::Packet;
use crate::node::processor::ProcessorHandle;
use crate::node::{FlowId, FlowIdExt, NodeIdExt};

#[derive(Debug, Clone)]
pub struct UserSpaceServerHandle {
    config: LocalConfig,

    processors: ProcessorHandle,

    // a hashmap of flow IDs to packet channel senders needs to be maintained since multiple processors
    // may request adding a new server for the same flow ID concurrently, but the new server thread should
    // only be created once for each flow ID. Subsequent requests will be served by consulting this hashmap.
    // This hashmap also needs to be shared across all processor tasks in a thread-safe way.
    packet_senders: Arc<Mutex<AHashMap<FlowId, mpsc::Sender<Packet>>>>,
}

impl UserSpaceServerHandle {
    pub fn new(config: LocalConfig, processors: ProcessorHandle) -> Self {
        Self {
            config,
            processors,
            packet_senders: Arc::new(Mutex::new(AHashMap::new())),
        }
    }

    // Starts a new server thread for a user-space TCP flow.
    pub fn add_server(&self, flow_id: FlowId) -> UserSpaceSender {
        // consults the shared hashmap for channels that may have just been created
        let mut senders = self.packet_senders.lock().unwrap();
        if let Some(existing_sender) = senders.get(&flow_id) {
            return existing_sender.clone();
        }

        // creates a new channel for processors to send to the user-space TCP server
        let (packet_sender, packet_receiver) = mpsc::channel(self.config.channel_capacity);

        // inserts into the shared hashmap for later retrieval, if the same flow ID is requested
        senders.insert(flow_id, packet_sender.clone());

        let sender: UserSpaceSender = packet_sender.clone();
        self.processors.connect_user_space_sender(flow_id, sender);

        let config = self.config.clone();
        let processors = self.processors.clone();

        let server = UserSpaceServer::new(config, flow_id, processors, packet_receiver);

        // spawns a new server thread for each user-space TCP flow
        thread::spawn(move || {
            server.run();
        });

        packet_sender
    }
}

struct UserSpaceServer {
    config: LocalConfig,
    flow_id: FlowId,
    processors: ProcessorHandle,
    packet_receiver: Option<mpsc::Receiver<Packet>>,
}

impl UserSpaceServer {
    fn new(
        config: LocalConfig,
        flow_id: FlowId,
        processors: ProcessorHandle,
        packet_receiver: mpsc::Receiver<Packet>,
    ) -> Self {
        info!("Creating a new user-space TCP server for a single flow.");

        Self {
            config,
            flow_id,
            processors,
            packet_receiver: Some(packet_receiver),
        }
    }

    fn run(mut self) {
        let packet_receiver = self.packet_receiver.take().unwrap();

        let mut device = VirtualDevice {
            config: self.config.clone(),
            receiver: packet_receiver,
            sender: self.processors.clone(),
        };

        // sets up Layer 3 using the provided IP address, without needing a hardware address
        let config = Config::new(HardwareAddress::Ip);
        let ip_addr = self
            .config
            .node_id
            .ip_addr(self.config.user_space_base_addr, self.config.local_netmask);

        let mut iface = Interface::new(config, &mut device, Instant::now());
        iface.update_ip_addrs(|addrs| {
            addrs
                .push(IpCidr::new(IpAddress::from(ip_addr), 24))
                .unwrap();
        });

        let mut sockets = SocketSet::new(vec![]);

        let rx_buffer = tcp::SocketBuffer::new(vec![0; SOCKET_BUFFER_SIZE]);
        let tx_buffer = tcp::SocketBuffer::new(vec![0; SOCKET_BUFFER_SIZE]);
        let socket = tcp::Socket::new(rx_buffer, tx_buffer);
        let socket_handle = sockets.add(socket);

        let socket = sockets.get_mut::<tcp::Socket>(socket_handle);

        // listens on the socket
        if !socket.is_open() {
            if let Err(e) = socket.listen(self.flow_id.dst_port()) {
                error!(
                    "Server failed to listen on port {}: {:?}",
                    self.flow_id.dst_port(),
                    e
                );
            }
        }

        loop {
            let timestamp = Instant::now();

            iface.poll(timestamp, &mut device, &mut sockets);

            let socket = sockets.get_mut::<tcp::Socket>(socket_handle);

            if socket.is_active() {
                self.recv(socket);
            } else {
                // removes the packet sender from the processors
                self.processors.disconnect_user_space_sender(self.flow_id);

                info!(
                    "The user-space TCP server on node {} has terminated. It has been receiving from node {}.",
                    self.config.node_id,
                    self.config.ip_to_node_id(self.flow_id.src_ip())
                );
                break;
            }

            if device.receiver.is_empty() {
                thread::sleep(Duration::from_nanos(1));
            }
        }
    }

    fn recv(&mut self, socket: &mut tcp::Socket) {
        if socket.can_recv() {
            match socket.recv(|buf| (buf.len(), buf.len())) {
                Err(e) => {
                    error!("Error receiving from a user-space TCP client: {:?}", e);
                }
                _ => {}
            }
        }
    }
}
