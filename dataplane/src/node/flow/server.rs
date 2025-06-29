// A TCP server for user-space flows, implemented using SmolTcp.
use ahash::AHashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::thread;
use std::time::Instant as StdInstant;

use smoltcp::iface::{Config, Interface, SocketHandle, SocketSet};
use smoltcp::socket::tcp;
use smoltcp::time::Instant;
use smoltcp::wire::{HardwareAddress, IpAddress, IpCidr};
use tokio::sync::mpsc;
use tracing::{error, info};

use crate::node::config::LocalConfig;
use crate::node::flow::device::VirtualDevice;
use crate::node::flow::state::ConnectionState;
use crate::node::flow::{SOCKET_BUFFER_SIZE, UserSpaceSender};
use crate::node::packet::Packet;
use crate::node::processor::ProcessorHandle;
use crate::node::{FlowId, FlowIdExt, NodeIdExt};

#[derive(Debug, Clone)]
pub struct UserSpaceServerHandle {
    config: LocalConfig,
    processor_handle: ProcessorHandle,
    packet_senders: Arc<Mutex<AHashMap<FlowId, mpsc::Sender<Packet>>>>,
}

impl UserSpaceServerHandle {
    pub fn new(config: LocalConfig, processor_handle: ProcessorHandle) -> Self {
        Self {
            config,
            processor_handle,
            packet_senders: Arc::new(Mutex::new(AHashMap::new())),
        }
    }

    // starts a new server thread for a given flow.
    pub fn add_server(&self, flow_id: FlowId) -> UserSpaceSender {
        let mut senders = self.packet_senders.lock().unwrap();
        if let Some(existing_sender) = senders.get(&flow_id) {
            return existing_sender.clone();
        }

        let (packet_sender, packet_receiver) = mpsc::channel(self.config.channel_capacity);

        // insert into HashMap to track this server
        senders.insert(flow_id, packet_sender.clone());

        let sender: UserSpaceSender = packet_sender.clone();
        self.processor_handle
            .connect_user_space_sender(flow_id, sender);

        let config = self.config.clone();
        let processor_handle = self.processor_handle.clone();

        let server = UserSpaceServer::new(config, flow_id, processor_handle, packet_receiver);

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
    processor_handle: ProcessorHandle,
    packet_receiver: Option<mpsc::Receiver<Packet>>,
    state: ConnectionState,
}

impl UserSpaceServer {
    fn new(
        config: LocalConfig,
        flow_id: FlowId,
        processor_handle: ProcessorHandle,
        packet_receiver: mpsc::Receiver<Packet>,
    ) -> Self {
        info!("Creating a new user-space TCP server for a single flow.");

        let state = ConnectionState {
            start_time: StdInstant::now(),
            time_last_updated: StdInstant::now(),
            bytes_last_updated: 0,
            bytes_total: 0,
        };

        Self {
            config,
            flow_id,
            processor_handle,
            packet_receiver: Some(packet_receiver),
            state,
        }
    }

    fn run(mut self) {
        let packet_receiver = self.packet_receiver.take().unwrap();

        let mut device = VirtualDevice {
            config: self.config.clone(),
            receiver: packet_receiver,
            sender: self.processor_handle.clone(),
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

        let server_rx_buffer = tcp::SocketBuffer::new(vec![0; SOCKET_BUFFER_SIZE]);
        let server_tx_buffer = tcp::SocketBuffer::new(vec![0; SOCKET_BUFFER_SIZE]);
        let server_socket = tcp::Socket::new(server_rx_buffer, server_tx_buffer);
        let socket_handle = sockets.add(server_socket);

        let socket = sockets.get_mut::<tcp::Socket>(socket_handle);

        // Set up listening socket if not already done.
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
            self.recv(&mut sockets, socket_handle);

            let now = Instant::now();
            match iface.poll_at(now, &sockets) {
                Some(poll_at) if now < poll_at => {
                    // waits for an incoming packet
                    let _ = device.receiver.recv();
                }
                Some(_) => {
                    // smoltcp wants to be polled immediately
                    continue;
                }
                None => {
                    // waits for an incoming packet
                    let _ = device.receiver.recv();
                }
            }
        }
    }

    fn recv(&mut self, sockets: &mut SocketSet, handle: SocketHandle) {
        let socket = sockets.get_mut::<tcp::Socket>(handle);

        if socket.can_recv() {
            match socket.recv(|buf| (buf.len(), buf.len())) {
                Ok(received) if received > 0 => {
                    let src_ip = self.flow_id.src_ip();
                    let dst_ip = self.flow_id.dst_ip();

                    let src_node_id = self.config.ip_to_node_id(src_ip);
                    let dst_node_id = self.config.ip_to_node_id(dst_ip);

                    self.state.update(dst_node_id, src_node_id, received as u64);
                }
                Err(e) => {
                    error!("Error receiving from a user-space TCP client: {:?}", e);
                }
                _ => {}
            }
        }
    }
}
