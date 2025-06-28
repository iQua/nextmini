// A TCP server for user-space flows, implemented using SmolTcp.
use std::sync::Arc;
use std::thread;
use std::time::Instant as StdInstant;

use smoltcp::iface::{Config, Interface, SocketHandle, SocketSet};
use smoltcp::socket::tcp;
use smoltcp::time::Instant;
use smoltcp::wire::{HardwareAddress, IpAddress, IpCidr};
use tokio::sync::mpsc;
use tracing::{error, info};

use crate::node::config::LocalConfig;
use crate::node::flow::SOCKET_BUFFER_SIZE;
use crate::node::flow::device::VirtualDevice;
use crate::node::flow::state::ConnectionState;
use crate::node::packet::Packet;
use crate::node::processor::ProcessorHandle;
use crate::node::{FlowId, FlowIdExt, NodeIdExt};

#[derive(Debug, Clone)]
pub struct UserSpaceServerHandle {}

impl UserSpaceServerHandle {
    pub fn new(config: LocalConfig, processor_handle: ProcessorHandle, flow_id: FlowId) -> Self {
        let (packet_sender, packet_receiver) = mpsc::channel(config.channel_capacity);

        // server as a local destination for packets destined to this flow
        processor_handle.connect_local_destination(flow_id, Arc::new(packet_sender));

        // spawn a single thread for one server/flow
        thread::spawn(move || {
            let server = UserSpaceServer::new(config, flow_id, processor_handle, packet_receiver);
            server.run();
        });

        Self {}
    }
}

struct UserSpaceServer {
    config: LocalConfig,
    flow_id: FlowId,
    processor_handle: ProcessorHandle,
    packet_receiver: Option<mpsc::Receiver<Packet>>,
    state: ConnectionState,
    listening: bool,
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
            connected: false,
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
            listening: false,
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

        loop {
            let timestamp = Instant::now();

            iface.poll(timestamp, &mut device, &mut sockets);
            self.recv(&mut sockets, socket_handle);
        }
    }

    fn recv(&mut self, sockets: &mut SocketSet, handle: SocketHandle) {
        let base_server_port = self.config.user_space_server_port;

        // Set up listening sockets if not already done
        if !self.listening {
            let socket = sockets.get_mut::<tcp::Socket>(handle);
            if !socket.is_listening() {
                if let Err(e) = socket.listen(base_server_port) {
                    error!(
                        "Server failed to listen on port {}: {:?}",
                        base_server_port, e
                    );
                }
            }

            self.listening = true;
        }

        let socket = sockets.get_mut::<tcp::Socket>(handle);

        if socket.is_active() && !self.state.connected {
            self.state.connected = true;
        }

        if socket.can_recv() {
            match socket.recv(|buf| (buf.len(), buf.len())) {
                Ok(received) if received > 0 => {
                    let src_ip = self.flow_id.src_ip();
                    let dst_ip = self.flow_id.dst_ip();

                    let src_node_id = self.config.ip_to_node_id(src_ip);
                    let dst_node_id = self.config.ip_to_node_id(dst_ip);

                    self.state.test_throughput(
                        "server", // node serves as a server
                        dst_node_id,
                        src_node_id,
                        self.flow_id.dst_port(),
                        received as u64,
                    );
                }
                Err(e) => {
                    error!("Error receiving from a user-space TCP client: {:?}", e);
                }
                _ => {}
            }
        }
    }
}
