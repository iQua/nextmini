// A TCP server for user-space flows, implemented using SmolTcp.
use std::sync::Arc;
use std::thread;
use std::time::Instant as StdInstant;

use smoltcp::iface::{Config, Interface, SocketHandle, SocketSet};
use smoltcp::socket::tcp;
use smoltcp::time::Instant;
use smoltcp::wire::{HardwareAddress, IpAddress, IpCidr};
use tracing::{error, info};

use nextmini_messages::Flow;

use crate::node::NodeIdExt;
use crate::node::config::LocalConfig;
use crate::node::flow::SOCKET_BUFFER_SIZE;
use crate::node::flow::device::{Receiver, VirtualDevice};
use crate::node::flow::state::ConnectionState;
use crate::node::packet::Packet;
use crate::node::processor::ProcessorHandle;

use flume;

#[derive(Debug, Clone)]
pub struct UserSpaceServerHandle {
    processor_handle: ProcessorHandle,
    config: LocalConfig,
}

impl UserSpaceServerHandle {
    pub fn new(config: LocalConfig, processor_handle: ProcessorHandle) -> Self {
        Self {
            config,
            processor_handle,
        }
    }

    pub fn add_flows(&self, flows: Vec<Flow>) {
        if flows.is_empty() {
            return;
        }

        let (packet_sender, packet_receiver) = flume::bounded(self.config.channel_capacity);

        self.processor_handle
            .connect_local_destination(self.config.user_space_server_port, Arc::new(packet_sender));

        // spawns a thread for each flow, all sharing the same flume receiver
        for flow in flows {
            let config = self.config.clone();
            let processor_handle = self.processor_handle.clone();
            let receiver_clone = packet_receiver.clone();

            thread::spawn(move || {
                let server = UserSpaceServer::new(config, flow, processor_handle, receiver_clone);

                server.run();
            });
        }
    }
}

struct UserSpaceServer {
    config: LocalConfig,
    flow: Flow,
    processor_handle: ProcessorHandle,
    packet_receiver: Option<flume::Receiver<Packet>>,
    state: ConnectionState,
    listening: bool,
}

impl UserSpaceServer {
    fn new(
        config: LocalConfig,
        flow: Flow,
        processor_handle: ProcessorHandle,
        packet_receiver: flume::Receiver<Packet>,
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
            flow,
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
            receiver: Receiver::Flume(packet_receiver),
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
                    self.state.test_throughput(
                        self.flow.src_node_id,
                        self.flow.src_node_id,
                        None,
                        received as u64,
                    );

                    if self
                        .flow
                        .flow_size
                        .exceeded(self.state.bytes_total, self.state.start_time)
                    {
                        info!("A user-space TCP server has finished receiving all its data.");
                        socket.close();
                    }
                }
                Err(e) => {
                    error!("Error receiving from a user-space TCP client: {:?}", e);
                }
                _ => {}
            }
        }
    }
}
