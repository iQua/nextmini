use std::thread;
use std::time::Instant as StdInstant;

use smoltcp::iface::{Config, Interface, SocketHandle, SocketSet};
use smoltcp::socket::tcp;
use smoltcp::time::Instant;
use smoltcp::wire::{HardwareAddress, IpAddress, IpCidr};
use tracing::{error, info};

use crate::node::config::LocalConfig;
use crate::node::flow::device::VirtualDevice;
use crate::node::flow::state::ConnectionState;
use crate::node::packet::Packet;
use crate::node::processor::ProcessorHandle;
use crate::node::{LocalDestination, NodeIdExt};
use nextmini_messages::Flow;

// socket buffer 655350 by default
const SOCKET_BUFFER_SIZE: usize = 655350;

// still handles multiple sockets for a single thread for now.
#[derive(Clone, Debug)]
pub struct ServerHandle {
    flow_sender: flume::Sender<Flow>,
    packet_sender: flume::Sender<Packet>,
}

impl ServerHandle {
    pub fn new(config: LocalConfig, processor_handle: ProcessorHandle) -> Self {
        let (flow_sender, flow_receiver) = flume::unbounded();
        let (packet_sender, packet_receiver) = flume::bounded(config.channel_capacity);

        let initial_flows = Vec::new();
        let initial_flow_count = initial_flows.len();

        // spawns a thread
        thread::spawn(move || {
            let server = UserSpaceTcpServer::new(
                config,
                processor_handle,
                flow_receiver,
                packet_receiver,
                initial_flows,
            );

            server.start();
        });

        info!(
            "Server handle created and thread started with {} initial flows",
            initial_flow_count
        );

        Self {
            flow_sender,
            packet_sender,
        }
    }

    pub fn add_flow(&self, flow: Flow) -> Result<(), String> {
        self.flow_sender
            .send(flow)
            .map_err(|e| format!("Failed to send flow to server: {}", e))
    }
}

struct UserSpaceTcpServer {
    config: LocalConfig,
    processor_handle: ProcessorHandle,
    flow_receiver: flume::Receiver<Flow>,
    packet_receiver: flume::Receiver<Packet>,
    handles: Vec<SocketHandle>,
    states: Vec<ConnectionState>,
    listening: Vec<bool>,
    flows: Vec<Flow>,
}

impl UserSpaceTcpServer {
    fn new(
        config: LocalConfig,
        processor_handle: ProcessorHandle,
        flow_receiver: flume::Receiver<Flow>,
        packet_receiver: flume::Receiver<Packet>,
        initial_flows: Vec<Flow>,
    ) -> Self {
        info!(
            "Creating user-space TCP server with {} initial flows",
            initial_flows.len()
        );

        let states = (0..initial_flows.len())
            .map(|_| ConnectionState {
                connected: false,
                start_time: StdInstant::now(),
                time_last_updated: StdInstant::now(),
                bytes_last_updated: 0,
                bytes_total: 0,
            })
            .collect();

        let listening = vec![false; initial_flows.len()];

        Self {
            config,
            processor_handle,
            flow_receiver,
            packet_receiver,
            handles: Vec::new(),
            states,
            listening,
            flows: initial_flows,
        }
    }

    // adds new incoming flows to the server
    pub fn add_flows(&mut self, sockets: &mut SocketSet) {
        while let Ok(flow) = self.flow_receiver.try_recv() {
            let i = self.handles.len();
            let server_rx_buffer = tcp::SocketBuffer::new(vec![0; SOCKET_BUFFER_SIZE]);
            let server_tx_buffer = tcp::SocketBuffer::new(vec![0; SOCKET_BUFFER_SIZE]);

            let server_socket = tcp::Socket::new(server_rx_buffer, server_tx_buffer);

            let server_handle = sockets.add(server_socket);
            self.handles.push(server_handle);

            info!("Created server socket {}.", i);

            self.states.push(ConnectionState {
                connected: false,
                start_time: StdInstant::now(),
                time_last_updated: StdInstant::now(),
                bytes_last_updated: 0,
                bytes_total: 0,
            });
            self.listening.push(false);
            self.flows.push(flow);

            info!("Added new server socket {} for incoming flow.", i);
        }
    }

    fn start(mut self) {
        info!("Starts running server.");

        let packet_receiver = self.packet_receiver.clone();
        let mut device = VirtualDevice {
            config: self.config.clone(),
            receiver: packet_receiver,
            sender: self.processor_handle.clone(),
        };

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

        // creates sockets for initial flows
        for i in 0..self.flows.len() {
            let server_rx_buffer = tcp::SocketBuffer::new(vec![0; SOCKET_BUFFER_SIZE]);
            let server_tx_buffer = tcp::SocketBuffer::new(vec![0; SOCKET_BUFFER_SIZE]);

            let server_socket = tcp::Socket::new(server_rx_buffer, server_tx_buffer);

            let server_handle = sockets.add(server_socket);
            self.handles.push(server_handle);

            info!("Created server socket {} for initial flow.", i);
        }

        info!("User-space TCP server initialized, starting main loop.");

        loop {
            let timestamp = Instant::now();

            iface.poll(timestamp, &mut device, &mut sockets);
            self.add_flows(&mut sockets);
            self.receive_data(&mut sockets);
        }
    }

    fn receive_data(&mut self, sockets: &mut SocketSet) {
        let base_server_port = self.config.user_space_server_port;

        for (i, &server_handle) in self.handles.iter().enumerate() {
            let socket = sockets.get_mut::<tcp::Socket>(server_handle);

            // Start listening
            if !socket.is_active() && !socket.is_listening() && !self.listening[i] {
                match socket.listen(base_server_port) {
                    Ok(_) => {
                        info!("Server {} listening on port {}", i, base_server_port);
                        self.listening[i] = true;
                    }
                    Err(e) => {
                        error!("Server {} failed to listen: {:?}", i, e);
                    }
                }
            }

            // if the socket is active and haven't connected
            if socket.is_active() {
                if !self.states[i].connected {
                    self.states[i].connected = true;
                    info!("Server {} accepted connection", i);
                }

                // server starts receives data
                if socket.can_recv() {
                    match socket.recv(|buffer| {
                        let len = buffer.len();
                        (len, len)
                    }) {
                        // if data received, print out the throughput
                        Ok(received) if received > 0 => {
                            self.states[i].test_throughput("Server", i, received as u64);

                            if self.flows[i]
                                .flow_size
                                .exceeded(self.states[i].bytes_total, self.states[i].start_time)
                            {
                                info!("Server {} received all flow data", i);
                                socket.close();
                            }
                        }
                        Err(e) => {
                            error!("Server {} recv error: {:?}", i, e);
                        }
                        Ok(_) => {}
                    }
                }
            }
        }
    }
}

impl LocalDestination for ServerHandle {
    fn send_packet(&self, packet: Packet) {
        if let Err(e) = self.packet_sender.try_send(packet) {
            error!("Failed to send packet to user-space TCP server: {:?}.", e);
        }
    }
}
