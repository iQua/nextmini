use std::thread;
use std::time::Instant as StdInstant;

use smoltcp::iface::{Config, Interface, SocketSet};
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
pub struct UserSpaceServerHandle {
    packet_sender: flume::Sender<Packet>,
    packet_receiver: flume::Receiver<Packet>,
    processor_handle: ProcessorHandle,
    config: LocalConfig,
}

impl UserSpaceServerHandle {
    pub fn new(config: LocalConfig, processor_handle: ProcessorHandle) -> Self {
        let (packet_sender, packet_receiver) = flume::bounded(config.channel_capacity);

        info!("Server handle created and thread started.");

        Self {
            packet_sender,
            packet_receiver,
            processor_handle,
            config,
        }
    }

    pub fn add_flows(&self, flows: Vec<Flow>) {
        for flow in flows {
            let config = self.config.clone();
            let processor_handle = self.processor_handle.clone();
            let packet_receiver = self.packet_receiver.clone();

            // spawns a thread
            thread::spawn(move || {
                let server =
                    UserSpaceTcpServer::new(config, flow, processor_handle, packet_receiver);
                server.start();
            });
        }
    }
}

struct UserSpaceTcpServer {
    config: LocalConfig,
    flow: Flow,
    processor_handle: ProcessorHandle,
    packet_receiver: flume::Receiver<Packet>,
    state: ConnectionState,
    listening: bool,
}

impl UserSpaceTcpServer {
    fn new(
        config: LocalConfig,
        flow: Flow,
        processor_handle: ProcessorHandle,
        packet_receiver: flume::Receiver<Packet>,
    ) -> Self {
        info!("Creating user-space TCP server.");

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
            packet_receiver,
            state,
            listening: false,
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

        let server_rx_buffer = tcp::SocketBuffer::new(vec![0; SOCKET_BUFFER_SIZE]);
        let server_tx_buffer = tcp::SocketBuffer::new(vec![0; SOCKET_BUFFER_SIZE]);

        let server_socket = tcp::Socket::new(server_rx_buffer, server_tx_buffer);
        let server_handle = sockets.add(server_socket);

        info!("User-space TCP server initialized, starting main loop.");

        loop {
            let timestamp = Instant::now();

            iface.poll(timestamp, &mut device, &mut sockets);

            let socket = sockets.get_mut::<tcp::Socket>(server_handle);
            self.receive_data(socket);
        }
    }

    fn receive_data(&mut self, socket: &mut tcp::Socket) {
        // receives data from the sockets
        // gets the base server port
        let base_server_port = self.config.user_space_server_port;

        // Start listening
        if !socket.is_active() && !socket.is_listening() && !self.listening {
            // listens on the base server port
            match socket.listen(base_server_port) {
                Ok(_) => {
                    info!("Server listening on port {}", base_server_port);
                    // sets the listening state to true
                    self.listening = true;
                }
                Err(e) => {
                    error!("Server failed to listen: {:?}", e);
                }
            }
        }

        // if the socket is active and haven't connected
        if socket.is_active() {
            if !self.state.connected {
                self.state.connected = true;
                info!("Server accepted connection");
            }

            // checks if there is data to receive
            if socket.can_recv() {
                // receives data from the socket
                match socket.recv(|buf| {
                    let len = buf.len();
                    (len, len)
                }) {
                    // if data received, print out the throughput
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
                            info!("Server received all flow data");
                            socket.close();
                        }
                    }
                    Err(e) => {
                        error!("Server receives error: {:?}.", e);
                    }
                    Ok(_) => {}
                }
            }
        }
    }
}

impl LocalDestination for UserSpaceServerHandle {
    fn send_packet(&self, packet: Packet) {
        if let Err(e) = self.packet_sender.try_send(packet) {
            error!("Failed to send packet to user-space TCP server: {:?}.", e);
        }
    }
}
