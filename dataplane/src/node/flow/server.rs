use std::sync::Arc;
use std::thread;
use std::time::Instant as StdInstant;

use smoltcp::iface::{Config, Interface, SocketHandle, SocketSet};
use smoltcp::socket::tcp;
use smoltcp::time::Instant;
use smoltcp::wire::{HardwareAddress, IpAddress, IpCidr};
use tracing::{error, info};

use crate::node::NodeIdExt;
use crate::node::config::LocalConfig;
use crate::node::flow::device::VirtualDevice;
use crate::node::flow::router::PacketRouter;
use crate::node::flow::state::ConnectionState;
use crate::node::packet::Packet;
use crate::node::processor::ProcessorHandle;
use nextmini_messages::Flow;

// socket buffer 65535000 by default
const SOCKET_BUFFER_SIZE: usize = 65535000;

// still handles multiple sockets for a single thread for now.
#[derive(Clone, Debug)]
pub struct ServerHandle {
    // sends flows to the server thread
    flow_sender: flume::Sender<Flow>,
}

impl ServerHandle {
    // creates a new server handle
    pub fn new(
        config: LocalConfig,
        processor_handle: ProcessorHandle,
        router: Arc<PacketRouter>,
    ) -> Self {
        // creates a channel to send flows to the server thread
        let (flow_sender, flow_receiver) = flume::unbounded();
        // creates a channel to receive packets from the router
        let (packet_sender, packet_receiver) = flume::bounded(config.channel_capacity);

        // Register the server's packet queue with the central router
        router.register_server(packet_sender);

        let initial_flows = Vec::new();
        let initial_flow_count = initial_flows.len();

        // spawns a thread to run the server
        thread::spawn(move || {
            // creates a new user-space TCP server
            let server = UserSpaceTcpServer::new(
                config,
                processor_handle,
                flow_receiver,
                packet_receiver,
                initial_flows,
            );

            // starts the server
            server.start();
        });

        info!(
            "Server handle created and thread started with {} initial flows",
            initial_flow_count
        );

        Self { flow_sender }
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
    // receives flows from the main thread
    flow_receiver: flume::Receiver<Flow>,
    // receives packets from the router
    packet_receiver: flume::Receiver<Packet>,
    // socket handles for each flow
    handles: Vec<SocketHandle>,
    // connection states for each flow
    states: Vec<ConnectionState>,
    // whether each socket is listening
    listening: Vec<bool>,
    flows: Vec<Flow>,
}

impl UserSpaceTcpServer {
    // creates a new user-space TCP server
    fn new(
        config: LocalConfig,
        processor_handle: ProcessorHandle,
        flow_receiver: flume::Receiver<Flow>,
        packet_receiver: flume::Receiver<Packet>,
        initial_flows: Vec<Flow>,
    ) -> Self {
        info!(
            "Creating user-space TCP server with {} flows",
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

    fn start(self) {
        let Self {
            config,
            processor_handle,
            flow_receiver,
            packet_receiver,
            mut handles,
            mut states,
            mut listening,
            mut flows,
        } = self;

        info!("Starts running server.");

        let mut device = VirtualDevice {
            config: config.clone(),
            receiver: packet_receiver,
            sender: processor_handle,
        };

        let iface_config = Config::new(HardwareAddress::Ip);
        let ip_addr = config
            .node_id
            .ip_addr(config.user_space_base_addr, config.local_netmask);

        let mut iface = Interface::new(iface_config, &mut device, Instant::now());
        iface.update_ip_addrs(|addrs| {
            addrs
                .push(IpCidr::new(IpAddress::from(ip_addr), 24))
                .unwrap();
        });

        let mut sockets = SocketSet::new(vec![]);

        // creates sockets for initial flows
        for i in 0..flows.len() {
            let server_rx_buffer = tcp::SocketBuffer::new(vec![0; SOCKET_BUFFER_SIZE]);
            let server_tx_buffer = tcp::SocketBuffer::new(vec![0; SOCKET_BUFFER_SIZE]);

            let server_socket = tcp::Socket::new(server_rx_buffer, server_tx_buffer);

            let server_handle = sockets.add(server_socket);
            handles.push(server_handle);

            info!("Created server socket {} for initial flow.", i);
        }

        info!("User-space TCP server initialized, starting main loop.");

        loop {
            let timestamp = Instant::now();

            iface.poll(timestamp, &mut device, &mut sockets);

            // adds new incoming flows to the server
            // while there are new flows
            while let Ok(flow) = flow_receiver.try_recv() {
                // gets the index of the new flow
                let i = handles.len();
                // creates a new server receive buffer
                let server_rx_buffer = tcp::SocketBuffer::new(vec![0; SOCKET_BUFFER_SIZE]);
                // creates a new server transmit buffer
                let server_tx_buffer = tcp::SocketBuffer::new(vec![0; SOCKET_BUFFER_SIZE]);

                // creates a new TCP socket
                let server_socket = tcp::Socket::new(server_rx_buffer, server_tx_buffer);

                // adds the socket to the socket set
                let server_handle = sockets.add(server_socket);
                handles.push(server_handle);

                info!("Created server socket {}.", i);

                // adds a new connection state
                states.push(ConnectionState {
                    connected: false,
                    start_time: StdInstant::now(),
                    time_last_updated: StdInstant::now(),
                    bytes_last_updated: 0,
                    bytes_total: 0,
                });
                // adds a new listening state
                listening.push(false);
                // adds the new flow
                flows.push(flow);

                info!("Added new server socket {} for incoming flow.", i);
            }

            // receives data from the sockets
            // gets the base server port
            let base_server_port = config.user_space_server_port;

            for (i, &server_handle) in handles.iter().enumerate() {
                // gets a mutable reference to the socket
                let socket = sockets.get_mut::<tcp::Socket>(server_handle);

                // Start listening
                if !socket.is_active() && !socket.is_listening() && !listening[i] {
                    // listens on the base server port
                    match socket.listen(base_server_port) {
                        Ok(_) => {
                            info!("Server {} listening on port {}", i, base_server_port);
                            // sets the listening state to true
                            listening[i] = true;
                        }
                        Err(e) => {
                            error!("Server {} failed to listen: {:?}", i, e);
                        }
                    }
                }

                // checks if the socket is closed
                if socket.state() == tcp::State::Closed {
                    info!("Server {} socket closed, removing.", i);
                    continue;
                }

                // if the socket is active and haven't connected
                if socket.is_active() {
                    if !states[i].connected {
                        states[i].connected = true;
                        info!("Server {} accepted connection", i);
                    }

                    // checks if there is data to receive
                    if socket.can_recv() {
                        // receives data from the socket
                        match socket.recv(|buf| {
                            let len = buf.len();
                            (len, len)
                        }) {
                            Ok(received) if received > 0 => {
                                if !states[i].connected {
                                    states[i].connected = true;
                                    info!("Server {} accepted connection", i);
                                }
                                states[i].test_throughput("Server", i, received as u64);

                                if flows[i]
                                    .flow_size
                                    .exceeded(states[i].bytes_total, states[i].start_time)
                                {
                                    info!("Server {} received all flow data", i);
                                    socket.close();
                                }
                            }
                            Err(e) => {
                                error!("Server {} receive error: {:?}", i, e);
                            }
                            Ok(_) => {}
                        }
                    }
                }
            }
        }
    }
}
