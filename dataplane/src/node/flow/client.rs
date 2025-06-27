use std::cmp;
use std::sync::Arc;
use std::sync::atomic::{AtomicU16, Ordering};
use std::thread;
use std::time::Instant as StdInstant;

use smoltcp::iface::SocketSet;
use smoltcp::socket::tcp;
use smoltcp::time::Instant;
use smoltcp::wire::IpAddress;
use tracing::{error, info};

use crate::node::NodeIdExt;
use crate::node::config::LocalConfig;
use crate::node::flow::device::VirtualDevice;
use crate::node::flow::router::PacketRouter;
use crate::node::flow::state::ConnectionState;
use crate::node::flow::tcp::{create_interface, SOCKET_BUFFER_SIZE};
use crate::node::packet::Packet;
use crate::node::processor::ProcessorHandle;
use nextmini_messages::{Flow, FlowSpec};

// creates a new thread for each flow
pub struct ClientHandle {
    config: LocalConfig,
    processor_handle: ProcessorHandle,
    router: Arc<PacketRouter>,
    // assigns a unique port to each client
    next_client_port: Arc<AtomicU16>,
}

impl ClientHandle {
    pub fn new(
        config: LocalConfig,
        processor_handle: ProcessorHandle,
        router: Arc<PacketRouter>,
    ) -> Self {
        // starts client ports from user-space base client port
        let next_client_port = Arc::new(AtomicU16::new(config.user_space_client_port));
        Self {
            config,
            processor_handle,
            router,
            next_client_port,
        }
    }

    pub fn add_flows(&self, flows: Vec<Flow>) {
        for flow in flows {
            let config = self.config.clone();
            let processor_handle = self.processor_handle.clone();
            // gets the next available client port
            let client_port = self.next_client_port.fetch_add(1, Ordering::SeqCst);
            let router = self.router.clone();

            // creates a new channel for the client
            let (packet_sender, packet_receiver) = flume::bounded(config.channel_capacity);
            // registers the client with the router
            self.router.register_client(client_port, packet_sender);

            // spawns a new thread as smoltcp is not designed to use async Rust and Tokio
            thread::spawn(move || {
                // creates a new user-space TCP client
                let client = UserSpaceTcpClient::new(
                    config,
                    flow,
                    processor_handle,
                    client_port,
                    packet_receiver,
                );
                // starts the client
                client.start();

                // deregisters the client when done
                router.deregister_client(client_port);
            });

            info!(
                "Client handle created and thread started for flow on port {}.",
                client_port
            );
        }
    }
}

struct ClientState {
    connection_state: ConnectionState,
    connecting: bool,
    flow: Flow,
    client_port: u16,
}

impl ClientState {
    fn new(flow: Flow, client_port: u16) -> Self {
        Self {
            connection_state: ConnectionState {
                connected: false,
                start_time: StdInstant::now(),
                time_last_updated: StdInstant::now(),
                bytes_last_updated: 0,
                bytes_total: 0,
            },
            connecting: false,
            flow,
            client_port,
        }
    }

    fn try_connect(&mut self, socket: &mut tcp::Socket, iface_context: &mut smoltcp::iface::Context, config: &LocalConfig) {
        // if the socket is not open and we are not connecting
        if !socket.is_open() && !self.connecting {
            // gets the remote address
            let remote_addr = IpAddress::from(
                self.flow.dst_node_id
                    .ip_addr(config.user_space_base_addr, config.local_netmask),
            );
            // sets the remote endpoint
            let remote_endpoint = (remote_addr, config.user_space_server_port);

            // connects to the remote endpoint
            match socket.connect(iface_context, remote_endpoint, self.client_port) {
                Ok(_) => {
                    info!(
                        "Client connecting from port {} to {}:{}",
                        self.client_port, remote_addr, config.user_space_server_port
                    );

                    self.connecting = true;
                }
                Err(e) => {
                    error!("Client connect error: {:?}", e);
                }
            }
        }
    }

    fn handle_data_sending(&mut self, socket: &mut tcp::Socket) {
        // if the socket is not active
        if !socket.is_active() {
            return;
        }

        // if not connected
        if !self.connection_state.connected {
            // sets connected to true
            self.connection_state.connected = true;
            info!("Client connected successfully");
        }

        // if we can't send data or the flow has exceeded its size
        if socket.can_send()
            && !self.flow
                .flow_size
                .exceeded(self.connection_state.bytes_total, self.connection_state.start_time)
        {
            let remaining = match self.flow.flow_size {
                FlowSpec::Bytes(size) => size as u64 - self.connection_state.bytes_total,
                _ => SOCKET_BUFFER_SIZE as u64, // For duration-based flows
            };

            // sends the data
            match socket.send(|buf| {
                let to_send = cmp::min(buf.len(), remaining as usize);
                // fills the buffer with data
                buf[..to_send].fill(0xAA);
                (to_send, to_send)
            }) {
                Ok(sent) if sent > 0 => {
                    // updates the throughput
                    self.connection_state.test_throughput("Client", 0, sent as u64);

                    // if the flow has finished
                    if self.flow
                        .flow_size
                        .exceeded(self.connection_state.bytes_total, self.connection_state.start_time)
                    {
                        info!("Client finished sending flow data");
                        // closes the socket
                        socket.close();
                    }
                }
                Err(e) => {
                    error!("Client send error: {:?}", e);
                }
                Ok(_) => {}
            }
        }
    }
}

struct UserSpaceTcpClient {
    config: LocalConfig,
    processor_handle: ProcessorHandle,
    client_state: ClientState,
    packet_receiver: flume::Receiver<Packet>,
}

impl UserSpaceTcpClient {
    fn new(
        config: LocalConfig,
        flow: Flow,
        processor_handle: ProcessorHandle,
        client_port: u16,
        packet_receiver: flume::Receiver<Packet>,
    ) -> Self {
        info!(
            "Creats user-space TCP client for outgoing flow on port {}.",
            client_port
        );

        Self {
            config,
            processor_handle,
            client_state: ClientState::new(flow, client_port),
            packet_receiver,
        }
    }

    /// Starts the user-space TCP source as a virtual device.
    fn start(self) {
        let Self {
            config,
            processor_handle,
            mut client_state,
            packet_receiver,
        } = self;

        info!("Starts user-space TCP client on port {}.", client_state.client_port);

        // creates a virtual device using the passed processor handle
        let mut device = VirtualDevice {
            config: config.clone(),
            receiver: packet_receiver,
            sender: processor_handle.clone(),
        };

        let mut iface = create_interface(&config, &mut device);

        // creates the TCP socket set for client
        let mut sockets = SocketSet::new(vec![]);

        // creates the client receive buffer
        let client_rx_buffer = tcp::SocketBuffer::new(vec![0; SOCKET_BUFFER_SIZE]);
        // creates the client transmit buffer
        let client_tx_buffer = tcp::SocketBuffer::new(vec![0; SOCKET_BUFFER_SIZE]);

        // creates a new TCP socket
        let client_socket = tcp::Socket::new(client_rx_buffer, client_tx_buffer);
        // adds the socket to the socket set
        let client_handle = sockets.add(client_socket);

        info!("User-space client initialized, starting main loop.");

        loop {
            // gets the current time
            let timestamp = Instant::now();

            // gets a mutable reference to the socket
            let mut socket = sockets.get_mut::<tcp::Socket>(client_handle);

            // try to establish a connection if we haven't started
            client_state.try_connect(&mut socket, iface.context(), &config);

            // polls the interface
            iface.poll(timestamp, &mut device, &mut sockets);

            let socket = sockets.get_mut::<tcp::Socket>(client_handle);

            // if the socket is not connected
            if socket.state() == tcp::State::Closed {
                info!(
                    "Socket for port {} is closed, client thread shutting down.",
                    client_state.client_port
                );
                break;
            }

            // sends data if possible
            let mut socket = sockets.get_mut::<tcp::Socket>(client_handle);
            client_state.handle_data_sending(&mut socket);
        }
    }
}
