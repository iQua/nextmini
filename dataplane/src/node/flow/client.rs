use std::cmp;
use std::sync::Arc;
use std::sync::atomic::{AtomicU16, Ordering};
use std::thread;
use std::time::Instant as StdInstant;

use smoltcp::iface::{Config, Interface, SocketSet};
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
use nextmini_messages::{Flow, FlowSpec};

// socket buffer 65535000 by default, increased to handle larger flows
const SOCKET_BUFFER_SIZE: usize = 65535000;

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

struct UserSpaceTcpClient {
    config: LocalConfig,
    flow: Flow,
    processor_handle: ProcessorHandle,
    state: ConnectionState,
    connecting: bool,
    client_port: u16,
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

        // initializes the connection state
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
            state,
            connecting: false,
            client_port,
            packet_receiver,
        }
    }

    /// Starts the user-space TCP source as a virtual device.
    fn start(self) {
        let Self {
            config,
            flow,
            processor_handle,
            state: mut client_state,
            mut connecting,
            client_port,
            packet_receiver,
        } = self;

        info!("Starts user-space TCP client on port {}.", client_port);

        // creates a virtual device using the passed processor handle
        let mut device = VirtualDevice {
            config: config.clone(),
            receiver: packet_receiver,
            sender: processor_handle.clone(),
        };

        // sets up for IP layer without needing hardware address
        let iface_config = Config::new(HardwareAddress::Ip);

        // sets up Layer 3 using the provided IP address
        let ip_addr = config
            .node_id
            .ip_addr(config.user_space_base_addr, config.local_netmask);
        let mut iface = Interface::new(iface_config, &mut device, Instant::now());
        iface.update_ip_addrs(|addrs| {
            // adds the IP address to the interface
            addrs
                .push(IpCidr::new(IpAddress::from(ip_addr), 24))
                .unwrap();
        });

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
            let socket = sockets.get_mut::<tcp::Socket>(client_handle);

            // if the socket is not open and we are not connecting
            if !socket.is_open() && !connecting {
                // gets the remote address
                let remote_addr = IpAddress::from(
                    flow.dst_node_id
                        .ip_addr(config.user_space_base_addr, config.local_netmask),
                );
                // sets the remote endpoint
                let remote_endpoint = (remote_addr, config.user_space_server_port);

                // connects to the remote endpoint
                match socket.connect(iface.context(), remote_endpoint, client_port) {
                    Ok(_) => {
                        info!(
                            "Client connecting from port {} to {}:{}",
                            client_port, remote_addr, config.user_space_server_port
                        );

                        connecting = true;
                    }
                    Err(e) => {
                        error!("Client connect error: {:?}", e);
                    }
                }
            }

            // polls the interface
            iface.poll(timestamp, &mut device, &mut sockets);

            let socket = sockets.get_mut::<tcp::Socket>(client_handle);

            // if the socket is not connected
            if socket.state() == tcp::State::Closed {
                info!(
                    "Socket for port {} is closed, client thread shutting down.",
                    client_port
                );
                break;
            }

            // sends data if possible
            // if the socket is not active
            if socket.is_active() {
                // if not connected
                if !client_state.connected {
                    // sets connected to true
                    client_state.connected = true;
                    info!("Client connected successfully");
                }

                // if we can't send data or the flow has exceeded its size
                if socket.can_send()
                    && !flow
                        .flow_size
                        .exceeded(client_state.bytes_total, client_state.start_time)
                {
                    let remaining = match flow.flow_size {
                        FlowSpec::Bytes(size) => size as u64 - client_state.bytes_total,
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
                            client_state.test_throughput("Client", 0, sent as u64);

                            // if the flow has finished
                            if flow
                                .flow_size
                                .exceeded(client_state.bytes_total, client_state.start_time)
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
    }
}
