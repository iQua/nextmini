use std::cmp;
use std::sync::Arc;
use std::sync::atomic::{AtomicU16, Ordering};
use std::thread;
use std::time::Instant as StdInstant;

use smoltcp::iface::{Config, Interface, SocketSet};
use smoltcp::socket::tcp;
use smoltcp::time::Instant;
use smoltcp::wire::{HardwareAddress, IpAddress, IpCidr};
use tokio::sync::mpsc;
use tracing::{error, info};

use crate::node::NodeIdExt;
use crate::node::config::LocalConfig;
use crate::node::flow::device::VirtualDevice;
use crate::node::flow::state::ConnectionState;
use crate::node::packet::Packet;
use crate::node::processor::ProcessorHandle;
use nextmini_messages::{Flow, FlowSpec};

// socket buffer 655350 by default
const SOCKET_BUFFER_SIZE: usize = 655350;

// creates a new thread for each flow
#[derive(Debug, Clone)]
pub struct UserSpaceClientHandle {
    config: LocalConfig,
    processor_handle: ProcessorHandle,
    next_client_port: Arc<AtomicU16>,
}

impl UserSpaceClientHandle {
    pub fn new(config: LocalConfig, processor_handle: ProcessorHandle) -> Self {
        // uses an atomic counter to ensure unique client ports
        let next_client_port = Arc::new(AtomicU16::new(config.user_space_client_port));

        Self {
            config,
            processor_handle,
            next_client_port,
        }
    }

    pub fn add_flows(&self, flows: Vec<Flow>) {
        for flow in flows {
            let config = self.config.clone();
            let processor_handle = self.processor_handle.clone();
            let client_port = self.next_client_port.fetch_add(1, Ordering::SeqCst);
            let (packet_sender, packet_receiver) = mpsc::channel(config.channel_capacity);

            self.processor_handle
                .connect_local_destination(client_port, Arc::new(packet_sender));

            // spawns a new thread as smoltcp is not designed to use async Rust and Tokio
            thread::spawn(move || {
                let client = UserSpaceTcpClient::new(
                    config,
                    flow,
                    processor_handle,
                    client_port,
                    packet_receiver,
                );
                client.start();
            });

            info!("Client handle created and thread started");
        }
    }
}

struct UserSpaceTcpClient {
    config: LocalConfig,
    flow: Flow,
    processor_handle: ProcessorHandle,
    packet_receiver: Option<mpsc::Receiver<Packet>>,
    state: ConnectionState,
    connecting: bool,
    client_port: u16,
}

impl UserSpaceTcpClient {
    fn new(
        config: LocalConfig,
        flow: Flow,
        processor_handle: ProcessorHandle,
        client_port: u16,
        packet_receiver: mpsc::Receiver<Packet>,
    ) -> Self {
        info!(
            "Creats user-space TCP client for outgoing flow on port {}.",
            client_port
        );

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
            connecting: false,
            client_port,
        }
    }

    /// Starts the user-space TCP source as a virtual device.
    fn start(mut self) {
        info!("Starts user-space TCP client.");

        // Take the packet_receiver out of the Option
        let packet_receiver = self.packet_receiver.take().unwrap();

        // creates a virtual device using the passed processor handle
        let mut device = VirtualDevice {
            config: self.config.clone(),
            receiver: packet_receiver,
            sender: self.processor_handle.clone(),
        };

        // sets up for IP layer without needing hardware address
        let config = Config::new(HardwareAddress::Ip);

        // sets up Layer 3 using the provided IP address
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

        // creates the TCP socket set for client
        let mut sockets = SocketSet::new(vec![]);

        let client_rx_buffer = tcp::SocketBuffer::new(vec![0; SOCKET_BUFFER_SIZE]);
        let client_tx_buffer = tcp::SocketBuffer::new(vec![0; SOCKET_BUFFER_SIZE]);

        let client_socket = tcp::Socket::new(client_rx_buffer, client_tx_buffer);
        let client_handle = sockets.add(client_socket);

        info!("User-space client initialized, starting main loop.");

        loop {
            // gets the current time
            let timestamp = Instant::now();

            // polls the interface for packet transmission/reception
            iface.poll(timestamp, &mut device, &mut sockets);

            let socket = sockets.get_mut::<tcp::Socket>(client_handle);

            // handles client connection and sends out data
            self.client_connection(socket, iface.context());
            self.send_data(socket);
        }
    }

    fn client_connection(
        &mut self,
        socket: &mut tcp::Socket,
        iface_context: &mut smoltcp::iface::Context,
    ) {
        if !socket.is_open() && !self.connecting {
            let remote_addr = IpAddress::from(
                self.flow
                    .dst_node_id
                    .ip_addr(self.config.user_space_base_addr, self.config.local_netmask),
            );
            let remote_endpoint = (remote_addr, self.config.user_space_server_port as u16);

            match socket.connect(iface_context, remote_endpoint, self.client_port) {
                Ok(_) => {
                    info!(
                        "Client connecting from port {} to {}:{}",
                        self.client_port, remote_addr, self.config.user_space_server_port
                    );
                    self.connecting = true;
                }
                Err(e) => {
                    error!("Client connect error: {:?}", e);
                }
            }
        }
    }

    fn send_data(&mut self, socket: &mut tcp::Socket) {
        if !socket.is_active() {
            return;
        }

        if !self.state.connected {
            self.state.connected = true;
            info!("Client connected successfully");
        }

        if !socket.can_send()
            || self
                .flow
                .flow_size
                .exceeded(self.state.bytes_total, self.state.start_time)
        {
            return;
        }

        let remaining = match self.flow.flow_size {
            FlowSpec::Bytes(size) => size as u64 - self.state.bytes_total,
            _ => SOCKET_BUFFER_SIZE as u64, // For duration-based flows
        };

        match socket.send(|buf| {
            let to_send = cmp::min(buf.len(), remaining as usize);
            buf[..to_send].fill(0xAA);
            (to_send, to_send)
        }) {
            Ok(sent) if sent > 0 => {
                self.state.test_throughput(
                    0,
                    self.flow.dst_node_id,
                    Some(self.client_port),
                    sent as u64,
                );

                if self
                    .flow
                    .flow_size
                    .exceeded(self.state.bytes_total, self.state.start_time)
                {
                    info!("Client finished sending flow data");
                    socket.close();
                }
            }
            Err(e) => {
                error!("Client sends error: {:?}", e);
            }
            Ok(_) => {}
        }
    }
}
