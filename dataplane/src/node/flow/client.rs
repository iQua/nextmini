// A TCP client for user-space flows, implemented using SmolTcp.
use std::cmp;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant as StdInstant};

use smoltcp::iface::{Config, Interface, SocketSet};
use smoltcp::socket::tcp;
use smoltcp::time::Instant;
use smoltcp::wire::{HardwareAddress, IpAddress, IpCidr};
use tokio::sync::mpsc;
use tracing::{error, info};

use nextmini_messages::{Flow, FlowLen};

use crate::node::NodeIdExt;
use crate::node::config::LocalConfig;
use crate::node::flow::SOCKET_BUFFER_SIZE;
use crate::node::flow::device::VirtualDevice;
use crate::node::flow::state::ConnectionState;
use crate::node::packet::Packet;
use crate::node::processor::ProcessorHandle;

#[derive(Debug, Clone)]
pub struct UserSpaceClientHandle {
    config: LocalConfig,
    processor_handle: ProcessorHandle,
    next_client_port: u16,
}

impl UserSpaceClientHandle {
    pub fn new(config: LocalConfig, processor_handle: ProcessorHandle) -> Self {
        let next_client_port = config.user_space_client_port;

        Self {
            config,
            processor_handle,
            next_client_port,
        }
    }

    pub fn add_flows(&mut self, flows: Vec<Flow>) {
        for flow in flows {
            let config = self.config.clone();
            let processor_handle = self.processor_handle.clone();
            self.next_client_port += 1;
            let client_port = self.next_client_port;
            let (packet_sender, packet_receiver) = mpsc::channel(config.channel_capacity);

            // extracts flow_id where server is source, client is destination
            let client_ip = config
                .node_id
                .ip_addr(config.user_space_base_addr, config.local_netmask);
            let server_ip = flow
                .dst_node_id
                .ip_addr(config.user_space_base_addr, config.local_netmask);
            let server_port = config.user_space_server_port;

            // connects this client as a local destination for packets destined to this flow
            let flow_id = ((u32::from(server_ip) as u128) << 96)
                | ((u32::from(client_ip) as u128) << 64)
                | ((server_port as u128) << 48)
                | ((client_port as u128) << 32);

            self.processor_handle
                .connect_local_destination(flow_id, Arc::new(packet_sender));

            // set flow weights for this flow
            if let Some(weight) = flow.flow_spec.flow_weight {
                let flow_id = ((u32::from(client_ip) as u128) << 96)
                    | ((u32::from(server_ip) as u128) << 64)
                    | ((client_port as u128) << 48)
                    | ((server_port as u128) << 32);

                info!(
                    "Set flow weight {} for user space flow from node {} to node {}.",
                    weight, flow.src_node_id, flow.dst_node_id
                );
                self.processor_handle.set_flow_weight(flow_id, weight);
            }

            let client =
                UserSpaceClient::new(config, flow, processor_handle, client_port, packet_receiver);

            // spawns a new thread as SmolTcp is not designed to use async Rust and Tokio
            thread::spawn(move || {
                client.run();
            });
        }
    }
}

struct UserSpaceClient {
    config: LocalConfig,
    flow: Flow,
    processor_handle: ProcessorHandle,
    packet_receiver: Option<mpsc::Receiver<Packet>>,
    state: ConnectionState,
    client_port: u16,
}

impl UserSpaceClient {
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
            client_port,
        }
    }

    /// Runs a user-space TCP client by connecting and sending to a server.
    fn run(mut self) {
        let packet_receiver = self.packet_receiver.take().unwrap();

        // creates a virtual device using the passed processor handle
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

        // creates a socket set for a new TCP client
        let mut sockets = SocketSet::new(vec![]);

        let rx_buffer = tcp::SocketBuffer::new(vec![0; SOCKET_BUFFER_SIZE]);
        let tx_buffer = tcp::SocketBuffer::new(vec![0; SOCKET_BUFFER_SIZE]);

        let socket = tcp::Socket::new(rx_buffer, tx_buffer);
        let socket_handle = sockets.add(socket);

        loop {
            // gets the current time
            let timestamp = Instant::now();

            // polls the interface for packet transmission/reception
            iface.poll(timestamp, &mut device, &mut sockets);

            let socket = sockets.get_mut::<tcp::Socket>(socket_handle);

            // handles client connection and sends out data
            self.connect(socket, iface.context());
            self.send(socket);
        }
    }

    // Connects to a user-space TCP server.
    fn connect(&mut self, socket: &mut tcp::Socket, iface_context: &mut smoltcp::iface::Context) {
        if !socket.is_open() {
            let remote_addr = IpAddress::from(
                self.flow
                    .dst_node_id
                    .ip_addr(self.config.user_space_base_addr, self.config.local_netmask),
            );
            let remote_endpoint = (remote_addr, self.config.user_space_server_port as u16);

            match socket.connect(iface_context, remote_endpoint, self.client_port) {
                Ok(_) => {
                    info!(
                        "A new user-space TCP client has connected to a server from port {} to {}:{}.",
                        self.client_port, remote_addr, self.config.user_space_server_port
                    );
                }
                Err(e) => {
                    error!("Error connecting to a user-space TCP server: {:?}", e);
                }
            }
        }
    }

    /// Sends data, as much as possible or up to a certain flow rate, to the user-space TCP server.
    fn send(&mut self, socket: &mut tcp::Socket) {
        if !socket.is_active() {
            return;
        }

        if !socket.can_send()
            || self
                .flow
                .flow_spec
                .flow_len
                .exceeded(self.state.bytes_total, self.state.start_time)
        {
            return;
        }

        let remaining = match self.flow.flow_spec.flow_len {
            FlowLen::Bytes(size) => size as u64 - self.state.bytes_total,
            _ => SOCKET_BUFFER_SIZE as u64, // For duration-based flows
        };

        let sending_start_time = StdInstant::now();

        match socket.send(|buf| {
            let to_send = cmp::min(buf.len(), remaining as usize);
            buf[..to_send].fill(0xAA);
            (to_send, to_send)
        }) {
            Ok(sent) if sent > 0 => {
                // Calculates the time to wait for specified flow rate
                if let Some(flow_rate) = self.flow.flow_spec.flow_rate {
                    let expected_time = Duration::from_secs_f64(sent as f64 / flow_rate as f64);
                    let actual_time = sending_start_time.elapsed();
                    if expected_time > actual_time {
                        thread::sleep(expected_time - actual_time);
                    }
                }

                self.state
                    .test_throughput(self.config.node_id, self.flow.dst_node_id, sent as u64);

                if self
                    .flow
                    .flow_spec
                    .flow_len
                    .exceeded(self.state.bytes_total, self.state.start_time)
                {
                    info!("A user-space TCP client has finished sending all its data.");
                    socket.close();
                }
            }
            Err(e) => {
                error!("Error sending to a user-space TCP server: {:?}", e);
            }
            Ok(_) => {}
        }
    }
}
