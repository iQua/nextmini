use std::sync::Arc;
use std::thread;
use std::time::Instant as StdInstant;

use smoltcp::iface::{Config, Interface, SocketHandle, SocketSet};
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
use nextmini_messages::Flow;

// socket buffer 655350 by default
const SOCKET_BUFFER_SIZE: usize = 655350;

#[derive(Debug, Clone)]
pub struct UserSpaceServerHandle {
    processor_handle: ProcessorHandle,
    config: LocalConfig,
}

impl UserSpaceServerHandle {
    pub fn new(config: LocalConfig, processor_handle: ProcessorHandle) -> Self {
        info!("Server handle created.");
        Self {
            config,
            processor_handle,
        }
    }

    pub fn add_flows(&self, flows: Vec<Flow>) {
        // for the server, all flows listen on the same port. we only need one listener thread.
        // this logic assumes we can have multiple TCP sockets listening on the same port,
        // which smoltcp supports.
        // we will create one thread to handle all incoming connections
        // on the well-known server port.

        // if we want to use the same port for multiple flows
        // socketset can know which flow is which socket.
        // but if we use a thread per server,
        if flows.is_empty() {
            return;
        }

        let config = self.config.clone();
        let processor_handle = self.processor_handle.clone();
        let (packet_sender, packet_receiver) = mpsc::channel(config.channel_capacity);

        self.processor_handle
            .connect_local_destination(self.config.user_space_server_port, Arc::new(packet_sender));

        // Spawns a single thread to manage all server sockets for the designated port
        thread::spawn(move || {
            let server = UserSpaceTcpServer::new(config, flows, processor_handle, packet_receiver);
            server.start();
        });
    }
}

struct UserSpaceTcpServer {
    config: LocalConfig,
    flows: Vec<Flow>,
    processor_handle: ProcessorHandle,
    packet_receiver: Option<mpsc::Receiver<Packet>>,
    states: Vec<ConnectionState>,
    listening: bool,
}

impl UserSpaceTcpServer {
    fn new(
        config: LocalConfig,
        flows: Vec<Flow>,
        processor_handle: ProcessorHandle,
        packet_receiver: mpsc::Receiver<Packet>,
    ) -> Self {
        info!("Creating user-space TCP server for {} flows.", flows.len());
        let states = flows
            .iter()
            .map(|_| ConnectionState {
                connected: false,
                start_time: StdInstant::now(),
                time_last_updated: StdInstant::now(),
                bytes_last_updated: 0,
                bytes_total: 0,
            })
            .collect();

        Self {
            config,
            flows,
            processor_handle,
            packet_receiver: Some(packet_receiver),
            states,
            listening: false,
        }
    }

    fn start(mut self) {
        info!("Starts running server.");

        let packet_receiver = self.packet_receiver.take().unwrap();
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
        let mut socket_handles = Vec::new();

        for _ in &self.flows {
            let server_rx_buffer = tcp::SocketBuffer::new(vec![0; SOCKET_BUFFER_SIZE]);
            let server_tx_buffer = tcp::SocketBuffer::new(vec![0; SOCKET_BUFFER_SIZE]);
            let server_socket = tcp::Socket::new(server_rx_buffer, server_tx_buffer);
            let handle = sockets.add(server_socket);
            socket_handles.push(handle);
        }

        info!("User-space TCP server initialized, starting main loop.");

        loop {
            let timestamp = Instant::now();
            iface.poll(timestamp, &mut device, &mut sockets);
            self.receive_data(&mut sockets, &socket_handles);
        }
    }

    fn receive_data(&mut self, sockets: &mut SocketSet, handles: &[SocketHandle]) {
        let base_server_port = self.config.user_space_server_port;

        // Set up listening sockets if not already done
        if !self.listening {
            for &handle in handles {
                let socket = sockets.get_mut::<tcp::Socket>(handle);
                if !socket.is_listening() {
                    if let Err(e) = socket.listen(base_server_port) {
                        error!(
                            "Server failed to listen on port {}: {:?}",
                            base_server_port, e
                        );
                    }
                }
            }
            self.listening = true;
        }

        for (i, &handle) in handles.iter().enumerate() {
            let socket = sockets.get_mut::<tcp::Socket>(handle);

            if socket.is_active() && !self.states[i].connected {
                self.states[i].connected = true;
                info!("Server accepted connection for flow {}", i);
            }

            if socket.can_recv() {
                match socket.recv(|buf| (buf.len(), buf.len())) {
                    Ok(received) if received > 0 => {
                        self.states[i].test_throughput(
                            self.flows[i].src_node_id,
                            self.flows[i].src_node_id,
                            None,
                            received as u64,
                        );

                        if self.flows[i]
                            .flow_size
                            .exceeded(self.states[i].bytes_total, self.states[i].start_time)
                        {
                            info!("Server received all data.");
                            socket.close();
                        }
                    }
                    Err(e) => {
                        error!("Server receives error: {:?}.", e);
                    }
                    _ => {}
                }
            }
        }
    }
}
