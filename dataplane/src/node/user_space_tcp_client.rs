use std::cmp;
use std::time::Instant as StdInstant;

use smoltcp::iface::SocketHandle;
use smoltcp::iface::{Context, SocketSet};
use smoltcp::socket::tcp;
use smoltcp::wire::IpAddress;
use tracing::{error, info};

use crate::node::NodeIdExt;
use crate::node::config::LocalConfig;
use crate::node::user_space_tcp_utils::ConnectionState;
use nextmini_messages::{Flow, FlowSpec};

// socket buffer 655350 by default
const SOCKET_BUFFER_SIZE: usize = 655350;

pub struct UserSpaceTcpClient {
    config: LocalConfig,
    handles: Vec<SocketHandle>,
    states: Vec<ConnectionState>,
    connecting: Vec<bool>,
    flows: Vec<Flow>,
}

impl UserSpaceTcpClient {
    pub fn new(config: LocalConfig, outgoing_flows: Vec<Flow>, sockets: &mut SocketSet) -> Self {
        let mut handles = Vec::new();

        for (i, _flow) in outgoing_flows.iter().enumerate() {
            let client_rx_buffer = tcp::SocketBuffer::new(vec![0; SOCKET_BUFFER_SIZE]);
            let client_tx_buffer = tcp::SocketBuffer::new(vec![0; SOCKET_BUFFER_SIZE]);

            let client_socket = tcp::Socket::new(client_rx_buffer, client_tx_buffer);
            let client_handle = sockets.add(client_socket);

            handles.push(client_handle);

            info!("Created client socket {} for an outgoing flow.", i);
        }

        let states = outgoing_flows
            .iter()
            .map(|_f| ConnectionState {
                connected: false,
                start_time: StdInstant::now(),
                time_last_updated: StdInstant::now(),
                bytes_last_updated: 0,
                bytes_total: 0,
            })
            .collect();

        let connecting = vec![false; handles.len()];

        Self {
            config,
            handles,
            states,
            connecting,
            flows: outgoing_flows,
        }
    }

    // adds new outgoing flows to the client.
    pub fn add_flows(&mut self, outgoing_flows: Vec<Flow>, sockets: &mut SocketSet) {
        for flow in outgoing_flows {
            let i = self.handles.len();

            let client_rx_buffer = tcp::SocketBuffer::new(vec![0; SOCKET_BUFFER_SIZE]);
            let client_tx_buffer = tcp::SocketBuffer::new(vec![0; SOCKET_BUFFER_SIZE]);

            let client_socket = tcp::Socket::new(client_rx_buffer, client_tx_buffer);
            let client_handle = sockets.add(client_socket);
            self.handles.push(client_handle);

            info!("Created client socket {} for an outgoing flow.", i);

            self.states.push(ConnectionState {
                connected: false,
                start_time: StdInstant::now(),
                time_last_updated: StdInstant::now(),
                bytes_last_updated: 0,
                bytes_total: 0,
            });
            self.connecting.push(false);
            self.flows.push(flow);
        }
    }

    pub fn process(&mut self, sockets: &mut SocketSet, iface_context: &mut Context) {
        let base_server_port = self.config.user_space_server_port;

        for (i, &client_handle) in self.handles.iter().enumerate() {
            let flow = &self.flows[i];
            let socket = sockets.get_mut::<tcp::Socket>(client_handle);

            if !socket.is_open() && !self.connecting[i] {
                // obtains remote_addr using ip_addr()
                let remote_addr = IpAddress::from(
                    flow.dst_node_id
                        .ip_addr(self.config.user_space_base_addr, self.config.local_netmask),
                );
                let remote_endpoint = (remote_addr, base_server_port as u16);
                // assigns client port automatically h
                let client_port = self.config.user_space_client_port + i as u16;

                // connects to a remote endpoint
                match socket.connect(iface_context, remote_endpoint, client_port) {
                    Ok(_) => {
                        info!(
                            "Client {} connecting from port {} to {}:{}.",
                            i, client_port, remote_addr, base_server_port
                        );
                        self.connecting[i] = true;
                    }
                    Err(e) => {
                        error!("Client {} connect error: {:?}.", i, e);
                    }
                }
            }

            // sees if the socket is active
            if socket.is_active() {
                if !self.states[i].connected {
                    self.states[i].connected = true;
                    info!("Client {} connected successfully.", i);
                }

                // sends data
                if socket.can_send()
                    && !self.flows[i]
                        .flow_size
                        .exceeded(self.states[i].bytes_total, self.states[i].start_time)
                {
                    let remaining = if let FlowSpec::Bytes(size) = self.flows[i].flow_size {
                        size as u64 - self.states[i].bytes_total
                    } else {
                        // For duration-based flows, we can send as much as the buffer allows.
                        // The check for finishing is handled by `exceeded`.
                        SOCKET_BUFFER_SIZE as u64
                    };

                    match socket.send(|buf| {
                        let to_send = cmp::min(buf.len(), remaining as usize);
                        buf[..to_send].fill(0xAA);
                        (to_send, to_send)
                    }) {
                        // prints out the throughput per sec for client
                        Ok(sent) if sent > 0 => {
                            self.states[i].test_throughput("Client", i, sent as u64);

                            if self.flows[i]
                                .flow_size
                                .exceeded(self.states[i].bytes_total, self.states[i].start_time)
                            {
                                println!("Client sent all the flows.");
                                socket.close();
                            }
                        }
                        Err(e) => {
                            error!("Client {} send error: {:?}.", i, e);
                        }
                        Ok(_) => {}
                    }
                }
            }
        }
    }
}
