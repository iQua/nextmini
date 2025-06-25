use std::time::Instant as StdInstant;

use smoltcp::iface::SocketHandle;
use smoltcp::iface::SocketSet;
use smoltcp::socket::tcp;
use tracing::{error, info};

use crate::node::config::LocalConfig;
use crate::node::user_space_tcp_utils::ConnectionState;
use nextmini_messages::Flow;

// socket buffer 655350 by default
const SOCKET_BUFFER_SIZE: usize = 655350;

pub struct UserSpaceTcpServer {
    config: LocalConfig,
    handles: Vec<SocketHandle>,
    states: Vec<ConnectionState>,
    listening: Vec<bool>,
    flows: Vec<Flow>,
}

impl UserSpaceTcpServer {
    pub fn new(config: LocalConfig, incoming_flows: Vec<Flow>, sockets: &mut SocketSet) -> Self {
        let mut handles = Vec::new();

        for i in 0..incoming_flows.len() {
            let server_rx_buffer = tcp::SocketBuffer::new(vec![0; SOCKET_BUFFER_SIZE]);
            let server_tx_buffer = tcp::SocketBuffer::new(vec![0; SOCKET_BUFFER_SIZE]);

            let server_socket = tcp::Socket::new(server_rx_buffer, server_tx_buffer);
            let server_handle = sockets.add(server_socket);
            handles.push(server_handle);

            info!("Created server socket {}", i);
        }

        let states = (0..handles.len())
            .map(|_i| ConnectionState {
                connected: false,
                start_time: StdInstant::now(),
                time_last_updated: StdInstant::now(),
                bytes_last_updated: 0,
                bytes_total: 0,
            })
            .collect();

        let listening = vec![false; handles.len()];

        Self {
            config,
            handles,
            states,
            listening,
            flows: incoming_flows,
        }
    }

    // adds new incoming flows to the server
    pub fn add_flows(&mut self, incoming_flows: Vec<Flow>, sockets: &mut SocketSet) {
        for flow in incoming_flows {
            let i = self.handles.len();
            let server_rx_buffer = tcp::SocketBuffer::new(vec![0; SOCKET_BUFFER_SIZE]);
            let server_tx_buffer = tcp::SocketBuffer::new(vec![0; SOCKET_BUFFER_SIZE]);

            let server_socket = tcp::Socket::new(server_rx_buffer, server_tx_buffer);
            let server_handle = sockets.add(server_socket);
            self.handles.push(server_handle);

            info!("Created server socket {}", i);

            self.states.push(ConnectionState {
                connected: false,
                start_time: StdInstant::now(),
                time_last_updated: StdInstant::now(),
                bytes_last_updated: 0,
                bytes_total: 0,
            });
            self.listening.push(false);
            self.flows.push(flow);
        }
    }

    pub fn process(&mut self, sockets: &mut SocketSet) {
        // always uses the same server port
        let base_server_port = self.config.user_space_server_port;

        for (i, &server_handle) in self.handles.iter().enumerate() {
            let socket = sockets.get_mut::<tcp::Socket>(server_handle);

            // it's fine to have N listening sockets on the same port,
            // incoming connections go to a random one.
            if !socket.is_active() && !socket.is_listening() && !self.listening[i] {
                match socket.listen(base_server_port) {
                    Ok(_) => {
                        info!("Server {} listening on port {}.", i, base_server_port);
                        self.listening[i] = true;
                    }
                    Err(e) => {
                        error!("Server {} failed to listen: {:?}.", i, e);
                    }
                }
            }

            if socket.is_active() {
                if !self.states[i].connected {
                    self.states[i].connected = true;
                    info!("Server {} accepted connection.", i);
                }

                if socket.can_recv() {
                    match socket.recv(|buffer| {
                        let len = buffer.len();
                        (len, len)
                    }) {
                        // prints out the throughput per sec for server
                        Ok(received) if received > 0 => {
                            self.states[i].test_throughput("Server", i, received as u64);

                            if self.flows[i]
                                .flow_size
                                .exceeded(self.states[i].bytes_total, self.states[i].start_time)
                            {
                                println!("Server received all the flows.");
                                socket.close();
                            }
                        }
                        Err(e) => {
                            info!("Server {} recv error: {:?}.", i, e);
                        }
                        Ok(_) => {}
                    }
                }
            }
        }
    }
}
