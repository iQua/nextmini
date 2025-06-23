use std::cmp;
use std::net::Ipv4Addr;
use std::thread;
use std::time::{Duration as StdDuration, Instant as StdInstant};

use flume;
use smoltcp::iface::{Config, Interface, SocketSet};
use smoltcp::phy::{Device, DeviceCapabilities, Medium};
use smoltcp::socket::tcp;
use smoltcp::time::{Duration, Instant};
use smoltcp::wire::{IpAddress, IpCidr};
use tracing::{error, info};

use crate::node::LocalDestination;
use crate::node::config::LocalConfig;
use crate::node::packet::Packet;
use crate::node::processor::ProcessorHandle;

#[derive(Clone, Debug)]
pub struct UserSpaceTcpSource {
    config: LocalConfig,
    ip_addr: Ipv4Addr,
    processor_handle: ProcessorHandle,
    packet_sender: flume::Sender<Packet>,
    packet_receiver: flume::Receiver<Packet>,
}

impl UserSpaceTcpSource {
    pub fn new(config: LocalConfig, ip_addr: Ipv4Addr, processor_handle: ProcessorHandle) -> Self {
        let (packet_sender, packet_receiver) = flume::bounded(config.channel_capacity);

        let tcp_source = Self {
            config,
            ip_addr,
            processor_handle,
            packet_sender: packet_sender.clone(),
            packet_receiver,
        };

        tcp_source
    }

    /// Starts the user-space TCP source as a virtual device.
    pub fn start(&self) {
        let device = VirtualDevice {
            config: self.config.clone(),
            receiver: self.packet_receiver.clone(),
            sender: self.processor_handle.clone(),
        };
        let config_clone = self.config.clone();

        // sets up for IP layer without needing hardware address
        let config = Config::new(smoltcp::wire::HardwareAddress::Ip);

        // sets up Layer 3 using the provided IP address
        let mut iface = Interface::new(config, &mut device.clone(), Instant::now());
        iface.update_ip_addrs(|addrs| {
            addrs
                .push(IpCidr::new(
                    IpAddress::v4(
                        self.ip_addr.octets()[0],
                        self.ip_addr.octets()[1],
                        self.ip_addr.octets()[2],
                        self.ip_addr.octets()[3],
                    ),
                    24,
                ))
                .unwrap();
        });

        // socket buffer
        const SOCKET_BUFFER_SIZE: usize = 65535;

        // connects to a remote endpoint
        // Creates the TCP socket set
        let mut sockets = SocketSet::new(vec![]);

        // first, creates server sockets based on the number of inbound flows
        let mut server_handles = Vec::new();
        for i in 0..self.config.incoming_flows_count {
            let server_rx_buffer = tcp::SocketBuffer::new(vec![0; SOCKET_BUFFER_SIZE]);
            let server_tx_buffer = tcp::SocketBuffer::new(vec![0; SOCKET_BUFFER_SIZE]);
            let server_socket = tcp::Socket::new(server_rx_buffer, server_tx_buffer);
            let server_handle = sockets.add(server_socket);
            server_handles.push(server_handle);
            info!("Created server socket {}", i);
        }

        let node_id = self.config.node_id;

        let client_flow_configs: Vec<_> = self
            .config
            .flow
            .iter()
            .filter(|f| f.src_node_id == node_id)
            .cloned()
            .collect();

        let mut client_handles = Vec::new();
        for (i, _flow) in client_flow_configs.iter().enumerate() {
            let client_rx_buffer = tcp::SocketBuffer::new(vec![0; SOCKET_BUFFER_SIZE]);
            let client_tx_buffer = tcp::SocketBuffer::new(vec![0; SOCKET_BUFFER_SIZE]);
            let client_socket = tcp::Socket::new(client_rx_buffer, client_tx_buffer);
            let client_handle = sockets.add(client_socket);
            client_handles.push(client_handle);
            info!("Created client socket {} for an outgoing flow.", i);
        }

        #[derive(Debug, Clone)]
        struct ConnectionState {
            connected: bool,
            start_time: Option<StdInstant>,
            bytes_transferred: u64,
            target_bytes: u64,
            finished: bool,
        }

        let base_server_port = self.config.user_space_server_port;

        // spawns a new thread as smoltcp is not designed to use async Rust and Tokio
        thread::spawn(move || {
            let mut device = device;

            let incoming_flows: Vec<_> = config_clone
                .flow
                .iter()
                .filter(|f| f.dst_node_id == node_id)
                .collect();
            let outgoing_flows: Vec<_> = config_clone
                .flow
                .iter()
                .filter(|f| f.src_node_id == node_id)
                .collect();

            let mut server_states: Vec<ConnectionState> = (0..server_handles.len())
                .map(|i| {
                    let expected_bytes = incoming_flows.get(i)
                        .and_then(|f| f.flow_size)
                        .expect(&format!("Server socket {} was created, but no corresponding incoming flow config was found at index {}", i, i));

                    ConnectionState {
                        connected: false,
                        start_time: None,
                        bytes_transferred: 0,
                        target_bytes: expected_bytes,
                        finished: false,
                    }
                })
                .collect();

            let mut client_states: Vec<ConnectionState> = outgoing_flows
                .iter()
                .map(|f| ConnectionState {
                    connected: false,
                    start_time: None,
                    bytes_transferred: 0,
                    target_bytes: f.flow_size.unwrap_or(u64::MAX), // Send indefinitely if not specified
                    finished: false,
                })
                .collect();

            let mut server_listening = vec![false; server_handles.len()];
            let mut client_connecting = vec![false; client_handles.len()];

            loop {
                let timestamp = Instant::now();
                iface.poll(timestamp, &mut device, &mut sockets);

                for (i, &server_handle) in server_handles.iter().enumerate() {
                    let socket = sockets.get_mut::<tcp::Socket>(server_handle);

                    if !socket.is_active() && !socket.is_listening() && !server_listening[i] {
                        match socket.listen(base_server_port) {
                            Ok(_) => {
                                info!("Server {} listening on port {}", i, base_server_port);
                                server_listening[i] = true;
                            }
                            Err(e) => {
                                error!("Server {} failed to listen: {:?}", i, e);
                            }
                        }
                    }

                    if socket.is_active() {
                        if !server_states[i].connected {
                            server_states[i].connected = true;
                            info!("Server {} accepted connection", i);
                        }

                        if socket.can_recv() {
                            match socket.recv(|buffer| {
                                let len = buffer.len();
                                (len, len)
                            }) {
                                Ok(received) if received > 0 => {
                                    if server_states[i].start_time.is_none() {
                                        server_states[i].start_time = Some(StdInstant::now());
                                        info!("Server {} started receiving data", i);
                                    }

                                    server_states[i].bytes_transferred += received as u64;

                                    if server_states[i].bytes_transferred % 1_000 == 0 {
                                        info!(
                                            "Server {} received {} kB",
                                            i,
                                            server_states[i].bytes_transferred / 1_000
                                        );
                                    }

                                    if server_states[i].bytes_transferred
                                        >= server_states[i].target_bytes
                                        && !server_states[i].finished
                                    {
                                        info!(
                                            "Server {} received all expected data ({} bytes), closing connection.",
                                            i, server_states[i].bytes_transferred
                                        );

                                        if let Some(start_time) = server_states[i].start_time {
                                            let end_time = StdInstant::now();
                                            let elapsed = end_time.duration_since(start_time);
                                            let elapsed_secs = elapsed.as_secs_f64();

                                            if server_states[i].bytes_transferred > 0 {
                                                if elapsed_secs < 0.001 {
                                                    let elapsed_micros = elapsed.as_micros();
                                                    let throughput_gbps =
                                                        (server_states[i].bytes_transferred as f64
                                                            * 8.0)
                                                            / (elapsed_micros as f64 * 1000.0);
                                                    info!(
                                                        "Server {} throughput: {:.3} Gbps ({} bytes in {} μs)",
                                                        i,
                                                        throughput_gbps,
                                                        server_states[i].bytes_transferred,
                                                        elapsed_micros
                                                    );
                                                } else {
                                                    let throughput_gbps =
                                                        (server_states[i].bytes_transferred as f64
                                                            * 8.0)
                                                            / (elapsed_secs * 1_000_000_000.0);
                                                    info!(
                                                        "Server {} throughput: {:.3} Gbps ({} bytes in {:.3}s)",
                                                        i,
                                                        throughput_gbps,
                                                        server_states[i].bytes_transferred,
                                                        elapsed_secs
                                                    );
                                                }
                                            } else {
                                                info!(
                                                    "Server {} received all data in a single burst, cannot calculate throughput accurately.",
                                                    i
                                                );
                                            }
                                        }
                                        socket.close();
                                        server_states[i].finished = true;
                                    }
                                }
                                Err(e) => {
                                    info!("Server {} recv error: {:?}", i, e);
                                }
                                Ok(_) => {}
                            }
                        }
                    }
                }

                for (i, &client_handle) in client_handles.iter().enumerate() {
                    let flow = &outgoing_flows[i];
                    let socket = sockets.get_mut::<tcp::Socket>(client_handle);
                    let cx = iface.context();

                    if !socket.is_open() && !client_connecting[i] && !client_states[i].finished {
                        if flow.remote_addr != [0, 0, 0, 0] {
                            let remote_addr = IpAddress::v4(
                                flow.remote_addr[0],
                                flow.remote_addr[1],
                                flow.remote_addr[2],
                                flow.remote_addr[3],
                            );
                            let remote_endpoint = (remote_addr, base_server_port as u16);

                            match socket.connect(cx, remote_endpoint, flow.client_port) {
                                Ok(_) => {
                                    info!(
                                        "Client {} connecting from port {} to {}:{}",
                                        i, flow.client_port, remote_addr, base_server_port
                                    );
                                    client_connecting[i] = true;
                                }
                                Err(e) => {
                                    error!("Client {} connect error: {:?}", i, e);
                                }
                            }
                        }
                    }

                    if socket.is_active() {
                        if !client_states[i].connected {
                            client_states[i].connected = true;
                            info!("Client {} connected successfully", i);
                        }

                        // sends data
                        if socket.can_send()
                            && client_states[i].bytes_transferred < client_states[i].target_bytes
                        {
                            let remaining =
                                client_states[i].target_bytes - client_states[i].bytes_transferred;

                            match socket.send(|buf| {
                                let to_send = cmp::min(buf.len(), remaining as usize);
                                buf[..to_send].fill(0xAA);
                                (to_send, to_send)
                            }) {
                                Ok(sent) if sent > 0 => {
                                    if client_states[i].start_time.is_none() {
                                        client_states[i].start_time = Some(StdInstant::now());
                                        info!("Client {} started transmission", i);
                                    }

                                    client_states[i].bytes_transferred += sent as u64;

                                    if client_states[i].bytes_transferred % 1_000 == 0 {
                                        info!(
                                            "Client {} sent {} kB",
                                            i,
                                            client_states[i].bytes_transferred / 1_000
                                        );
                                    }

                                    if client_states[i].bytes_transferred
                                        >= client_states[i].target_bytes
                                    {
                                        if let Some(start_time) = client_states[i].start_time {
                                            let end_time = StdInstant::now();
                                            let elapsed = end_time.duration_since(start_time);
                                            let elapsed_secs = elapsed.as_secs_f64();

                                            if elapsed_secs < 0.001 {
                                                let elapsed_micros = elapsed.as_micros();
                                                let throughput_gbps =
                                                    (client_states[i].bytes_transferred as f64
                                                        * 8.0)
                                                        / (elapsed_micros as f64 * 1000.0); // Convert from microseconds to Gbps
                                                info!(
                                                    "Client {} throughput: {:.3} Gbps ({} bytes in {} μs)",
                                                    i,
                                                    throughput_gbps,
                                                    client_states[i].bytes_transferred,
                                                    elapsed_micros
                                                );
                                            } else {
                                                let throughput_gbps =
                                                    (client_states[i].bytes_transferred as f64
                                                        * 8.0)
                                                        / (elapsed_secs * 1_000_000_000.0);
                                                info!(
                                                    "Client {} throughput: {:.3} Gbps ({} bytes in {:.3}s)",
                                                    i,
                                                    throughput_gbps,
                                                    client_states[i].bytes_transferred,
                                                    elapsed_secs
                                                );
                                            }
                                        }
                                        socket.close();
                                        client_states[i].finished = true;
                                    }
                                }
                                Err(e) => {
                                    error!("Client {} send error: {:?}", i, e);
                                }
                                Ok(_) => {}
                            }
                        }
                    }
                }

                match iface.poll_delay(timestamp, &sockets) {
                    Some(Duration::ZERO) => {
                        continue;
                    }
                    Some(delay) => {
                        thread::sleep(delay.into());
                    }
                    None => {
                        thread::sleep(StdDuration::from_millis(1));
                    }
                }
            }
        });
    }
}

impl LocalDestination for UserSpaceTcpSource {
    fn send_packet(&self, packet: Packet) {
        if let Err(e) = self.packet_sender.try_send(packet) {
            error!("Failed to send packet to user-space TCP source: {:?}", e);
        }
    }
}

#[derive(Clone)]
struct VirtualDevice {
    config: LocalConfig,
    receiver: flume::Receiver<Packet>,
    sender: ProcessorHandle,
}

impl Device for VirtualDevice {
    type RxToken<'a> = PacketRxToken;
    type TxToken<'a> = PacketTxToken;

    fn receive(&mut self, _timestamp: Instant) -> Option<(Self::RxToken<'_>, Self::TxToken<'_>)> {
        self.receiver
            .try_recv()
            .ok()
            .map(|packet| (PacketRxToken(packet), PacketTxToken(self.sender.clone())))
    }

    fn transmit(&mut self, _timestamp: Instant) -> Option<Self::TxToken<'_>> {
        Some(PacketTxToken(self.sender.clone()))
    }

    fn capabilities(&self) -> DeviceCapabilities {
        let mut caps = DeviceCapabilities::default();
        caps.medium = Medium::Ip; // needs IP packet format
        caps.max_transmission_unit = self.config.mtu as usize;
        caps
    }
}

struct PacketRxToken(Packet);

impl smoltcp::phy::RxToken for PacketRxToken {
    fn consume<R, F>(self, f: F) -> R
    where
        F: FnOnce(&[u8]) -> R,
    {
        f(&self.0.buf[0..self.0.packet_size])
    }
}

struct PacketTxToken(ProcessorHandle);

impl smoltcp::phy::TxToken for PacketTxToken {
    fn consume<R, F>(self, len: usize, f: F) -> R
    where
        F: FnOnce(&mut [u8]) -> R,
    {
        let mut buf = vec![0; len];
        let result = f(&mut buf);
        let packet = Packet::new(len, buf);

        // uses non-blocking send() to send the outbound packet
        self.0.process_packet(packet);

        result
    }
}
