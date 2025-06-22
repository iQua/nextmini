use std::cmp;
use std::io::{Read, Write};
use std::net::Ipv4Addr;
use std::os::unix::io::AsRawFd;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Instant as StdInstant;

use flume;
use nix::fcntl::{self, FcntlArg, OFlag};
use os_pipe::{PipeReader, PipeWriter, pipe};
use smoltcp::iface::{Config, Interface, SocketSet};
use smoltcp::phy::{Device, DeviceCapabilities, Medium, wait as phy_wait};
use smoltcp::socket::tcp;
use smoltcp::time::Instant;
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
    signal_writer: Arc<Mutex<Option<PipeWriter>>>,
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
            signal_writer: Arc::new(Mutex::new(None)),
        };

        tcp_source
    }

    /// Starts the user-space TCP source as a virtual device.
    pub fn start(&self) {
        let (signal_reader, signal_writer) = pipe().expect("Failed to create pipe.");
        fcntl::fcntl(&signal_reader, FcntlArg::F_SETFL(OFlag::O_NONBLOCK))
            .expect("Failed to set pipe to non-blocking");
        *self.signal_writer.lock().unwrap() = Some(signal_writer);

        let device = VirtualDevice {
            config: self.config.clone(),
            receiver: self.packet_receiver.clone(),
            sender: self.processor_handle.clone(),
            signal_reader: Arc::new(signal_reader),
        };

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

        // creates the TCP socket
        let mut sockets = SocketSet::new(vec![]);

        // Connects to a remote endpoint: needs to be revised to obtain the destination IP and
        // port number from the local (or controller's) configuration file. In addition, the
        // current user-space TCP source is a client-only implementation, as it does not implement
        // bind(), listen(), and accept().

        // creates server sockets based on incoming flows count
        let mut server_handles = Vec::new();
        for _i in 0..self.config.incoming_flows_count {
            let server_rx_buffer = tcp::SocketBuffer::new(vec![0; 65535]);
            let server_tx_buffer = tcp::SocketBuffer::new(vec![0; 65535]);
            let server_socket = tcp::Socket::new(server_rx_buffer, server_tx_buffer);
            let server_handle = sockets.add(server_socket);
            server_handles.push(server_handle);
        }

        // creates client sockets
        let mut client_handles = Vec::new();
        for _ in &self.config.flow_configs {
            let client_rx_buffer = tcp::SocketBuffer::new(vec![0; 65535]);
            let client_tx_buffer = tcp::SocketBuffer::new(vec![0; 65535]);
            let client_socket = tcp::Socket::new(client_rx_buffer, client_tx_buffer);
            let client_handle = sockets.add(client_socket);
            client_handles.push(client_handle);
        }

        // spawns a new thread as smoltcp is not designed to use async Rust and Tokio
        let base_server_port = self.config.smoltcp_server_port; // base server port for smoltcp
        let flows = self.config.flow_configs.clone();
        thread::spawn(move || {
            let mut client_connections_status = vec![false; flows.len()];
            let mut bytes_left: Vec<u64> = flows
                .iter()
                .map(|f| f.flow_size.unwrap_or(10_000_000))
                .collect();
            let mut bytes_sent: Vec<u64> = vec![0; flows.len()]; // for client test
            let mut bytes_received: Vec<u64> = vec![0; server_handles.len()]; // for server test
            let mut client_start_times: Vec<Option<StdInstant>> = vec![None; flows.len()];
            let mut server_start_times: Vec<Option<StdInstant>> = vec![None; server_handles.len()];
            let mut client_transmission_started = vec![false; flows.len()];
            let mut server_transmission_started = vec![false; server_handles.len()];
            let mut client_closing_initiated = vec![false; flows.len()];
            let mut device = device;

            loop {
                let timestamp = Instant::now();
                iface.poll(timestamp, &mut device, &mut sockets);

                // multiple server sockets handling
                for i in 0..server_handles.len() {
                    let server_handle = server_handles[i];
                    let server_socket = sockets.get_mut::<tcp::Socket>(server_handle);
                    let server_port = base_server_port;

                    if !server_socket.is_active() && !server_socket.is_listening() {
                        if let Ok(_) = server_socket.listen(server_port) {
                            info!("Server {} listening on port {}", i, server_port);
                        }
                    }

                    if server_socket.is_active() && server_socket.can_recv() {
                        if !server_transmission_started[i] {
                            server_start_times[i] = Some(StdInstant::now());
                            server_transmission_started[i] = true;
                            info!("Server {} started receiving data", i);
                        }

                        while server_socket.can_recv() {
                            match server_socket.recv(|buffer| {
                                let received_len = buffer.len();
                                (received_len, received_len)
                            }) {
                                Ok(received) => {
                                    if received > 0 {
                                        bytes_received[i] += received as u64;

                                        if bytes_received[i] % 1_000_000 == 0 {
                                            info!(
                                                "Server {} received {} KB",
                                                i,
                                                bytes_received[i] / 1_000
                                            );
                                        }
                                    }
                                }
                                Err(_) => {
                                    break;
                                }
                            }
                        }
                    }

                    // when the connection is closed
                    if !server_socket.is_active() && server_transmission_started[i] {
                        if let Some(start_time) = server_start_times[i] {
                            let end_time = StdInstant::now();
                            let elapsed = end_time.duration_since(start_time).as_secs_f64();
                            if elapsed > 0.0 {
                                let throughput_gbps =
                                    (bytes_received[i] as f64 * 8.0) / (elapsed * 1_000_000_000.0);
                                info!(
                                    "Server {} throughput: {:.3} Gbps ({} bytes in {:.3}s)",
                                    i, throughput_gbps, bytes_received[i], elapsed
                                );
                            }
                            server_transmission_started[i] = false;
                        }
                    }
                }

                // Client sockets handling
                for i in 0..flows.len() {
                    let client_handle = client_handles[i];
                    let flow = &flows[i];
                    let client_socket = sockets.get_mut::<tcp::Socket>(client_handle);

                    if !client_connections_status[i] && !client_socket.is_open() {
                        if flow.remote_addr != [0, 0, 0, 0] {
                            let remote_addr = IpAddress::v4(
                                flow.remote_addr[0],
                                flow.remote_addr[1],
                                flow.remote_addr[2],
                                flow.remote_addr[3],
                            );
                            let remote_port = base_server_port as u16;

                            client_socket
                                .connect(
                                    iface.context(),
                                    (remote_addr, remote_port),
                                    flow.client_port,
                                )
                                .unwrap();
                            info!(
                                "Client {} connecting from port {} to {}:{}",
                                i, flow.client_port, remote_addr, remote_port
                            );
                        }
                    }

                    if client_socket.is_active() && !client_connections_status[i] {
                        client_connections_status[i] = true;
                        info!(
                            "Client {} connected successfully to remote address {}",
                            i,
                            format!(
                                "{}.{}.{}.{}",
                                flow.remote_addr[0],
                                flow.remote_addr[1],
                                flow.remote_addr[2],
                                flow.remote_addr[3]
                            )
                        );
                    }

                    // sending packets with per-connection traffic settings
                    if client_socket.is_active() {
                        if bytes_left[i] > 0 {
                            if !client_transmission_started[i] && client_socket.can_send() {
                                client_start_times[i] = Some(StdInstant::now());
                                client_transmission_started[i] = true;
                                info!("Client {} started transmission", i);
                            }

                            while client_socket.can_send() && bytes_left[i] > 0 {
                                match client_socket.send(|buf| {
                                    let to_write = cmp::min(buf.len(), bytes_left[i] as usize);
                                    // buf[..to_write].fill(0xAA); // removed
                                    (to_write, to_write)
                                }) {
                                    Ok(sent) => {
                                        if sent > 0 {
                                            bytes_left[i] -= sent as u64;
                                            bytes_sent[i] += sent as u64;

                                            if bytes_sent[i] % 1_000_000 == 0 {
                                                info!(
                                                    "Client {} sent {} KB",
                                                    i,
                                                    bytes_sent[i] / 1_000
                                                );
                                            }
                                        }
                                    }
                                    Err(_) => {
                                        error!(
                                            "Failed to send data block from client {}, packets dropped",
                                            i
                                        );
                                        break;
                                    }
                                }
                            }
                        }

                        if bytes_left[i] == 0 && !client_closing_initiated[i] {
                            let end_time = StdInstant::now();
                            if let Some(start_time) = client_start_times[i] {
                                let elapsed = end_time.duration_since(start_time).as_secs_f64();
                                let throughput_gbps =
                                    (bytes_sent[i] as f64 * 8.0) / (elapsed * 1_000_000_000.0);
                                info!(
                                    "Client {} throughput: {:.3} Gbps ({} bytes in {:.3}s)",
                                    i, throughput_gbps, bytes_sent[i], elapsed
                                );
                            }
                            client_socket.close();
                            client_closing_initiated[i] = true;
                        }
                    }
                }

                iface.poll(timestamp, &mut device, &mut sockets);

                match iface.poll_at(timestamp, &sockets) {
                    Some(poll_at) if timestamp < poll_at => {
                        phy_wait(device.signal_reader.as_raw_fd(), Some(poll_at - timestamp))
                            .expect("wait error");
                    }
                    Some(_) => (),
                    None => {
                        phy_wait(
                            device.signal_reader.as_raw_fd(),
                            Some(smoltcp::time::Duration::from_millis(100)),
                        )
                        .expect("wait error");
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
        // write bytes to send signal
        if let Some(writer) = &mut *self.signal_writer.lock().unwrap() {
            if writer.write(&[1]).is_err() {}
        }
    }
}

#[derive(Clone)]
struct VirtualDevice {
    config: LocalConfig,
    receiver: flume::Receiver<Packet>,
    sender: ProcessorHandle,
    signal_reader: Arc<PipeReader>,
}

impl Device for VirtualDevice {
    type RxToken<'a> = PacketRxToken;
    type TxToken<'a> = PacketTxToken;

    fn receive(&mut self, _timestamp: Instant) -> Option<(Self::RxToken<'_>, Self::TxToken<'_>)> {
        let mut buf = [0u8; 64];
        while self.signal_reader.as_ref().read(&mut buf).is_ok() {}

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
