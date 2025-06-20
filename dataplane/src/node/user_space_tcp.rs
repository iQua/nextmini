use std::net::Ipv4Addr;
use std::thread;

use flume;
use smoltcp::iface::{Config, Interface, SocketSet};
use smoltcp::phy::{Device, DeviceCapabilities, Medium};
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
            let mut device = device;

            loop {
                let now = Instant::now();
                iface.poll(now, &mut device, &mut sockets);

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

                    if server_socket.is_active() {
                        if server_socket.can_recv() {
                            let mut buffer = [0u8; 4096];
                            if let Ok(len) = server_socket.recv_slice(&mut buffer) {
                                bytes_received[i] += len as u64;

                                if bytes_received[i] % 1_000 == 0 || len > 0 {
                                    info!(
                                        "Server {} received {} KB total ({} bytes this time)",
                                        i,
                                        bytes_received[i] / 1_000,
                                        len
                                    );
                                }
                            }
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
                    if client_socket.is_active() && client_socket.can_send() && bytes_left[i] > 0 {
                        match client_socket.send(|buf| {
                            let to_write = std::cmp::min(
                                std::cmp::min(buf.len(), flow.data_size),
                                bytes_left[i] as usize,
                            );
                            buf[..to_write].fill(0xAA);

                            (to_write, to_write) // (bytes_to_enqueue, return_value)
                        }) {
                            Ok(sent) => {
                                bytes_left[i] -= sent as u64;
                                bytes_sent[i] += sent as u64;

                                if bytes_sent[i] % 1_000 == 0 || bytes_left[i] == 0 {
                                    let total_size = flow.flow_size.unwrap_or(10_000_000);
                                    let progress =
                                        (bytes_sent[i] as f64 / total_size as f64 * 100.0) as u32;
                                    info!(
                                        "Client {} sent {} KB / {} KB ({}%) to {}",
                                        i,
                                        bytes_sent[i] / 1_000,
                                        total_size / 1_000,
                                        progress,
                                        format!(
                                            "{}.{}.{}.{}",
                                            flow.remote_addr[0],
                                            flow.remote_addr[1],
                                            flow.remote_addr[2],
                                            flow.remote_addr[3]
                                        )
                                    );
                                }

                                if bytes_left[i] == 0 {
                                    info!(
                                        "Client {} completed sending {} KB to {}",
                                        i,
                                        bytes_sent[i] / 1_000,
                                        format!(
                                            "{}.{}.{}.{}",
                                            flow.remote_addr[0],
                                            flow.remote_addr[1],
                                            flow.remote_addr[2],
                                            flow.remote_addr[3]
                                        )
                                    );
                                }
                            }
                            Err(_) => {
                                error!(
                                    "Failed to send data block from client {}, packets dropped",
                                    i
                                );
                            }
                        }
                    }
                }

                thread::sleep(std::time::Duration::from_millis(10)); // Control polling rate
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
