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
    pub fn new(
        config: LocalConfig,
        ip_addr: Ipv4Addr,
        processor_handle: ProcessorHandle,
    ) -> (Self, flume::Sender<Packet>) {
        let (packet_sender, packet_receiver) = flume::bounded(config.channel_capacity);

        let tcp_source = Self {
            config,
            ip_addr,
            processor_handle,
            packet_sender: packet_sender.clone(),
            packet_receiver,
        };

        (tcp_source, packet_sender)
    }

    /// Starts the user-space TCP source as a virtual device.
    pub fn start(&self) {
        let device = VirtualDevice {
            config: self.config.clone(),
            receiver: self.packet_receiver.clone(),
            sender: self.processor_handle.clone(),
        };

        // sets up Layer 2
        let config = Config::new(smoltcp::wire::HardwareAddress::Ethernet(
            smoltcp::wire::EthernetAddress([0x02, 0x00, 0x00, 0x00, 0x00, 0x01]),
        ));

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
        let tcp_rx_buffer = tcp::SocketBuffer::new(vec![0; 65535]);
        let tcp_tx_buffer = tcp::SocketBuffer::new(vec![0; 65535]);
        let tcp_socket = tcp::Socket::new(tcp_rx_buffer, tcp_tx_buffer);
        let tcp_handle = sockets.add(tcp_socket);

        // connects to a remote endpoint: needs to be revised to obtain the destination IP and
        // port number from the local (or controller's) configuration file
        let remote_addr = IpAddress::v4(192, 168, 1, 1);
        let remote_port = 80;
        {
            let socket = sockets.get_mut::<tcp::Socket>(tcp_handle);
            socket
                .connect(iface.context(), (remote_addr, remote_port), 12345)
                .unwrap();
        }

        // spawns a new thread as smoltcp is not designed to use async Rust and Tokio
        thread::spawn(move || {
            loop {
                let now = Instant::now();
                iface.poll(now, &mut device.clone(), &mut sockets);

                let socket = sockets.get_mut::<tcp::Socket>(tcp_handle);

                if socket.can_send() {
                    socket.send_slice(b"Hello from user-space TCP!").unwrap();
                }

                if socket.may_recv() {
                    let mut buffer = [0u8; 1024];
                    if let Ok(len) = socket.recv_slice(&mut buffer) {
                        info!("Received {} bytes: {:?}", len, &buffer[..len]);
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
        caps.medium = Medium::Ethernet;
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
