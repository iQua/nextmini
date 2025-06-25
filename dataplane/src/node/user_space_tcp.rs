use std::net::Ipv4Addr;
use std::thread;

use flume;
use smoltcp::iface::{Config, Interface, SocketSet};
use smoltcp::time::{Duration, Instant};
use smoltcp::wire::{IpAddress, IpCidr};
use tracing::error;

use crate::node::config::LocalConfig;
use crate::node::user_space_tcp_client::UserSpaceTcpClient;
use crate::node::user_space_tcp_server::UserSpaceTcpServer;
use crate::node::user_space_tcp_utils::VirtualDevice;

use crate::node::LocalDestination;
use crate::node::packet::Packet;
use crate::node::processor::ProcessorHandle;
use nextmini_messages::Flow;

#[derive(Clone, Debug)]
pub struct UserSpaceTcpSource {
    config: LocalConfig,
    pub ip_addr: Ipv4Addr,
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
    pub fn start(&self, flows: Vec<Flow>) {
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
                .push(IpCidr::new(IpAddress::from(self.ip_addr), 24))
                .unwrap();
        });

        // creates the TCP socket set
        let mut sockets = SocketSet::new(vec![]);

        // creates server sockets based on the number of inbound flows
        // TODO: thinks of a new design
        let incoming_flows: Vec<_> = flows
            .iter()
            .filter(|f| f.dst_node_id == self.config.node_id)
            .cloned() // gets the ownership of Flow
            .collect();

        let node_id = self.config.node_id;

        let outgoing_flows: Vec<_> = flows
            .iter()
            .filter(|f| f.src_node_id == node_id)
            .cloned()
            .collect();

        // spawns a new thread as smoltcp is not designed to use async Rust and Tokio
        thread::spawn(move || {
            let mut device = device;

            let mut server =
                UserSpaceTcpServer::new(config_clone.clone(), incoming_flows, &mut sockets);

            let mut client =
                UserSpaceTcpClient::new(config_clone.clone(), outgoing_flows, &mut sockets);

            loop {
                let timestamp = Instant::now();

                iface.poll(timestamp, &mut device, &mut sockets);

                // starts listening from client
                server.process(&mut sockets);
                // starts connecting to server
                client.process(&mut sockets, iface.context());

                // match iface.poll_delay(timestamp, &sockets) {
                //     Some(Duration::ZERO) => {
                //         continue;
                //     }
                //     Some(delay) => {
                //         // println!("Delayed for {}", delay);
                //         // thread::sleep(delay.into());
                //         continue;
                //     }
                //     None => {
                //         continue;
                //         // thread::sleep(StdDuration::from_millis(1));
                //     }
                // }
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
