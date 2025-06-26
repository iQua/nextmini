use std::net::Ipv4Addr;
use std::thread;

use flume;
use smoltcp::iface::{Config, Interface, SocketSet};
use smoltcp::time::Instant;
use smoltcp::wire::{IpAddress, IpCidr};
use tracing::error;

use nextmini_messages::Flow;

use crate::node::LocalDestination;
use crate::node::config::LocalConfig;
use crate::node::flow::client::UserSpaceTcpClient;
use crate::node::flow::device::VirtualDevice;
use crate::node::flow::server::UserSpaceTcpServer;
use crate::node::packet::Packet;
use crate::node::processor::ProcessorHandle;

pub enum UserSpaceTcpMessage {
    AddFlows(Vec<Flow>),
}

#[derive(Clone)]
pub struct UserSpaceTcpHandle {
    sender: flume::Sender<UserSpaceTcpMessage>,
}

// wraps the sending AddFlows message channel
impl UserSpaceTcpHandle {
    pub fn new(sender: flume::Sender<UserSpaceTcpMessage>) -> Self {
        Self { sender }
    }

    // sends a vector of flows to be added to the user-space TCP source.
    pub fn add_flows(&self, flows: Vec<Flow>) {
        let _ = self.sender.send(UserSpaceTcpMessage::AddFlows(flows));
    }
}

#[derive(Clone, Debug)]
pub struct UserSpaceTcpSource {
    config: LocalConfig,
    pub ip_addr: Ipv4Addr,
    processor_handle: ProcessorHandle,
    packet_sender: flume::Sender<Packet>,
    packet_receiver: flume::Receiver<Packet>,

    // the sender for sending AddFlows message to the user-space TCP thread.
    pub flow_sender: flume::Sender<UserSpaceTcpMessage>,
}

// creates a new user-space TCP source and a channel for receiving AddFlows message.
impl UserSpaceTcpSource {
    pub fn new(
        config: LocalConfig,
        ip_addr: Ipv4Addr,
        processor_handle: ProcessorHandle,
    ) -> (Self, flume::Receiver<UserSpaceTcpMessage>) {
        let (packet_sender, packet_receiver) = flume::bounded(config.channel_capacity);

        // an unbounded channel for receiving flows
        let (flow_sender, flow_receiver) = flume::unbounded();

        let tcp_source = Self {
            config,
            ip_addr,
            processor_handle,
            packet_sender: packet_sender.clone(),
            packet_receiver,
            flow_sender,
        };

        (tcp_source, flow_receiver)
    }

    /// Starts the user-space TCP source as a virtual device.
    pub fn start(&self, flow_receiver: flume::Receiver<UserSpaceTcpMessage>) {
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
        let node_id = self.config.node_id;

        // spawns a new thread as smoltcp is not designed to use async Rust and Tokio
        thread::spawn(move || {
            let mut device = device;

            // creates server and client instances immediately with empty initial flows.
            // waits for the new AddFlows messgaes.
            let mut server = UserSpaceTcpServer::new(config_clone.clone(), vec![], &mut sockets);
            let mut client = UserSpaceTcpClient::new(config_clone.clone(), vec![], &mut sockets);

            loop {
                let timestamp = Instant::now();

                // transmits packets queued in the sockets
                // and receives packets queued in the device.
                iface.poll(timestamp, &mut device, &mut sockets);

                // checks for new flow control messages with non-blocking.
                if let Ok(message) = flow_receiver.try_recv() {
                    match message {
                        UserSpaceTcpMessage::AddFlows(flows) => {
                            // filters for incoming flows for this node.
                            let incoming_flows: Vec<_> = flows
                                .iter()
                                .filter(|f| f.dst_node_id == node_id) // as server side
                                .cloned()
                                .collect();

                            // filters for outgoing flows from this node.
                            let outgoing_flows: Vec<_> = flows
                                .iter()
                                .filter(|f| f.src_node_id == node_id) // as client side
                                .cloned()
                                .collect();

                            // adds flows to the existing server and client for the first time or afterwards.
                            server.add_flows(incoming_flows, &mut sockets);
                            client.add_flows(outgoing_flows, &mut sockets);
                        }
                    }
                }

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
