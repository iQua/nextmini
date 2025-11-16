// A TCP server for user-space flows, implemented using SmolTcp.
use ahash::AHashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::thread;
use std::time::Duration;

use smoltcp::iface::{Config, Interface, SocketSet};
use smoltcp::socket::tcp;
use smoltcp::time::Instant;
use smoltcp::wire::{HardwareAddress, IpAddress, IpCidr};
use tokio::sync::mpsc;
use tracing::{error, info};

use nextmini_messages::Flow;
use nextmini_messages::FlowSpec;

use crate::node::config::LocalConfig;
use crate::node::flow::device::VirtualDevice;
use crate::node::flow::{SOCKET_BUFFER_SIZE, UserSpaceSender};
use crate::node::packet::Packet;
use crate::node::processor::ProcessorHandle;
use crate::node::{FlowId, FlowIdExt, NodeIdExt};

#[derive(Debug, Clone)]
pub struct UserSpaceServerHandle {
    config: LocalConfig,

    processors: ProcessorHandle,

    // stores flow specifications keyed by source IP address to retrieve flow configuration
    flow_specs: Arc<Mutex<AHashMap<IpAddress, FlowSpec>>>,

    // a hashmap of flow IDs to packet channel senders needs to be maintained since multiple processors
    // may request adding a new server for the same flow ID concurrently, but the new server thread should
    // only be created once for each flow ID. Subsequent requests will be served by consulting this hashmap.
    // This hashmap also needs to be shared across all processor tasks in a thread-safe way.
    packet_senders: Arc<Mutex<AHashMap<FlowId, mpsc::Sender<Packet>>>>,
}

impl UserSpaceServerHandle {
    pub fn new(config: LocalConfig, processors: ProcessorHandle) -> Self {
        Self {
            config,
            processors,
            flow_specs: Arc::new(Mutex::new(AHashMap::new())),
            packet_senders: Arc::new(Mutex::new(AHashMap::new())),
        }
    }

    // Stores flow specification for later retrieval by servers.
    // This creates a mapping from source IP to flow configuration so that when
    // a server is created for an incoming connection, it can consult the correct
    // flow rate limits.
    pub fn store_flow_spec(&self, flow: Flow) {
        let src_ip = flow
            .src_node_id
            .ip_addr(self.config.user_space_base_addr, self.config.local_netmask);
        let mut specs = self.flow_specs.lock().unwrap();

        specs.insert(IpAddress::from(src_ip), flow.flow_spec);
    }

    // Starts a new server thread for a user-space TCP flow.
    pub fn add_server(&self, flow_id: FlowId) -> UserSpaceSender {
        // consults the shared hashmap for channels that may have just been created
        let mut senders = self.packet_senders.lock().unwrap();
        if let Some(existing_sender) = senders.get(&flow_id) {
            return existing_sender.clone();
        }

        // creates a new channel for processors to send to the user-space TCP server
        let (packet_sender, packet_receiver) = mpsc::channel(self.config.channel_capacity);

        // inserts into the shared hashmap for later retrieval, if the same flow ID is requested
        senders.insert(flow_id, packet_sender.clone());

        let sender: UserSpaceSender = packet_sender.clone();
        self.processors.connect_user_space_sender(flow_id, sender);

        let config = self.config.clone();
        let processors = self.processors.clone();

        // consults the FlowSpec hashmap using the source IP of the incoming packet
        let src_ip = flow_id.src_ip();
        let specs = self.flow_specs.lock().unwrap();

        let flow_rate = specs
            .get(&IpAddress::from(src_ip))
            .and_then(|spec| spec.flow_rate);

        let server = UserSpaceServer::new(config, flow_id, flow_rate, processors, packet_receiver);

        // spawns a new server thread for each user-space TCP flow
        thread::spawn(move || {
            server.run();
        });

        packet_sender
    }
}

struct UserSpaceServer {
    config: LocalConfig,
    flow_id: FlowId,
    flow_rate: Option<usize>,
    processors: ProcessorHandle,
    packet_receiver: Option<mpsc::Receiver<Packet>>,
}

impl UserSpaceServer {
    fn new(
        config: LocalConfig,
        flow_id: FlowId,
        flow_rate: Option<usize>,
        processors: ProcessorHandle,
        packet_receiver: mpsc::Receiver<Packet>,
    ) -> Self {
        info!("Creating a new user-space TCP server for a single flow.");

        Self {
            config,
            flow_id,
            flow_rate,
            processors,
            packet_receiver: Some(packet_receiver),
        }
    }

    fn run(mut self) {
        let packet_receiver = self.packet_receiver.take().unwrap();

        let mut device = VirtualDevice {
            config: self.config.clone(),
            receiver: packet_receiver,
            sender: self.processors.clone(),
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

        let mut sockets = SocketSet::new(vec![]);

        let rx_buffer = tcp::SocketBuffer::new(vec![0; SOCKET_BUFFER_SIZE]);
        let tx_buffer = tcp::SocketBuffer::new(vec![0; SOCKET_BUFFER_SIZE]);
        let socket = tcp::Socket::new(rx_buffer, tx_buffer);
        let socket_handle = sockets.add(socket);

        let socket = sockets.get_mut::<tcp::Socket>(socket_handle);

        // listens on the socket
        if !socket.is_open()
            && let Err(e) = socket.listen(self.flow_id.dst_port())
        {
            error!(
                "Server failed to listen on port {}: {:?}",
                self.flow_id.dst_port(),
                e
            );
        }

        loop {
            let timestamp = Instant::now();

            iface.poll(timestamp, &mut device, &mut sockets);

            let socket = sockets.get_mut::<tcp::Socket>(socket_handle);

            if socket.is_active() {
                self.recv(socket);
            } else {
                // removes the packet sender from the processors
                self.processors.disconnect_user_space_sender(self.flow_id);

                info!(
                    "The user-space TCP server on node {} has terminated. It has been receiving from node {}.",
                    self.config.node_id,
                    self.config.ip_to_node_id(self.flow_id.src_ip())
                );
                break;
            }

            match self.flow_rate {
                Some(rate) if rate < 200_000_000 => {
                    // for lower flow rates, sleep briefly to reduce CPU usage
                    if device.receiver.is_empty() {
                        thread::sleep(Duration::from_nanos(1));
                    }
                }
                _ => {
                    // for higher flow rates, CPU will run at 100% for maximum performance
                }
            }
        }
    }

    fn recv(&mut self, socket: &mut tcp::Socket) {
        if socket.can_recv()
            && let Err(e) = socket.recv(|buf| (buf.len(), buf.len()))
        {
            error!("Error receiving from a user-space TCP client: {:?}", e);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use nextmini_messages::FlowLen;
    use nextmini_messages::FlowTransport;

    use super::*;

    fn make_test_config() -> LocalConfig {
        LocalConfig {
            node_id: 2,
            num_packet_processors: 1,
            channel_capacity: 16,
            user_space_server_port: 5000,
            ..Default::default()
        }
    }

    fn make_flow(src_node_id: usize, dst_node_id: usize, rate: Option<usize>) -> Flow {
        Flow {
            controller_id: None,
            src_node_id,
            dst_node_id,
            flow_spec: FlowSpec {
                flow_len: FlowLen::Bytes(1024),
                flow_rate: rate,
                flow_weight: None,
                transport: FlowTransport::Tcp,
            },
        }
    }

    fn make_flow_id(config: &LocalConfig, flow: &Flow, src_port: u16) -> FlowId {
        let src_ip = flow
            .src_node_id
            .ip_addr(config.user_space_base_addr, config.local_netmask);
        let dst_ip = flow
            .dst_node_id
            .ip_addr(config.user_space_base_addr, config.local_netmask);
        let dst_port = config.user_space_server_port;

        ((u32::from(src_ip) as u128) << 96)
            | ((u32::from(dst_ip) as u128) << 64)
            | ((src_port as u128) << 48)
            | ((dst_port as u128) << 32)
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn store_flow_spec_registers_flow_rate() {
        let config = make_test_config();
        let processors = ProcessorHandle::new(config.clone());
        let handle = UserSpaceServerHandle::new(config.clone(), processors);

        let flow = make_flow(5, config.node_id, Some(25_000));
        handle.store_flow_spec(flow.clone());

        let src_ip = flow
            .src_node_id
            .ip_addr(config.user_space_base_addr, config.local_netmask);
        let specs = handle.flow_specs.lock().unwrap();
        let stored = specs
            .get(&IpAddress::from(src_ip))
            .expect("flow spec should be recorded for source IP");

        assert_eq!(
            stored.flow_rate, flow.flow_spec.flow_rate,
            "stored flow spec should preserve the controller-provided rate"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn add_server_reuses_existing_sender_for_same_flow() {
        let config = make_test_config();
        let processors = ProcessorHandle::new(config.clone());
        let handle = UserSpaceServerHandle::new(config.clone(), processors);

        let flow = make_flow(6, config.node_id, Some(10));
        handle.store_flow_spec(flow.clone());

        let flow_id = make_flow_id(&config, &flow, 4100);

        let sender_first = handle.add_server(flow_id);
        // give the server thread a moment to start
        tokio::time::sleep(Duration::from_millis(5)).await;
        let sender_second = handle.add_server(flow_id);

        assert!(
            sender_first.same_channel(&sender_second),
            "a subsequent request for the same flow should reuse the existing sender"
        );

        let senders = handle.packet_senders.lock().unwrap();
        assert_eq!(
            senders.len(),
            1,
            "only one sender entry should exist for duplicate add_server calls"
        );
    }
}
