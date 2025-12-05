// A TCP client for user-space flows, implemented using SmolTcp.
use std::cmp;
use std::thread;
use std::time::{Duration, Instant as StdInstant};

use smoltcp::iface::{Config, Interface, SocketSet};
use smoltcp::socket::tcp;
use smoltcp::time::Instant;
use smoltcp::wire::{HardwareAddress, IpAddress, IpCidr};
use tokio::sync::mpsc;
use tracing::{error, info};

use nextmini_messages::{Flow, FlowLen};

use crate::node::config::LocalConfig;
use crate::node::controller::flowstats::FlowStatsReporterHandle;
use crate::node::flow::SOCKET_BUFFER_SIZE;
use crate::node::flow::device::VirtualDevice;
use crate::node::flow::state::ConnectionState;
use crate::node::packet::Packet;
use crate::node::processor::ProcessorHandle;
use crate::node::{FlowId, FlowIdExt, NodeIdExt};

#[derive(Debug, Clone)]
pub struct UserSpaceClientHandle {
    config: LocalConfig,
    processors: ProcessorHandle,
    flowstats_reporter: FlowStatsReporterHandle,
    next_client_port: u16,
}

impl UserSpaceClientHandle {
    pub fn new(
        config: LocalConfig,
        processors: ProcessorHandle,
        flowstats_reporter: FlowStatsReporterHandle,
    ) -> Self {
        let next_client_port = config.user_space_client_port;

        Self {
            config,
            processors,
            flowstats_reporter,
            next_client_port,
        }
    }

    pub fn add_flows(&mut self, flows: Vec<Flow>) {
        for flow in flows {
            let config = self.config.clone();
            let processors = self.processors.clone();
            let flowstats_reporter = self.flowstats_reporter.clone();
            self.next_client_port += 1;
            let client_port = self.next_client_port;
            let (packet_sender, packet_receiver) = mpsc::channel(config.channel_capacity);

            // extracts the flow ID, where server is source, client is destination
            let client_ip = config
                .node_id
                .ip_addr(config.user_space_base_addr, config.local_netmask);
            let server_ip = flow
                .dst_node_id
                .ip_addr(config.user_space_base_addr, config.local_netmask);
            let server_port = config.user_space_server_port;

            // connects this client as a local destination for packets destined to this flow
            let flow_id = ((u32::from(server_ip) as u128) << 96)
                | ((u32::from(client_ip) as u128) << 64)
                | ((server_port as u128) << 48)
                | ((client_port as u128) << 32);

            if let Some(route_id) = flow.route_id {
                // route binding should follow client -> server direction
                self.processors
                    .set_route_for_flow(flow_id.reverse(), route_id);
            }

            self.processors
                .connect_user_space_sender(flow_id, packet_sender);

            // sets flow weights for this flow, where the client is the source and the server is the destination
            if let Some(weight) = flow.flow_spec.flow_weight {
                let flow_id = flow_id.reverse();

                info!(
                    "Set flow weight {} for a user space TCP flow from node {} (port {}) to node {} (port {}).",
                    weight, flow.src_node_id, client_port, flow.dst_node_id, server_port
                );
                self.processors.set_flow_weight(flow_id, weight);
            }

            let client = UserSpaceClient::new(
                config,
                flow,
                processors,
                flowstats_reporter,
                flow_id,
                client_port,
                packet_receiver,
            );

            // spawns a new thread as SmolTcp is not designed to use async Rust and Tokio
            thread::spawn(move || {
                client.run();
            });
        }
    }
}

struct UserSpaceClient {
    config: LocalConfig,
    flow: Flow,
    processors: ProcessorHandle,
    flowstats_reporter: FlowStatsReporterHandle,
    packet_receiver: Option<mpsc::Receiver<Packet>>,
    state: ConnectionState,
    client_port: u16,
    flow_id: FlowId,
    start_reported: bool,
}

impl UserSpaceClient {
    fn new(
        config: LocalConfig,
        flow: Flow,
        processors: ProcessorHandle,
        flowstats_reporter: FlowStatsReporterHandle,
        flow_id: FlowId,
        client_port: u16,
        packet_receiver: mpsc::Receiver<Packet>,
    ) -> Self {
        let state = ConnectionState {
            start_time: StdInstant::now(),
            time_last_updated: StdInstant::now(),
            bytes_last_updated: 0,
            bytes_total: 0,
        };

        Self {
            config,
            flow,
            processors,
            flowstats_reporter,
            packet_receiver: Some(packet_receiver),
            state,
            client_port,
            flow_id,
            start_reported: false,
        }
    }

    /// Runs a user-space TCP client by connecting and sending to a server.
    fn run(mut self) {
        let packet_receiver = self.packet_receiver.take().unwrap();

        // creates a virtual device using the passed processor handle
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

        // creates a socket set for a new TCP client
        let mut sockets = SocketSet::new(vec![]);

        let rx_buffer = tcp::SocketBuffer::new(vec![0; SOCKET_BUFFER_SIZE]);
        let tx_buffer = tcp::SocketBuffer::new(vec![0; SOCKET_BUFFER_SIZE]);

        let socket = tcp::Socket::new(rx_buffer, tx_buffer);
        let socket_handle = sockets.add(socket);

        // handles client connection and sends out data
        let socket = sockets.get_mut::<tcp::Socket>(socket_handle);
        self.connect(socket, iface.context());

        loop {
            // gets the current time
            let timestamp = Instant::now();

            // polls the interface for packet transmission/reception
            iface.poll(timestamp, &mut device, &mut sockets);

            let socket = sockets.get_mut::<tcp::Socket>(socket_handle);

            if socket.is_active() {
                self.send(socket);
            } else {
                // removes the user-space packet sender from the processors
                self.processors.disconnect_user_space_sender(self.flow_id);

                // reports flow completion to the controller
                self.flowstats_reporter
                    .report_flow_finished(self.flow_id, self.flow.controller_id);

                info!(
                    "The user-space TCP flow from node {} to node {} has finished. The client is closing.",
                    self.flow.src_node_id, self.flow.dst_node_id
                );
                break;
            }
        }
    }

    // Connects to a user-space TCP server.
    fn connect(&mut self, socket: &mut tcp::Socket, iface_context: &mut smoltcp::iface::Context) {
        if !socket.is_open() {
            let remote_addr = IpAddress::from(
                self.flow
                    .dst_node_id
                    .ip_addr(self.config.user_space_base_addr, self.config.local_netmask),
            );
            let remote_endpoint = (remote_addr, self.config.user_space_server_port);

            match socket.connect(iface_context, remote_endpoint, self.client_port) {
                Ok(_) => {
                    info!(
                        "A new user-space TCP client has connected to a server from port {} to {}:{}.",
                        self.client_port, remote_addr, self.config.user_space_server_port
                    );
                }
                Err(e) => {
                    error!("Error connecting to a user-space TCP server: {:?}", e);
                }
            }
        }
    }

    /// Sends data, as much as possible or up to a certain flow rate, to the user-space TCP server.
    fn send(&mut self, socket: &mut tcp::Socket) {
        if socket.can_send() {
            if !self.start_reported {
                match self.flow.controller_id {
                    Some(controller_id) => {
                        self.flowstats_reporter
                            .report_user_flow_start(self.flow_id, controller_id);

                        self.start_reported = true;

                        info!(
                            "Reported start of user-space flow {} from node {} to node {}.",
                            controller_id, self.flow.src_node_id, self.flow.dst_node_id
                        );
                    }
                    None => {
                        error!(
                            "User-space flow from node {} to node {} was not from the controller.",
                            self.flow.src_node_id, self.flow.dst_node_id
                        );

                        self.start_reported = true;
                    }
                }
            }

            let remaining = match self.flow.flow_spec.flow_len {
                FlowLen::Bytes(size) => size as u64 - self.state.bytes_total,
                _ => SOCKET_BUFFER_SIZE as u64, // For duration-based flows
            };

            let sending_start_time = StdInstant::now();

            match socket.send(|buf| {
                let to_send = cmp::min(buf.len(), remaining as usize);
                buf[..to_send].fill(0xAA);
                (to_send, to_send)
            }) {
                Ok(sent) if sent > 0 => {
                    // calculates the time to wait for a specified flow rate
                    if let Some(flow_rate) = self.flow.flow_spec.flow_rate {
                        let expected_time = Duration::from_secs_f64(sent as f64 / flow_rate as f64);
                        let actual_time = sending_start_time.elapsed();

                        if expected_time > actual_time {
                            thread::sleep(expected_time - actual_time);
                        }
                    }

                    self.state
                        .update(self.config.node_id, self.flow.dst_node_id, sent as u64);

                    if self
                        .flow
                        .flow_spec
                        .flow_len
                        .exceeded(self.state.bytes_total, self.state.start_time)
                    {
                        socket.abort();
                    }
                }
                Err(e) => {
                    error!("Error sending to a user-space TCP server: {:?}", e);
                }
                Ok(_) => {}
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use tokio::time::{Duration, sleep, timeout};

    use nextmini_messages::{FlowSpec, FlowTransport};

    use crate::node::FlowIdExt;
    use crate::node::controller::interface::ControllerInterfaceHandle;
    use crate::node::processor::ProcessorMessage;

    fn make_test_config() -> LocalConfig {
        LocalConfig {
            node_id: 1,
            num_packet_processors: 1,
            channel_capacity: 32,
            user_space_client_port: 4000,
            user_space_server_port: 5000,
            ..Default::default()
        }
    }

    fn make_flow(dst_node_id: usize, weight: Option<usize>) -> Flow {
        Flow {
            controller_id: Some(42),
            src_node_id: 1,
            dst_node_id,
            route_id: None,
            flow_spec: FlowSpec {
                flow_len: FlowLen::Bytes(1024),
                flow_rate: Some(1_000_000),
                flow_weight: weight,
                transport: FlowTransport::Tcp,
            },
        }
    }

    fn expected_flow_id(config: &LocalConfig, flow: &Flow, client_port: u16) -> FlowId {
        let client_ip = config
            .node_id
            .ip_addr(config.user_space_base_addr, config.local_netmask);
        let server_ip = flow
            .dst_node_id
            .ip_addr(config.user_space_base_addr, config.local_netmask);

        ((u32::from(server_ip) as u128) << 96)
            | ((u32::from(client_ip) as u128) << 64)
            | ((config.user_space_server_port as u128) << 48)
            | ((client_port as u128) << 32)
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn add_flows_registers_sender_and_weight() {
        let config = make_test_config();
        let processors = ProcessorHandle::new(config.clone());

        let (controller, _controller_rx) = ControllerInterfaceHandle::test_handle();
        let flowstats = FlowStatsReporterHandle::new(controller, config.clone());

        let mut client_handle =
            UserSpaceClientHandle::new(config.clone(), processors.clone(), flowstats);
        let mut broadcast_rx = processors.broadcast_sender().subscribe();

        let weight = 7usize;
        let flow = make_flow(2, Some(weight));

        client_handle.add_flows(vec![flow.clone()]);
        // allow spawned client thread to progress
        sleep(Duration::from_millis(10)).await;

        let expected_flow_id = expected_flow_id(&config, &flow, config.user_space_client_port + 1);

        let mut saw_connect = false;
        let mut saw_weight = false;

        for _ in 0..5 {
            if let Ok(msg) = timeout(Duration::from_millis(200), broadcast_rx.recv()).await {
                match msg.expect("processor channel open") {
                    ProcessorMessage::ConnectUserSpaceSender { flow_id, .. } => {
                        assert_eq!(flow_id, expected_flow_id);
                        saw_connect = true;
                    }
                    ProcessorMessage::SetFlowWeight(flow_id, value) => {
                        assert_eq!(flow_id, expected_flow_id.reverse());
                        assert_eq!(value, weight);
                        saw_weight = true;
                    }
                    ProcessorMessage::DisconnectUserSpaceSender(_flow_id) => {}
                    _ => {}
                }
            }
        }

        assert!(
            saw_connect,
            "add_flows should connect a user-space sender for the flow"
        );
        assert!(
            saw_weight,
            "add_flows should set the flow weight when provided"
        );
        assert_eq!(
            client_handle.next_client_port,
            config.user_space_client_port + 1,
            "client port counter should advance after provisioning a flow"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn add_flows_without_weight_skips_weight_update() {
        let config = make_test_config();
        let processors = ProcessorHandle::new(config.clone());

        let (controller, _controller_rx) = ControllerInterfaceHandle::test_handle();
        let flowstats = FlowStatsReporterHandle::new(controller, config.clone());

        let mut client_handle =
            UserSpaceClientHandle::new(config.clone(), processors.clone(), flowstats);
        let mut broadcast_rx = processors.broadcast_sender().subscribe();

        let flow = make_flow(3, None);
        client_handle.add_flows(vec![flow.clone()]);
        sleep(Duration::from_millis(10)).await;

        let mut saw_weight_message = false;

        for _ in 0..4 {
            if let Ok(msg) = timeout(Duration::from_millis(200), broadcast_rx.recv()).await
                && matches!(
                    msg.expect("processor channel open"),
                    ProcessorMessage::SetFlowWeight(..)
                )
            {
                saw_weight_message = true;
                break;
            }
        }

        assert!(
            !saw_weight_message,
            "flow weight should not be updated when the specification omits it"
        );
        assert_eq!(
            client_handle.next_client_port,
            config.user_space_client_port + 1,
            "port counter should still advance after handling the flow"
        );
    }
}
