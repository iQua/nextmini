use std::sync::Arc;
use tracing::{error, info};

use crate::node::NodeIdExt;
use crate::node::config::LocalConfig;
use crate::node::flow::client::ClientHandle;
use crate::node::flow::router::PacketRouter;
use crate::node::flow::server::ServerHandle;
use crate::node::processor::{ProcessorHandle, ProcessorMessage};
use nextmini_messages::Flow;

// represents the user-space TCP stack
pub struct UserSpaceTcp {
    config: LocalConfig,
    processors: ProcessorHandle,
    // client handle
    client_handle: Option<ClientHandle>,
    // server handle
    server_handle: Option<Arc<ServerHandle>>,
}

impl UserSpaceTcp {
    // creates a new user-space TCP stack
    pub fn new(config: LocalConfig, processors: ProcessorHandle) -> Self {
        Self {
            config,
            processors,
            client_handle: None,
            server_handle: None,
        }
    }

    // adds flows to the user-space TCP stack
    pub fn add_flows(&mut self, flows: Vec<Flow>) {
        info!(
            "Add {} flows for node {}.",
            flows.len(),
            self.config.node_id
        );

        // creates the user space TCP handle on the first AddFlows message.
        self.create_handles();

        // identifies incoming or outgoing flows to create client or server thread
        let node_id = self.config.node_id;

        // filters for incoming flows
        let incoming_flows: Vec<_> = flows
            .iter()
            .filter(|f| f.dst_node_id == node_id)
            .cloned()
            .collect();

        // filters for outgoing flows
        let outgoing_flows: Vec<_> = flows
            .iter()
            .filter(|f| f.src_node_id == node_id)
            .cloned()
            .collect();

        // adds flows to server
        // one thread for multiple flows/servers
        if !incoming_flows.is_empty() {
            if let Some(server_handle) = &self.server_handle {
                for flow in incoming_flows {
                    if let Err(e) = server_handle.add_flow(flow) {
                        error!("Failed to add flow to server: {}", e);
                    }
                }
            }
        }

        // adds flows to client
        if !outgoing_flows.is_empty() {
            if let Some(client_handle) = &self.client_handle {
                client_handle.add_flows(outgoing_flows);
            }
        }
    }

    // creates handles for client or server
    fn create_handles(&mut self) {
        if self.client_handle.is_none()
            && self.server_handle.is_none()
            && !self.config.user_space_address.is_unspecified()
        {
            info!("Starting to create client and server handles.");

            // creates a new packet router
            let router = Arc::new(PacketRouter::new(&self.config));

            // creates client handle
            self.client_handle = Some(ClientHandle::new(
                self.config.clone(),
                self.processors.clone(),
                router.clone(),
            ));

            // creates server handle
            self.server_handle = Some(Arc::new(ServerHandle::new(
                self.config.clone(),
                self.processors.clone(),
                router.clone(),
            )));

            // obtains user-space router as LocalDestination
            // gets the IP address of the node
            let ip_addr = self
                .config
                .node_id
                .ip_addr(self.config.user_space_base_addr, self.config.local_netmask);

            // connects the local destination to the processor
            let _ = self
                .processors
                .broadcast_sender()
                .send(ProcessorMessage::ConnectLocalDestination(ip_addr, router))
                .map(|_| {
                    info!(
                        "Obtains user-space tcp router as LocalDestination for IP {}.",
                        ip_addr
                    );
                });
        }
    }
}
