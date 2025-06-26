use std::sync::Arc;
use tracing::{error, info};

use crate::node::NodeIdExt;
use crate::node::config::LocalConfig;
use crate::node::flow::client::ClientHandle;
use crate::node::flow::server::ServerHandle;
use crate::node::processor::{ProcessorHandle, ProcessorMessage};
use nextmini_messages::Flow;

pub struct UserSpaceTcp {
    config: LocalConfig,
    processors: ProcessorHandle,
    client_handle: Option<ClientHandle>,
    server_handle: Option<Arc<ServerHandle>>,
}

impl UserSpaceTcp {
    pub fn new(config: LocalConfig, processors: ProcessorHandle) -> Self {
        Self {
            config,
            processors,
            client_handle: None,
            server_handle: None,
        }
    }

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

        let incoming_flows: Vec<_> = flows
            .iter()
            .filter(|f| f.dst_node_id == node_id)
            .cloned()
            .collect();

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

            // creates client handle
            self.client_handle = Some(ClientHandle::new(
                self.config.clone(),
                self.processors.clone(),
            ));

            // creates server handle
            // used by processor and user-space tcp
            let server_handle = Arc::new(ServerHandle::new(
                self.config.clone(),
                self.processors.clone(),
            ));

            // obtains user-space server as LocalDestination
            let ip_addr = self
                .config
                .node_id
                .ip_addr(self.config.user_space_base_addr, self.config.local_netmask);

            let _ = self
                .processors
                .broadcast_sender()
                .send(ProcessorMessage::ConnectLocalDestination(
                    ip_addr,
                    server_handle.clone(),
                ))
                .map(|_| {
                    info!(
                        "Obtains user-space tcp server as LocalDestination for IP {}.",
                        ip_addr
                    );
                });

            self.server_handle = Some(server_handle);
        }
    }
}
