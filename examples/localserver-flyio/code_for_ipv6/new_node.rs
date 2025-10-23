use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::Instant;

use tokio::time::Duration;

use futures_util::SinkExt;
use sqlx::{Pool, Postgres};
use tokio::sync::{Mutex, RwLock, broadcast};
use tokio_tungstenite::tungstenite::Message;
use tracing::{error, info, warn};

use nextmini_messages::{ControllerToDataplane, TokenBucketSpec};

use crate::NodeWriterMap;
use crate::WebSocketWriter;
use crate::config::Config;
use crate::models::{DbFlow, Node};
use crate::utils::build_flows_for_node;

/// Helper function to parse and reconstruct address with a new port.
/// Handles both IPv4 (addr:port) and IPv6 ([addr]:port) formats.
fn replace_port(addr: &str, new_port: u16) -> String {
    // Try parsing as SocketAddr first (handles [ipv6]:port and ipv4:port)
    if let Ok(socket_addr) = addr.parse::<SocketAddr>() {
        // Reconstruct with new port
        match socket_addr.ip() {
            IpAddr::V6(ip) => format!("[{}]:{}", ip, new_port),
            IpAddr::V4(ip) => format!("{}:{}", ip, new_port),
        }
    } else {
        // Try parsing as just an IP address (no port)
        if let Ok(ip_addr) = addr.parse::<IpAddr>() {
            match ip_addr {
                IpAddr::V6(ip) => format!("[{}]:{}", ip, new_port),
                IpAddr::V4(ip) => format!("{}:{}", ip, new_port),
            }
        } else {
            // Fallback: assume it's already in the right format or a hostname
            // For hostnames or edge cases, try to intelligently handle
            if addr.contains('[') {
                // Looks like IPv6 with brackets, strip port if exists
                let ip_part = addr
                    .split(']')
                    .next()
                    .unwrap_or(addr)
                    .trim_start_matches('[');
                format!("[{}]:{}", ip_part, new_port)
            } else {
                // Assume IPv4 or hostname, strip port if exists
                let ip_part = addr.split(':').next().unwrap_or(addr);
                format!("{}:{}", ip_part, new_port)
            }
        }
    }
}

// Event to be sent when a new node has connected to the controller.
#[derive(Debug, Clone)]
pub struct NodeConnectedEvent {
    pub node_id: usize,
    pub connected_node_count: usize,
}

/// A background task that checks if all the expected nodes have connected, and performs additional
/// processing when this occurs.
pub async fn new_node_connected(
    mut event_receiver: broadcast::Receiver<NodeConnectedEvent>,
    config: Config,
    node_ws: Arc<RwLock<HashMap<usize, Arc<Mutex<WebSocketWriter>>>>>,
    db_pool: Arc<Pool<Postgres>>,
) {
    let mut all_nodes_handled = false;
    let mut start_time = None;

    while let Ok(event) = event_receiver.recv().await {
        if all_nodes_handled {
            continue; // already handled all the work after all nodes connected
        }

        // sends flows and link rates when all nodes are connected
        if let Some(expected_node_count) = config.topology.compute_node_count() {
            if start_time.is_none() {
                start_time = Some(Instant::now());
                info!("The first node has connected. Starting the timer.");
            }

            if event.connected_node_count == expected_node_count {
                all_nodes_handled = true;

                // waits for all links to be established
                tokio::time::sleep(Duration::from_secs(1)).await;

                info!(
                    "All {} nodes are now connected. Sending node addresses, link rates and flows to all nodes.",
                    expected_node_count
                );

                // updates remote node addresses for the connector
                send_node_addresses(config.clone(), node_ws.clone(), db_pool.clone()).await;

                // waits for all nodes to receive the AddNode messages
                tokio::time::sleep(Duration::from_millis(100)).await;
                send_link_rates(config.clone(), node_ws.clone()).await;

                // waits for all link rates to be set before sending the flows
                tokio::time::sleep(Duration::from_millis(100)).await;
                send_flows(node_ws.clone(), db_pool.clone()).await;

                let duration_secs = start_time.unwrap().elapsed().as_secs_f32();
                info!(
                    "All dataplane nodes have connected. It takes {:.2} seconds since the first node arrived.",
                    duration_secs
                );
            } else {
                info!(
                    "Node {} connected. At time of insertion, {} nodes were connected (including this one), out of {} expected.",
                    event.node_id, event.connected_node_count, expected_node_count
                );
            }
        }
    }
}

async fn send_flows(node_ws: NodeWriterMap, db_pool: Arc<Pool<Postgres>>) {
    let db_flows: Vec<DbFlow> =
        match sqlx::query_as("SELECT * FROM flows WHERE is_finished = false")
            .fetch_all(&*db_pool)
            .await
        {
            Ok(flows) => flows,
            Err(e) => {
                error!("Failed to fetch flows from database: {}.", e);
                return;
            }
        };

    let node_ws_guard = node_ws.read().await;

    for (&node_id, writer) in node_ws_guard.iter() {
        let flows: Vec<DbFlow> = db_flows
            .iter()
            .filter(|flow| flow.src_node_id == node_id as i32 || flow.dst_node_id == node_id as i32)
            .cloned()
            .collect();

        if !flows.is_empty() {
            info!(
                "Adding {} user-space TCP flows to node {}.",
                flows.len(),
                node_id
            );

            let msg = build_flows_for_node(flows);

            match writer
                .lock()
                .await
                .send(Message::binary(rmp_serde::to_vec(&msg).unwrap()))
                .await
            {
                Ok(_) => {
                    info!("Successfully sent flows to node {}.", node_id);
                }
                Err(e) => {
                    error!("Failed to send flows to node {}: {}.", node_id, e);
                }
            }
        }
    }
}

async fn send_link_rates(config: Config, node_ws: NodeWriterMap) {
    let node_ws_guard = node_ws.read().await;

    for (node_id, writer) in node_ws_guard.iter() {
        let link_rates: Vec<_> = config
            .link_rates
            .iter()
            .filter(|link_rate| link_rate.src_node_id == *node_id)
            .cloned()
            .collect();

        if !link_rates.is_empty() {
            info!("Setting link rates for node {}.", node_id);
        }

        for link_rate in link_rates {
            let msg = ControllerToDataplane::SetLinkRate {
                node_id: link_rate.dst_node_id,
                spec: TokenBucketSpec {
                    rate: link_rate.rate,
                    bucket_size: link_rate.bucket_size,
                },
            };

            match writer
                .lock()
                .await
                .send(Message::binary(rmp_serde::to_vec(&msg).unwrap()))
                .await
            {
                Ok(_) => {}
                Err(e) => error!(
                    "Failed to send the SetLinkRate message to node {}: {}.",
                    link_rate.src_node_id, e
                ),
            }
        }
    }
}

async fn send_node_addresses(config: Config, node_ws: NodeWriterMap, db_pool: Arc<Pool<Postgres>>) {
    let nodes: Vec<Node> = match sqlx::query_as("SELECT * FROM nodes")
        .fetch_all(&*db_pool)
        .await
    {
        Ok(nodes) => nodes,
        Err(e) => {
            error!("Failed to fetch nodes from database: {}", e);
            return;
        }
    };

    let node_ws_guard = node_ws.read().await;

    info!(
        "Sending AddNodeAddress messages to {} nodes.",
        node_ws_guard.len()
    );

    for (node_id, writer) in node_ws_guard.iter() {
        let current_node = match nodes.iter().find(|n| n.id == *node_id as i32) {
            Some(node) => node,
            None => {
                warn!(
                    "Node {} is in the writer map but not in the database. Skipping.",
                    node_id
                );
                continue;
            }
        };

        let remote_nodes: Vec<_> = nodes
            .iter()
            .filter(|node| node.id != *node_id as i32)
            .collect();

        for node in remote_nodes {
            let remote_addr = if node.private_network_name == current_node.private_network_name {
                node.private_network_addr.clone()
            } else {
                node.public_network_addr.clone()
            };

            // replace the port with the Tcp max server port
            // This handles both IPv4 and IPv6 addresses correctly
            let remote_addr = replace_port(&remote_addr, config.max_server_port);

            let msg = ControllerToDataplane::AddNodeAddress {
                remote_node_id: node.id as usize,
                remote_max_server_addr: remote_addr,
            };

            match writer
                .lock()
                .await
                .send(Message::binary(rmp_serde::to_vec(&msg).unwrap()))
                .await
            {
                Ok(_) => {}
                Err(e) => error!(
                    "Failed to send an AddNodeAddress message to node {}: {}.",
                    node.id, e
                ),
            }
        }
    }
}
