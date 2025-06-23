use std::collections::HashMap;
use std::sync::Arc;

use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
use sqlx::{Pool, Postgres};

use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tokio::sync::{Mutex, RwLock};
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::{accept_async, tungstenite::Message};
use tracing::{error, info, warn};
use tracing_subscriber;

use nextmini_messages::{ControllerToDataplane, DataplaneToController, Flow, TokenBucketSpec};

use crate::config::{Config, get_config};
use crate::db::{init_db, setup_notification};
use crate::models::{Node, Route};
use crate::utils::{build_routes_for_node, build_startup_response, create_new_virtual_addr};

mod config;
mod db;
mod models;
mod utils;

type WebSocketReader = SplitStream<WebSocketStream<TcpStream>>;
type WebSocketWriter = SplitSink<WebSocketStream<TcpStream>, Message>;
type NodeWriterMap = Arc<RwLock<HashMap<usize, Arc<Mutex<WebSocketWriter>>>>>;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt().init();
    let config = get_config("config.toml");
    let db_pool = Arc::new(init_db(&config).await);
    let listener = TcpListener::bind(format!("0.0.0.0:{}", config.port))
        .await
        .expect("Failed to bind to port");
    info!("The controller is now listening on port {}.", config.port);

    let node_ws: NodeWriterMap = Arc::new(RwLock::new(HashMap::new()));

    // Set up database notifications
    setup_notification(db_pool.clone(), node_ws.clone()).await;

    while let Ok((stream, _)) = listener.accept().await {
        let peer = stream
            .peer_addr()
            .expect("Connected streams should have a peer address");

        info!("New connection from {}", peer);

        let ws_stream = accept_async(stream)
            .await
            .expect("Failed to accept WebSocket connection");
        let (write, read) = ws_stream.split();

        tokio::spawn(handle_connection(
            read,
            write,
            Arc::clone(&db_pool),
            config.clone(),
            Arc::clone(&node_ws),
        ));
    }
}

async fn handle_connection(
    mut read: WebSocketReader,
    write: WebSocketWriter,
    db_pool: Arc<Pool<Postgres>>,
    config: Config,
    node_ws: Arc<RwLock<HashMap<usize, Arc<Mutex<WebSocketWriter>>>>>,
) {
    let write_arc = Arc::new(Mutex::new(write));
    let mut current_node_id = None;

    while let Some(msg) = read.next().await {
        match msg {
            Ok(Message::Binary(data)) => {
                let dataplane_msg = match rmp_serde::from_slice::<DataplaneToController>(&data) {
                    Ok(msg) => msg,
                    Err(e) => {
                        error!("Failed to parse dataplane message: {}", e);
                        continue;
                    }
                };

                match dataplane_msg {
                    DataplaneToController::StartUp {
                        private_network_name,
                        private_network_addr,
                        public_network_addr,
                        node_id: maybe_node_id,
                    } => {
                        info!(
                            "Received StartUp message from {} (public), {} (private), requested ID: {:?}",
                            &public_network_addr, &private_network_addr, maybe_node_id
                        );

                        let assign_new_id = match maybe_node_id {
                            Some(0) => true,  // ID is present but is 0
                            None => true,     // ID is not present
                            Some(_) => false, // ID is present and not 0
                        };

                        let node_id = if assign_new_id {
                            // assigns a new node ID
                            let node_ws_guard = node_ws.read().await;
                            let new_id = if node_ws_guard.is_empty() {
                                1
                            } else {
                                *node_ws_guard.keys().max().unwrap_or(&0) + 1
                            };

                            info!("Assigning a new node ID: {}.", new_id);

                            new_id
                        } else {
                            // ID was Some(id) and id was not 0
                            maybe_node_id.unwrap()
                        };

                        // checks if the node_id is already used
                        if node_ws.read().await.contains_key(&node_id) {
                            warn!("Node ID {} is already used.", node_id);
                            continue;
                        }

                        // assigns a virtual address to this node for tun interface
                        let virtual_addr = match create_new_virtual_addr(
                            config.base_addr,
                            config.net_mask,
                            node_id,
                        ) {
                            Some(addr) => addr,
                            None => {
                                error!(
                                    "Failed to create a tun virtual address for node {}.",
                                    node_id
                                );
                                continue;
                            }
                        };

                        // assigns a virtual address to this node for smoltcp interface
                        let smoltcp_addr = match create_new_virtual_addr(
                            config.smoltcp_base_addr,
                            config.smoltcp_net_mask,
                            node_id,
                        ) {
                            Some(addr) => addr,
                            None => {
                                error!(
                                    "Failed to create a smoltcp virtual address for node {}.",
                                    node_id
                                );
                                continue;
                            }
                        };

                        // adds TUN network
                        let virtual_network_addr = virtual_addr
                            .iter()
                            .map(|x| x.to_string())
                            .collect::<Vec<_>>()
                            .join(".");

                        // adds smoltcp network
                        let smoltcp_virtual_addr = smoltcp_addr
                            .iter()
                            .map(|x| x.to_string())
                            .collect::<Vec<_>>()
                            .join(".");

                        info!(
                            "Created new node {} with private address {}, public address {}, tun virtual address {}, and smoltcp vitrual address {}.",
                            node_id,
                            private_network_addr,
                            public_network_addr,
                            &virtual_network_addr,
                            &smoltcp_virtual_addr,
                        );

                        let new_node = Node {
                            id: node_id as i32,
                            private_network_name: Some(private_network_name.clone()),
                            private_network_addr,
                            public_network_addr,
                            virtual_network_addr,
                            smoltcp_virtual_addr,
                        };

                        // Insert node into database
                        match sqlx::query(
                            r#"
                            INSERT INTO nodes (id, private_network_name, private_network_addr, public_network_addr, virtual_network_addr, smoltcp_virtual_addr, smoltcp_port)
                            VALUES ($1, $2, $3, $4, $5, $6, $7)
                            ON CONFLICT (id) DO UPDATE SET
                                private_network_name = EXCLUDED.private_network_name,
                                private_network_addr = EXCLUDED.private_network_addr,
                                public_network_addr = EXCLUDED.public_network_addr,
                                virtual_network_addr = EXCLUDED.virtual_network_addr,
                                smoltcp_virtual_addr = EXCLUDED.smoltcp_virtual_addr,
                            "#
                        )
                        .bind(new_node.id)
                        .bind(&new_node.private_network_name)
                        .bind(&new_node.private_network_addr)
                        .bind(&new_node.public_network_addr)
                        .bind(&new_node.virtual_network_addr)
                        .bind(&new_node.smoltcp_virtual_addr)
                        .execute(&*db_pool)
                        .await {
                            Ok(_) => info!("Node {} added to database", node_id),
                            Err(e) => {
                                error!("Failed to insert node into database: {}", e);
                                continue;
                            }
                        }

                        // Send startup response

                        let flow_configs: Vec<Flow> = config
                            .flows
                            .iter()
                            .filter(|flow| {
                                flow.src_node_id == node_id || flow.dst_node_id == node_id
                            })
                            .enumerate()
                            .filter_map(|(i, flow)| {
                                create_new_virtual_addr(
                                    config.smoltcp_base_addr,
                                    config.smoltcp_net_mask,
                                    flow.dst_node_id,
                                )
                                .map(|remote_addr| Flow {
                                    src_node_id: flow.src_node_id,
                                    dst_node_id: flow.dst_node_id,
                                    flow_size: flow.flow_size,
                                })
                            })
                            .collect();

                        info!(
                            "Found {} flows for node {} from configuration",
                            flow_configs.len(),
                            node_id
                        );

                        let response = build_startup_response(
                            node_id,
                            virtual_addr,
                            config.net_mask,
                            smoltcp_addr,
                            config.smoltcp_net_mask,
                            config.protocol.clone(),
                        );

                        match write_arc
                            .lock()
                            .await
                            .send(Message::binary(rmp_serde::to_vec(&response).unwrap()))
                            .await
                        {
                            Ok(_) => info!("Sent StartUp response to node {}", node_id),
                            Err(e) => {
                                error!("Failed to send StartUp response: {}", e);
                                continue;
                            }
                        }

                        // registers the WebSocket connection and associate it with the new node ID
                        {
                            let mut node_ws_guard = node_ws.write().await;
                            node_ws_guard.insert(node_id, write_arc.clone());
                        }

                        current_node_id = Some(node_id);

                        // asks the new node to connect to other nodes in the topology

                        // first fetches all nodes from the database
                        let nodes: Vec<Node> = match sqlx::query_as("SELECT * FROM nodes")
                            .fetch_all(&*db_pool)
                            .await
                        {
                            Ok(nodes) => nodes,
                            Err(e) => {
                                error!("Failed to fetch nodes: {}", e);
                                continue;
                            }
                        };

                        // establishes connections between all pairs of nodes by sending AddNode messages
                        for node in nodes {
                            if node.id == node_id as i32 {
                                continue;
                            }

                            // determines the address to use (private or public)
                            // if two nodes share the same private network name, then we use the private
                            // network address for this connection; otherwise, we use the public network
                            // address.
                            let addr = if node.private_network_name
                                == Some(private_network_name.clone())
                            {
                                node.private_network_addr
                            } else {
                                node.public_network_addr
                            };

                            // sends an AddNode message to the new node
                            let msg = ControllerToDataplane::AddNode {
                                remote_node_id: node.id as usize,
                                remote_addr: addr,
                            };

                            // informs the existing nodes about the new node by updating their connections
                            match write_arc
                                .lock()
                                .await
                                .send(Message::binary(rmp_serde::to_vec(&msg).unwrap()))
                                .await
                            {
                                Ok(_) => info!(
                                    "Sent an AddNode message for node {} to node {}.",
                                    node.id, node_id
                                ),
                                Err(e) => error!(
                                    "Failed to send an AddNode message to node {}: {}.",
                                    node_id, e
                                ),
                            }
                        }

                        // installs routes
                        info!("Installing routes for node {}", node_id);

                        let routes: Vec<Route> = match sqlx::query_as("SELECT * FROM routes")
                            .fetch_all(&*db_pool)
                            .await
                        {
                            Ok(routes) => routes,
                            Err(e) => {
                                error!("Failed to fetch routes: {}", e);
                                continue;
                            }
                        };

                        if let Some(msg) = build_routes_for_node(routes, node_id as i32) {
                            match write_arc
                                .lock()
                                .await
                                .send(Message::binary(rmp_serde::to_vec(&msg).unwrap()))
                                .await
                            {
                                Ok(_) => {
                                    info!("Sent an InstallRoutes message to node {}.", node_id)
                                }
                                Err(e) => error!(
                                    "Failed to send InstallRoutes message to node {}: {}.",
                                    node_id, e
                                ),
                            }
                        } else {
                            error!("No routes to install for node {}.", node_id);
                        }

                        // sets the link rates
                        info!("Setting link rates for node {}", node_id);

                        for link_rate in &config.link_rates {
                            if link_rate.src_node_id == node_id {
                                let spec = TokenBucketSpec {
                                    rate: link_rate.rate,
                                    bucket_size: link_rate.bucket_size,
                                };
                                let msg = ControllerToDataplane::SetLinkRate {
                                    node_id: link_rate.dst_node_id,
                                    spec,
                                };

                                match write_arc
                                    .lock()
                                    .await
                                    .send(Message::binary(rmp_serde::to_vec(&msg).unwrap()))
                                    .await
                                {
                                    Ok(_) => info!(
                                        "Set link rate for node {} to node {} at {} bytes/second with bucket size {} bytes.",
                                        node_id,
                                        link_rate.dst_node_id,
                                        link_rate.rate,
                                        link_rate.bucket_size
                                    ),
                                    Err(e) => error!(
                                        "Failed to send the SetLinkRate message to node {}: {}.",
                                        node_id, e
                                    ),
                                }
                            }
                        }
                    }
                    DataplaneToController::Metrics { metrics } => {
                        if let Some(_) = current_node_id {
                            for metric in metrics {
                                if metric.bytes == 0 {
                                    continue;
                                }

                                // Direct mapping to database schema that matches Metric struct
                                match sqlx::query(
                                    r#"
                                    INSERT INTO metrics (flow_id, local_node_id, remote_node_id, bytes, time_read)
                                    VALUES ($1, $2, $3, $4, $5)
                                    "#
                                )
                                .bind(metric.flow_id.as_ref())
                                .bind(metric.local_node_id as i32)
                                .bind(metric.remote_node_id as i32)
                                .bind(metric.bytes as i32)
                                .bind(metric.time_read)
                                .execute(&*db_pool)
                                .await {
                                    Ok(_) => {},
                                    Err(e) => error!("Failed to insert metric: {}", e)
                                }
                            }
                        } else {
                            warn!(
                                "Received metrics but no node ID is associated with this connection."
                            );
                        }
                    }
                }
            }
            Ok(Message::Ping(_)) => {
                // just received a ping message to keep the connection alive. Do nothing.
                continue;
            }
            Ok(_) => warn!(
                "Received a message that is not a binary or a ping message. Something may be wrong."
            ),
            Err(e) => {
                error!("Error receiving the message: {}", e);
                break;
            }
        }
    }

    if let Some(node_id) = current_node_id {
        info!("Connection closed for node {}.", node_id);
        node_ws.write().await.remove(&node_id);
    }
}
