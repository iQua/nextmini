mod config;
mod db;
mod models;
mod new_node;
mod route_ser;
mod routing;
mod topo;
mod utils;

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
use sqlx::{Pool, Postgres};
use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tokio::sync::{Mutex, RwLock, broadcast};
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::{accept_async, tungstenite::Message};
use tracing::{error, info, warn};
use tracing_subscriber;

use nextmini_messages::{ControllerToDataplane, DataplaneToController};

use crate::config::{Config, get_config};
use crate::db::{init_db, setup_flow_notification, setup_route_notification};
use crate::models::{DbRoute, Node, Route};
use crate::new_node::{NodeConnectedEvent, new_node_connected};
use crate::utils::{build_routes_for_node, build_startup_response};

type WebSocketReader = SplitStream<WebSocketStream<TcpStream>>;
pub type WebSocketWriter = SplitSink<WebSocketStream<TcpStream>, Message>;
pub type NodeWriterMap = Arc<RwLock<HashMap<usize, Arc<Mutex<WebSocketWriter>>>>>;

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

    // Set up a channel for a background task to process the event as a new node connects
    let (new_node_connected_sender, new_node_connected_receiver) =
        broadcast::channel::<NodeConnectedEvent>(100);

    // Spawn the centralized node connection coordinator
    tokio::spawn(new_node_connected(
        new_node_connected_receiver,
        config.clone(),
        node_ws.clone(),
        db_pool.clone(),
    ));

    // Set up database notifications
    setup_route_notification(db_pool.clone(), node_ws.clone()).await;
    setup_flow_notification(db_pool.clone(), node_ws.clone()).await;

    while let Ok((stream, _)) = listener.accept().await {
        let peer = stream
            .peer_addr()
            .expect("Connected streams should have a peer address");

        info!("New connection from {}", peer);

        let ws_stream = match accept_async(stream).await {
            Ok(ws) => ws,
            Err(e) => {
                error!(
                    "Failed to accept WebSocket connection from {}: {}. Skipping.",
                    peer, e
                );
                continue;
            }
        };

        let (write, read) = ws_stream.split();

        tokio::spawn(handle_connection(
            read,
            write,
            Arc::clone(&db_pool),
            config.clone(),
            Arc::clone(&node_ws),
            new_node_connected_sender.clone(),
        ));
    }
}

async fn handle_connection(
    mut read: WebSocketReader,
    write: WebSocketWriter,
    db_pool: Arc<Pool<Postgres>>,
    config: Config,
    node_ws: Arc<RwLock<HashMap<usize, Arc<Mutex<WebSocketWriter>>>>>,
    new_node_connected_sender: broadcast::Sender<NodeConnectedEvent>,
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

                        // Determine the node ID to use.
                        let node_id = match maybe_node_id {
                            // If the node_id is already in use,
                            // controller should reject the requested node_id from dataplane
                            Some(id) => {
                                let node_ws_guard = node_ws.read().await;
                                if node_ws_guard.contains_key(&id) {
                                    error!(
                                        "Requested node ID {} is already used. This connection will be rejected.",
                                        id
                                    );
                                    continue;
                                }
                                id
                            }
                            // If the requested node_id is none, then controller assigns a new ID to dataplane node.
                            // In this case, nodes on different instances could be assigned the node ID automatically.
                            None => {
                                let node_ws_guard = node_ws.read().await;
                                // Find the smallest available node ID starting from 1
                                let mut new_id = 1;
                                while node_ws_guard.contains_key(&new_id) {
                                    new_id += 1;
                                }
                                info!("No node ID requested. Assigning new ID: {}", new_id);
                                new_id
                            }
                        };

                        // Re-acquire write lock to insert
                        let mut node_ws_guard = node_ws.write().await;
                        // inserts immediately after check to reserve this node_id
                        node_ws_guard.insert(node_id, write_arc.clone());
                        let connected_node_count = node_ws_guard.len();

                        info!(
                            "Node {} ({}, {}) successfully registered. Total nodes now: {}",
                            node_id,
                            private_network_addr,
                            public_network_addr,
                            connected_node_count,
                        );

                        // note: We keep nodes in node_ws even if setup fails. The websocket connection is established, so the
                        // node is considered connected even if configuration failed.

                        // checks if the node ID is correct
                        info!(
                            "Registered new node {} with private address {} and public address {}.",
                            node_id, private_network_addr, public_network_addr,
                        );

                        let new_node = Node {
                            id: node_id as i32,
                            private_network_name: Some(private_network_name.clone()),
                            private_network_addr,
                            public_network_addr,
                        };

                        // inserts the new node into the database
                        match sqlx::query(
                            r#"
                            INSERT INTO nodes (id, private_network_name, private_network_addr, public_network_addr)
                            VALUES ($1, $2, $3, $4)
                            ON CONFLICT (id) DO UPDATE SET
                                private_network_name = EXCLUDED.private_network_name,
                                private_network_addr = EXCLUDED.private_network_addr,
                                public_network_addr = EXCLUDED.public_network_addr
                            "#,
                        )
                        .bind(new_node.id)
                        .bind(&new_node.private_network_name)
                        .bind(&new_node.private_network_addr)
                        .bind(&new_node.public_network_addr)
                        .execute(&*db_pool)
                        .await {
                            Ok(_) => info!("Node {} added to database", node_id),
                            Err(e) => {
                                error!("Failed to insert node {} into database: {}. Continuing with partial setup.", node_id, e);
                                // Don't remove from node_ws - the websocket is connected
                            }
                        }

                        // finds the node specification for the current node
                        let node_spec = config
                            .nodes
                            .iter()
                            .find(|node| node.node_id == node_id)
                            .cloned();

                        // sends the startup response
                        let response = build_startup_response(
                            node_id,
                            config.net_mask,
                            config.base_addr,
                            config.user_space_base_addr,
                            config.external_base_addr,
                            config.max_server_port,
                            config.protocol.clone(),
                            config.scheduler_type,
                            node_spec,
                        );

                        match write_arc
                            .lock()
                            .await
                            .send(Message::binary(rmp_serde::to_vec(&response).unwrap()))
                            .await
                        {
                            Ok(_) => info!("Sent StartUp response to node {}", node_id),
                            Err(e) => {
                                error!(
                                    "Failed to send StartUp response to node {}: {}.",
                                    node_id, e
                                );
                            }
                        }

                        current_node_id = Some(node_id);

                        // asks the new node to connect to other nodes in the route

                        // first fetches all nodes from the database
                        let nodes: Vec<Node> = match sqlx::query_as("SELECT * FROM nodes")
                            .fetch_all(&*db_pool)
                            .await
                        {
                            Ok(nodes) => nodes,
                            Err(e) => {
                                error!(
                                    "Failed to fetch nodes for node {}: {}. Skipping neighbor setup.",
                                    node_id, e
                                );
                                Vec::new() // Continue with empty node list
                            }
                        };

                        // gets the topology edges
                        let topology_edges =
                            topo::topo::build_topology(&config).unwrap_or_default();

                        // collects neighbors of the new node
                        let mut neighbors: HashSet<i32> = HashSet::new();

                        for &(a, b) in &topology_edges {
                            if a == node_id as u32 {
                                neighbors.insert(b as i32);
                            } else if b == node_id as u32 {
                                neighbors.insert(a as i32);
                            }
                        }

                        // establishes connections between the new node and its neighbors by sending AddNode messages
                        for node in nodes {
                            if node.id == node_id as i32 {
                                continue;
                            }

                            if !neighbors.contains(&node.id) {
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

                            // informs the new node to connect to the existing node
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
                        info!("Installing routes for node {}.", node_id);

                        let routes = match sqlx::query_as(
                            r#"SELECT route_id, src_node_id, dst_node_id, edges FROM routes"#,
                        )
                        .fetch_all(&*db_pool)
                        .await
                        {
                            Ok(rows) => rows
                                .into_iter()
                                .map(|row: DbRoute| {
                                    // converts i32 to u32 edges
                                    let edges: Vec<(i32, i32)> =
                                        serde_json::from_value(row.edges.clone())
                                            .unwrap_or_default();
                                    let edges: Vec<(u32, u32)> = edges
                                        .into_iter()
                                        .map(|(a, b)| (a as u32, b as u32))
                                        .collect();

                                    Route {
                                        route_id: row.route_id as usize,
                                        src_node_id: row.src_node_id as u32,
                                        dst_node_id: row.dst_node_id as u32,
                                        edges,
                                    }
                                })
                                .collect::<Vec<_>>(),
                            Err(e) => {
                                error!(
                                    "Failed to fetch routes for node {}: {}. Skipping route installation.",
                                    node_id, e
                                );
                                Vec::new() // Continue with empty route list
                            }
                        };

                        if let Some(msg) = build_routes_for_node(routes, node_id as u32) {
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

                        // as a new node connects, checks if all the expected nodes are now connected
                        info!(
                            "Node {} setup completed successfully. At the time of insertion, there were {} nodes connected.",
                            node_id, connected_node_count
                        );

                        let _ = new_node_connected_sender.send(NodeConnectedEvent {
                            node_id,
                            connected_node_count,
                        });
                    }

                    DataplaneToController::Metrics { metrics } => {
                        if current_node_id.is_some() {
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
                    DataplaneToController::FlowFinished { controller_id } => {
                        info!("Received FlowFinished message for flow {}.", controller_id);

                        match sqlx::query(
                            r#"
                            UPDATE flows
                            SET is_finished = TRUE
                            WHERE id = $1
                            "#,
                        )
                        .bind(controller_id)
                        .execute(&*db_pool)
                        .await
                        {
                            Ok(result) => {
                                if result.rows_affected() > 0 {
                                    info!(
                                        "Marked flow {} as finished in the database.",
                                        controller_id
                                    );
                                } else {
                                    warn!(
                                        "Flow with ID {} not found in the database.",
                                        controller_id
                                    );
                                }
                            }
                            Err(e) => {
                                error!(
                                    "Failed to update flow {} in the database: {}.",
                                    controller_id, e
                                );
                            }
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
                error!("Error receiving the message: {}.", e);
                break;
            }
        }
    }

    if let Some(node_id) = current_node_id {
        info!("Connection closed for node {}.", node_id);
        node_ws.write().await.remove(&node_id);
    }
}
