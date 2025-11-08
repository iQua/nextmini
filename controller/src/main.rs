mod config;
mod db;
mod models;
mod new_node;
mod route_ser;
mod routing;
mod topology;
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

use nextmini_messages::{ControllerToDataplane, DataplaneToController};

use crate::config::{Config, get_config};
use crate::db::{
    add_group_member, create_group, init_db, remove_group_member, setup_flow_notification,
    setup_group_notification, setup_route_notification,
};
use crate::models::{DbRoute, Node, Route};
use crate::new_node::{NodeConnectedEvent, new_node_connected};
use crate::utils::{StartupResponseParams, build_routes_for_node, build_startup_response};

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
        .expect("Failed to bind to port.");
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
    setup_group_notification(db_pool.clone(), node_ws.clone()).await;

    while let Ok((stream, _)) = listener.accept().await {
        let peer = stream
            .peer_addr()
            .expect("Connected streams should have a peer address.");

        info!("New connection from {}.", peer);

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
    node_ws: NodeWriterMap,
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
                        error!("Failed to parse dataplane message: {}.", e);
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
                            "Received StartUp message from {} (public), {} (private), requested ID: {:?}.",
                            &public_network_addr, &private_network_addr, maybe_node_id
                        );

                        // assigns a node ID as the dataplane node requests
                        let node_id = maybe_node_id.unwrap();

                        info!(
                            "Node with ID {} is attempting to connect (private: {}, public: {}).",
                            node_id, private_network_addr, public_network_addr
                        );

                        // checks if the node ID is already used and registers immediately
                        let connected_node_count = {
                            let mut node_ws_guard = node_ws.write().await;

                            if node_ws_guard.contains_key(&node_id) {
                                error!(
                                    "Node ID {} is already used. This connection will be rejected.",
                                    node_id
                                );

                                continue;
                            }

                            // inserts immediately after check to reserve this node_id
                            node_ws_guard.insert(node_id, write_arc.clone());
                            let count_after_insert = node_ws_guard.len();

                            info!(
                                "Node {} successfully inserted into node_ws. Total nodes now: {}.",
                                node_id, count_after_insert
                            );

                            count_after_insert
                        };

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
                        let response = build_startup_response(StartupResponseParams {
                            node_id,
                            net_mask: config.net_mask,
                            virtual_base_addr: config.base_addr,
                            user_space_base_addr: config.user_space_base_addr,
                            external_base_addr: config.external_base_addr,
                            multicast_pool_base: config.multicast_pool_base,
                            multicast_pool_mask: config.multicast_pool_mask,
                            max_server_port: config.max_server_port,
                            protocol: config.protocol.clone(),
                            scheduler_type: config.scheduler_type,
                            node_spec,
                        });

                        match write_arc
                            .lock()
                            .await
                            .send(Message::binary(rmp_serde::to_vec(&response).unwrap()))
                            .await
                        {
                            Ok(_) => info!("Sent StartUp response to node {}.", node_id),
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
                            topology::topo::build_topology(&config).unwrap_or_default();

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
                    DataplaneToController::UserFlowStart { flows } => {
                        for flow_start in flows {
                            match sqlx::query(
                                r#"
                                UPDATE flows
                                SET start_time = $2, is_finished = FALSE
                                WHERE id = $1
                                "#,
                            )
                            .bind(flow_start.controller_id)
                            .bind(flow_start.start_time)
                            .execute(&*db_pool)
                            .await
                            {
                                Ok(result) => {
                                    if result.rows_affected() > 0 {
                                        info!(
                                            "Recorded start_time {} for user space flow {}.",
                                            flow_start.start_time, flow_start.controller_id
                                        );
                                    } else {
                                        warn!(
                                            "User space flow {} not found when recording start_time.",
                                            flow_start.controller_id
                                        );
                                    }
                                }
                                Err(e) => {
                                    error!(
                                        "Failed to update start_time for user space flow {}: {}.",
                                        flow_start.controller_id, e
                                    );
                                }
                            }
                        }
                    }
                    DataplaneToController::FlowFinished { flows } => {
                        for flow_finished in flows {
                            let flow_id = flow_finished.flow_id;
                            let controller_id = flow_finished.controller_id;

                            if let Some(id) = controller_id {
                                // user space flow with controller_id
                                info!("Received FlowFinished message for user space flow {}.", id);

                                match sqlx::query(
                                    r#"
                                    UPDATE flows
                                    SET is_finished = TRUE, finish_time = $2, start_time = COALESCE(start_time, $3)
                                    WHERE id = $1
                                    "#,
                                )
                                .bind(id)
                                .bind(flow_finished.finish_time)
                                .bind(flow_finished.start_time)
                                .execute(&*db_pool)
                                .await
                                {
                                    Ok(result) => {
                                        if result.rows_affected() > 0 {
                                            info!("Marked user space flow {} as finished.", id);
                                        } else {
                                            warn!("User space flow {} not found in database.", id);
                                        }
                                    }
                                    Err(e) => {
                                        error!("Failed to update user space flow {}: {}.", id, e);
                                    }
                                }
                            } else {
                                // application flows reading from TUN interface without controller_id
                                let flow_id_slice = flow_id.as_ref();
                                let start_time = flow_finished.start_time;
                                let finish_time = flow_finished.finish_time;

                                // Debug!: this message is now used for debugging
                                info!(
                                    "Received FlowFinished message for application flow [{}.{}.{}.{}:{} → {}.{}.{}.{}:{}] at start_time {} and finish_time {}.",
                                    flow_id_slice[0],
                                    flow_id_slice[1],
                                    flow_id_slice[2],
                                    flow_id_slice[3],
                                    u16::from_be_bytes([flow_id_slice[8], flow_id_slice[9]]),
                                    flow_id_slice[4],
                                    flow_id_slice[5],
                                    flow_id_slice[6],
                                    flow_id_slice[7],
                                    u16::from_be_bytes([flow_id_slice[10], flow_id_slice[11]]),
                                    start_time,
                                    finish_time
                                );

                                // updates the flow as finished (only if it already exists)
                                // if AppFlowStart hasn't arrived yet, this update will be ignored
                                match sqlx::query(
                                    r#"
                                    UPDATE app_flows
                                    SET is_finished = TRUE, finish_time = $3
                                    WHERE flow_id = $1 AND start_time = $2
                                    "#,
                                )
                                .bind(flow_id_slice)
                                .bind(start_time)
                                .bind(finish_time)
                                .execute(&*db_pool)
                                .await
                                {
                                    Ok(result) => {
                                        if result.rows_affected() > 0 {
                                            info!(
                                                "Marked application flow as finished (start_time: {}).",
                                                start_time
                                            );
                                        } else {
                                            warn!(
                                                "FlowFinished for non-existent flow [{}.{}.{}.{}:{} → {}.{}.{}.{}:{}] (start_time: {}). \
                                                Either AppFlowStart hasn't arrived or flow_id was reused.",
                                                flow_id_slice[0],
                                                flow_id_slice[1],
                                                flow_id_slice[2],
                                                flow_id_slice[3],
                                                u16::from_be_bytes([
                                                    flow_id_slice[8],
                                                    flow_id_slice[9]
                                                ]),
                                                flow_id_slice[4],
                                                flow_id_slice[5],
                                                flow_id_slice[6],
                                                flow_id_slice[7],
                                                u16::from_be_bytes([
                                                    flow_id_slice[10],
                                                    flow_id_slice[11]
                                                ]),
                                                start_time
                                            );
                                        }
                                    }
                                    Err(e) => {
                                        error!("Failed to update application flow: {}.", e);
                                    }
                                }
                            }
                        }
                    }
                    DataplaneToController::AppFlowStart { appflows } => {
                        for appflow in appflows {
                            let flow_id_slice = appflow.flow_id.as_ref();
                            let start_time = appflow.start_time;

                            // checks if the record already exists to avoid consuming sequence numbers
                            match sqlx::query(
                                r#"
                                SELECT 1 FROM app_flows WHERE flow_id = $1 AND start_time = $2
                                "#,
                            )
                            .bind(flow_id_slice)
                            .bind(start_time)
                            .fetch_optional(&*db_pool)
                            .await
                            {
                                Ok(Some(_)) => {
                                    // if the record already exists, skips insertion
                                    info!(
                                        "Duplicate AppFlowStart ignored (flow_id + start_time already exists)."
                                    );
                                    continue;
                                }
                                Ok(None) => {
                                    // if the record doesn't exist, proceeds with insertion
                                }
                                Err(e) => {
                                    error!("Failed to check app flow existence: {}.", e);
                                    continue;
                                }
                            }

                            // inserts the new record
                            match sqlx::query(
                                r#"
                                INSERT INTO app_flows (flow_id, start_time, src_node_id, dst_node_id, is_finished)
                                VALUES ($1, $2, $3, $4, FALSE)
                                "#,
                            )
                            .bind(flow_id_slice)
                            .bind(start_time)
                            .bind(appflow.src_node_id as i32)
                            .bind(appflow.dst_node_id as i32)
                            .execute(&*db_pool)
                            .await
                            {
                                Ok(_) => {
                                    info!(
                                        "Registered new app flow from node {} to {} at start_time {}.",
                                        appflow.src_node_id, appflow.dst_node_id, start_time
                                    );
                                }
                                Err(e) => error!("Failed to insert app flow: {}.", e),
                            }
                        }
                    }
                    DataplaneToController::RouteAssigned { assignments } => {
                        for assignment in assignments {
                            let flow_id = assignment.flow_id;
                            let route_id = assignment.route_id;
                            let start_time = assignment.time;
                            let flow_id_slice = flow_id.as_ref();

                            // updates route_id in app_flows table (only if flow already exists)
                            match sqlx::query(
                                r#"
                                UPDATE app_flows
                                SET route_id = $1
                                WHERE flow_id = $2 AND start_time = $3
                                "#,
                            )
                            .bind(route_id as i32)
                            .bind(flow_id_slice)
                            .bind(start_time)
                            .execute(&*db_pool)
                            .await
                            {
                                Ok(result) => {
                                    if result.rows_affected() == 0 {
                                        error!("RouteAssigned for non-existent flow.");
                                    } else {
                                        info!(
                                            "Updated route_id={} for flow [{}.{}.{}.{}:{} → {}.{}.{}.{}:{}] (start_time: {}).",
                                            route_id,
                                            flow_id_slice[0],
                                            flow_id_slice[1],
                                            flow_id_slice[2],
                                            flow_id_slice[3],
                                            u16::from_be_bytes([
                                                flow_id_slice[8],
                                                flow_id_slice[9]
                                            ]),
                                            flow_id_slice[4],
                                            flow_id_slice[5],
                                            flow_id_slice[6],
                                            flow_id_slice[7],
                                            u16::from_be_bytes([
                                                flow_id_slice[10],
                                                flow_id_slice[11]
                                            ]),
                                            start_time
                                        );
                                    }
                                }
                                Err(e) => {
                                    error!("Failed to update app_flows.route_id: {}.", e);
                                }
                            }
                        }
                    }
                    DataplaneToController::CreateGroup { group_id, label } => {
                        let Some(node_id) = current_node_id else {
                            warn!("CreateGroup received before node registration; ignoring.");
                            continue;
                        };

                        let (success, error_msg) = match create_group(
                            &db_pool,
                            group_id,
                            &label,
                            node_id,
                            config.multicast_pool_base,
                        )
                        .await
                        {
                            Ok(group) => {
                                info!(
                                    "Created multicast group {} ('{}', IP: {}) for node {}.",
                                    group.id, group.label, group.group_ip, node_id
                                );
                                (true, None)
                            }
                            Err(e) => {
                                error!("Failed to create group {} ('{}'): {}", group_id, label, e);
                                (false, Some(e.to_string()))
                            }
                        };

                        // broadcasts to all nodes
                        if let Err(e) = broadcast_group_created(
                            &node_ws,
                            group_id,
                            node_id,
                            &label,
                            success,
                            error_msg,
                        )
                        .await
                        {
                            error!("Failed to broadcast GroupCreated for group {}: {}.", group_id, e);
                        }
                    }
                    DataplaneToController::JoinGroup { group_id } => {
                        let Some(node_id) = current_node_id else {
                            warn!("JoinGroup received before node registration; ignoring.");
                            continue;
                        };

                        if let Err(e) = add_group_member(&db_pool, group_id as i32, node_id).await {
                            error!("Node {} failed to join group {}: {}", node_id, group_id, e);
                        } else {
                            info!("Node {} joined multicast group {}.", node_id, group_id);
                        }
                    }
                    DataplaneToController::LeaveGroup { group_id } => {
                        let Some(node_id) = current_node_id else {
                            warn!("LeaveGroup received before node registration; ignoring.");
                            continue;
                        };

                        if let Err(e) =
                            remove_group_member(&db_pool, group_id as i32, node_id).await
                        {
                            error!("Node {} failed to leave group {}: {}", node_id, group_id, e);
                        } else {
                            info!("Node {} left multicast group {}.", node_id, group_id);
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

/// Broadcast group created event to all connected nodes.
async fn broadcast_group_created(
    node_ws: &NodeWriterMap,
    group_id: usize,
    src_node_id: usize,
    label: &str,
    success: bool,
    error_msg: Option<String>,
) -> anyhow::Result<()> {
    let message = ControllerToDataplane::GroupCreated {
        group_id,
        src_node_id,
        label: label.to_string(),
        success,
        error_msg,
    };
    let payload = rmp_serde::to_vec(&message)?;

    let guard = node_ws.read().await;
    let mut broadcast_count = 0;
    let mut failed_count = 0;

    for (node_id, writer) in guard.iter() {
        match writer
            .lock()
            .await
            .send(Message::binary(payload.clone()))
            .await
        {
            Ok(_) => broadcast_count += 1,
            Err(e) => {
                error!(
                    "Failed to broadcast GroupCreated (group {}) to node {}: {}",
                    group_id, node_id, e
                );
                failed_count += 1;
            }
        }
    }

    info!(
        "Broadcasted GroupCreated (group {}, src_node {}, success={}) to {} nodes ({} failed).",
        group_id, src_node_id, success, broadcast_count, failed_count
    );

    Ok(())
}
