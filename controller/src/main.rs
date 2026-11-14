mod addr;
mod config;
mod db;
mod db_sync;
mod models;
mod new_node;
mod route_ser;
mod routing;
mod topology;
mod utils;

use std::collections::{BTreeMap, HashMap, HashSet};
use std::net::Ipv4Addr;
use std::sync::Arc;
use std::time::Duration;

use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
use sqlx::{Pool, Postgres};
use tokio::net::TcpSocket;
use tokio::net::TcpStream;
use tokio::sync::{Mutex, RwLock, broadcast, mpsc};
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::{accept_async, tungstenite::Message};
use tracing::{error, info, warn};

use anyhow::Result as AnyResult;
use nextmini_messages::{
    ControllerToDataplane, DataplaneToController, GroupDirectoryEntry, GroupRouteTree,
};

use crate::addr::{normalize_private_network_name, shares_private_network};
use crate::config::{Config, get_config};
use crate::db::{
    add_group_member, create_group, init_db, load_group_directory, load_group_members,
    remove_group_member, setup_flow_notification, setup_group_notification,
    setup_probe_notification, setup_route_notification,
};
use crate::db_sync::spawn_db_sync;
use crate::models::{DbGroupRoute, DbRoute, Node, Route};
use crate::new_node::{NodeConnectedEvent, TopologyEvent, new_node_connected};
use crate::utils::{
    StartupResponseParams, build_group_routes_for_node_multitree, build_routes_for_node,
    build_startup_response, canonicalize_group_route_trees,
};

type WebSocketReader = SplitStream<WebSocketStream<TcpStream>>;
pub type WebSocketWriter = SplitSink<WebSocketStream<TcpStream>, Message>;
pub type NodeWriterMap = Arc<RwLock<HashMap<usize, Arc<Mutex<WebSocketWriter>>>>>;

struct ConnectionContext {
    db_pool: Arc<Pool<Postgres>>,
    config: Config,
    node_ws: NodeWriterMap,
    new_node_connected_sender: broadcast::Sender<TopologyEvent>,
    topology_neighbors: Arc<Vec<Vec<i32>>>,
    multicast_snapshot_needed: bool,
}

fn build_neighbor_index(edges: &[(u32, u32)]) -> Vec<Vec<i32>> {
    let mut max_node_id = 0usize;
    for &(a, b) in edges {
        max_node_id = max_node_id.max(a as usize).max(b as usize);
    }

    let mut neighbors: Vec<Vec<i32>> = vec![Vec::new(); max_node_id.saturating_add(1)];
    for &(a, b) in edges {
        let a = a as usize;
        let b = b as usize;

        if a < neighbors.len() {
            neighbors[a].push(b as i32);
        }
        if b < neighbors.len() {
            neighbors[b].push(a as i32);
        }
    }

    for list in &mut neighbors {
        if list.len() > 1 {
            list.sort_unstable();
            list.dedup();
        }
    }

    neighbors
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt().init();
    let config = get_config("config.toml");
    let expected_node_count = config.topology.compute_node_count().unwrap_or(1024);
    let db_pool = Arc::new(init_db(&config).await);

    let listen_backlog = expected_node_count.saturating_mul(2).clamp(1024, 131_072) as u32;
    let addr = std::net::SocketAddr::from(([0, 0, 0, 0], config.port));
    let listener = match TcpSocket::new_v4().and_then(|socket| {
        let _ = socket.set_reuseaddr(true);
        socket.bind(addr)?;
        socket.listen(listen_backlog)
    }) {
        Ok(listener) => listener,
        Err(e) => {
            error!(
                "Failed to bind controller port {}: {}. Exiting.",
                config.port, e
            );
            return;
        }
    };
    info!(
        "The controller is now listening on port {} (backlog requested: {}).",
        config.port, listen_backlog
    );

    let node_ws: NodeWriterMap = Arc::new(RwLock::new(HashMap::new()));

    let topology_edges = topology::topo::build_topology(&config).unwrap_or_default();
    let topology_neighbors = Arc::new(build_neighbor_index(&topology_edges));
    let multicast_snapshot_needed = match sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS (SELECT 1 FROM groups) OR EXISTS (SELECT 1 FROM group_routes)",
    )
    .fetch_one(&*db_pool)
    .await
    {
        Ok(enabled) => enabled,
        Err(e) => {
            warn!(
                "Failed to determine multicast snapshot state ({}); sending snapshots by default.",
                e
            );
            true
        }
    };

    // Set up a channel for a background task to process the event as a new node connects.
    let event_capacity = expected_node_count.saturating_mul(2).max(1024);
    let (new_node_connected_sender, new_node_connected_receiver) =
        broadcast::channel::<TopologyEvent>(event_capacity);

    // Spawn the centralized node connection coordinator.
    tokio::spawn(new_node_connected(
        new_node_connected_receiver,
        config.clone(),
        node_ws.clone(),
        db_pool.clone(),
    ));

    let (db_event_sender, db_event_receiver) = mpsc::channel(256);
    spawn_db_sync(
        db_pool.clone(),
        node_ws.clone(),
        db_event_receiver,
        config.flow_transport,
    );

    // Set up database notifications.
    setup_route_notification(db_pool.clone(), db_event_sender.clone()).await;
    setup_flow_notification(db_pool.clone(), db_event_sender.clone()).await;
    setup_group_notification(db_pool.clone(), db_event_sender.clone()).await;
    setup_probe_notification(db_pool.clone(), db_event_sender.clone()).await;

    loop {
        let (stream, _) = match listener.accept().await {
            Ok(result) => result,
            Err(e) => {
                error!(
                    "Failed to accept an incoming connection: {}. \
                     This can happen when the process hits the file descriptor limit (ulimit -n). \
                     Retrying in 1s.",
                    e
                );
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            }
        };
        let peer = match stream.peer_addr() {
            Ok(p) => p,
            Err(e) => {
                error!("Missing peer address on accepted stream: {}. Skipping.", e);
                continue;
            }
        };

        info!("New connection from {}.", peer);

        let db_pool = Arc::clone(&db_pool);
        let config = config.clone();
        let node_ws = Arc::clone(&node_ws);
        let new_node_connected_sender = new_node_connected_sender.clone();
        let topology_neighbors = Arc::clone(&topology_neighbors);
        let context = ConnectionContext {
            db_pool,
            config,
            node_ws,
            new_node_connected_sender,
            topology_neighbors,
            multicast_snapshot_needed,
        };

        tokio::spawn(async move {
            let ws_stream = match accept_async(stream).await {
                Ok(ws) => ws,
                Err(e) => {
                    error!(
                        "Failed to accept WebSocket connection from {}: {}. Skipping.",
                        peer, e
                    );
                    return;
                }
            };

            let (write, read) = ws_stream.split();
            handle_connection(read, write, context).await;
        });
    }
}

async fn handle_connection(
    mut read: WebSocketReader,
    write: WebSocketWriter,
    context: ConnectionContext,
) {
    let ConnectionContext {
        db_pool,
        config,
        node_ws,
        new_node_connected_sender,
        topology_neighbors,
        multicast_snapshot_needed,
    } = context;
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
                        let private_network_name =
                            normalize_private_network_name(&private_network_name);

                        info!(
                            "Received StartUp message from {} (public), {} (private), requested ID: {:?}.",
                            &public_network_addr, &private_network_addr, maybe_node_id
                        );

                        // assigns a node ID as the dataplane node requests
                        let Some(node_id) = maybe_node_id else {
                            error!("Startup message missing node_id; rejecting connection.");
                            continue;
                        };

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
                            private_network_name: private_network_name.clone(),
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
                            max_server_port: config.max_server_port,
                            protocol: config.protocol.clone(),
                            scheduler_type: config.scheduler_type,
                            node_spec,
                        });

                        // Safely encode and send StartUp response
                        let msg_bytes = match rmp_serde::to_vec(&response) {
                            Ok(b) => b,
                            Err(e) => {
                                error!(
                                    "Failed to encode StartUp response for node {}: {}",
                                    node_id, e
                                );
                                continue;
                            }
                        };
                        match write_arc
                            .lock()
                            .await
                            .send(Message::binary(msg_bytes))
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

                        // Fetch neighbor nodes only (instead of scanning the whole node table). If the
                        // topology has no edges, skip neighbor wiring but still continue with startup
                        // (routes, multicast snapshots, and NodeConnected event emission).
                        let neighbor_ids =
                            topology_neighbors.get(node_id).cloned().unwrap_or_default();

                        if !neighbor_ids.is_empty() {
                            let nodes: Vec<Node> = match sqlx::query_as(
                                "SELECT * FROM nodes WHERE id = ANY($1)",
                            )
                            .bind(&neighbor_ids)
                            .fetch_all(&*db_pool)
                            .await
                            {
                                Ok(nodes) => nodes,
                                Err(e) => {
                                    error!(
                                        "Failed to fetch neighbor nodes for node {}: {}. Skipping neighbor setup.",
                                        node_id, e
                                    );
                                    Vec::new()
                                }
                            };

                            // establishes connections between the new node and its neighbors by sending AddNode messages
                            for node in nodes {
                                // determines the address to use (private or public)
                                // if two nodes share the same private network name, then we use the private
                                // network address for this connection; otherwise, we use the public network
                                // address.
                                let addr = if shares_private_network(
                                    node.private_network_name.as_deref(),
                                    private_network_name.as_deref(),
                                ) {
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
                                    .send({
                                        match rmp_serde::to_vec(&msg) {
                                            Ok(buf) => Message::binary(buf),
                                            Err(e) => {
                                                error!(
                                                    "Failed to encode AddNode for {}: {}.",
                                                    node.id, e
                                                );
                                                continue;
                                            }
                                        }
                                    })
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

                                let reciprocal_addr = if shares_private_network(
                                    new_node.private_network_name.as_deref(),
                                    node.private_network_name.as_deref(),
                                ) {
                                    new_node.private_network_addr.clone()
                                } else {
                                    new_node.public_network_addr.clone()
                                };

                                let neighbor_writer = {
                                    let guard = node_ws.read().await;
                                    guard.get(&(node.id as usize)).cloned()
                                };

                                if let Some(neighbor_writer) = neighbor_writer {
                                    let addr_msg = ControllerToDataplane::AddNodeAddress {
                                        remote_node_id: node_id,
                                        remote_max_server_addr: reciprocal_addr,
                                    };

                                    match neighbor_writer
                                        .lock()
                                        .await
                                        .send({
                                            match rmp_serde::to_vec(&addr_msg) {
                                                Ok(buf) => Message::binary(buf),
                                                Err(e) => {
                                                    error!(
                                                        "Failed to encode AddNodeAddress for {}: {}.",
                                                        node.id, e
                                                    );
                                                    continue;
                                                }
                                            }
                                        })
                                        .await
                                    {
                                        Ok(_) => info!(
                                            "Sent an AddNodeAddress message for node {} to node {}.",
                                            node_id, node.id
                                        ),
                                        Err(e) => error!(
                                            "Failed to send an AddNodeAddress message to node {}: {}.",
                                            node.id, e
                                        ),
                                    }
                                }
                            }
                        }

                        // installs routes (always send, even if empty, so dataplane can mark routes_installed)
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
                                    "Failed to fetch routes for node {}: {}. Sending empty route list.",
                                    node_id, e
                                );
                                Vec::new()
                            }
                        };

                        // Always send InstallRoutes (even empty) so dataplane sets routes_installed = true
                        let msg = build_routes_for_node(routes, node_id as u32)
                            .unwrap_or(ControllerToDataplane::InstallRoutes { routes: vec![] });
                        match write_arc
                            .lock()
                            .await
                            .send({
                                match rmp_serde::to_vec(&msg) {
                                    Ok(buf) => Message::binary(buf),
                                    Err(e) => {
                                        error!(
                                            "Failed to encode InstallRoutes for {}: {}.",
                                            node_id, e
                                        );
                                        continue;
                                    }
                                }
                            })
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

                        if multicast_snapshot_needed {
                            if let Err(e) =
                                send_multicast_state_to_node(&db_pool, node_id, &write_arc).await
                            {
                                error!(
                                    "Failed to send multicast state to node {} during startup: {}.",
                                    node_id, e
                                );
                            }
                        } else {
                            let message =
                                ControllerToDataplane::InstallGroupDirectory { groups: Vec::new() };
                            match rmp_serde::to_vec(&message) {
                                Ok(payload) => {
                                    if let Err(e) =
                                        write_arc.lock().await.send(Message::binary(payload)).await
                                    {
                                        error!(
                                            "Failed to send InstallGroupDirectory to node {}: {}",
                                            node_id, e
                                        );
                                    }
                                }
                                Err(e) => {
                                    error!(
                                        "Failed to encode InstallGroupDirectory for node {}: {}",
                                        node_id, e
                                    );
                                }
                            }
                        }

                        // as a new node connects, checks if all the expected nodes are now connected
                        info!(
                            "Node {} setup completed successfully. At the time of insertion, there were {} nodes connected.",
                            node_id, connected_node_count
                        );

                        let _ = new_node_connected_sender.send(TopologyEvent::NodeConnected(
                            NodeConnectedEvent {
                                node_id,
                                connected_node_count,
                            },
                        ));
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
                    DataplaneToController::NodeTopologyReady { node_id } => {
                        let Some(registered_id) = current_node_id else {
                            warn!("NodeTopologyReady received before node registration; ignoring.");
                            continue;
                        };

                        if registered_id != node_id {
                            warn!(
                                "Node {} reported NodeTopologyReady for {}; using registered ID.",
                                registered_id, node_id
                            );
                        }

                        info!(
                            "Dataplane node {} reports its local topology is ready.",
                            registered_id
                        );

                        let _ = new_node_connected_sender.send(TopologyEvent::NodeLocallyReady {
                            node_id: registered_id,
                        });
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
                    DataplaneToController::CreateGroup { label } => {
                        let Some(node_id) = current_node_id else {
                            warn!("CreateGroup received before node registration; ignoring.");
                            continue;
                        };

                        match create_group(
                            &db_pool,
                            &label,
                            node_id,
                            config.multicast_pool_base,
                            config.multicast_pool_mask,
                        )
                        .await
                        {
                            Ok(group) => match group.group_ip.parse::<Ipv4Addr>() {
                                Ok(group_ip) => {
                                    let response = ControllerToDataplane::GroupCreated {
                                        group_id: group.id as usize,
                                        group_ip,
                                        src_node_id: group.src_node_id as usize,
                                    };

                                    // Pre-encode response and handle errors without panicking.
                                    let msg_bytes = match rmp_serde::to_vec(&response) {
                                        Ok(b) => b,
                                        Err(e) => {
                                            error!("Failed to encode GroupCreated response: {}", e);
                                            continue;
                                        }
                                    };
                                    if let Err(e) = write_arc
                                        .lock()
                                        .await
                                        .send(Message::binary(msg_bytes))
                                        .await
                                    {
                                        error!(
                                            "Failed to send GroupCreated to node {}: {}",
                                            node_id, e
                                        );
                                    } else {
                                        info!(
                                            "Created multicast group {} ({}) for node {}.",
                                            group.id, group.group_ip, node_id
                                        );
                                    }

                                    if let Err(e) =
                                        broadcast_group_directory(&db_pool, &node_ws).await
                                    {
                                        error!(
                                            "Failed to broadcast group directory after creating group {}: {}",
                                            group.id, e
                                        );
                                    }
                                }
                                Err(e) => error!(
                                    "Invalid group IP {} stored for group {}: {}",
                                    group.group_ip, group.id, e
                                ),
                            },
                            Err(e) => error!(
                                "Failed to create multicast group \"{}\" for node {}: {}",
                                label, node_id, e
                            ),
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
                    DataplaneToController::SetGroupRoutes { group_id, edges } => {
                        let Some(node_id) = current_node_id else {
                            warn!("SetGroupRoutes received before node registration; ignoring.");
                            continue;
                        };
                        let trees = vec![GroupRouteTree {
                            tree_id: 0,
                            weight: None,
                            edges,
                        }];
                        if let Err(e) = handle_set_group_routes_update(
                            &db_pool,
                            &node_ws,
                            node_id,
                            group_id,
                            trees,
                            "SetGroupRoutes",
                        )
                        .await
                        {
                            error!(
                                "SetGroupRoutes: failed to update routes for group {}: {}",
                                group_id, e
                            );
                        }
                    }
                    DataplaneToController::SetGroupRoutesMulti { group_id, trees } => {
                        let Some(node_id) = current_node_id else {
                            warn!(
                                "SetGroupRoutesMulti received before node registration; ignoring."
                            );
                            continue;
                        };

                        if let Err(e) = handle_set_group_routes_update(
                            &db_pool,
                            &node_ws,
                            node_id,
                            group_id,
                            trees,
                            "SetGroupRoutesMulti",
                        )
                        .await
                        {
                            error!(
                                "SetGroupRoutesMulti: failed to update routes for group {}: {}",
                                group_id, e
                            );
                        }
                    }

                    DataplaneToController::ProbeLinkResult {
                        probe_id,
                        from_node_id,
                        to_node_id,
                        bandwidth_mbps,
                    } => {
                        info!(
                            "Probe {} result: node {} → node {} = {:.2} Mbps",
                            probe_id, from_node_id, to_node_id, bandwidth_mbps,
                        );
                        if let Err(e) = sqlx::query(
                            "INSERT INTO probe_results (probe_id, from_node_id, to_node_id, bandwidth_mbps) VALUES ($1, $2, $3, $4)"
                        )
                            .bind(probe_id as i64)
                            .bind(from_node_id as i32)
                            .bind(to_node_id as i32)
                            .bind(bandwidth_mbps)
                            .execute(&*db_pool)
                            .await
                        {
                            error!("Failed to insert probe result: {}", e);
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

async fn broadcast_group_directory(
    db_pool: &Pool<Postgres>,
    node_ws: &NodeWriterMap,
) -> AnyResult<()> {
    let entries = load_group_directory_entries(db_pool).await?;

    let message = ControllerToDataplane::InstallGroupDirectory {
        groups: entries.clone(),
    };
    let payload = rmp_serde::to_vec(&message)?;

    let guard = node_ws.read().await;
    for (node_id, sender) in guard.iter() {
        if let Err(e) = sender
            .lock()
            .await
            .send(Message::binary(payload.clone()))
            .await
        {
            error!(
                "Failed to send InstallGroupDirectory to node {}: {}",
                node_id, e
            );
        } else {
            info!(
                "Broadcasted InstallGroupDirectory with {} entries to node {}.",
                entries.len(),
                node_id
            );
        }
    }

    Ok(())
}

async fn load_group_directory_entries(
    db_pool: &Pool<Postgres>,
) -> AnyResult<Vec<GroupDirectoryEntry>> {
    let groups = load_group_directory(db_pool).await?;
    let mut entries = Vec::with_capacity(groups.len());

    for group in groups {
        match group.group_ip.parse::<Ipv4Addr>() {
            Ok(ip) => entries.push(GroupDirectoryEntry {
                group_id: group.id as usize,
                group_ip: ip,
            }),
            Err(e) => warn!(
                "Skipping group {} due to invalid IP {}: {}",
                group.id, group.group_ip, e
            ),
        }
    }

    Ok(entries)
}

async fn handle_set_group_routes_update(
    db_pool: &Pool<Postgres>,
    node_ws: &NodeWriterMap,
    requester_node_id: usize,
    group_id: usize,
    trees: Vec<GroupRouteTree>,
    label: &str,
) -> AnyResult<()> {
    let group = sqlx::query_as::<_, crate::models::Group>(
        "SELECT id, src_node_id, group_ip FROM groups WHERE id = $1",
    )
    .bind(group_id as i32)
    .fetch_optional(db_pool)
    .await?;

    let Some(group) = group else {
        warn!("{}: unknown group id {}; ignoring.", label, group_id);
        return Ok(());
    };

    if group.src_node_id as usize != requester_node_id {
        warn!(
            "{}: node {} attempted to set routes for group {} owned by node {}. Ignoring.",
            label, requester_node_id, group_id, group.src_node_id
        );
        return Ok(());
    }

    let trees = match canonicalize_group_route_trees(group_id, &trees) {
        Ok(trees) => trees,
        Err(e) => {
            warn!(
                "{}: invalid trees payload for group {}: {}",
                label, group_id, e
            );
            return Ok(());
        }
    };

    let previous_trees = load_group_route_trees_for_group(db_pool, group_id as i32).await?;
    let previous_members = load_group_members(db_pool, group_id as i32).await?;
    let previous_member_node_ids: Vec<u32> =
        previous_members.iter().map(|m| m.node_id as u32).collect();
    replace_group_route_trees(db_pool, group_id as i32, group.src_node_id, &trees).await?;

    let members = load_group_members(db_pool, group_id as i32).await?;
    let member_node_ids: Vec<u32> = members.iter().map(|m| m.node_id as u32).collect();
    let member_node_set: HashSet<u32> = member_node_ids.iter().copied().collect();

    // Notify union(previous_nodes, new_nodes, src, members) so stale entries are cleared.
    let mut nodes_to_notify: HashSet<u32> = previous_trees
        .iter()
        .flat_map(|tree| tree.edges.iter().flat_map(|(a, b)| [*a, *b]))
        .collect();
    nodes_to_notify.extend(
        trees
            .iter()
            .flat_map(|tree| tree.edges.iter().flat_map(|(a, b)| [*a, *b])),
    );
    nodes_to_notify.insert(group.src_node_id as u32);
    nodes_to_notify.extend(previous_member_node_ids.iter().copied());
    nodes_to_notify.extend(member_node_ids.iter().copied());

    // Snapshot writers to avoid holding the lock while sending.
    let send_targets: Vec<(u32, Arc<tokio::sync::Mutex<WebSocketWriter>>)> = {
        let guard = node_ws.read().await;
        nodes_to_notify
            .iter()
            .filter_map(|node| {
                guard
                    .get(&(*node as usize))
                    .map(|writer| (*node, Arc::clone(writer)))
            })
            .collect()
    };

    if send_targets.is_empty() {
        warn!(
            "{}: no active websocket connections for group {} update.",
            label, group_id
        );
        return Ok(());
    }

    for (target_node_id, writer) in send_targets {
        let routes = match build_group_routes_for_node_multitree(
            group.id as usize,
            group.src_node_id as u32,
            &trees,
            target_node_id,
            &member_node_set,
        ) {
            Ok(routes) => routes,
            Err(e) => {
                error!(
                    "{}: failed to build InstallGroupRoutes for group {} node {}: {}",
                    label, group_id, target_node_id, e
                );
                continue;
            }
        };

        let message = ControllerToDataplane::InstallGroupRoutes {
            group_id: group.id as usize,
            src_node_id: group.src_node_id as usize,
            routes,
        };
        let payload = match rmp_serde::to_vec(&message) {
            Ok(payload) => payload,
            Err(e) => {
                error!(
                    "{}: failed to encode InstallGroupRoutes for group {}: {}",
                    label, group_id, e
                );
                continue;
            }
        };

        if let Err(e) = writer.lock().await.send(Message::binary(payload)).await {
            error!(
                "{}: failed to send InstallGroupRoutes for group {} to node {}: {}",
                label, group_id, target_node_id, e
            );
        }
    }

    let total_edges = trees.iter().map(|tree| tree.edges.len()).sum::<usize>();
    info!(
        "{}: installed {} tree(s) ({} total edges) for group {} (src {}).",
        label,
        trees.len(),
        total_edges,
        group_id,
        group.src_node_id
    );

    Ok(())
}

async fn load_group_route_trees_for_group(
    db_pool: &Pool<Postgres>,
    group_id: i32,
) -> AnyResult<Vec<GroupRouteTree>> {
    let rows = sqlx::query_as::<_, DbGroupRoute>(
        r#"
        SELECT group_id, tree_id, src_node_id, weight, edges
        FROM group_routes
        WHERE group_id = $1
        ORDER BY tree_id ASC
        "#,
    )
    .bind(group_id)
    .fetch_all(db_pool)
    .await?;

    let mut trees = Vec::with_capacity(rows.len());
    for row in rows {
        let tree_id = usize::try_from(row.tree_id).map_err(|_| {
            anyhow::anyhow!(
                "group_routes.tree_id must be non-negative (group_id={}, tree_id={})",
                group_id,
                row.tree_id
            )
        })?;
        trees.push(GroupRouteTree {
            tree_id,
            weight: row.weight,
            edges: decode_tree_edges(row.edges)?,
        });
    }

    Ok(trees)
}

async fn replace_group_route_trees(
    db_pool: &Pool<Postgres>,
    group_id: i32,
    src_node_id: i32,
    trees: &[GroupRouteTree],
) -> AnyResult<()> {
    let mut tx = db_pool.begin().await?;
    sqlx::query("DELETE FROM group_routes WHERE group_id = $1")
        .bind(group_id)
        .execute(&mut *tx)
        .await?;

    for tree in trees {
        let tree_id = i32::try_from(tree.tree_id).map_err(|_| {
            anyhow::anyhow!(
                "tree_id {} exceeds i32 range for group {}",
                tree.tree_id,
                group_id
            )
        })?;

        let edges_json = encode_tree_edges(&tree.edges)?;
        sqlx::query(
            r#"
            INSERT INTO group_routes (group_id, tree_id, src_node_id, weight, edges, updated_at)
            VALUES ($1, $2, $3, $4, $5, EXTRACT(EPOCH FROM NOW())::BIGINT * 1000)
            "#,
        )
        .bind(group_id)
        .bind(tree_id)
        .bind(src_node_id)
        .bind(tree.weight)
        .bind(edges_json)
        .execute(&mut *tx)
        .await?;
    }

    tx.commit().await?;
    Ok(())
}

fn decode_tree_edges(value: serde_json::Value) -> AnyResult<Vec<(u32, u32)>> {
    if let Ok(raw) = serde_json::from_value::<Vec<[u32; 2]>>(value.clone()) {
        return Ok(raw.into_iter().map(|pair| (pair[0], pair[1])).collect());
    }

    Ok(serde_json::from_value::<Vec<(u32, u32)>>(value)?)
}

fn encode_tree_edges(edges: &[(u32, u32)]) -> AnyResult<serde_json::Value> {
    Ok(serde_json::to_value(
        edges
            .iter()
            .map(|(a, b)| [*a, *b])
            .collect::<Vec<[u32; 2]>>(),
    )?)
}

async fn send_multicast_state_to_node(
    db_pool: &Pool<Postgres>,
    node_id: usize,
    writer: &Arc<Mutex<WebSocketWriter>>,
) -> AnyResult<()> {
    send_group_directory_to_node(db_pool, node_id, writer).await?;
    send_group_routes_snapshot_to_node(db_pool, node_id, writer).await?;
    Ok(())
}

async fn send_group_directory_to_node(
    db_pool: &Pool<Postgres>,
    node_id: usize,
    writer: &Arc<Mutex<WebSocketWriter>>,
) -> AnyResult<()> {
    let entries = load_group_directory_entries(db_pool).await?;
    let message = ControllerToDataplane::InstallGroupDirectory {
        groups: entries.clone(),
    };
    let payload = rmp_serde::to_vec(&message)?;

    if let Err(e) = writer.lock().await.send(Message::binary(payload)).await {
        error!(
            "Failed to send InstallGroupDirectory to node {}: {}",
            node_id, e
        );
    } else {
        info!(
            "Sent InstallGroupDirectory with {} entries to node {}.",
            entries.len(),
            node_id
        );
    }

    Ok(())
}

async fn send_group_routes_snapshot_to_node(
    db_pool: &Pool<Postgres>,
    node_id: usize,
    writer: &Arc<Mutex<WebSocketWriter>>,
) -> AnyResult<()> {
    let groups = load_group_directory(db_pool).await?;
    if groups.is_empty() {
        return Ok(());
    }

    let stored_routes = sqlx::query_as::<_, DbGroupRoute>(
        r#"
        SELECT group_id, tree_id, src_node_id, weight, edges
        FROM group_routes
        ORDER BY group_id ASC, tree_id ASC
        "#,
    )
    .fetch_all(db_pool)
    .await?;

    let by_group = build_group_routes_snapshot_trees(groups, stored_routes)?;

    for (group_id, (src_node_id, trees)) in by_group {
        let members = load_group_members(db_pool, group_id).await?;
        let member_set: HashSet<u32> = members.iter().map(|m| m.node_id as u32).collect();
        let payload = match encode_group_routes_snapshot_payload(
            group_id,
            src_node_id,
            &trees,
            node_id,
            &member_set,
        ) {
            Ok(payload) => payload,
            Err(e) => {
                warn!(
                    "Skipping invalid group_routes snapshot for group {} on node {}: {}",
                    group_id, node_id, e
                );
                continue;
            }
        };

        if let Err(e) = writer.lock().await.send(Message::binary(payload)).await {
            error!(
                "Failed to send InstallGroupRoutes for group {} to node {}: {}",
                group_id, node_id, e
            );
        } else {
            info!(
                "Sent InstallGroupRoutes snapshot for group {} to node {}.",
                group_id, node_id
            );
        }
    }

    Ok(())
}

fn build_group_routes_snapshot_trees(
    groups: Vec<crate::models::Group>,
    stored_routes: Vec<DbGroupRoute>,
) -> AnyResult<BTreeMap<i32, (i32, Vec<GroupRouteTree>)>> {
    let mut by_group: BTreeMap<i32, (i32, Vec<GroupRouteTree>)> = groups
        .into_iter()
        .map(|group| (group.id, (group.src_node_id, Vec::new())))
        .collect();

    for group_route in stored_routes {
        let Some((group_src_node_id, trees)) = by_group.get_mut(&group_route.group_id) else {
            warn!(
                "Skipping group_routes row for unknown group {}.",
                group_route.group_id
            );
            continue;
        };

        let tree_id = match usize::try_from(group_route.tree_id) {
            Ok(tree_id) => tree_id,
            Err(_) => {
                warn!(
                    "Skipping invalid group_routes row with negative tree_id={} for group {}.",
                    group_route.tree_id, group_route.group_id
                );
                continue;
            }
        };

        if *group_src_node_id != group_route.src_node_id {
            warn!(
                "Group {} has mismatched src_node_id values (groups={}, group_routes={}).",
                group_route.group_id, *group_src_node_id, group_route.src_node_id
            );
        }

        trees.push(GroupRouteTree {
            tree_id,
            weight: group_route.weight,
            edges: decode_tree_edges(group_route.edges)?,
        });
    }

    Ok(by_group)
}

fn encode_group_routes_snapshot_payload(
    group_id: i32,
    src_node_id: i32,
    trees: &[GroupRouteTree],
    node_id: usize,
    member_set: &HashSet<u32>,
) -> AnyResult<Vec<u8>> {
    let group_id_usize = usize::try_from(group_id).map_err(|_| {
        anyhow::anyhow!("group_routes.group_id must be non-negative (group_id={group_id})")
    })?;
    let src_node_id_u32 = u32::try_from(src_node_id).map_err(|_| {
        anyhow::anyhow!(
            "group_routes.src_node_id must be non-negative (group_id={}, src_node_id={})",
            group_id,
            src_node_id
        )
    })?;
    let routes = build_group_routes_for_node_multitree(
        group_id_usize,
        src_node_id_u32,
        trees,
        node_id as u32,
        member_set,
    )
    .map_err(anyhow::Error::msg)?;

    let message = ControllerToDataplane::InstallGroupRoutes {
        group_id: group_id_usize,
        src_node_id: src_node_id_u32 as usize,
        routes,
    };
    Ok(rmp_serde::to_vec(&message)?)
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use crate::models::{DbGroupRoute, Group};
    use nextmini_messages::{ControllerToDataplane, GroupRouteTree};
    use serde_json::json;

    use super::{build_group_routes_snapshot_trees, encode_group_routes_snapshot_payload};

    #[test]
    fn reconnect_snapshot_encodes_empty_routes_for_nonparticipant_node() {
        let trees = vec![GroupRouteTree {
            tree_id: 0,
            weight: None,
            edges: vec![(1, 2), (2, 3)],
        }];
        let members = HashSet::from([3u32]);

        let payload = encode_group_routes_snapshot_payload(42, 1, &trees, 99, &members)
            .expect("snapshot payload should encode even when routes are empty");
        let decoded: ControllerToDataplane =
            rmp_serde::from_slice(&payload).expect("snapshot payload should decode");

        match decoded {
            ControllerToDataplane::InstallGroupRoutes {
                group_id,
                src_node_id,
                routes,
            } => {
                assert_eq!(group_id, 42);
                assert_eq!(src_node_id, 1);
                assert!(
                    routes.is_empty(),
                    "reconnect snapshot must still emit InstallGroupRoutes with an empty routes list to clear stale entries"
                );
            }
            other => panic!("expected InstallGroupRoutes snapshot, got {other:?}"),
        }
    }

    #[test]
    fn reconnect_snapshot_includes_groups_with_zero_trees() {
        let groups = vec![
            Group {
                id: 42,
                src_node_id: 1,
                group_ip: "239.0.0.1".to_string(),
            },
            Group {
                id: 77,
                src_node_id: 9,
                group_ip: "239.0.0.2".to_string(),
            },
        ];
        let stored_routes = vec![DbGroupRoute {
            group_id: 42,
            tree_id: 0,
            src_node_id: 99,
            weight: None,
            edges: json!([[1, 2], [2, 3]]),
        }];

        let by_group = build_group_routes_snapshot_trees(groups, stored_routes)
            .expect("snapshot trees should decode");

        assert_eq!(
            by_group.len(),
            2,
            "every group must appear in reconnect snapshot"
        );

        let (src_42, trees_42) = by_group.get(&42).expect("group 42 must be present");
        assert_eq!(
            *src_42, 1,
            "reconnect snapshot should use groups.src_node_id as source of truth"
        );
        assert_eq!(trees_42.len(), 1);

        let (src_77, trees_77) = by_group.get(&77).expect("group 77 must be present");
        assert_eq!(*src_77, 9);
        assert!(
            trees_77.is_empty(),
            "groups with no rows in group_routes must still emit an empty InstallGroupRoutes snapshot"
        );
    }
}
