mod config;
mod db;
mod db_sync;
mod models;
mod new_node;
mod route_ser;
mod routing;
mod topology;
mod utils;

use std::collections::{HashMap, HashSet};
use std::net::Ipv4Addr;
use std::sync::Arc;

use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
use sqlx::{Pool, Postgres};
use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tokio::sync::{Mutex, RwLock, broadcast, mpsc};
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::{accept_async, tungstenite::Message};
use tracing::{error, info, warn};

use anyhow::Result as AnyResult;
use nextmini_messages::{
    ControllerToDataplane, DataplaneToController, GroupDirectoryEntry, GroupRoutingTableEntry,
};

use crate::config::{Config, get_config};
use crate::db::{
    add_group_member, create_group, init_db, load_group_directory, load_group_members,
    remove_group_member, setup_flow_notification, setup_group_notification,
    setup_route_notification,
};
use crate::db_sync::spawn_db_sync;
use crate::models::{DbGroupRoute, DbRoute, Node, Route};
use crate::new_node::{NodeConnectedEvent, TopologyEvent, new_node_connected};
use crate::utils::{
    StartupResponseParams, build_group_routes_for_node, build_routes_for_node,
    build_startup_response,
};

type WebSocketReader = SplitStream<WebSocketStream<TcpStream>>;
pub type WebSocketWriter = SplitSink<WebSocketStream<TcpStream>, Message>;
pub type NodeWriterMap = Arc<RwLock<HashMap<usize, Arc<Mutex<WebSocketWriter>>>>>;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt().init();
    let config = get_config("config.toml");
    let db_pool = Arc::new(init_db(&config).await);
    let listener = match TcpListener::bind(format!("0.0.0.0:{}", config.port)).await {
        Ok(l) => l,
        Err(e) => {
            error!(
                "Failed to bind controller port {}: {}. Exiting.",
                config.port, e
            );
            return;
        }
    };
    info!("The controller is now listening on port {}.", config.port);

    let node_ws: NodeWriterMap = Arc::new(RwLock::new(HashMap::new()));

    // Set up a channel for a background task to process the event as a new node connects.
    let (new_node_connected_sender, new_node_connected_receiver) =
        broadcast::channel::<TopologyEvent>(100);

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

    while let Ok((stream, _)) = listener.accept().await {
        let peer = match stream.peer_addr() {
            Ok(p) => p,
            Err(e) => {
                error!("Missing peer address on accepted stream: {}. Skipping.", e);
                continue;
            }
        };

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
    new_node_connected_sender: broadcast::Sender<TopologyEvent>,
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
                        } else {
                            error!("No routes to install for node {}.", node_id);
                        }

                        if let Err(e) =
                            send_multicast_state_to_node(&db_pool, node_id, &write_arc).await
                        {
                            error!(
                                "Failed to send multicast state to node {} during startup: {}.",
                                node_id, e
                            );
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

                        // Load group to validate ownership and get src_node_id.
                        let group = match sqlx::query_as::<_, crate::models::Group>(
                            "SELECT id, label, src_node_id, group_ip FROM groups WHERE id = $1",
                        )
                        .bind(group_id as i32)
                        .fetch_optional(&*db_pool)
                        .await
                        {
                            Ok(opt) => opt,
                            Err(e) => {
                                error!(
                                    "SetGroupRoutes: failed to load group {} from DB: {}",
                                    group_id, e
                                );
                                continue;
                            }
                        };

                        let Some(group) = group else {
                            warn!("SetGroupRoutes: unknown group id {}; ignoring.", group_id);
                            continue;
                        };

                        if group.src_node_id as usize != node_id {
                            warn!(
                                "SetGroupRoutes: node {} attempted to set routes for group {} owned by node {}. Ignoring.",
                                node_id, group_id, group.src_node_id
                            );
                            continue;
                        }

                        // Fetch previous edges so we can clear stale routes on nodes that are no
                        // longer part of the DAG after the override.
                        let previous_edges_value: Option<serde_json::Value> =
                            match sqlx::query_scalar("SELECT edges FROM group_routes WHERE group_id = $1")
                                .bind(group_id as i32)
                                .fetch_optional(&*db_pool)
                                .await
                            {
                                Ok(v) => v,
                                Err(e) => {
                                    error!(
                                        "SetGroupRoutes: failed to read previous group_routes for group {}: {}",
                                        group_id, e
                                    );
                                    None
                                }
                            };

                        let previous_edges: Vec<(u32, u32)> = previous_edges_value
                            .as_ref()
                            .map(|value| {
                                serde_json::from_value::<Vec<(u32, u32)>>(value.clone())
                                    .unwrap_or_default()
                            })
                            .unwrap_or_default();

                        // Persist override edges.
                        let edges_json = match serde_json::to_value(
                            edges.iter().map(|(a, b)| [*a, *b]).collect::<Vec<[u32; 2]>>(),
                        ) {
                            Ok(v) => v,
                            Err(e) => {
                                error!(
                                    "SetGroupRoutes: failed to encode edges JSON for group {}: {}",
                                    group_id, e
                                );
                                continue;
                            }
                        };

                        if let Err(e) = sqlx::query(
                            r#"
                            INSERT INTO group_routes (group_id, src_node_id, edges)
                            VALUES ($1, $2, $3)
                            ON CONFLICT (group_id)
                            DO UPDATE SET
                                src_node_id = EXCLUDED.src_node_id,
                                edges = EXCLUDED.edges,
                                updated_at = EXTRACT(EPOCH FROM NOW())::BIGINT * 1000
                            "#,
                        )
                        .bind(group_id as i32)
                        .bind(group.src_node_id)
                        .bind(edges_json)
                        .execute(&*db_pool)
                        .await
                        {
                            error!(
                                "SetGroupRoutes: failed to upsert group_routes for group {}: {}",
                                group_id, e
                            );
                            continue;
                        }

                        // Load members and compute node set for delivery and notifications.
                        let members = match load_group_members(&db_pool, group_id as i32).await {
                            Ok(m) => m,
                            Err(e) => {
                                error!(
                                    "SetGroupRoutes: failed to load members for group {}: {}",
                                    group_id, e
                                );
                                continue;
                            }
                        };
                        let member_node_ids: Vec<u32> =
                            members.iter().map(|m| m.node_id as u32).collect();
                        let member_node_set: HashSet<u32> =
                            member_node_ids.iter().copied().collect();

                        // Notify union(previous_nodes, new_nodes, src, members) so stale entries are cleared.
                        let mut nodes_to_notify: HashSet<u32> = previous_edges
                            .iter()
                            .flat_map(|(a, b)| [*a, *b])
                            .collect();
                        nodes_to_notify.extend(edges.iter().flat_map(|(a, b)| [*a, *b]));
                        nodes_to_notify.insert(group.src_node_id as u32);
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
                                "SetGroupRoutes: no active websocket connections for group {} update.",
                                group_id
                            );
                            continue;
                        }

                        for (target_node_id, writer) in send_targets {
                            let entry = build_group_routes_for_node(
                                group.id as usize,
                                group.src_node_id as u32,
                                &edges,
                                target_node_id,
                                &member_node_set,
                            );
                            let routes: Vec<GroupRoutingTableEntry> = entry.into_iter().collect();
                            let message = ControllerToDataplane::InstallGroupRoutes {
                                group_id: group.id as usize,
                                src_node_id: group.src_node_id as usize,
                                routes,
                            };

                            let payload = match rmp_serde::to_vec(&message) {
                                Ok(p) => p,
                                Err(e) => {
                                    error!(
                                        "SetGroupRoutes: failed to encode InstallGroupRoutes for group {}: {}",
                                        group_id, e
                                    );
                                    continue;
                                }
                            };
                            if let Err(e) =
                                writer.lock().await.send(Message::binary(payload)).await
                            {
                                error!(
                                    "SetGroupRoutes: failed to send InstallGroupRoutes for group {} to node {}: {}",
                                    group_id, target_node_id, e
                                );
                            }
                        }

                        info!(
                            "SetGroupRoutes: installed override DAG ({} edges) for group {} (src {}).",
                            edges.len(),
                            group_id,
                            group.src_node_id
                        );
                    }
                    DataplaneToController::ReliableStats { stats } => {
                        info!(
                            "ReliableStats: sid={} node={} role={} bytes={} chunks={} resends={} repairs={} fec_used={} ts_ms={}",
                            stats.session_id,
                            stats.node_id,
                            stats.role,
                            stats.bytes,
                            stats.chunks,
                            stats.resends,
                            stats.repairs,
                            stats.fec_used,
                            stats.ts_ms
                        );
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
    let stored_routes =
        sqlx::query_as::<_, DbGroupRoute>("SELECT group_id, src_node_id, edges FROM group_routes")
            .fetch_all(db_pool)
            .await?;

    if stored_routes.is_empty() {
        return Ok(());
    }

    for group_route in stored_routes {
        let dag_raw: Vec<[u32; 2]> = serde_json::from_value(group_route.edges.clone())?;
        if dag_raw.is_empty() {
            continue;
        }

        let dag_edges: Vec<(u32, u32)> =
            dag_raw.into_iter().map(|pair| (pair[0], pair[1])).collect();
        let members = load_group_members(db_pool, group_route.group_id).await?;
        let member_set: HashSet<u32> = members.iter().map(|m| m.node_id as u32).collect();

        let Some(entry) = build_group_routes_for_node(
            group_route.group_id as usize,
            group_route.src_node_id as u32,
            &dag_edges,
            node_id as u32,
            &member_set,
        ) else {
            continue;
        };

        let message = ControllerToDataplane::InstallGroupRoutes {
            group_id: group_route.group_id as usize,
            src_node_id: group_route.src_node_id as usize,
            routes: vec![entry],
        };
        let payload = rmp_serde::to_vec(&message)?;

        if let Err(e) = writer.lock().await.send(Message::binary(payload)).await {
            error!(
                "Failed to send InstallGroupRoutes for group {} to node {}: {}",
                group_route.group_id, node_id, e
            );
        } else {
            info!(
                "Sent InstallGroupRoutes snapshot for group {} to node {}.",
                group_route.group_id, node_id
            );
        }
    }

    Ok(())
}
