---
title: "Source Walkthrough"
description: "This document is a linear walkthrough of the Nextmini codebase."
---

This document is a linear walkthrough of the Nextmini codebase from process startup to steady-state packet forwarding and telemetry.

The order below mirrors runtime behavior rather than directory order, so you can trace how data and control move through the system.

Linear walkthrough plan:

1. Workspace composition and crate boundaries.
2. Shared wire protocol (`messages`) used by controller/dataplane.
3. Controller startup, DB wiring, and websocket accept loop.
4. Node onboarding and topology-wide readiness coordination.
5. DB-driven live sync for routes/flows/group trees.
6. Dataplane startup and conductor orchestration.
7. Controller control-plane receiver inside dataplane.
8. Local TUN I/O, processor routing, scheduler forwarding.
9. Max-mode connector plus user-space/lossless flow paths.
10. Python embedding (`python-api`) and end-to-end lifecycle recap.

```bash
sed -n '1,80p' Cargo.toml
```

```rust
[workspace]
members = ["messages", "controller", "dataplane", "cert-gen", "python-api", "raptorq"]
resolver = "3"
```

### Step 1: the workspace root declares the Rust crates that make up the runtime:

- `messages`: shared control/data message schema.
- `controller`: control plane, DB, topology, route/group orchestration.
- `dataplane`: node runtime, packet processing, networking/scheduling.
- `python-api`: PyO3 bindings embedding dataplane behavior in Python.
- `cert-gen` and `raptorq`: support tools/components.

```bash
sed -n '64,170p' messages/src/lib.rs
```

```rust
pub enum DataplaneToController {
    StartUp {
        private_network_name: String,
        private_network_addr: String,
        public_network_addr: String,
        node_id: Option<usize>,
    },
    Metrics {
        metrics: Vec<Metric>,
    },
    FlowFinished {
        flows: Vec<FlowFinishedInfo>,
    },
    /// Indicates that a dataplane node has finished wiring its local topology.
    NodeTopologyReady {
        node_id: usize,
    },
    UserFlowStart {
        flows: Vec<UserFlowStart>,
    },
    AppFlowStart {
        appflows: Vec<AppFlow>,
    },
    RouteAssigned {
        assignments: Vec<RouteAssignment>,
    },
    CreateGroup {
        label: String,
    },
    JoinGroup {
        group_id: GroupId,
    },
    LeaveGroup {
        group_id: GroupId,
    },
    /// Sets multicast DAG edges for a group. Intended for external optimizers (e.g. LP) that want
    /// to directly control the multicast tree without rewriting unicast routes.
    ///
    /// The controller is expected to validate that the sender is the group's source node.
    SetGroupRoutes {
        group_id: GroupId,
        /// Directed edges (from_node_id, to_node_id) describing the multicast DAG.
        edges: Vec<(u32, u32)>,
    },
    /// Sets multiple multicast trees for a group.
    SetGroupRoutesMulti {
        group_id: GroupId,
        trees: Vec<GroupRouteTree>,
    },
}

/// The new app flow message reported to controller from a src node to dest node.
#[derive(Serialize, Deserialize, Debug)]
pub struct AppFlow {
    pub flow_id: [u8; 16],
    pub src_node_id: usize,
    pub dst_node_id: usize,
    pub start_time: i64,
}

/// The information about a finished flow.
#[derive(Serialize, Deserialize, Debug)]
pub struct FlowFinishedInfo {
    pub flow_id: [u8; 16],
    pub controller_id: Option<i32>,
    pub start_time: i64,
    pub finish_time: i64,
}

/// The information about the start of a user-space flow.
#[derive(Serialize, Deserialize, Debug)]
pub struct UserFlowStart {
    pub controller_id: i32,
    pub flow_id: [u8; 16],
    pub start_time: i64,
}

/// The information about a route assignment.
#[derive(Serialize, Deserialize, Debug)]
pub struct RouteAssignment {
    pub flow_id: [u8; 16],
    pub route_id: usize,
    pub time: i64,
}

/// Performance metrics for a particular flow on a link from a local node to remote node.
#[derive(Serialize, Deserialize, Debug)]
pub struct Metric {
```

```bash
sed -n '285,410p' messages/src/lib.rs
```

```rust
pub enum FlowTransport {
    #[default]
    Tcp,
    LosslessUnicast,
}

/// The traffic specification for a user-space TCP flow.
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct Flow {
    pub controller_id: Option<i32>,
    pub src_node_id: usize,
    pub dst_node_id: usize,
    #[serde(default)]
    pub route_id: Option<usize>,
    pub flow_spec: FlowSpec,
}

/// The specification of a user-space TCP flow.
#[derive(Serialize, PartialEq, Debug, Clone, Copy)]
pub struct FlowSpec {
    pub flow_len: FlowLen,
    #[serde(default)]
    pub flow_rate: Option<usize>, // bytes per second
    #[serde(default)]
    pub flow_weight: Option<usize>,
    #[serde(default)]
    pub transport: FlowTransport,
}

impl<'de> Deserialize<'de> for FlowSpec {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct FlowSpecSerde {
            flow_len: FlowLen,
            #[serde(default)]
            flow_rate: Option<usize>,
            #[serde(default)]
            flow_weight: Option<usize>,
            #[serde(default)]
            transport: FlowTransport,
        }

        let helper = FlowSpecSerde::deserialize(deserializer)?;
        let spec = FlowSpec {
            flow_len: helper.flow_len,
            flow_rate: helper.flow_rate,
            flow_weight: helper.flow_weight,
            transport: helper.transport,
        };
        spec.validate().map_err(de::Error::custom)?;
        Ok(spec)
    }
}

#[derive(Debug)]
pub enum FlowSpecValidationError {
    DurationMissingRate,
}

impl fmt::Display for FlowSpecValidationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FlowSpecValidationError::DurationMissingRate => {
                write!(f, "duration-based flows require flow_rate (bytes/sec)")
            }
        }
    }
}

impl std::error::Error for FlowSpecValidationError {}

impl FlowSpec {
    pub fn validate(&self) -> Result<(), FlowSpecValidationError> {
        match self.flow_len {
            FlowLen::Duration(_)
                if self.flow_rate.is_none()
                    && matches!(self.transport, FlowTransport::LosslessUnicast) =>
            {
                Err(FlowSpecValidationError::DurationMissingRate)
            }
            _ => Ok(()),
        }
    }
}

/// The length of a user-space TCP flow, specified either by the number of bytes or by the duration of the flow.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum FlowLen {
    Bytes(usize),
    Duration(f64),
}

impl FlowLen {
    pub fn exceeded(&self, sent_size: u64, start_time: std::time::Instant) -> bool {
        match *self {
            FlowLen::Bytes(size) => sent_size >= size as u64,
            FlowLen::Duration(duration) => start_time.elapsed().as_secs_f64() >= duration,
        }
    }
}

impl Hash for FlowLen {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            FlowLen::Bytes(size) => {
                0u8.hash(state);
                size.hash(state);
            }
            FlowLen::Duration(duration) => {
                1u8.hash(state);
                duration.to_bits().hash(state);
            }
        }
    }
}

/// The specification of a token bucket traffic shaper.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TokenBucketSpec {
    pub rate: usize,
    pub bucket_size: usize,
}

```

```bash
sed -n '414,510p' messages/src/lib.rs
```

```rust
pub enum ControllerToDataplane {
    StartUp {
        node_id: usize,
        #[serde(with = "ip_ser")]
        net_mask: Ipv4Addr,
        #[serde(with = "ip_ser")]
        virtual_base_addr: Ipv4Addr,
        #[serde(with = "ip_ser")]
        user_space_base_addr: Ipv4Addr,
        #[serde(with = "ip_ser")]
        external_base_addr: Ipv4Addr,
        max_server_port: u16,
        protocol: Protocol,
        scheduler_type: SchedulingDiscipline,
        node_spec: NodeSpec,
    },
    AddNode {
        remote_node_id: usize,
        remote_addr: String,
    },
    AddNodeAddress {
        remote_node_id: usize,
        remote_max_server_addr: String,
    },
    InstallRoutes {
        routes: Vec<RoutingTableEntry>,
    },
    SetLinkRate {
        node_id: usize,
        spec: TokenBucketSpec,
    },
    AddFlows {
        flows: Vec<Flow>,
    },
    /// Signals that the controller has seen every expected dataplane node.
    TopologyReady,
    GroupCreated {
        group_id: GroupId,
        #[serde(with = "ip_ser")]
        group_ip: Ipv4Addr,
        src_node_id: usize,
    },
    InstallGroupDirectory {
        groups: Vec<GroupDirectoryEntry>,
    },
    InstallGroupRoutes {
        group_id: GroupId,
        src_node_id: usize,
        routes: Vec<GroupRoutingTableEntry>,
    },
}

/// Routing table entry: route_id → next_hop, with source and destination node IDs.
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct RoutingTableEntry {
    pub route_id: usize,
    pub next_hops: Vec<usize>,
    pub src_node_id: usize,
    pub dst_node_id: usize,
    #[serde(
        default = "default_route_forwarding_mode",
        deserialize_with = "deserialize_forward_mode"
    )]
    pub forward_mode: RouteForwardingMode,
}
```

### Step 2: `messages/src/lib.rs` is the on-wire contract.

`DataplaneToController` captures telemetry and runtime events (startup, metrics, flow lifecycle, multicast/group operations).
`ControllerToDataplane` is the controller's command stream (topology links, routes, flows, group directories/routes, readiness).

Two details that matter later in runtime flow:
- `FlowSpec::validate` enforces transport-specific invariants (for example, duration-based lossless flows must carry `flow_rate`).
- `RoutingTableEntry.forward_mode` allows the dataplane to treat unicast and multicast forwarding differently while using one route-install channel.

### Step 3: starts in `controller/src/main.rs`.

The controller process initializes tracing, reads `config.toml`, initializes Postgres, computes topology neighbors, starts DB notification listeners, and then accepts websocket connections from dataplane nodes.

This is the central control-plane event loop.

```bash
LC_ALL=C LANG=C sed -n '1,220p' controller/src/main.rs
```

```rust
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
```

Inside the connection handler, `DataplaneToController::StartUp` drives node onboarding:

- reserve node id in `node_ws`
- upsert node metadata into DB
- build/send `ControllerToDataplane::StartUp`
- send `AddNode` for neighbors
- send `InstallRoutes`
- optionally send multicast snapshots (`InstallGroupDirectory` and `InstallGroupRoutes`)

The same handler also receives metrics, flow-finished signals, group create/join/leave, and tree updates from dataplanes.

```bash
LC_ALL=C LANG=C sed -n '217,560p' controller/src/main.rs
```

```rust
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
```

### Step 4: once nodes connect, a background coordinator (`new_node_connected`) waits for expected topology membership, then broadcasts additional control data in order.

Order of operations:
1. optional `AddNodeAddress` (for Max mode)
2. `SetLinkRate`
3. `AddFlows`
4. `TopologyReady` only after every node reports local topology readiness

```bash
LC_ALL=C LANG=C sed -n '1,240p' controller/src/new_node.rs
```

```rust
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;

use tokio::time::Duration;

use futures_util::SinkExt;
use sqlx::{Pool, Postgres};
use tokio::sync::{Mutex, RwLock, broadcast};
use tokio_tungstenite::tungstenite::Message;
use tracing::{error, info, warn};

use nextmini_messages::OperatingMode;
use nextmini_messages::{ControllerToDataplane, FlowTransport, TokenBucketSpec};

use crate::NodeWriterMap;
use crate::WebSocketWriter;
use crate::config::Config;
use crate::models::{DbFlow, DbFlowRoute, Node};
use crate::utils::build_flows_for_node;

// Event to be sent when a new node has connected to the controller.
#[derive(Debug, Clone)]
pub struct NodeConnectedEvent {
    pub node_id: usize,
    pub connected_node_count: usize,
}

#[derive(Debug, Clone)]
pub enum TopologyEvent {
    NodeConnected(NodeConnectedEvent),
    NodeLocallyReady { node_id: usize },
}

/// A background task that checks if all the expected nodes have connected, and performs additional
/// processing when this occurs.
pub async fn new_node_connected(
    mut event_receiver: broadcast::Receiver<TopologyEvent>,
    config: Config,
    node_ws: Arc<RwLock<HashMap<usize, Arc<Mutex<WebSocketWriter>>>>>,
    db_pool: Arc<Pool<Postgres>>,
) {
    let mut config_dispatched = false;
    let mut topology_ready_sent = false;
    let mut start_time = None;
    let expected_node_count = config.topology.compute_node_count();
    let needs_node_addresses = config
        .nodes
        .iter()
        .any(|spec| spec.operating_mode == OperatingMode::Max);
    let mut connected_nodes = HashSet::new();
    let mut locally_ready_nodes = HashSet::new();

    loop {
        let event = match event_receiver.recv().await {
            Ok(event) => event,
            Err(broadcast::error::RecvError::Lagged(skipped)) => {
                warn!(
                    "New node coordinator lagged; skipped {} events. Continuing.",
                    skipped
                );
                continue;
            }
            Err(broadcast::error::RecvError::Closed) => break,
        };

        match event {
            TopologyEvent::NodeConnected(event) => {
                if !connected_nodes.insert(event.node_id) {
                    continue;
                }

                if start_time.is_none() {
                    start_time = Some(Instant::now());
                    info!("The first node has connected. Starting the timer.");
                }

                if let Some(expected_node_count) = expected_node_count {
                    if connected_nodes.len() == expected_node_count && !config_dispatched {
                        config_dispatched = true;

                        // waits for all links to be established
                        tokio::time::sleep(Duration::from_secs(1)).await;

                        info!(
                            "All {} nodes are now connected. Sending node addresses, link rates and flows to all nodes.",
                            expected_node_count
                        );

                        if needs_node_addresses {
                            // updates remote node addresses for the connector
                            send_node_addresses(config.clone(), node_ws.clone(), db_pool.clone())
                                .await;
                        } else {
                            info!(
                                "Skipping AddNodeAddress broadcast (no Max-mode nodes configured)."
                            );
                        }

                        // waits for all nodes to receive the AddNode messages
                        tokio::time::sleep(Duration::from_millis(100)).await;
                        send_link_rates(config.clone(), node_ws.clone()).await;

                        // waits for all link rates to be set before sending the flows
                        tokio::time::sleep(Duration::from_millis(100)).await;
                        send_flows(node_ws.clone(), db_pool.clone(), config.flow_transport).await;

                        let duration_secs = match start_time {
                            Some(t0) => t0.elapsed().as_secs_f32(),
                            None => 0.0,
                        };
                        info!(
                            "All dataplane nodes have connected. It takes {:.2} seconds since the first node arrived.",
                            duration_secs
                        );
                    } else if !config_dispatched {
                        info!(
                            "Node {} connected. At time of insertion, {} nodes were connected (including this one), out of {} expected.",
                            event.node_id, event.connected_node_count, expected_node_count
                        );
                    }
                }
            }
            TopologyEvent::NodeLocallyReady { node_id } => {
                if locally_ready_nodes.insert(node_id) {
                    info!(
                        "Dataplane node {} reports its local topology is ready.",
                        node_id
                    );
                }
            }
        }

        maybe_broadcast_topology_ready(
            expected_node_count,
            &connected_nodes,
            &locally_ready_nodes,
            &mut topology_ready_sent,
            node_ws.clone(),
        )
        .await;
    }
}

async fn maybe_broadcast_topology_ready(
    expected_node_count: Option<usize>,
    connected_nodes: &HashSet<usize>,
    locally_ready_nodes: &HashSet<usize>,
    topology_ready_sent: &mut bool,
    node_ws: NodeWriterMap,
) {
    let Some(expected_node_count) = expected_node_count else {
        return;
    };

    if *topology_ready_sent {
        return;
    }

    if connected_nodes.len() == expected_node_count
        && locally_ready_nodes.len() == expected_node_count
    {
        *topology_ready_sent = true;

        info!(
            "All dataplane nodes have finished wiring their topologies. Broadcasting topology-ready signal."
        );

        send_topology_ready(node_ws).await;
    }
}

async fn send_flows(
    node_ws: NodeWriterMap,
    db_pool: Arc<Pool<Postgres>>,
    flow_transport: FlowTransport,
) {
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

    // Query all flow routes for route pinning
    let flow_routes: Vec<DbFlowRoute> = match sqlx::query_as("SELECT * FROM flow_routes")
        .fetch_all(&*db_pool)
        .await
    {
        Ok(routes) => routes,
        Err(e) => {
            error!("Failed to fetch flow routes from database: {}.", e);
            Vec::new()
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
                "Adding {} controller flows ({:?}) to node {}.",
                flows.len(),
                flow_transport,
                node_id
            );

            let msg = build_flows_for_node(flows, &flow_routes, flow_transport);

            match writer
                .lock()
                .await
                .send(Message::binary(match rmp_serde::to_vec(&msg) {
                    Ok(b) => b,
                    Err(e) => {
                        error!("Failed to encode controller message: {}", e);
                        continue;
                    }
                }))
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
```

### Step 5: live synchronization path.

Database listeners emit `DbEvent`s and `spawn_db_sync` translates those into incremental websocket pushes:

- route table recomputation -> `InstallRoutes`
- new flow insert -> `AddFlows`
- multicast membership/tree change -> `InstallGroupRoutes`

```bash
LC_ALL=C LANG=C sed -n '1,240p' controller/src/db_sync.rs
```

```rust
use std::collections::HashSet;
use std::sync::Arc;

use futures_util::SinkExt;
use sqlx::{Pool, Postgres};
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;
use tracing::{error, info, warn};

use nextmini_messages::{ControllerToDataplane, FlowTransport};

use crate::db::{DbEvent, RecomputedGroupRoutes};
use crate::models::{DbFlow, DbFlowRoute, DbRoute, Route};
use crate::utils::{
    build_flows_for_node, build_group_routes_for_node_multitree, build_routes_for_node,
};
use crate::{NodeWriterMap, WebSocketWriter};

pub fn spawn_db_sync(
    db_pool: Arc<Pool<Postgres>>,
    node_ws: NodeWriterMap,
    mut receiver: mpsc::Receiver<DbEvent>,
    flow_transport: FlowTransport,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        while let Some(event) = receiver.recv().await {
            match event {
                DbEvent::RoutesChanged => {
                    if let Err(e) = sync_routes(&db_pool, &node_ws).await {
                        error!("Failed to sync routes: {}", e);
                    }
                }
                DbEvent::FlowInserted { flow_id } => {
                    if let Err(e) = sync_flow(&db_pool, &node_ws, flow_transport, flow_id).await {
                        error!("Failed to sync flow {}: {}", flow_id, e);
                    }
                }
                DbEvent::GroupRoutesSync {
                    group_id,
                    prior_member_node_id,
                } => {
                    if let Err(e) =
                        sync_group_routes(&db_pool, &node_ws, group_id, prior_member_node_id).await
                    {
                        error!("Failed to sync group routes for group {}: {}", group_id, e);
                    }
                }
            }
        }
        warn!("DB sync task exiting: event channel closed.");
    })
}

async fn sync_routes(db_pool: &Pool<Postgres>, node_ws: &NodeWriterMap) -> anyhow::Result<()> {
    info!("Syncing routes into dataplane connections.");

    let routes_db: Vec<DbRoute> = sqlx::query_as::<_, DbRoute>("SELECT * FROM routes")
        .fetch_all(db_pool)
        .await?;

    let routes: Vec<Route> = routes_db
        .iter()
        .map(|r| {
            let edges_i32: Vec<(i32, i32)> =
                serde_json::from_value(r.edges.clone()).unwrap_or_default();
            let edges: Vec<(u32, u32)> = edges_i32
                .into_iter()
                .map(|(a, b)| (a as u32, b as u32))
                .collect();

            Route {
                route_id: r.route_id as usize,
                src_node_id: r.src_node_id as u32,
                dst_node_id: r.dst_node_id as u32,
                edges,
            }
        })
        .collect();

    let send_targets: Vec<(usize, Arc<tokio::sync::Mutex<WebSocketWriter>>)> = {
        let guard = node_ws.read().await;
        guard
            .iter()
            .map(|(node_id, writer)| (*node_id, Arc::clone(writer)))
            .collect()
    };

    for (node_id, writer) in send_targets {
        if let Some(msg) = build_routes_for_node(routes.clone(), node_id as u32) {
            let msg_binary = rmp_serde::to_vec(&msg)?;
            if let Err(e) = writer.lock().await.send(Message::binary(msg_binary)).await {
                error!("Failed to install routes on node {}: {}", node_id, e);
            } else {
                info!("Installed routes on node {}.", node_id);
            }
        } else {
            warn!("No routes to install for node {}.", node_id);
        }
    }

    Ok(())
}

async fn sync_flow(
    db_pool: &Pool<Postgres>,
    node_ws: &NodeWriterMap,
    flow_transport: FlowTransport,
    flow_id: i32,
) -> anyhow::Result<()> {
    let flow = sqlx::query_as::<_, DbFlow>("SELECT * FROM flows WHERE id = $1")
        .bind(flow_id)
        .fetch_one(db_pool)
        .await?;

    // Query for any route pinning for this flow
    let flow_routes: Vec<DbFlowRoute> =
        sqlx::query_as::<_, DbFlowRoute>("SELECT * FROM flow_routes WHERE flow_id = $1")
            .bind(flow_id)
            .fetch_all(db_pool)
            .await?;

    let msg = build_flows_for_node(vec![flow.clone()], &flow_routes, flow_transport);
    let msg_binary = rmp_serde::to_vec(&msg)?;

    let src_node_id = flow.src_node_id as usize;
    let dst_node_id = flow.dst_node_id as usize;

    send_to_node(node_ws, src_node_id, &msg_binary, flow_id, "source").await;
    send_to_node(node_ws, dst_node_id, &msg_binary, flow_id, "destination").await;
    Ok(())
}

async fn sync_group_routes(
    db_pool: &Pool<Postgres>,
    node_ws: &NodeWriterMap,
    group_id: i32,
    prior_member_node_id: Option<u32>,
) -> anyhow::Result<()> {
    let Some(plan) = crate::db::recompute_group_routes(group_id, db_pool).await? else {
        return Ok(());
    };

    let nodes_to_notify = multicast_nodes_to_notify(&plan, prior_member_node_id);
    if nodes_to_notify.is_empty() {
        return Ok(());
    }

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
            "No active websocket connections available for multicast group {} update.",
            group_id
        );
        return Ok(());
    }

    for (node_id, writer) in send_targets {
        let routes = match build_group_routes_for_node_multitree(
            plan.group.id as usize,
            plan.group.src_node_id as u32,
            &plan.trees,
            node_id,
            &plan.member_node_set,
        ) {
            Ok(routes) => routes,
            Err(e) => {
                error!(
                    "Failed to build multi-tree InstallGroupRoutes payload for group {} node {}: {}",
                    plan.group.id, node_id, e
                );
                continue;
            }
        };
        let message = ControllerToDataplane::InstallGroupRoutes {
            group_id: plan.group.id as usize,
            src_node_id: plan.group.src_node_id as usize,
            routes,
        };
        let payload = rmp_serde::to_vec(&message)?;
        if let Err(e) = writer.lock().await.send(Message::binary(payload)).await {
            error!(
                "Failed to send InstallGroupRoutes for group {} to node {}: {}",
                plan.group.id, node_id, e
            );
        } else {
            info!(
                "Pushed InstallGroupRoutes for group {} to node {}.",
                plan.group.id, node_id
            );
        }
    }

    Ok(())
}

fn multicast_nodes_to_notify(
    plan: &RecomputedGroupRoutes,
    prior_member_node_id: Option<u32>,
) -> HashSet<u32> {
    let mut nodes: HashSet<u32> = plan
        .trees
        .iter()
        .flat_map(|tree| tree.edges.iter().flat_map(|(a, b)| [*a, *b]))
        .collect();
    nodes.extend(plan.dag_nodes.iter().copied());
    nodes.insert(plan.group.src_node_id as u32);
    nodes.extend(plan.member_node_ids.iter().copied());
    if let Some(node_id) = prior_member_node_id {
        nodes.insert(node_id);
    }
    nodes
}

async fn send_to_node(
    node_ws: &NodeWriterMap,
    node_id: usize,
    msg_binary: &[u8],
    flow_id: i32,
    label: &str,
) {
    let ws_arc_opt = { node_ws.read().await.get(&node_id).cloned() };
    let Some(ws_arc) = ws_arc_opt else {
        return;
    };

    match ws_arc
        .lock()
        .await
        .send(Message::binary(msg_binary.to_vec()))
```

Controller route/message construction lives in `controller/src/utils.rs`.

`build_flows_for_node`, `build_routes_for_node`, and multicast helpers convert DB/config shapes into transport-ready `ControllerToDataplane` messages.

```bash
LC_ALL=C LANG=C sed -n '1,180p' controller/src/utils.rs
```

```rust
/// Implements utility functions for the controller.
use std::collections::{HashMap, HashSet};
use std::net::Ipv4Addr;

use petgraph::Direction;
use petgraph::graph::DiGraph;
use tracing::{debug, info, warn};

use nextmini_messages::{
    ControllerToDataplane, Flow, FlowLen, FlowSpec, FlowTransport, GroupId, GroupRouteTree,
    GroupRoutingTableEntry, INVALID, MULTICAST_ROUTE_FLAG, MULTITREE_STRIDE, NodeSpec,
    OperatingMode, Protocol, RouteForwardingMode, RoutingTableEntry, SchedulingDiscipline,
};

use crate::config;
use crate::models::{DbFlow, DbFlowRoute, Route};
use crate::routing;
use crate::routing::RoutingProtocol;
use crate::topology::topo;

/// Describes a directed path between two nodes as a list of edges.
pub type RoutePath = Vec<(u32, u32)>;
/// Aggregates route metadata: source node, destination node, and the path edges.
pub type RouteDescriptor = (u32, u32, RoutePath);
/// Collection of route descriptors.
pub type RouteCollection = Vec<RouteDescriptor>;

/// Bundles the parameters required to build a startup message for the dataplane.
#[derive(Clone, Debug)]
pub struct StartupResponseParams {
    pub node_id: usize,
    pub net_mask: Ipv4Addr,
    pub virtual_base_addr: Ipv4Addr,
    pub user_space_base_addr: Ipv4Addr,
    pub external_base_addr: Ipv4Addr,
    pub max_server_port: u16,
    pub protocol: Protocol,
    pub scheduler_type: SchedulingDiscipline,
    pub node_spec: Option<NodeSpec>,
}

/// Builds a startup message for the dataplane, which includes basic information about the node.
pub fn build_startup_response(params: StartupResponseParams) -> ControllerToDataplane {
    let StartupResponseParams {
        node_id,
        net_mask,
        virtual_base_addr,
        user_space_base_addr,
        external_base_addr,
        max_server_port,
        protocol,
        scheduler_type,
        node_spec,
    } = params;

    // Set default node specification if None
    let node_spec = node_spec.unwrap_or(NodeSpec {
        node_id,
        operating_mode: OperatingMode::Normal,
    });

    // Building the startup message.
    ControllerToDataplane::StartUp {
        node_id,
        net_mask,
        virtual_base_addr,
        user_space_base_addr,
        external_base_addr,
        max_server_port,
        protocol,
        scheduler_type,
        node_spec,
    }
}

/// Builds an AddFlow message for flows.
pub fn build_flows_for_node(
    flows: Vec<DbFlow>,
    flow_routes: &[DbFlowRoute],
    transport: FlowTransport,
) -> ControllerToDataplane {
    // Build a lookup map from flow_id to route_id
    let route_map: HashMap<i32, i32> = flow_routes
        .iter()
        .map(|fr| (fr.flow_id, fr.route_id))
        .collect();

    let mut built = Vec::new();

    for flow in flows {
        debug!("Building an AddFlow message for flow id {}", flow.id);

        let flow_len = match flow.flow_len_type.as_str() {
            "bytes" => FlowLen::Bytes(flow.flow_len_bytes.unwrap_or(0) as usize),
            "duration" => FlowLen::Duration(flow.flow_len_duration.unwrap_or(0.0)),
            other => {
                warn!(
                    "Flow {} has unsupported flow_len_type {}; skipping.",
                    flow.id, other
                );
                continue;
            }
        };

        let flow_spec = FlowSpec {
            flow_len,
            flow_rate: flow.flow_rate.map(|r| r as usize),
            flow_weight: flow.flow_weight.map(|w| w as usize),
            transport,
        };

        if let Err(err) = flow_spec.validate() {
            warn!(
                "Skipping flow {} ({} -> {}): {}.",
                flow.id, flow.src_node_id, flow.dst_node_id, err
            );
            continue;
        }

        // Look up route_id from flow_routes table
        let route_id = route_map.get(&flow.id).map(|&r| r as usize);

        built.push(Flow {
            controller_id: Some(flow.id),
            src_node_id: flow.src_node_id as usize,
            dst_node_id: flow.dst_node_id as usize,
            route_id,
            flow_spec,
        });
    }

    ControllerToDataplane::AddFlows { flows: built }
}

/// Creates a DiGraph with proper node mapping from edges, preserving the relationship
/// between original node IDs and internal graph indices.
/// Returns (node_ids_vec, node_map, graph) where node_ids_vec[idx.index()] gives the original node_id.
fn create_graph_with_mapping(
    edges: &[(u32, u32)],
) -> (
    Vec<u32>,
    HashMap<u32, petgraph::graph::NodeIndex>,
    DiGraph<u32, ()>,
) {
    // Collect all unique node IDs and sort them
    let mut all_nodes = HashSet::new();
    for &(a, b) in edges {
        all_nodes.insert(a);
        all_nodes.insert(b);
    }
    let mut node_ids: Vec<u32> = all_nodes.into_iter().collect();
    node_ids.sort();

    // Create graph manually to ensure NodeIndex order matches our sorted node_ids
    let mut graph = DiGraph::<u32, ()>::new();
    let mut node_map = HashMap::new();

    // Add nodes in sorted order
    for &node_id in &node_ids {
        let node_idx = graph.add_node(node_id);
        node_map.insert(node_id, node_idx);
    }

    // Add edges
    for &(a, b) in edges {
        let a_idx = node_map[&a];
        let b_idx = node_map[&b];
        graph.add_edge(a_idx, b_idx, ());
    }

    (node_ids, node_map, graph)
}

fn route_has_multiple_destinations(route: &Route) -> bool {
    if route.edges.is_empty() {
        return false;
    }

    let (_node_ids, node_map, graph) = create_graph_with_mapping(&route.edges);
    let Some(&src_idx) = node_map.get(&route.src_node_id) else {
```

```bash
LC_ALL=C LANG=C sed -n '377,620p' controller/src/utils.rs
```

```rust
pub fn build_group_routes_for_node_multitree(
    group_id: GroupId,
    src_node_id: u32,
    trees: &[GroupRouteTree],
    node_id: u32,
    member_node_ids: &HashSet<u32>,
) -> Result<Vec<GroupRoutingTableEntry>, String> {
    let trees = canonicalize_group_route_trees(group_id, trees)?;
    let mut routes = Vec::with_capacity(trees.len());

    for tree in &trees {
        let route_id = compute_multitree_route_id(group_id, tree.tree_id)?;
        if let Some(route) = build_group_routes_for_node_with_route_id(
            group_id,
            route_id,
            src_node_id,
            &tree.edges,
            node_id,
            member_node_ids,
        ) {
            routes.push(route);
        }
    }

    Ok(routes)
}

/// Build per-node multicast routing entries including local delivery for members.
#[allow(dead_code)]
pub fn build_group_routes_for_node(
    group_id: GroupId,
    src_node_id: u32,
    dag_edges: &[(u32, u32)],
    node_id: u32,
    member_node_ids: &HashSet<u32>,
) -> Option<GroupRoutingTableEntry> {
    let route_id = match compute_multitree_route_id(group_id, 0) {
        Ok(route_id) => route_id,
        Err(e) => {
            warn!(
                "Skipping legacy multicast route for group {} due to invalid route-id: {}",
                group_id, e
            );
            return None;
        }
    };

    build_group_routes_for_node_with_route_id(
        group_id,
        route_id,
        src_node_id,
        dag_edges,
        node_id,
        member_node_ids,
    )
}

/// Merge all routes from configuration (both custom and topology-generated).
pub fn merge_all_routes(config: &config::Config) -> RouteCollection {
    let mut routes: RouteCollection = Vec::new();

    // adds custom routes from config
    for route in &config.routes {
        if !route.route.is_empty() {
            let graph = DiGraph::<u32, ()>::from_edges(&route.route);

            // finds nodes with no outgoing edges but with incoming edges (destinations)
            let dst_nodes: Vec<usize> = graph
                .node_indices()
                .filter(|&node_idx| {
                    graph
                        .neighbors_directed(node_idx, petgraph::Outgoing)
                        .count()
                        == 0
                        && graph
                            .neighbors_directed(node_idx, petgraph::Incoming)
                            .count()
                            != 0
                })
                .map(|node_idx| node_idx.index())
                .collect();

            // finds nodes with no incoming edges but with outgoing edges (sources)
            let src_nodes: Vec<usize> = graph
                .node_indices()
                .filter(|&node_idx| {
                    graph
                        .neighbors_directed(node_idx, petgraph::Incoming)
                        .count()
                        == 0
                        && graph
                            .neighbors_directed(node_idx, petgraph::Outgoing)
                            .count()
                            != 0
                })
                .map(|node_idx| node_idx.index())
                .collect();

            if let (Some(&src_idx), Some(&dst_idx)) = (src_nodes.first(), dst_nodes.first()) {
                routes.push((src_idx as u32, dst_idx as u32, route.route.clone()));
            }
        }
    }

    // adds topology routes after implementing (shortest path) routing protocol
    // obtains all the edges from preset topology and custom edges
    if let Some(edges) = topo::build_topology(config) {
        // builds routes from all topology edges using the specified routing protocol
        let topology_routes = build_routes_from_topology(&edges, &config.routing.protocol);

        for (src_node_id, dst_node_id, route_edges) in topology_routes {
            if !route_edges.is_empty() {
                routes.push((src_node_id, dst_node_id, route_edges));
            }
        }
    }

    routes
}

/// Builds route-level next-hop information for a specific node.
pub fn build_routes_for_node(routes: Vec<Route>, node_id: u32) -> Option<ControllerToDataplane> {
    let mut route_entries: Vec<RoutingTableEntry> = Vec::new();

    info!(
        "Computing next hops for all routes going through node {}.",
        node_id
    );

    for route in &routes {
        let forward_mode = if route_has_multiple_destinations(route) {
            RouteForwardingMode::Multicast
        } else {
            RouteForwardingMode::Unicast
        };
        // finds next hops for the current node using PetGraph
        let mut next_hops: Vec<usize> = Vec::new();

        // creates proper node mapping like in build_routes_from_topology
        let (_node_ids, node_map, graph) = create_graph_with_mapping(&route.edges);

        if let Some(&node_idx) = node_map.get(&node_id) {
            for neighbor_idx in graph.neighbors_directed(node_idx, Direction::Outgoing) {
                let neighbor_node_id = graph[neighbor_idx];
                let hop = neighbor_node_id as usize;

                if !next_hops.contains(&hop) {
                    next_hops.push(hop);
                }
            }
        }

        if next_hops.is_empty() {
            // determines whether this node should perform local delivery by checking
            // if it's a leaf node (sink) in the route's edge graph.
            let (_node_ids, node_map, graph) = create_graph_with_mapping(&route.edges);

            if let Some(&node_idx) = node_map.get(&node_id) {
                let outgoing_count = graph
                    .neighbors_directed(node_idx, Direction::Outgoing)
                    .count();
                let incoming_count = graph
                    .neighbors_directed(node_idx, Direction::Incoming)
                    .count();

                // sinks in the route graph should deliver packets locally
                if outgoing_count == 0 && incoming_count > 0 {
                    next_hops = vec![node_id as usize];
                } else {
                    next_hops = vec![INVALID];
                }
            } else {
                // this node does not belong to this route; mark INVALID so we can skip
                next_hops = vec![INVALID];
            }
        }

        if next_hops.len() == 1 && next_hops[0] == INVALID {
            debug!(
                "Skipping route {} for node {} because no valid next hops were found.",
                route.route_id, node_id
            );
            continue;
        }

        route_entries.push(RoutingTableEntry {
            route_id: route.route_id,
            next_hops,
            src_node_id: route.src_node_id as usize,
            dst_node_id: route.dst_node_id as usize,
            forward_mode,
        });
    }

    if route_entries.is_empty() {
        None
    } else {
        info!(
            "Finished building routes for node {}. Total routing table entries: {}.",
            node_id,
            route_entries.len()
        );

        // logs each routing table entry for debugging
        for e in &route_entries {
            debug!(
                "RoutingTableEntry node {}: route_id={} src={} dst={} next_hops={:?} mode={:?}",
                node_id, e.route_id, e.src_node_id, e.dst_node_id, e.next_hops, e.forward_mode
            );
        }

        Some(ControllerToDataplane::InstallRoutes {
            routes: route_entries,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config;
    use crate::models::{DbFlow, DbFlowRoute, Route};
    use nextmini_messages::{
        FlowLen, FlowTransport, GroupRouteTree, MULTICAST_ROUTE_FLAG, NodeSpec, OperatingMode,
        Protocol,
    };
    use std::collections::HashSet;
    use std::net::Ipv4Addr;

    fn expect_install_routes(result: Option<ControllerToDataplane>) -> Vec<RoutingTableEntry> {
        match result {
            Some(ControllerToDataplane::InstallRoutes { routes }) => routes,
            _ => panic!("Expected InstallRoutes message."),
        }
    }

    #[test]
    fn test_build_startup_response_defaults_node_spec() {
        let params = StartupResponseParams {
            node_id: 3,
            net_mask: Ipv4Addr::new(255, 255, 0, 0),
            virtual_base_addr: Ipv4Addr::new(10, 0, 0, 0),
            user_space_base_addr: Ipv4Addr::new(192, 168, 0, 0),
            external_base_addr: Ipv4Addr::new(172, 16, 0, 0),
```

At this point the controller side is fully covered: startup, per-node onboarding, global readiness coordination, and continuous DB-driven reconciliation.

### Step 6 moves to dataplane startup.

`dataplane/src/main.rs` decides single-node vs multi-namespace deployment, creates Tokio runtime, starts `Conductor`, and coordinates graceful shutdown with `TaskTracker`.

```bash
sed -n '1,140p' dataplane/src/main.rs
```

```rust
/// The main entry point for the Nextmini dataplane node, with support for:
///
/// Single-node deployment: runs a single dataplane node, typically within a Docker container.
/// Multiple-node deployment: deploys multiple dataplane nodes in isolated network namespaces.
mod node;
mod tests;

use std::error::Error;

use tokio::runtime;
use tokio::signal;
use tokio_util::task::task_tracker::TaskTracker;
use tracing::info;

use node::conductor::Conductor;
use node::config::LocalConfig;
#[cfg(target_os = "linux")]
use node::namespace::manager::NamespaceManager;

fn main() -> Result<(), Box<dyn Error>> {
    tracing_subscriber::fmt::init();

    let config = LocalConfig::new();

    // checks if we should run in virtual network namespaces on the same machine
    if config.n_nodes > 1 {
        #[cfg(target_os = "linux")]
        {
            info!(
                "Started deploying {} dataplane nodes in isolated network namespaces.",
                config.n_nodes
            );
            deploy_multiple(config);
            return Ok(());
        }

        #[cfg(not(target_os = "linux"))]
        {
            return Err(
                "Running multiple dataplane nodes with network namespaces requires Linux.".into(),
            );
        }
    }

    info!("Started deploying a single dataplane node.");
    let rt = runtime::Runtime::new().expect("Failed to create the Tokio runtime.");

    rt.block_on(deploy(config));

    Ok(())
}

// Deploys multiple dataplane nodes, each in its isolated network namespace.
#[cfg(target_os = "linux")]
fn deploy_multiple(config: LocalConfig) {
    let mut manager = NamespaceManager::new(config);

    manager.spawn_all_nodes();
}

// Deploys a single dataplane node.
//
// We use TaskTracker in Tokio (https://tokio.rs/tokio/topics/shutdown) to manage graceful
// shutdowns, similar to fork/join data parallelism or a structured concurrency model.
//
// Reference:
// https://vorpus.org/blog/notes-on-structured-concurrency-or-go-statement-considered-harmful/
async fn deploy(config: LocalConfig) {
    let tracker = TaskTracker::new();

    // spawns the Conductor task with the receiver
    tracker.spawn(async move {
        let conductor = Conductor::new(config).await;
        conductor.run().await;
    });

    tracker.close();

    tokio::select! {
        _ = tracker.wait() => {
            info!("Nextmini finished normally.");
        },
        _ = signal::ctrl_c() => {
            info!("Received Ctrl + C. Shutting down Nextmini gracefully...");
            tracker.wait().await;
        },
    }
}
```

`Conductor` is the orchestrator that assembles dataplane subsystems.

Construction path (`Conductor::new`):
- build `ProcessorHandle`
- establish controller interface handle
- build flow-stats reporter handle
- build local interface handle

Run path (`Conductor::start`):
- launch selected network servers (`tcp`/`udp`/`quic`)
- in Max mode, optionally launch dedicated max server listeners
- keep actor handles alive while tasks run

```bash
sed -n '1,220p' dataplane/src/node/conductor.rs
```

```rust
/// The conductor actor is a 'mastermind' who is reponsible for overseeing the entire operation of
/// the dataplane node, including the controller interface actor, the processors actor, and the local
/// interface actor. Optionally, from the Python interface, it also manages the controller interface actor
/// and the lossless session manager actor.
use tracing::info;

use nextmini_messages::Protocol;

use crate::node::config::LocalConfig;
use crate::node::controller::interface::ControllerInterfaceHandle;
use crate::node::controller::reporter::ControllerReporterHandle;
use crate::node::local::interface::LocalInterfaceHandle;
use crate::node::network::quic::QuicServer;
use crate::node::network::tcp::TcpServer;
use crate::node::network::tcp_max::TcpMaxServer;
use crate::node::network::udp::UdpServer;
use crate::node::processor::ProcessorHandle;
#[cfg(feature = "python-extension")]
use crate::node::session::api::LosslessRuntimeHandle;

pub struct Conductor {
    config: LocalConfig,

    /// the local interface readers and writers
    local_interface: LocalInterfaceHandle,

    /// the processors
    processors: ProcessorHandle,

    /// the reporter that allows the dataplane node to communicate with the controller
    reporter: ControllerReporterHandle,

    /// the controller interface for sending messages upstream to the controller
    #[cfg(feature = "python-extension")]
    controller: ControllerInterfaceHandle,

    /// the lossless session runtime
    #[cfg(feature = "python-extension")]
    lossless_runtime: LosslessRuntimeHandle,
}

impl Conductor {
    pub async fn new(config: LocalConfig) -> Self {
        // connects the processors with its downstream local interface writers to send packets out
        let (controller_interface, lossless_runtime, reporter, flowstats_reporter) =
            ControllerInterfaceHandle::new(config.clone()).await;

        #[cfg(not(feature = "python-extension"))]
        let _ = lossless_runtime;
        let config = controller_interface.config.clone();
        let processors = controller_interface.processors.clone();

        let local_interface: LocalInterfaceHandle =
            LocalInterfaceHandle::new(config.clone(), processors.clone(), flowstats_reporter);
        processors.connect_local_interface(local_interface.clone());

        Conductor {
            config,
            local_interface,
            processors,
            reporter,
            #[cfg(feature = "python-extension")]
            controller: controller_interface,
            #[cfg(feature = "python-extension")]
            lossless_runtime,
        }
    }

    pub async fn run(&self) {
        self.start().await;

        // At this point, the conductor actor has finished normally
        info!("Nextmini is shutting down...");

        // handle the shutdown logic
        self.local_interface.shutdown().await;
    }

    /// Starts the server and, if needed, listens for incoming connections.
    pub async fn start(&self) {
        info!(
            "Starting Nextmini node {} on {}:{}...",
            self.config.node_id, self.config.private_network_addr, self.config.private_network_port
        );

        // starts listening with either TCP or QUIC on published ports (private and/or public)
        let public_port = self.config.public_network_port.clone();
        let private_port = self.config.private_network_port.clone();
        let max_server_port = self.config.max_server_port;

        match self.config.protocol {
            Protocol::Tcp => {
                // uses TcpMaxServer to handle the connections for max operating mode
                let mut tcp_max_server = TcpMaxServer::new(self.processors.clone());

                // uses TcpServer to handle the connections for normal operating mode
                if public_port == private_port {
                    let mut tcp_server = TcpServer::new(
                        self.config.clone(),
                        self.processors.clone(),
                        self.reporter.clone(),
                    );

                    let tcp_max_server_addr = format!("{}:{}", "0.0.0.0", max_server_port);
                    let tcp_server_addr = format!("{}:{}", "0.0.0.0", public_port);

                    tokio::select! {
                        _ = tcp_max_server.start_listening(&tcp_max_server_addr) => {},
                        _ = tcp_server.start_listening(&tcp_server_addr) => {},
                    }
                } else {
                    let mut tcp_server_public = TcpServer::new(
                        self.config.clone(),
                        self.processors.clone(),
                        self.reporter.clone(),
                    );
                    let mut tcp_server_private = TcpServer::new(
                        self.config.clone(),
                        self.processors.clone(),
                        self.reporter.clone(),
                    );

                    let tcp_server_public_addr = format!("{}:{}", "0.0.0.0", public_port);
                    let tcp_server_private_addr = format!("{}:{}", "0.0.0.0", private_port);
                    let tcp_max_server_addr = format!("{}:{}", "0.0.0.0", max_server_port);

                    tokio::select! {
                                    _ = tcp_server_public.start_listening(&tcp_server_public_addr) => {},
                                    _ = tcp_server_private.start_listening(&tcp_server_private_addr) => {},
                                    _ = tcp_max_server.start_listening(&tcp_max_server_addr) => {},
                    }
                }
            }
            Protocol::Quic => {
                if public_port == private_port {
                    let mut quic_server = QuicServer::new(
                        self.config.clone(),
                        self.processors.clone(),
                        self.reporter.clone(),
                    );
                    quic_server
                        .start_listening(&format!("{}:{}", "0.0.0.0", public_port))
                        .await;
                } else {
                    let mut quic_server_public = QuicServer::new(
                        self.config.clone(),
                        self.processors.clone(),
                        self.reporter.clone(),
                    );
                    let mut quic_server_private = QuicServer::new(
                        self.config.clone(),
                        self.processors.clone(),
                        self.reporter.clone(),
                    );

                    let public_addr = format!("{}:{}", "0.0.0.0", public_port);
                    let private_addr = format!("{}:{}", "0.0.0.0", private_port);

                    tokio::select! {
                        _ = quic_server_public.start_listening(&public_addr) => {},
                        _ = quic_server_private.start_listening(&private_addr) => {},
                    }
                }
            }
            Protocol::Udp => {
                if public_port == private_port {
                    let mut udp_server = UdpServer::new(
                        self.config.clone(),
                        self.processors.clone(),
                        self.reporter.clone(),
                    );

                    let udp_addr = format!("{}:{}", "0.0.0.0", public_port);
                    udp_server.start_listening(&udp_addr).await;
                } else {
                    let mut udp_server_public = UdpServer::new(
                        self.config.clone(),
                        self.processors.clone(),
                        self.reporter.clone(),
                    );
                    let mut udp_server_private = UdpServer::new(
                        self.config.clone(),
                        self.processors.clone(),
                        self.reporter.clone(),
                    );

                    let public_addr = format!("{}:{}", "0.0.0.0", public_port);
                    let private_addr = format!("{}:{}", "0.0.0.0", private_port);

                    tokio::select! {
                        _ = udp_server_public.start_listening(&public_addr) => {},
                        _ = udp_server_private.start_listening(&private_addr) => {},
                    }
                }
            }
        }
    }

    /// Returns a clone of the processor handle so external callers can attach additional interfaces.
    #[cfg(feature = "python-extension")]
    #[allow(dead_code)]
    pub fn processor_handle(&self) -> ProcessorHandle {
        // used by the optional `nextmini_py` extension to wire the in-process interface
        self.processors.clone()
    }

    /// Exposes the loaded `LocalConfig`, useful when bridging with language bindings.
    #[cfg(feature = "python-extension")]
    #[allow(dead_code)]
    pub fn local_config(&self) -> LocalConfig {
        // consumed by `nextmini_py` to mirror dataplane configuration inside Python
        self.config.clone()
    }

    /// Exposes a controller handle so bindings can emit DataplaneToController messages.
    #[cfg(feature = "python-extension")]
    #[allow(dead_code)]
    pub fn controller_handle(&self) -> ControllerInterfaceHandle {
        // consumed by `nextmini_py` to attach the Python interface and send messages upstream to the controller
        self.controller.clone()
```

### Step 7: the dataplane-side control receiver.

`ControllerInterfaceHandle::new` opens websocket to controller and spawns sender/receiver actors.
`process_control_msg` applies controller directives to live dataplane state:
- add peers (`AddNode`/`AddNodeAddress`)
- install routes
- set link rates
- register flows
- install group directory/routes
- flush deferred setup when `TopologyReady` arrives

```bash
sed -n '1,240p' dataplane/src/node/controller/interface.rs
```

```rust
use std::collections::HashMap;
use std::net::Ipv4Addr;
#[cfg(feature = "python-extension")]
use std::sync::Arc;

use futures::stream::{SplitSink, SplitStream};
use futures::{SinkExt, StreamExt};
use rand::Rng;
use tokio::net::TcpStream;
#[cfg(feature = "python-extension")]
use tokio::sync::Mutex;
use tokio::sync::mpsc;
use tokio::time::{Duration, interval, timeout};
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, connect_async, tungstenite::protocol::Message,
};
use tracing::{error, info, warn};

use nextmini_messages::{
    ControllerToDataplane, DataplaneToController, Flow, FlowTransport, GroupId,
};

use crate::node::config::LocalConfig;
use crate::node::controller::flowstats::FlowStatsReporterHandle;
use crate::node::controller::reporter::ControllerReporterHandle;
use crate::node::flow::client::UserSpaceClientHandle;
use crate::node::flow::server::UserSpaceServerHandle;
use crate::node::network::interface::NetworkInterfaceHandle;
use crate::node::network::tcp_max::TcpMaxClient;
use crate::node::processor::ProcessorHandle;
#[cfg(feature = "python-extension")]
use crate::node::python::interface::{PythonEvent, PythonInterfaceHandle};
use crate::node::scheduler::sched::SchedulerHandle;
use crate::node::session::api::LosslessRuntimeHandle;
use crate::node::controller::lossless_unicast::LosslessUnicastFlowManager;

#[derive(Clone)]
pub struct ControllerInterfaceHandle {
    pub config: LocalConfig,
    pub processors: ProcessorHandle,
    northbridge_sender: mpsc::UnboundedSender<DataplaneToController>,
    #[cfg(feature = "python-extension")]
    python_interface: Arc<Mutex<Option<PythonInterfaceHandle>>>,
}

/// The handle for the controller interface, which allows sending messages to the controller.
impl ControllerInterfaceHandle {
    pub async fn new(
        config: LocalConfig,
    ) -> (
        Self,
        LosslessRuntimeHandle,
        ControllerReporterHandle,
        FlowStatsReporterHandle,
    ) {
        // creates an unbounded channel, the 'northbridge', for sending messages to the controller
        let (northbridge_sender, northbridge_receiver) = mpsc::unbounded_channel();

        // connects to the controller over WebSockets
        let (config, processors, ws_stream) = ControllerInterfaceHandle::connect(config).await;

        let (sender_stream, receiver_stream) = ws_stream.split();

        // initializes the controller sender and receiver
        let mut controller_sender = DataplaneToControllerSender {
            sender_stream,
            northbridge_receiver,
        };

        #[cfg(feature = "python-extension")]
        let python_interface = Arc::new(Mutex::new(None));

        let controller_interface = Self {
            config: config.clone(),
            processors: processors.clone(),
            northbridge_sender,
            #[cfg(feature = "python-extension")]
            python_interface: python_interface.clone(),
        };

        let reporter = ControllerReporterHandle::new(
            controller_interface.clone(),
            config.metrics_collection_interval,
        );

        let flowstats_reporter =
            FlowStatsReporterHandle::new(controller_interface.clone(), config.clone());

        // passes flowstats reporter to routing table for automatic route assignment reporting
        processors
            .set_flowstats_reporter(flowstats_reporter.clone())
            .await;

        // adds flowstats reporter to report flow finish
        let user_space_client = UserSpaceClientHandle::new(
            config.clone(),
            processors.clone(),
            flowstats_reporter.clone(),
        );

        // creates the server handle for the processor to use
        let user_space_server = UserSpaceServerHandle::new(config.clone(), processors.clone());
        processors.connect_server(user_space_server.clone());

        // creates a TCP max client for the processor to use
        let tcp_max_client =
            TcpMaxClient::new(config.clone(), processors.clone(), reporter.clone());
        processors.connect_tcp_max_client(tcp_max_client).await;

        // creates the lossless runtime handle with the correct processors
        let lossless_runtime =
            LosslessRuntimeHandle::new(processors.clone(), config.lossless_runtime_config.clone());
        processors.connect_lossless_handle(lossless_runtime.clone());

        // creates the controller-facing lossless unicast flow manager with the correct processors
        let lossless_unicast = LosslessUnicastFlowManager::new(
            config.clone(),
            processors.clone(),
            flowstats_reporter.clone(),
            lossless_runtime.clone(),
        );

        let mut controller_receiver = ControllerToDataplaneReceiver {
            controller: controller_interface.clone(),
            config: config.clone(),
            receiver_stream,
            processors: processors.clone(),
            reporter: reporter.clone(),
            user_space_client,
            user_space_server,
            #[cfg(feature = "python-extension")]
            python_interface,
            lossless_runtime: lossless_runtime.clone(),
            group_ip_by_id: HashMap::new(),
            lossless_unicast,
            topology_ready: false,
            pending_tcp_flows: Vec::new(),
            pending_lossless_flows: Vec::new(),
            expected_neighbor_count: 0,
            connected_neighbor_count: 0,
            routes_installed: false,
            group_directory_installed: false,
            local_topology_ready_sent: false,
        };

        tokio::spawn(async move {
            controller_sender.run().await;
        });
        tokio::spawn(async move {
            controller_receiver.run().await;
        });

        (
            controller_interface,
            lossless_runtime,
            reporter,
            flowstats_reporter,
        )
    }

    pub async fn connect(
        mut config: LocalConfig,
    ) -> (
        LocalConfig,
        ProcessorHandle,
        WebSocketStream<MaybeTlsStream<TcpStream>>,
    ) {
        let url = url::Url::parse(&config.controller_addr).unwrap();
        let mut ws_stream: WebSocketStream<MaybeTlsStream<TcpStream>>;
        let connect_timeout = Duration::from_millis(config.controller_connect_timeout_ms.max(1));

        loop {
            let connect_fut = connect_async(url.as_str());

            match timeout(connect_timeout, connect_fut).await {
                Ok(Ok((ws, _))) => {
                    ws_stream = ws;
                    info!("WebSocket handshake has been successfully completed.");
                    break;
                }
                Ok(Err(e)) => {
                    // Connection attempt failed quickly (e.g., refused, handshake error)
                    warn!("Failed to connect to the controller: {}. Retrying...", e);
                }
                Err(_) => {
                    // Timed out
                    warn!(
                        "Timed out attempting to connect to the controller after {:?}. Retrying...",
                        connect_timeout
                    );
                }
            }

            // Linear backoff with small jitter (0 - 500ms)
            let jitter_ms = rand::rng().random_range(0..500);
            let backoff = Duration::from_secs(2) + Duration::from_millis(jitter_ms);

            tokio::time::sleep(backoff).await;
        }

        let startup_msg = DataplaneToController::StartUp {
            private_network_name: config.private_network_name.clone(),
            private_network_addr: config.private_network_addr.clone()
                + ":"
                + &config.private_network_port.clone(),
            public_network_addr: config.public_network_addr.clone()
                + ":"
                + &config.public_network_port.clone(),
            node_id: config.node_id.to_string().parse().ok(),
        };

        ws_stream
            .send(Message::binary(rmp_serde::to_vec(&startup_msg).unwrap()))
            .await
            .expect("Failed to send the startup message to the controller");

        // waits for the controller's response
        if let Some(response) = ws_stream.next().await {
            // updates the local configuration with settings from the controller
            config.update(response);
        } else {
            error!("No response has been received from controller.");
        }

        // starts the processor actor
        let processors = ProcessorHandle::new(config.clone());

        (config, processors, ws_stream)
    }

    /// Sends a message to the controller.
    pub async fn send(&self, msg: DataplaneToController) {
        if let Err(e) = self.northbridge_sender.send(msg) {
            error!(
                "Error sending messages to the controller interface actor: {}",
                e
            );
        };
    }

```

```bash
sed -n '330,520p' dataplane/src/node/controller/interface.rs
```

```rust
            match msg {
                Message::Binary(data) => {
                    let ctrl_msg: ControllerToDataplane =
                        rmp_serde::from_slice(&data).expect("Failed to parse control message");
                    self.process_control_msg(ctrl_msg).await;
                }
                Message::Pong(_) => {
                    // received a ping message to keep the connection alive. Do nothing.
                    continue;
                }
                _ => {
                    error!("Received a message that is not a binary or a ping message.");
                }
            };
        }
    }

    async fn process_control_msg(&mut self, msg: ControllerToDataplane) {
        match msg {
            ControllerToDataplane::AddNode {
                remote_node_id,
                remote_addr,
            } => {
                // creates a new persistent TCP connection to the remote node
                self.expected_neighbor_count += 1;

                let network_interface = NetworkInterfaceHandle::new_as_client(
                    self.config.clone(),
                    remote_node_id,
                    remote_addr.clone(),
                    self.processors.clone(),
                    self.reporter.clone(),
                )
                .await;

                let scheduler = SchedulerHandle::new(self.config.clone(), network_interface);

                let _ = self.processors.add_node(remote_node_id, scheduler);

                self.record_neighbor_connected(remote_node_id).await;
            }

            ControllerToDataplane::AddNodeAddress {
                remote_node_id,
                remote_max_server_addr,
            } => {
                self.processors
                    .add_node_address(remote_node_id, remote_max_server_addr)
                    .await;
            }

            ControllerToDataplane::SetLinkRate { node_id, spec } => {
                info!(
                    "The link rate from node {} to node {} is now set to {} bytes/second, with a bucket size of {} bytes.",
                    self.config.node_id, node_id, spec.rate, spec.bucket_size,
                );

                self.processors.limit_rate(node_id, spec);
            }

            ControllerToDataplane::InstallRoutes { routes } => {
                info!(
                    "Installing {} routes on node {}.",
                    routes.len(),
                    self.config.node_id
                );

                self.processors.update_routing_table(routes).await;
                self.routes_installed = true;
                self.maybe_send_local_topology_ready().await;
            }

            ControllerToDataplane::AddFlows { flows } => {
                let mut tcp_flows = Vec::new();
                let mut lossless_flows = Vec::new();

                for flow in &flows {
                    match flow.flow_spec.transport {
                        FlowTransport::Tcp => {
                            if flow.dst_node_id == self.config.node_id {
                                // this node is the server for this flow
                                self.user_space_server.store_flow_spec(flow.clone());
                            }
                            if flow.src_node_id == self.config.node_id {
                                // this node is the client for this flow
                                tcp_flows.push(flow.clone());
                            }
                        }
                        FlowTransport::LosslessUnicast => {
                            if flow.src_node_id == self.config.node_id
                                || flow.dst_node_id == self.config.node_id
                            {
                                lossless_flows.push(flow.clone());
                            }
                        }
                    }
                }

                if !tcp_flows.is_empty() {
                    if self.topology_ready {
                        self.start_tcp_flows(tcp_flows);
                    } else {
                        info!(
                            "Deferring {} user-space TCP flows on node {} until the topology is ready.",
                            tcp_flows.len(),
                            self.config.node_id
                        );
                        self.pending_tcp_flows.extend(tcp_flows.into_iter());
                    }
                }

                if !lossless_flows.is_empty() {
                    if self.topology_ready {
                        info!(
                            "Adding {} lossless unicast flows to node {}.",
                            lossless_flows.len(),
                            self.config.node_id
                        );
                        self.lossless_unicast.add_flows(lossless_flows);
                    } else {
                        info!(
                            "Deferring {} lossless unicast flows on node {} until the topology is ready.",
                            lossless_flows.len(),
                            self.config.node_id
                        );
                        self.pending_lossless_flows.extend(lossless_flows);
                    }
                }
            }

            ControllerToDataplane::TopologyReady => {
                info!(
                    "Controller signaled that all nodes are connected; topology state is ready on node {}.",
                    self.config.node_id
                );

                self.topology_ready = true;
                self.lossless_runtime.set_topology_ready(true);

                // Emit Python event so Python code can wait for topology ready
                #[cfg(feature = "python-extension")]
                if let Some(py_if) = self.python_handle().await {
                    py_if.publish_event(PythonEvent::TopologyReady).await;
                }

                self.flush_pending_flows();
            }

            ControllerToDataplane::GroupCreated {
                group_id,
                group_ip,
                src_node_id,
            } => {
                info!(
                    "Registered multicast group {} ({}) owned by node {}.",
                    group_id, group_ip, src_node_id
                );

                #[cfg(feature = "python-extension")]
                if let Some(py_if) = self.python_handle().await {
                    py_if
                        .publish_event(PythonEvent::GroupCreated {
                            group_id,
                            src_node_id,
                            group_ip,
                        })
                        .await;
                }
            }

            ControllerToDataplane::InstallGroupDirectory { groups } => {
                info!(
                    "Installing multicast group directory ({} entries) on node {}.",
                    groups.len(),
                    self.config.node_id
                );
                self.processors.update_group_directory(groups.clone()).await;
                self.group_ip_by_id.clear();
                for entry in &groups {
                    self.group_ip_by_id.insert(entry.group_id, entry.group_ip);
                }

                #[cfg(feature = "python-extension")]
                if let Some(py_if) = self.python_handle().await {
                    py_if
                        .publish_event(PythonEvent::GroupDirectoryUpdated { entries: groups })
                        .await;
                }

                self.group_directory_installed = true;
                self.maybe_send_local_topology_ready().await;
```

### Step 8: covers packet movement inside the dataplane.

`LocalInterfaceHandle` owns local TUN reader/writer actors.
`Processor` receives packets/control messages, looks up routes, and decides local delivery vs remote forwarding.
`SchedulerHandle` decouples processor from network send pacing/queueing and applies FIFO/WRR plus drop/backpressure policies.

```bash
sed -n '1,180p' dataplane/src/node/local/interface.rs
```

```rust
use std::net::Ipv4Addr;
use std::sync::Arc;

use tokio::sync::{broadcast, mpsc};
use tracing::{debug, error, info};
use tun_rs::{AsyncDevice, DeviceBuilder};

use crate::node::FlowIdExt;
use crate::node::config::LocalConfig;
use crate::node::controller::flowstats::FlowStatsReporterHandle;
use crate::node::packet::Packet;
use crate::node::processor::ProcessorHandle;

#[cfg(target_os = "linux")]
use crate::node::local::reader_tso::LocalReader;
#[cfg(target_os = "linux")]
use crate::node::local::writer_tso::LocalWriter;

#[cfg(not(target_os = "linux"))]
use crate::node::local::reader::LocalReader;
#[cfg(not(target_os = "linux"))]
use crate::node::local::writer::LocalWriter;

/// Message types for LocalInterface, which manages the LocalReader and LocalWriter actors.
#[derive(Clone)]
pub enum ShutdownMessage {
    Shutdown, // shuts down LocalInterface gracefully, stopping all LocalReader and LocalWriter actors
}

pub enum LocalInterfaceMessage {
    WritePacket(Packet), // the processor sends a packet to the application via the local interface
}

/// Handle for Processors to interact with LocalInterface.
#[derive(Clone, Debug)]
pub struct LocalInterfaceHandle {
    shutdown_sender: broadcast::Sender<ShutdownMessage>,
    write_senders: Vec<mpsc::Sender<LocalInterfaceMessage>>,
}

impl LocalInterfaceHandle {
    pub fn new(
        config: LocalConfig,
        processor: ProcessorHandle,
        flowstats_reporter: FlowStatsReporterHandle,
    ) -> Self {
        let (shutdown_sender, _) = broadcast::channel(config.channel_capacity);

        if !config.enable_local_interface {
            debug!(
                "Local interface disabled via configuration; skipping TUN interface initialization."
            );
            let _ = processor;
            let _ = flowstats_reporter;
            return Self {
                shutdown_sender,
                write_senders: Vec::new(),
            };
        }

        // creates local TUN devices. On Linux, this creates multiple queues for parallel processing,
        // each queue corresponding to its own device. On non-Linux platforms, it creates one device only.
        let tun_devices = Self::create_tun_devices(config.clone());

        let mut write_senders = Vec::with_capacity(tun_devices.len());

        for dev in tun_devices.iter() {
            // for each LocalWriter, creates its MPSC channel
            let (write_sender, write_receiver) = mpsc::channel(config.channel_capacity);
            write_senders.push(write_sender);

            let mut reader = LocalReader::new(
                dev.clone(),
                shutdown_sender.subscribe(),
                processor.clone(),
                flowstats_reporter.clone(),
            );

            tokio::spawn(async move {
                reader.run().await;
            });

            let mut writer = LocalWriter::new(
                config.clone(),
                dev.clone(),
                shutdown_sender.subscribe(),
                write_receiver,
            );

            tokio::spawn(async move {
                writer.run().await;
            });
        }

        Self {
            shutdown_sender,
            write_senders,
        }
    }

    pub fn write_packet(&self, packet: Packet) {
        if self.write_senders.is_empty() {
            debug!(
                "Local interface disabled; dropping packet with flow {}.",
                packet.flow_id
            );
            return;
        }

        let idx = packet.flow_id.hash(self.write_senders.len());
        let sender = &self.write_senders[idx];

        if let Err(e) = sender.try_send(LocalInterfaceMessage::WritePacket(packet)) {
            error!(
                "Error sending a packet to the local interface writer: {}. Dropped.",
                e
            );
        }
    }

    pub async fn shutdown(&self) {
        if let Err(e) = self.shutdown_sender.send(ShutdownMessage::Shutdown) {
            error!("Error shutting down all the actors: {}", e);
        };
    }

    /// Converts a netmask tuple to prefix length. Used in 'LocalInterfaceHandle::create_tun_device()'.
    fn mask_to_prefix(mask: Ipv4Addr) -> u8 {
        let mask_u32 = u32::from_be_bytes(mask.octets());
        mask_u32.count_ones() as u8
    }

    /// Creates local TUN devices for communicating with the application.
    #[cfg(not(target_os = "linux"))]
    pub fn create_tun_devices(config: LocalConfig) -> Vec<Arc<AsyncDevice>> {
        let ipv4_prefix = Self::mask_to_prefix(config.local_netmask);

        let dev = DeviceBuilder::new()
            .ipv4(config.local_address, ipv4_prefix, None)
            .mtu(config.mtu as u16)
            .build_async()
            .expect("Failed to create tun device");

        // creates a single TUN queue on non-Linux platforms without multi-queue support
        info!("Creating one TUN queue on non-Linux platforms without multi-queue support.");
        let queues = vec![Arc::new(dev)];

        queues
    }

    /// Creates local TUN devices for communicating with the application.
    #[cfg(target_os = "linux")]
    pub fn create_tun_devices(config: LocalConfig) -> Vec<Arc<AsyncDevice>> {
        let num_queues = config.num_tun_queues;

        let if_name = config.tun_interface_name.clone();
        let ipv4_prefix = Self::mask_to_prefix(config.local_netmask);

        let dev = DeviceBuilder::new()
            .name(&if_name)
            .ipv4(config.local_address, ipv4_prefix, None)
            .mtu(config.mtu as u16)
            .multi_queue(true) // enables multi-queue support
            .offload(true) // enables TSO support
            .build_async()
            .expect("Failed to create tun device");

        let mut queues = Vec::with_capacity(num_queues);

        // creates multiple TUN queues with error handling
        info!("Creating {num_queues} TUN queues.");
        for _ in 0..num_queues - 1 {
            match dev.try_clone() {
                Ok(cloned_dev) => {
                    queues.push(Arc::new(cloned_dev));
                }
                Err(e) => {
                    // if we are unable to create all the queues, use what we have
                    error!(
                        "Could not create all TUN queues ({}), continuing with {} queues",
```

```bash
sed -n '900,1160p' dataplane/src/node/processor.rs
```

```rust
            server: None,
            routing_table: RoutingTable::new(config.clone()),
            flowstats_reporter: None,
            schedulers: AHashMap::new(),
            config,
            #[cfg(feature = "python-extension")]
            python_interface: None,
            lossless_handle: None,
        }
    }

    async fn run(&mut self) {
        loop {
            tokio::select! {
                // waits for the first packet or a broadcast message
                Some(msg) = self.packet_receiver.recv() => {
                    match msg {
                        ProcessorPacket::ProcessPacket(first_packet) => {
                            // starts a batch with the first packet
                            self.process_packet(first_packet).await;

                            // starts processing packets in batches
                            while let Ok(ProcessorPacket::ProcessPacket(packet)) = self.packet_receiver.try_recv() {
                                self.process_packet(packet).await;
                            }
                        }
                    }
                }
                Ok(broadcast_msg) = self.broadcast_receiver.recv() => {
                    self.handle_message(broadcast_msg).await;
                }
            }
        }
    }

    async fn handle_message(&mut self, msg: ProcessorMessage) {
        match msg {
            ProcessorMessage::UpdateRoutingTable(routes) => {
                self.routing_table.install_routes(routes);
            }
            ProcessorMessage::UpdateGroupDirectory(groups) => {
                self.routing_table.install_group_directory(groups);
            }
            ProcessorMessage::UpdateGroupRoutes {
                group_id,
                src_node_id,
                routes,
            } => {
                self.routing_table
                    .install_group_routes(group_id, src_node_id, routes);
            }
            ProcessorMessage::AddNode(node_id, scheduler) => {
                self.schedulers.insert(node_id, scheduler);
            }
            ProcessorMessage::ConnectLocalInterface(local_interface) => {
                self.local_interface = Some(Arc::new(local_interface));
            }
            ProcessorMessage::ConnectUserSpaceSender { flow_id, sender } => {
                self.user_space_senders.insert(flow_id, sender);
            }
            ProcessorMessage::DisconnectUserSpaceSender(flow_id) => {
                self.user_space_senders.remove(&flow_id);
            }
            ProcessorMessage::RateLimit(node_id, spec) => {
                if let Some(scheduler) = self.schedulers.get(&node_id) {
                    scheduler.limit_rate(spec);
                }
            }
            ProcessorMessage::ConnectServerHandle(user_space_server) => {
                self.server = Some(*user_space_server);
            }
            ProcessorMessage::SetFlowWeight(flow_id, weight) => {
                // updates the flow weight for all schedulers
                for (_, scheduler) in self.schedulers.iter_mut() {
                    scheduler.set_flow_weight(flow_id, weight);
                }
            }
            ProcessorMessage::PinRouteForFlow(flow_id, route_id) => {
                if let Err(e) = self.routing_table.pin_route_for_flow(flow_id, route_id) {
                    warn!(
                        "Failed to pin route {} for flow {:032x}: {}",
                        route_id, flow_id, e
                    );
                }
            }
            ProcessorMessage::SetFlowStatsReporter(flowstats_reporter) => {
                self.flowstats_reporter = Some(*flowstats_reporter);
            }
            #[cfg(feature = "python-extension")]
            ProcessorMessage::ConnectPythonInterface(interface) => {
                self.python_interface = Some(interface);
            }
            ProcessorMessage::ConnectLosslessHandle(handle) => {
                self.lossless_handle = Some(handle);
            }
        }
    }

    /// Processes inbound packets for outbound delivery.
    async fn process_packet(&mut self, packet: Packet) {
        let packet_flow_id = packet.flow_id;
        let fec_tree_id = packet.lossless_fec_tree_id();

        let reporter = self.flowstats_reporter.as_ref();
        match self.routing_table.get_next_hops_by_flow_and_tree(
            packet_flow_id,
            fec_tree_id,
            reporter,
        ) {
            Ok(next_hops) => {
                if next_hops.is_empty() {
                    error!("No next hops available for flow {}.", packet_flow_id);
                    return;
                }

                let last = next_hops.len() - 1;
                let mut primary_packet = Some(packet);

                for (idx, next_hop_id) in next_hops.into_iter().enumerate() {
                    let pkt = if idx == last {
                        primary_packet
                            .take()
                            .expect("packet already dispatched to last hop")
                    } else {
                        primary_packet
                            .as_ref()
                            .expect("packet missing during multicast fan-out")
                            .clone()
                    };

                    self.send_packet(pkt, next_hop_id).await;
                }
            }
            Err(e) => {
                if let Some(tree_id) = fec_tree_id
                    && e.contains("Unknown multicast tree route")
                {
                    warn!(
                        flow_id = packet_flow_id,
                        tree_id,
                        reason = %e,
                        "Dropping packet because multicast tree route is unknown"
                    );
                    return;
                }
                error!("Error resolving route for flow {}: {}", packet_flow_id, e);
            }
        }
    }

    /// Locates a channel sender for delivering packets in user-space flows, based on the flow ID.
    fn user_space_sender(&mut self, flow_id: FlowId) -> Option<UserSpaceSender> {
        if let Some(sender) = self.user_space_senders.get(&flow_id) {
            Some(sender.clone())
        } else {
            if flow_id.dst_port() != self.config.user_space_server_port {
                return None;
            }

            let server_handle = self
                .server
                .clone()
                .expect("The user-space server has not yet been connected.");

            let sender = server_handle.add_server(flow_id);
            self.user_space_senders.insert(flow_id, sender.clone());

            Some(sender)
        }
    }

    /// Sends a packet to its destined next hop, including local delivery to the TUN interface,
    /// a user-space TCP client, or a user-space TCP server.
    async fn send_packet(&mut self, packet: Packet, next_hop_id: NodeId) {
        #[allow(unused_mut)]
        let mut packet = packet;

        // checks if the next hop is the dst node
        if next_hop_id == self.routing_table.local_id {
            // if possible, deliver to the lossless transport subsystem
            if self.try_deliver_lossless(&packet) {
                return;
            }

            // local TUN delivery: use the destination IP address to distinguish between the TUN interface
            // and user-space TCP clients or servers
            if packet.flow_id.dst_ip() == self.config.local_address {
                if let Some(ref local_interface) = self.local_interface {
                    local_interface.write_packet(packet);
                } else {
                    error!("The local interface has not yet been connected.");
                }
            } else {
                #[cfg(feature = "python-extension")]
                if let Some(ref py_if) = self.python_interface {
                    match py_if.deliver(packet).await {
                        Ok(()) => return,
                        Err(returned_packet) => {
                            packet = returned_packet;
                        }
                    }
                }

                let flow_id = packet.flow_id;

                let dest = self.user_space_sender(flow_id);
                if let Some(sender) = dest
                    && sender.try_send(packet).is_err()
                {
                    error!("Failed to send a packet in user-space flows to its local destination.");
                }
            }
        } else if let Some(scheduler) = self.schedulers.get(&next_hop_id) {
            scheduler.send(packet).await;
        }
    }

    fn try_deliver_lossless(&mut self, packet: &Packet) -> bool {
        let Some(handle) = self.lossless_handle.clone() else {
            return false;
        };
        let Some(payload) = packet.tcp_payload() else {
            return false;
        };
        let session_id = if let Some((hdr, _, _)) = lossless_session::decode_data(payload) {
            hdr.session_id
        } else if let Some((hdr, _)) = lossless_session::decode_control(payload) {
            hdr.session_id
        } else if let Some((hdr, _, _)) = lossless_session::decode_fec_data(payload) {
            hdr.session_id
        } else {
            return false;
        };

        let src_node = self.config.ip_to_node_id(packet.flow_id.src_ip());
        let peer_id = if src_node == INVALID {
            None
        } else {
            Some(src_node)
        };

        let payload_vec = payload.to_vec();

        handle.deliver(
            session_id,
            LosslessInboundFrame {
                bytes: payload_vec,
                peer_id,
            },
        );
        true
    }
}

#[cfg(test)]
mod tests {
    use std::net::Ipv4Addr;

    use super::*;

    fn base_config(operating_mode: OperatingMode) -> LocalConfig {
```

```bash
sed -n '1,220p' dataplane/src/node/scheduler/sched.rs
```

```rust
use std::sync::Arc;

use tokio::sync::{Notify, Semaphore, mpsc};
use tracing::{debug, error};

use nextmini_messages::{SchedulingDiscipline, TokenBucketSpec};

use crate::node::FlowId;
use crate::node::config::LocalConfig;
use crate::node::network::interface::NetworkInterfaceHandle;
use crate::node::packet::Packet;
use crate::node::scheduler::drop::{CapacityUnit, DropStrategy, PacketDrop, Red, TailDrop};
use crate::node::scheduler::fifo::FifoQueue;
use crate::node::scheduler::queue::SchedulerQueue;
use crate::node::scheduler::reader::SchedulerReader;
use crate::node::scheduler::writer::SchedulerWriter;
use crate::node::scheduler::wrr::WrrQueue;

/// The types of messages sent to the scheduler.
pub enum SchedulerReaderMessage {
    InboundPacket(Packet),
}

/// The rate limit is to be sent by the processor, and in the unit of bytes per second.
pub enum SchedulerWriterMessage {
    RateLimit(TokenBucketSpec),
    SetFlowWeight(FlowId, usize),
}

/// The handle for the scheduler actor, which is between the processors and the network interface.
#[derive(Clone, Debug)]
pub struct SchedulerHandle {
    reader_sender: mpsc::Sender<SchedulerReaderMessage>,
    writer_sender: mpsc::UnboundedSender<SchedulerWriterMessage>,
    backpressure: bool,
}

impl SchedulerHandle {
    pub fn new(config: LocalConfig, net_interface: NetworkInterfaceHandle) -> Self {
        let (reader_sender, reader_receiver) = mpsc::channel(config.channel_capacity);
        let (writer_sender, writer_receiver) = mpsc::unbounded_channel();
        let backpressure = config.channel_backpressure;

        let scheduler = Scheduler::new(config, net_interface, reader_receiver, writer_receiver);
        scheduler.run();

        Self {
            reader_sender,
            writer_sender,
            backpressure,
        }
    }

    /// Sends a packet to the scheduler.
    pub async fn send(&self, packet: Packet) {
        let msg = SchedulerReaderMessage::InboundPacket(packet);

        if self.backpressure {
            if let Err(e) = self.reader_sender.send(msg).await {
                error!("SchedulerHandle: reader channel closed; dropping packet: {e}");
            }
        } else if let Err(e) = self.reader_sender.try_send(msg) {
            error!("SchedulerHandle: Error sending a packet to the scheduler: {e}.");
        }
    }

    /// Limits the rate of sending packets the outbound network connection, in bytes/second.
    pub fn limit_rate(&self, spec: TokenBucketSpec) {
        if let Err(e) = self
            .writer_sender
            .send(SchedulerWriterMessage::RateLimit(spec))
        {
            error!(
                "SchedulerHandle: Error sending a rate limit to the scheduler: {}.",
                e
            );
        }
    }

    /// Sets the weight of a flow.
    pub fn set_flow_weight(&self, flow_id: FlowId, weight: usize) {
        if let Err(e) = self
            .writer_sender
            .send(SchedulerWriterMessage::SetFlowWeight(flow_id, weight))
        {
            error!(
                "SchedulerHandle: Error sending a flow weight to the scheduler: {}.",
                e
            );
        }
    }
}

pub struct Scheduler {
    config: LocalConfig,
}

impl Scheduler {
    pub fn new(
        config: LocalConfig,
        net_interface: NetworkInterfaceHandle,
        reader_receiver: mpsc::Receiver<SchedulerReaderMessage>,
        writer_receiver: mpsc::UnboundedReceiver<SchedulerWriterMessage>,
    ) -> Self {
        let capacity = config.queue_capacity;
        let capacity_unit = CapacityUnit::Packets;

        let packet_drop: Box<dyn PacketDrop + Send + Sync> = match config.scheduler_drop_strategy {
            DropStrategy::TailDrop => Box::new(TailDrop::new(capacity, capacity_unit)),
            DropStrategy::Red => Box::new(Red::new(capacity, capacity_unit, 0.7, 0.9, 0.8)),
        };

        let queue_strategy: Arc<dyn SchedulerQueue + Send + Sync> = match config.scheduler_type {
            SchedulingDiscipline::Fifo => Arc::new(FifoQueue::new(capacity)),
            SchedulingDiscipline::Wrr => Arc::new(WrrQueue::new(capacity)),
        };

        let queues_not_empty = Arc::new(Notify::new());

        // When channel backpressure is enabled, also apply it to the scheduler queue so we block
        // instead of dropping when the queue reaches capacity.
        let capacity_semaphore = if config.channel_backpressure && capacity > 0 {
            Some(Arc::new(Semaphore::new(capacity)))
        } else {
            None
        };

        let mut reader = SchedulerReader::new(
            queue_strategy.clone(),
            packet_drop,
            queues_not_empty.clone(),
            capacity,
            capacity_semaphore.clone(),
            reader_receiver,
            config.scheduler_type,
        );

        let mut writer = SchedulerWriter::new(
            queue_strategy,
            net_interface,
            queues_not_empty,
            capacity_semaphore,
            writer_receiver,
        );

        tokio::task::spawn(async move {
            let _ = reader.run().await;
        });

        tokio::task::spawn(async move {
            let _ = writer.run().await;
        });

        Self { config }
    }

    pub fn run(&self) {
        // This method is intentionally left empty as the actual run logic is handled in the
        // FifoReader and FifoWriter tasks spawned above.
        debug!(
            "A {:?} scheduler has just been started.",
            self.config.scheduler_type
        );
    }
}
```

### Step 9: covers specialized paths.

- Max mode connector (`connector.rs`) manages explicit node-address/routing updates and TCP-max stream handoff.
- User-space flow client/server modules create SmolTcp-backed threads for controller-installed app flows.
- Lossless runtime tracks session actors and reacts to topology readiness.

```bash
sed -n '1,220p' dataplane/src/node/connector.rs
```

```rust
use ahash::AHashMap;
use tokio;
#[cfg(not(target_os = "linux"))]
use tokio::io::copy_bidirectional;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
#[cfg(target_os = "linux")]
use tokio_splice::zero_copy_bidirectional;
use tracing::{error, info};

use nextmini_messages::RoutingTableEntry;

use crate::node::config::LocalConfig;
use crate::node::controller::flowstats::FlowStatsReporterHandle;
use crate::node::network::tcp_max::TcpMaxClient;
use crate::node::packet::Packet;
use crate::node::processor::ProcessorPacket;
use crate::node::route::RoutingTable;
use crate::node::scheduler::sched::SchedulerHandle;
use crate::node::{FlowId, FlowIdExt, NodeId};

pub enum ConnectorMessage {
    AddNodeAddress(NodeId, String),
    UpdateRoutingTable(Vec<RoutingTableEntry>),
    ConnectTcpMaxClient(Box<TcpMaxClient>),
    InboundMaxRequest(FlowId, TcpStream),
    SetFlowStatsReporter(Box<FlowStatsReporterHandle>),
}

pub struct Connector {
    /// receives packets from the processor handle at the src node in max mode
    packet_receiver: mpsc::Receiver<ProcessorPacket>,

    /// receives messages from the processor handle
    message_receiver: mpsc::Receiver<ConnectorMessage>,

    /// the TCP max client
    tcp_max_client: Option<TcpMaxClient>,

    /// the routing table
    routing_table: RoutingTable,

    /// the remote node addresses
    node_addresses: AHashMap<NodeId, String>,

    /// a hashmap for the schedulers in max mode
    schedulers: AHashMap<FlowId, SchedulerHandle>,

    /// optional flow stats reporter for route reporting
    flowstats_reporter: Option<FlowStatsReporterHandle>,
}

impl Connector {
    pub fn new(
        packet_receiver: mpsc::Receiver<ProcessorPacket>,
        message_receiver: mpsc::Receiver<ConnectorMessage>,
        config: LocalConfig,
    ) -> Self {
        Self {
            packet_receiver,
            message_receiver,
            tcp_max_client: None,
            routing_table: RoutingTable::new(config),
            node_addresses: AHashMap::new(),
            schedulers: AHashMap::new(),
            flowstats_reporter: None,
        }
    }

    pub async fn run(&mut self) {
        loop {
            tokio::select! {
                // waits for a first packet or a message
                Some(msg) = self.packet_receiver.recv() => {
                    match msg {
                        // starts a batch with the first packet
                        ProcessorPacket::ProcessPacket(first_packet) => {
                            self.process_packet(first_packet).await;

                            // starts processing packets in batches
                            while let Ok(ProcessorPacket::ProcessPacket(packet)) = self.packet_receiver.try_recv() {
                                self.process_packet(packet).await;
                            }
                        }
                    }
                }
                Some(msg) = self.message_receiver.recv() => {
                    self.handle_message(msg).await;
                }
                else => {
                    info!("Connector channels closed; connector task exiting.");
                    break;
                }
            }
        }
    }

    async fn handle_message(&mut self, msg: ConnectorMessage) {
        match msg {
            ConnectorMessage::ConnectTcpMaxClient(tcp_max_client) => {
                self.tcp_max_client = Some(*tcp_max_client);
            }
            ConnectorMessage::AddNodeAddress(node_id, address) => {
                self.node_addresses.insert(node_id, address);
            }
            ConnectorMessage::UpdateRoutingTable(routes) => {
                self.routing_table.install_routes(routes);
            }
            ConnectorMessage::InboundMaxRequest(flow_id, stream) => {
                self.handle_inbound_request(flow_id, stream).await;
            }
            ConnectorMessage::SetFlowStatsReporter(flowstats_reporter) => {
                self.flowstats_reporter = Some(*flowstats_reporter);
            }
        }
    }

    /// Processes packets at the source node.
    async fn process_packet(&mut self, packet: Packet) {
        let flow_id = packet.flow_id;

        // sends directly when the TCP max connection is already established, or initiates a new connection
        if let Some(scheduler) = self.schedulers.get(&flow_id) {
            // if the scheduler is already initialized at the source node, sends the packet to the next hop directly
            scheduler.send(packet).await;
        } else {
            // if the scheduler is not initialized, initiates a new connection for the first packet of the flow

            // obtains the next hop id from the routing table
            let next_hop_id = match self
                .routing_table
                .get_next_hop_by_flow(flow_id, self.flowstats_reporter.as_ref())
            {
                Ok(next_hop_id) => next_hop_id,
                Err(e) => {
                    error!("Error getting the next hop: {}", e);
                    return;
                }
            };

            if next_hop_id == self.routing_table.local_id {
                error!(
                    "Connector received a flow {} destined for the local node; dropping packet.",
                    flow_id
                );
                return;
            }

            // obtains the remote address for the next hop node
            let remote_addr = match self.node_addresses.get(&next_hop_id) {
                Some(addr) => addr.clone(),
                None => {
                    error!("No remote address found for node id: {}", next_hop_id);
                    return;
                }
            };

            let tcp_max_client = match self.tcp_max_client.as_ref() {
                Some(client) => client,
                None => {
                    error!(
                        "TCP Max client not yet connected; dropping packet for {}.",
                        flow_id
                    );
                    return;
                }
            };

            // establishes a TCP max connection to the next-hop node
            let stream = tcp_max_client.connect(packet.flow_id, &remote_addr).await;

            // initializes a scheduler which is then stored and used for all subsequent packets in that flow
            let scheduler = tcp_max_client
                .initialize_scheduler(stream, next_hop_id)
                .await;

            // sends the first packet of the flow with the new scheduler
            scheduler.send(packet).await;

            // stores the flow id to the scheduler into hashmap for sending subsequent packets
            self.schedulers.insert(flow_id, scheduler);
        }
    }

    /// Handles an inbound request as the destination node or as a relay node.
    async fn handle_inbound_request(&mut self, flow_id: FlowId, mut inbound_stream: TcpStream) {
        let next_hop_id = match self
            .routing_table
            .get_next_hop_by_flow(flow_id, self.flowstats_reporter.as_ref())
        {
            Ok(next_hop_id) => next_hop_id,
            Err(e) => {
                error!("Error getting the next hop: {}", e);
                return;
            }
        };

        let tcp_max_client = self.tcp_max_client.as_ref().unwrap();

        // handles flows where this node is the final destination
        if next_hop_id == self.routing_table.local_id {
            // creates a new scheduler for response packets
            let scheduler = tcp_max_client
                .initialize_scheduler(inbound_stream, next_hop_id)
                .await;

            // reverses the flow ID and stores it in a hashmap from flow IDs to schedulers. This is for sending
            // response packets from the destination node to the source node
            self.schedulers.insert(flow_id.reverse(), scheduler);
        } else {
            // handles flows that need to be forwarded to the next hop as a relay node

            // obtains the next hop address from the hashmap of node addresses (only stored for dataplane nodes)
            let next_hop_addr = match self.node_addresses.get(&next_hop_id) {
                Some(addr) => addr.clone(),
                // redirects to the external server if the next hop is not a registered node (for sending external traffic)
                None => {
                    let external_server_addr =
                        format!("{}:{}", flow_id.dst_ip(), flow_id.dst_port());

```

```bash
sed -n '1,220p' dataplane/src/node/flow/client.rs
```

```rust
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

            // pins a specific route for this flow if route_id is specified
            if let Some(route_id) = flow.route_id {
                // use reverse flow_id: client -> server direction
                let flow_id = flow_id.reverse();

                info!(
                    "Pinning route {} for user space TCP flow from node {} to node {}.",
                    route_id, flow.src_node_id, flow.dst_node_id
                );
                self.processors.pin_route_for_flow(flow_id, route_id);
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
```

```bash
sed -n '1,220p' dataplane/src/node/flow/server.rs
```

```rust
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

use nextmini_messages::{Flow, FlowSpec};

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
    use super::*;

    use std::time::Duration;
```

```bash
sed -n '1,280p' dataplane/src/node/session/runtime.rs
```

```rust
use std::net::Ipv4Addr;
use std::sync::Arc;

use ahash::AHashMap;
use bytes::Bytes;
use tokio::sync::{Mutex, mpsc, oneshot, watch};
use tokio::task::JoinHandle;
use tracing::warn;

use nextmini_messages::TokenBucketSpec;
use nextmini_messages::lossless_session::{FecCapabilities, FecManifest};

use crate::node::config::LosslessConfig;
use crate::node::processor::ProcessorHandle;
use crate::node::session::api::{Command, InboundFrame, SessionId};
use crate::node::session::{fec_policy, receiver, sender};

pub use crate::node::session::fec_policy::PreflightError;

/// Shared per-session settings.
#[derive(Clone, Debug)]
pub struct SessionConfig {
    pub session_id: SessionId,
    pub block_size: usize,
}

/// Precomputed transport envelope used for data/control traffic.
#[derive(Clone, Debug)]
pub struct TransportRoute {
    pub src_ip: Ipv4Addr,
    pub dst_ip: Ipv4Addr,
    pub src_port: u16,
    pub dst_port: u16,
}

/// Sender-only configuration (fan-out, source path, pacing, ready grace, etc.).
#[derive(Clone, Debug)]
pub struct SenderRequest {
    pub session: SessionConfig,
    pub route: TransportRoute,
    pub pacing: Option<TokenBucketSpec>,
    pub receiver_ids: Vec<usize>,
    pub total_bytes: u64,
    pub source_buffer: Bytes,
    pub ready_grace_ms: u64,
}

/// Receiver-only request payload accepted at the runtime API boundary.
#[derive(Clone, Debug)]
pub struct ReceiverRequest {
    pub session_id: SessionId,
    pub route: TransportRoute,
    pub local_node_id: usize,
    pub sink_buffer: Option<Arc<Mutex<Vec<u8>>>>,
}

/// Sender task configuration after runtime derives internal FEC policy.
#[derive(Clone, Debug)]
pub struct SenderConfig {
    pub session: SessionConfig,
    pub route: TransportRoute,
    pub pacing: Option<TokenBucketSpec>,
    pub receiver_ids: Vec<usize>,
    pub source_buffer: Bytes,
    pub manifest: LosslessSessionManifest,
    pub ready_grace_ms: u64,
    pub topology_ready: Option<watch::Receiver<bool>>,
}

/// Receiver task configuration after runtime derives internal FEC policy.
#[derive(Clone, Debug)]
pub struct ReceiverConfig {
    pub session_id: SessionId,
    pub route: TransportRoute,
    pub local_node_id: usize,
    pub sink_buffer: Option<Arc<Mutex<Vec<u8>>>>,
    pub fec_enabled: bool,
}

/// Handle for communicating with the lossless runtime actor.
/// This handle can be cloned and used to manage lossless sessions.
#[derive(Clone, Debug)]
pub struct LosslessRuntimeHandle {
    command_tx: mpsc::UnboundedSender<Command>,
}

impl LosslessRuntimeHandle {
    pub fn new(processors: ProcessorHandle, config: LosslessConfig) -> Self {
        let (command_tx, command_rx) = mpsc::unbounded_channel();

        let runtime = LosslessRuntime::new(processors, config, command_rx);

        // spawns the lossless runtime actor task
        tokio::spawn(async move {
            let mut runtime = runtime;

            runtime.run().await;
        });

        Self { command_tx }
    }

    /// Requests that the runtime spin up a sender session with the supplied
    /// configuration and return its session ID.
    pub async fn start_sender(&self, cfg: SenderRequest) -> Result<SessionId, PreflightError> {
        let (reply_tx, reply_rx) = oneshot::channel();

        if self
            .command_tx
            .send(Command::StartSender {
                cfg,
                reply: reply_tx,
            })
            .is_err()
        {
            return Err(PreflightError::RuntimeChannelClosed);
        }

        reply_rx
            .await
            .unwrap_or(Err(PreflightError::RuntimeChannelClosed))
    }

    /// Request that the runtime spin up a receiver immediately.
    pub async fn start_receiver(&self, cfg: ReceiverRequest) -> SessionId {
        let (reply_tx, reply_rx) = oneshot::channel();

        let _ = self.command_tx.send(Command::StartReceiver {
            cfg,
            reply: reply_tx,
        });

        reply_rx.await.expect("The session ID.")
    }

    /// Cancels a session regardless of whether it is a sender or receiver.
    pub fn stop(&self, session: SessionId) {
        let _ = self.command_tx.send(Command::Stop { session });
    }

    /// Delivers an inbound frame to the owning session's queue.
    pub fn deliver(&self, session: SessionId, frame: InboundFrame) {
        let _ = self.command_tx.send(Command::Deliver { session, frame });
    }

    /// Waits until the runtime observes completion (EOT/ACKs) for a session.
    pub async fn wait_completion(&self, session: SessionId) -> bool {
        let (reply_tx, reply_rx) = oneshot::channel();
        let _ = self.command_tx.send(Command::Wait {
            session,
            reply: reply_tx,
        });
        reply_rx.await.unwrap_or(false)
    }

    /// Reserves the next session identifier from the runtime's allocator.
    #[allow(dead_code)]
    pub async fn allocate_session_id(&self) -> SessionId {
        let (reply_tx, reply_rx) = oneshot::channel();

        let _ = self
            .command_tx
            .send(Command::AllocateSession { reply: reply_tx });

        reply_rx.await.expect("The session ID.")
    }

    /// Notifies the runtime that the control plane finished setting up the topology.
    pub fn set_topology_ready(&self, ready: bool) {
        let _ = self.command_tx.send(Command::SetTopologyReady { ready });
    }
}

/// Tracks running lossless sessions along with their inboxes and join handles.
/// This is the actor that processes commands and manages session lifecycle.
struct LosslessRuntime {
    processors: ProcessorHandle,
    config: LosslessConfig,
    tasks: AHashMap<SessionId, JoinHandle<()>>,
    inputs: AHashMap<SessionId, mpsc::Sender<InboundFrame>>,
    next_session_id: SessionId,
    topology_ready_tx: watch::Sender<bool>,
    topology_ready: bool,
    command_rx: mpsc::UnboundedReceiver<Command>,
}

impl LosslessRuntime {
    /// Constructs a runtime that can spawn sender/receiver tasks and track their lifetimes.
    fn new(
        processors: ProcessorHandle,
        config: LosslessConfig,
        command_rx: mpsc::UnboundedReceiver<Command>,
    ) -> Self {
        let (topology_ready_tx, _) = watch::channel(false);

        Self {
            processors,
            config,
            tasks: AHashMap::default(),
            inputs: AHashMap::default(),
            next_session_id: 1,
            topology_ready_tx,
            topology_ready: false,
            command_rx,
        }
    }

    /// Main event loop for the lossless runtime actor that processes inbound commands.
    async fn run(&mut self) {
        while let Some(cmd) = self.command_rx.recv().await {
            match cmd {
                Command::StartSender { cfg, reply } => {
                    let sid = self.spawn_sender(cfg);

                    let _ = reply.send(sid);
                }
                Command::StartReceiver { cfg, reply } => {
                    let sid = self.spawn_receiver(cfg);

                    let _ = reply.send(sid);
                }
                Command::Stop { session } => {
                    self.stop(session).await;
                }
                Command::Deliver { session, frame } => {
                    self.deliver_frame(session, frame).await;
                }
                Command::Wait { session, reply } => {
                    // handles a wait command asynchronously without blocking the main loop
                    self.handle_wait(session, reply);
                }
                Command::AllocateSession { reply } => {
                    let sid = self.allocate_session_id();

                    let _ = reply.send(sid);
                }
                Command::SetTopologyReady { ready } => {
                    self.set_topology_ready(ready);
                }
            }
        }
    }

    /// Delivers inbound frames to sessions.
    async fn deliver_frame(&mut self, session: SessionId, frame: InboundFrame) {
        if let Some(tx) = self.input_sender(session) {
            if tx.send(frame).await.is_err() {
                warn!(
                    session_id = session,
                    "Lossless runtime: receiver dropped inbound frame."
                );
            }
        } else {
            warn!(
                session_id = session,
                "Lossless runtime: no receiver for inbound frame."
            );
        }
    }

    /// Handles wait command by spawning a separate task.
    fn handle_wait(&mut self, session: SessionId, reply: oneshot::Sender<bool>) {
        // Take ownership of the task handle. Note: we don't remove inputs here
        // because in-flight frames may still arrive. Cleanup happens in stop()
        // after wait completes.
        let handle = self.take_task(session);

        tokio::spawn(async move {
            if let Some(handle) = handle {
                let _ = handle.await; // ignore join errors; treat as completion
                let _ = reply.send(true);
            } else {
                let _ = reply.send(false);
            }
        });
    }
```

At this point we have the complete dataplane runtime lifecycle from process boot through control-plane synchronization and packet forwarding/transport execution.

### Step 10: adds the Python embedding layer (`python-api`).

The `Dataplane` PyO3 class wraps dataplane runtime startup and exposes methods that map directly to internal packet/session operations.

This keeps Python experiments aligned with the same control/data paths used by the Rust binaries.

```bash
sed -n '1,220p' python-api/src/lib.rs
```

```rust
mod buffer;

#[cfg(feature = "python-extension")]
use std::collections::HashMap;
use std::collections::VecDeque;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::net::Ipv4Addr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use bytes::Bytes;
use once_cell::sync::OnceCell;
use pyo3::conversion::IntoPyObject;
use pyo3::exceptions::{PyKeyError, PyRuntimeError};
use pyo3::prelude::PyModuleMethods;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyModule};
use pyo3_async_runtimes::tokio::future_into_py;
use tokio::sync::Mutex;
use tokio::sync::mpsc;
use tracing::info;
use tracing_subscriber::EnvFilter;

use nextmini::node::conductor::Conductor;
use nextmini::node::config::LocalConfig;
#[cfg(feature = "python-extension")]
use nextmini::node::controller::interface::ControllerInterfaceHandle;
use nextmini::node::packet::Packet;
use nextmini::node::processor::ProcessorHandle;
use nextmini::node::python::interface::{
    PayloadDelivery as RustPayloadDelivery, PythonDelivery, PythonEvent, PythonInterfaceHandle,
};
#[cfg(feature = "python-extension")]
use nextmini::node::session;
#[cfg(feature = "python-extension")]
use nextmini::node::session::api::LosslessRuntimeHandle;
use nextmini::node::{NodeId, NodeIdExt};
#[cfg(feature = "python-extension")]
use nextmini_messages::DataplaneToController;

pub use crate::buffer::{PacketBuilder, PacketView};

static RUNTIME: OnceCell<tokio::runtime::Runtime> = OnceCell::new();
static TRACING: OnceCell<()> = OnceCell::new();

#[cfg(feature = "python-extension")]
type BufferRegistry = Arc<Mutex<HashMap<u64, Arc<Mutex<Vec<u8>>>>>>;

fn rt() -> &'static tokio::runtime::Runtime {
    RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .thread_name("nextmini-py")
            .build()
            .expect("unable to create tokio runtime for nextmini_py")
    })
}

fn init_tracing_subscriber() {
    TRACING.get_or_init(|| {
        let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("debug"));
        let _ = tracing_subscriber::fmt()
            .with_env_filter(filter)
            .with_thread_ids(true)
            .with_target(true)
            .try_init();
    });
}

static PY_MESSAGE_ID_SEQ: AtomicU64 = AtomicU64::new(1);

#[pyclass]
struct PacketReceiver {
    inner: Arc<Mutex<mpsc::Receiver<PythonDelivery>>>,
}

#[pymethods]
impl PacketReceiver {
    #[pyo3(signature = (timeout_ms=None))]
    fn recv(&self, timeout_ms: Option<u64>, py: Python<'_>) -> PyResult<Option<Py<PyAny>>> {
        let inner = self.inner.clone();
        let maybe_delivery = Python::detach(py, move || {
            rt().block_on(async move {
                match timeout_ms {
                    Some(ms) => tokio::time::timeout(
                        std::time::Duration::from_millis(ms),
                        inner.lock().await.recv(),
                    )
                    .await
                    .unwrap_or_default(),
                    None => inner.lock().await.recv().await,
                }
            })
        });
        maybe_delivery
            .map(|delivery| delivery_to_pyobject(py, delivery))
            .transpose()
    }

    fn recv_async<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        future_into_py(py, async move {
            let delivery = inner.lock().await.recv().await;
            Python::attach(|py| {
                delivery
                    .map(|delivery| delivery_to_pyobject(py, delivery))
                    .transpose()
            })
        })
    }
}

fn delivery_to_pyobject(py: Python<'_>, delivery: PythonDelivery) -> PyResult<Py<PyAny>> {
    // PythonDelivery is now just PayloadDelivery (type alias)
    let obj = Py::new(py, PyPayloadDelivery::from(delivery))?;

    Ok(obj.into_pyobject(py)?.unbind().into())
}

#[pyclass(name = "PayloadDelivery")]
struct PyPayloadDelivery {
    buffer: PacketView,
    flow_id: u128,
    src_ip: String,
    dst_ip: String,
    src_port: u16,
    dst_port: u16,
    message_id: Option<u64>,
    total_len: Option<u32>,
    fragment_count: Option<u16>,
}

#[pymethods]
impl PyPayloadDelivery {
    #[getter]
    fn payload<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
        PyBytes::new(py, self.buffer.inner.as_ref())
    }

    #[getter]
    fn frozen_payload(&self) -> PacketView {
        self.buffer.clone()
    }

    #[getter]
    fn flow_id(&self) -> u128 {
        self.flow_id
    }

    #[getter]
    fn src_ip(&self) -> &str {
        &self.src_ip
    }

    #[getter]
    fn dst_ip(&self) -> &str {
        &self.dst_ip
    }

    #[getter]
    fn src_port(&self) -> u16 {
        self.src_port
    }

    #[getter]
    fn dst_port(&self) -> u16 {
        self.dst_port
    }

    #[getter]
    fn message_id(&self) -> Option<u64> {
        self.message_id
    }

    #[getter]
    fn total_len(&self) -> Option<u32> {
        self.total_len
    }

    #[getter]
    fn fragment_count(&self) -> Option<u16> {
        self.fragment_count
    }
}

impl From<RustPayloadDelivery> for PyPayloadDelivery {
    fn from(payload: RustPayloadDelivery) -> Self {
        Self {
            buffer: PacketView::from_bytes(payload.bytes),
            flow_id: payload.flow_id,
            src_ip: payload.src_ip.to_string(),
            dst_ip: payload.dst_ip.to_string(),
            src_port: payload.src_port,
            dst_port: payload.dst_port,
            message_id: payload.message_id,
            total_len: payload.total_len,
            fragment_count: payload.fragment_count,
        }
    }
}

#[pyclass]
struct Dataplane {
    cfg: LocalConfig,
    py_if: PythonInterfaceHandle,
    processor: ProcessorHandle,
    controller: ControllerInterfaceHandle,
    _join: tokio::task::JoinHandle<()>,
    #[cfg(feature = "python-extension")]
    lossless_runtime: Option<LosslessRuntimeHandle>,
    #[cfg(feature = "python-extension")]
    buffer_registry: BufferRegistry,
    event_stash: Arc<Mutex<VecDeque<PythonEvent>>>,
}

impl Dataplane {
    #[cfg(feature = "python-extension")]
    fn remember_buffer_sink(&self, session_id: u64, buf: Arc<Mutex<Vec<u8>>>) {
```

```bash
sed -n '220,760p' python-api/src/lib.rs
```

```rust
    fn remember_buffer_sink(&self, session_id: u64, buf: Arc<Mutex<Vec<u8>>>) {
        let mut guard = rt().block_on(self.buffer_registry.lock());
        guard.insert(session_id, buf);
    }
}

#[pymethods]
impl Dataplane {
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (group_id, dest_ip, receiver_ids, buffer, *, block_size=8500, src_port=None, dst_port=None))]
    fn send_data(
        &self,
        group_id: u64,
        dest_ip: &str,
        receiver_ids: Vec<usize>,
        buffer: PacketView,
        block_size: usize,
        src_port: Option<u16>,
        dst_port: Option<u16>,
    ) -> PyResult<u64> {
        #[allow(unused_variables)]
        let dest_ip_addr = parse_ipv4(dest_ip)?;
        if receiver_ids.is_empty() {
            return Err(PyRuntimeError::new_err(
                "receiver_ids must contain at least one entry.",
            ));
        }

        if block_size == 0 {
            return Err(PyRuntimeError::new_err("block_size must be positive."));
        }

        let total_bytes = buffer.inner.len() as u64;
        if total_bytes == 0 {
            return Err(PyRuntimeError::new_err(
                "buffer is empty; nothing to transmit.",
            ));
        }

        // Compute deterministic session_id from group_id and source_node_id
        #[allow(unused_variables)]
        let sid = multicast_session_id(group_id, self.cfg.node_id);
        #[cfg(feature = "python-extension")]
        {
            if let Some(handle) = &self.lossless_runtime {
                let runtime_config = &self.cfg.lossless_runtime_config;
                let sp = src_port.unwrap_or(self.cfg.user_space_client_port);
                let dp = dst_port.unwrap_or(self.cfg.user_space_server_port);
                let session_cfg = session::runtime::SessionConfig {
                    session_id: sid,
                    block_size,
                };
                let route = session::runtime::TransportRoute {
                    src_ip: self
                        .cfg
                        .node_id
                        .ip_addr(self.cfg.user_space_base_addr, self.cfg.local_netmask),
                    dst_ip: dest_ip_addr,
                    src_port: sp,
                    dst_port: dp,
                };
                let cfg = session::runtime::SenderRequest {
                    session: session_cfg,
                    route,
                    pacing: runtime_config.data_bucket.clone(),
                    receiver_ids,
                    total_bytes,
                    source_buffer: buffer.inner.clone(),
                    ready_grace_ms: runtime_config.ready_grace_ms,
                };
                let started_sid = rt().block_on(handle.start_sender(cfg)).map_err(|err| {
                    PyRuntimeError::new_err(format!(
                        "lossless sender preflight rejected session {sid}: {err}"
                    ))
                })?;
                return Ok(started_sid);
            }
        }

        #[cfg(not(feature = "python-extension"))]
        let _ = total_bytes;

        Ok(sid)
    }

    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (group_id, source_node_id, *, src_port=None, dst_port=None))]
    fn receive_data(
        &self,
        group_id: u64,
        source_node_id: usize,
        src_port: Option<u16>,
        dst_port: Option<u16>,
    ) -> PyResult<u64> {
        // Compute deterministic session_id from group_id and source_node_id
        #[allow(unused_variables)]
        let sid = multicast_session_id(group_id, source_node_id);
        #[cfg(feature = "python-extension")]
        {
            if let Some(handle) = &self.lossless_runtime {
                let sink_buf = Arc::new(Mutex::new(Vec::new()));
                let cfg = session::runtime::ReceiverRequest {
                    session_id: sid,
                    route: session::runtime::TransportRoute {
                        src_ip: self.cfg.node_id.user_space_ip(
                            self.cfg.user_space_base_addr,
                            self.cfg.local_netmask,
                        ),
                        dst_ip: source_node_id.user_space_ip(
                            self.cfg.user_space_base_addr,
                            self.cfg.local_netmask,
                        ),
                        src_port: src_port.unwrap_or(self.cfg.user_space_client_port),
                        dst_port: dst_port.unwrap_or(self.cfg.user_space_server_port),
                    },
                    local_node_id: self.cfg.node_id,
                    sink_buffer: Some(sink_buf.clone()),
                };
                // Direct registration - both sender and receiver compute same session_id
                let started_sid = rt().block_on(handle.start_receiver(cfg));
                self.remember_buffer_sink(started_sid, sink_buf);
                return Ok(started_sid);
            }
        }

        #[cfg(not(feature = "python-extension"))]
        let _ = (src_port, dst_port);

        Ok(sid)
    }

    // This was an intentional breaking change in the Python receive API:
    // receivers no longer pass expected_bytes or receiver-local block_size.
    // The first sender manifest is now authoritative for receive geometry.

    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (group_id, source_node_id, *, src_port=None, dst_port=None))]
    fn receive_data_async<'py>(
        &self,
        py: Python<'py>,
        group_id: u64,
        source_node_id: usize,
        src_port: Option<u16>,
        dst_port: Option<u16>,
    ) -> PyResult<Bound<'py, PyAny>> {
        // Compute deterministic session_id from group_id and source_node_id
        let sid = multicast_session_id(group_id, source_node_id);

        #[cfg(feature = "python-extension")]
        {
            if let Some(handle) = &self.lossless_runtime {
                let handle = handle.clone();
                let buffer_registry = self.buffer_registry.clone();
                let sp = src_port.unwrap_or(self.cfg.user_space_client_port);
                let dp = dst_port.unwrap_or(self.cfg.user_space_server_port);
                let local_node_id = self.cfg.node_id;
                let base_addr = self.cfg.user_space_base_addr;
                let netmask = self.cfg.local_netmask;

                return future_into_py(py, async move {
                    let sink_buf = Arc::new(Mutex::new(Vec::new()));
                    let cfg = session::runtime::ReceiverRequest {
                        session_id: sid,
                        route: session::runtime::TransportRoute {
                            src_ip: local_node_id.user_space_ip(base_addr, netmask),
                            dst_ip: source_node_id.user_space_ip(base_addr, netmask),
                            src_port: sp,
                            dst_port: dp,
                        },
                        local_node_id,
                        sink_buffer: Some(sink_buf.clone()),
                    };

                    // Direct registration - both sender and receiver compute same session_id
                    let started_sid = handle.start_receiver(cfg).await;

                    {
                        let mut guard = buffer_registry.lock().await;
                        guard.insert(started_sid, sink_buf);
                    }

                    Ok(started_sid)
                });
            }
        }

        #[cfg(not(feature = "python-extension"))]
        let _ = (src_port, dst_port);

        // Fallback if feature disabled (immediate return)
        future_into_py(py, async move {
            info!("receive_data_async: lossless runtime not available, returning immediate sid");
            Ok(sid)
        })
    }

    #[pyo3(signature = (session_id, timeout_ms=None))]
    fn lossless_wait(&self, session_id: u64, timeout_ms: Option<u64>) -> PyResult<bool> {
        #[cfg(feature = "python-extension")]
        {
            if let Some(handle) = &self.lossless_runtime {
                let fut = handle.wait_completion(session_id);
                let ok = if let Some(ms) = timeout_ms {
                    rt().block_on(async move {
                        tokio::time::timeout(std::time::Duration::from_millis(ms), fut)
                            .await
                            .unwrap_or(false)
                    })
                } else {
                    rt().block_on(fut)
                };

                // Proactively stop the session to clean up runtime state (tasks, inputs).
                // This prevents stale senders/receivers from holding onto session IDs that
                // may be reused by subsequent lossless transfers (e.g., RL rollouts).
                handle.stop(session_id);

                return Ok(ok);
            }
        }
        // feature disabled ⇒ nothing to wait for
        let _ = (session_id, timeout_ms);
        Ok(false)
    }

    #[pyo3(signature = (session_id, timeout_ms=None))]
    fn lossless_wait_async<'py>(
        &self,
        py: Python<'py>,
        session_id: u64,
        timeout_ms: Option<u64>,
    ) -> PyResult<Bound<'py, PyAny>> {
        #[cfg(feature = "python-extension")]
        {
            if let Some(handle) = &self.lossless_runtime {
                let handle = handle.clone();
                return future_into_py(py, async move {
                    let fut = handle.wait_completion(session_id);
                    let ok = if let Some(ms) = timeout_ms {
                        tokio::time::timeout(std::time::Duration::from_millis(ms), fut)
                            .await
                            .unwrap_or(false)
                    } else {
                        fut.await
                    };

                    // After completion, stop the session to drop its task and inputs.
                    // Safe to call even if the session was already cleaned up.
                    handle.stop(session_id);

                    Ok(ok)
                });
            }
        }
        // Fallback
        future_into_py(py, async move {
            info!("lossless_wait_async: lossless runtime not available, returning false");
            Ok(false)
        })
    }

    #[cfg(feature = "python-extension")]
    #[pyo3(signature = (session_id, consume=true))]
    fn get_data_buffer(&self, session_id: u64, consume: bool) -> PyResult<PacketView> {
        let buf_arc = {
            let guard = rt().block_on(self.buffer_registry.lock());
            guard
                .get(&session_id)
                .cloned()
                .ok_or_else(|| PyKeyError::new_err(format!("no buffer for session {session_id}")))?
        };

        let bytes = {
            let mut guard = rt().block_on(buf_arc.lock());
            if consume {
                Bytes::from(std::mem::take(&mut *guard))
            } else {
                Bytes::copy_from_slice(&guard)
            }
        };

        if consume {
            let mut guard = rt().block_on(self.buffer_registry.lock());
            guard.remove(&session_id);
        }

        Ok(PacketView::from_bytes(bytes))
    }

    #[new]
    fn new(config_path: &str) -> PyResult<Self> {
        let toml_str = std::fs::read_to_string(config_path)
            .map_err(|e| PyRuntimeError::new_err(format!("failed to read config: {e}")))?;
        let mut initial_cfg = LocalConfig::from_toml_str(&toml_str)
            .map_err(|e| PyRuntimeError::new_err(format!("failed to parse config: {e}")))?;
        initial_cfg.enable_local_interface = false;
        initial_cfg.config_path = config_path.to_string();
        initial_cfg.populate_runtime_defaults();

        let conductor = rt().block_on(async { Conductor::new(initial_cfg.clone()).await });
        let processor = conductor.processor_handle();
        let mut cfg = conductor.local_config();
        cfg.config_path = config_path.to_string();
        let controller = conductor.controller_handle();

        #[cfg(feature = "python-extension")]
        let lossless_runtime = conductor.lossless_runtime_handle();

        // enters the bindings runtime so tokio::spawn inside PythonInterfaceHandle::new() succeeds
        let py_if = {
            let _rt_guard = rt().enter();

            PythonInterfaceHandle::new(cfg.channel_capacity, cfg.channel_backpressure)
        };

        processor.connect_python_interface(py_if.clone());
        rt().block_on(controller.attach_python_interface(py_if.clone()));

        let join = rt().spawn(async move {
            conductor.run().await;
        });

        #[cfg(feature = "python-extension")]
        let buffer_registry = Arc::new(Mutex::new(HashMap::new()));

        Ok(Self {
            cfg,
            py_if,
            processor,
            controller,
            _join: join,
            #[cfg(feature = "python-extension")]
            lossless_runtime: Some(lossless_runtime),
            #[cfg(feature = "python-extension")]
            buffer_registry,
            event_stash: Arc::new(Mutex::new(VecDeque::new())),
        })
    }

    #[pyo3(signature = (src_node_id, src_port=None, dst_port=None))]
    fn register_receiver_from_node(
        &self,
        py: Python<'_>,
        src_node_id: usize,
        src_port: Option<u16>,
        dst_port: Option<u16>,
    ) -> PyResult<Py<PacketReceiver>> {
        let src_ip =
            (src_node_id as NodeId).ip_addr(self.cfg.user_space_base_addr, self.cfg.local_netmask);
        let dst_ip = self.cfg.user_space_address;
        let sp = src_port.unwrap_or(self.cfg.user_space_client_port);
        let dp = dst_port.unwrap_or(self.cfg.user_space_server_port);
        let flow_id = Packet::flow_id_from_parts(src_ip, sp, dst_ip, dp);
        let rx = rt().block_on(self.py_if.register_receiver(flow_id));
        Py::new(
            py,
            PacketReceiver {
                inner: Arc::new(Mutex::new(rx)),
            },
        )
    }

    #[pyo3(signature = (src_node_id, group_ip, src_port=None, dst_port=None))]
    fn register_receiver_for_group(
        &self,
        py: Python<'_>,
        src_node_id: usize,
        group_ip: &str,
        src_port: Option<u16>,
        dst_port: Option<u16>,
    ) -> PyResult<Py<PacketReceiver>> {
        let src_ip =
            (src_node_id as NodeId).ip_addr(self.cfg.user_space_base_addr, self.cfg.local_netmask);
        let dst_ip = parse_ipv4(group_ip)?;
        let sp = src_port.unwrap_or(self.cfg.user_space_client_port);
        let dp = dst_port.unwrap_or(self.cfg.user_space_server_port);
        let flow_id = Packet::flow_id_from_parts(src_ip, sp, dst_ip, dp);
        let rx = rt().block_on(self.py_if.register_receiver(flow_id));
        Py::new(
            py,
            PacketReceiver {
                inner: Arc::new(Mutex::new(rx)),
            },
        )
    }

    #[pyo3(signature = (dst_node_id, frozen, src_port=None, dst_port=None))]
    fn send_to_node(
        &self,
        dst_node_id: usize,
        frozen: PacketView,
        src_port: Option<u16>,
        dst_port: Option<u16>,
    ) -> PyResult<u64> {
        let body = frozen.inner.clone();

        let src_ip = self.cfg.user_space_address;
        let dst_ip =
            (dst_node_id as NodeId).ip_addr(self.cfg.user_space_base_addr, self.cfg.local_netmask);
        let sp = src_port.unwrap_or(self.cfg.user_space_client_port);
        let dp = dst_port.unwrap_or(self.cfg.user_space_server_port);
        self.transmit_python_payload(src_ip, dst_ip, sp, dp, body)
    }

    #[pyo3(signature = (label))]
    fn create_group(&self, label: String) -> PyResult<()> {
        rt().block_on(async {
            self.controller
                .send(DataplaneToController::CreateGroup { label })
                .await;
        });
        Ok(())
    }

    #[pyo3(signature = (group_id))]
    fn join_group(&self, group_id: usize) -> PyResult<()> {
        rt().block_on(async {
            self.controller
                .send(DataplaneToController::JoinGroup { group_id })
                .await;
        });
        Ok(())
    }

    #[pyo3(signature = (group_id))]
    fn leave_group(&self, group_id: usize) -> PyResult<()> {
        rt().block_on(async {
            self.controller
                .send(DataplaneToController::LeaveGroup { group_id })
                .await;
        });
        self.emit_python_event(PythonEvent::LocalMemberLeft {
            group_id,
            node_id: self.cfg.node_id,
        });
        Ok(())
    }

    /// Set multicast DAG edges for a group (directed edges).
    ///
    /// Intended for external optimizers (e.g. LP solvers) that want the controller to install a
    /// specific multicast tree without rewriting unicast routes.
    #[pyo3(signature = (group_id, edges))]
    fn set_group_routes(&self, group_id: usize, edges: Vec<(u32, u32)>) -> PyResult<()> {
        rt().block_on(async {
            self.controller
                .send(DataplaneToController::SetGroupRoutes { group_id, edges })
                .await;
        });
        Ok(())
    }

    #[pyo3(signature = (timeout_ms=None))]
    fn group_is_ready(&self, timeout_ms: Option<u64>) -> PyResult<Option<(usize, String, usize)>> {
        let timeout = timeout_ms.map(Duration::from_millis);
        let matched = self.wait_for_event_matching(timeout, |event| {
            matches!(event, PythonEvent::GroupCreated { .. })
        });

        match matched {
            Some(PythonEvent::GroupCreated {
                group_id,
                src_node_id,
                group_ip,
            }) => Ok(Some((group_id, group_ip.to_string(), src_node_id))),
            _ => Ok(None),
        }
    }

    #[pyo3(signature = (group_id, timeout_ms=None))]
    fn wait_for_local_membership(
        &self,
        group_id: usize,
        timeout_ms: Option<u64>,
    ) -> PyResult<bool> {
        let timeout = timeout_ms.map(Duration::from_millis);
        let local_node = self.cfg.node_id;
        let matched = self.wait_for_event_matching(timeout, |event| {
            matches!(
                event,
                PythonEvent::LocalMemberJoined {
                    group_id: gid,
                    node_id
                } if *gid == group_id && *node_id == local_node
            )
        });

        Ok(matched.is_some())
    }

    #[pyo3(signature = (group_id, src_node_id, min_routes=1, timeout_ms=None))]
    fn wait_for_group_routes(
        &self,
        group_id: usize,
        src_node_id: usize,
        min_routes: usize,
        timeout_ms: Option<u64>,
    ) -> PyResult<bool> {
        let timeout = timeout_ms.map(Duration::from_millis);
        let matched = self.wait_for_event_matching(timeout, |event| {
            matches!(
                event,
                PythonEvent::GroupRoutesInstalled {
                    group_id: gid,
                    src_node_id: sid,
                    routes,
                } if *gid == group_id && *sid == src_node_id && routes.len() >= min_routes
            )
        });

        Ok(matched.is_some())
    }

    /// Waits for the topology to be ready (all nodes connected and routes installed).
```

```bash
sed -n '1,220p' python-api/src/buffer.rs
```

```rust
use std::ffi::{c_char, c_int, c_void};

use bytes::{Bytes, BytesMut};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyBytes;

/// Mutable builder for constructing packets incrementally.
#[pyclass]
#[derive(Debug)]
pub struct PacketBuilder {
    inner: BytesMut,
}

#[pymethods]
impl PacketBuilder {
    #[new]
    #[pyo3(signature = (size=4096))]
    fn new(size: usize) -> Self {
        Self {
            inner: BytesMut::with_capacity(size),
        }
    }

    fn write(&mut self, data: &Bound<'_, PyBytes>) -> usize {
        let bytes = data.as_bytes();
        self.inner.extend_from_slice(bytes);
        bytes.len()
    }

    fn __len__(&self) -> usize {
        self.inner.len()
    }

    /// Zero-copy conversion to immutable PacketView.
    fn freeze(&mut self) -> PacketView {
        let bytes = std::mem::take(&mut self.inner).freeze();
        PacketView { inner: bytes }
    }
}

/// Read-only, reference-counted view of packet data.
#[pyclass]
#[derive(Clone, Debug)]
pub struct PacketView {
    pub(crate) inner: Bytes,
}

#[pymethods]
impl PacketView {
    #[new]
    fn new(data: &Bound<'_, PyBytes>) -> Self {
        Self {
            inner: Bytes::copy_from_slice(data.as_bytes()),
        }
    }

    #[staticmethod]
    fn from_buffer(data: &Bound<'_, PyBytes>) -> Self {
        Self {
            inner: Bytes::copy_from_slice(data.as_bytes()),
        }
    }

    fn __len__(&self) -> usize {
        self.inner.len()
    }

    fn read<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
        PyBytes::new(py, &self.inner)
    }

    #[pyo3(signature = (start, length=None))]
    fn slice(&self, start: usize, length: Option<usize>) -> PyResult<Self> {
        let total = self.inner.len();
        if start > total {
            return Err(PyValueError::new_err(format!(
                "slice start {} exceeds buffer length {}",
                start, total
            )));
        }

        let end = match length {
            Some(len) => start
                .checked_add(len)
                .ok_or_else(|| PyValueError::new_err("slice length overflow"))?,
            None => total,
        };

        if end > total {
            return Err(PyValueError::new_err(format!(
                "slice end {} exceeds buffer length {}",
                end, total
            )));
        }

        Ok(Self {
            inner: self.inner.slice(start..end),
        })
    }

    unsafe fn __getbuffer__(
        slf: PyRefMut<'_, Self>,
        view: *mut pyo3::ffi::Py_buffer,
        flags: c_int,
    ) -> PyResult<()> {
        if view.is_null() {
            return Err(pyo3::exceptions::PyBufferError::new_err(
                "view pointer is null",
            ));
        }

        if (flags & pyo3::ffi::PyBUF_WRITABLE) == pyo3::ffi::PyBUF_WRITABLE {
            return Err(pyo3::exceptions::PyBufferError::new_err(
                "PacketView is read-only",
            ));
        }

        let bytes = &slf.inner;

        // Format string for unsigned char buffer protocol
        static FORMAT: &[u8] = b"B\0";

        unsafe {
            (*view).buf = bytes.as_ptr() as *mut c_void;
            (*view).len = bytes.len() as isize;
            (*view).readonly = 1;
            (*view).itemsize = 1;
            // Format string: "B" = unsigned char (required for proper buffer protocol support)
            (*view).format = FORMAT.as_ptr() as *mut c_char;
            (*view).ndim = 1;
            // For 1D contiguous arrays, shape and strides can be NULL (means C-contiguous)
            // Previously these pointed to stack memory which would be invalid after return
            (*view).shape = std::ptr::null_mut();
            (*view).strides = std::ptr::null_mut();
            (*view).suboffsets = std::ptr::null_mut();
            (*view).internal = std::ptr::null_mut();

            let obj_ptr = slf.into_ptr();
            pyo3::ffi::Py_INCREF(obj_ptr);
            (*view).obj = obj_ptr;
        }

        Ok(())
    }

    unsafe fn __releasebuffer__(&self, _view: *mut pyo3::ffi::Py_buffer) {}
}

impl PacketView {
    /// Internal constructor from Bytes (not exposed to Python)
    pub fn from_bytes(bytes: Bytes) -> Self {
        Self { inner: bytes }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn packet_view_empty() {
        Python::attach(|py| {
            let data = PyBytes::new(py, b"");
            let buffer = PacketView::new(&data);
            assert_eq!(buffer.__len__(), 0);
        });
    }

    #[test]
    fn packet_view_read_roundtrip() {
        Python::attach(|py| {
            let original = b"test data";
            let data = PyBytes::new(py, original);
            let buffer = PacketView::new(&data);
            let readback = buffer.read(py);
            assert_eq!(buffer.__len__(), 9);
            assert_eq!(readback.as_bytes(), original);
        });
    }
    #[test]
    fn packet_view_slice_with_length() {
        Python::attach(|py| {
            let data = PyBytes::new(py, b"0123456789");
            let buffer = PacketView::new(&data);
            let sliced = buffer.slice(2, Some(5)).expect("slice");
            assert_eq!(sliced.__len__(), 5);
            assert_eq!(sliced.read(py).as_bytes(), b"23456");
        });
    }

    #[test]
    fn packet_view_slice_to_end() {
        Python::attach(|py| {
            let data = PyBytes::new(py, b"0123456789");
            let buffer = PacketView::new(&data);
            let sliced = buffer.slice(5, None).expect("slice");
            assert_eq!(sliced.__len__(), 5);
            assert_eq!(sliced.read(py).as_bytes(), b"56789");
        });
    }

    #[test]
    fn packet_view_slice_empty() {
        Python::attach(|py| {
            let data = PyBytes::new(py, b"0123456789");
            let buffer = PacketView::new(&data);
            let sliced = buffer.slice(5, Some(0)).expect("slice");
            assert_eq!(sliced.__len__(), 0);
        });
    }

    #[test]
    fn packet_view_slice_at_boundary() {
        Python::attach(|py| {
            let data = PyBytes::new(py, b"0123456789");
            let buffer = PacketView::new(&data);
            // Start at beginning
            let sliced = buffer.slice(0, Some(10)).expect("slice");
            assert_eq!(sliced.read(py).as_bytes(), b"0123456789");
```

End-to-end runtime sequence recap:

1. Controller starts, loads config/DB, and waits for websocket nodes.
2. Dataplane starts, connects to controller, sends `StartUp`.
3. Controller responds with startup config and peer topology hints.
4. Controller installs routes/flows/groups; dataplane stages until topology-ready conditions are met.
5. Dataplane local readers + processors + schedulers forward packets, while reporters push metrics and flow events upstream.
6. DB notifications trigger incremental route/flow/group updates, keeping live nodes converged.
7. Python API can embed the same dataplane behavior in-process for experiments and tooling.

Use this page as a code-first map when making changes: start from the step corresponding to your symptom and follow the adjacent modules in this exact order.
