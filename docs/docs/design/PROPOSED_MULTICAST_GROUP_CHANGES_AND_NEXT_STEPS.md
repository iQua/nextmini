File: docs/docs/design/multicast-groups.md
Change: **New design document — end‑to‑end plan for multicast group support (source‑created, dynamic join/leave)**

```md
# Multicast Groups in Nextmini

This document proposes an end-to-end design to add **multicast group** support to Nextmini. It enables a source node to create a group and send to a **group IP**, while any number of destination nodes can **join or leave** the group at any time. Packets are **replicated** along a multicast tree (a DAG), reusing the controller’s route representation and the dataplane’s packet scheduler.

---

## Goals

- **(S, G) semantics**: Each group `G` has exactly one **source node** `S`. Multiple groups can co-exist.
- **Dynamic membership**: Destinations can join/leave `G` at any time. Controller updates the multicast tree and pushes new next-hops.
- **Data path replication**: Dataplane **duplicates packets to all next hops** at each branching node (already supported by our multicast fan-out change).
- **Backwards compatible**: Unicast routes and flows continue to work unchanged.
- **Minimal control plane surface**: Add small set of messages & tables to manage groups and memberships.

Non-goals (for the first iteration):
- IGMP compatibility / L3 snooping on arbitrary apps (we’ll use a control message–based join/leave).
- Multi-source groups (*,G). We implement (S,G) where S is the owner/creator.

---

## High-Level Architecture

1. **Group identity**  
   - `group_id` (integer) and `group_ip` (IPv4 in a reserved range, e.g., `239.255.0.0/16` inside the virtual network).
   - `group_ip` is where the source application sends traffic (TUN sees packets to `group_ip`).

2. **Control-plane (Controller)**
   - New DB tables: `groups`, `group_members`, `group_routes`.
   - New WS protocol (MessagePack) messages for CreateGroup/JoinGroup/LeaveGroup and route installs.
   - Tree computation: **Union of shortest paths** from `S` to each member, producing a **directed acyclic graph** (DAG) of edges.
   - Per-node **next_hops** built from DAG; pushed via `InstallGroupRoutes`.

3. **Dataplane**
   - Maintains a **group directory** (map `group_ip -> group_id`) installed by controller.
   - RoutingTable supports **two keys**:
     - `Unicast(src_node, dst_node)`
     - `Multicast(src_node, group_id)`
   - Packet classification: if `dst_ip` ∈ group directory ⇒ multicast; else unicast.
   - `get_next_hops_by_flow` returns **all next hops** for multicast; **Processor** duplicates packet to each next hop.
   - Leaf nodes that are members receive **local-delivery** via `next_hops` containing the local node id. Non-members never have local delivery for that group.

4. **Membership dynamics**
   - **Join**: controller adds row to `group_members`, recomputes DAG, updates `group_routes`, and pushes `InstallGroupRoutes`.
   - **Leave**: controller removes member, recomputes DAG. If no more members, routes become empty and the controller may optionally tear down the group.

---

## Control Messages (summary)

- **Dataplane → Controller**
  - `CreateGroup { label }` (from source)
  - `JoinGroup { group_id }` (from any node)
  - `LeaveGroup { group_id }`

- **Controller → Dataplane**
  - `GroupCreated { group_id, group_ip, src_node_id }` (to source)
  - `InstallGroupDirectory { groups: [{group_id, group_ip}] }` (to all nodes; incremental updates supported)
  - `InstallGroupRoutes { group_id, src_node_id, routes: [GroupRoutingTableEntry...] }` (to all nodes that appear in the DAG)

---

## Data Model

- `groups(group_id SERIAL PK, label TEXT UNIQUE, src_node_id INT NOT NULL, group_ip TEXT UNIQUE NOT NULL, created_at BIGINT)`
- `group_members(group_id INT FK, node_id INT, joined_at BIGINT, PRIMARY KEY (group_id, node_id))`
- `group_routes(group_id INT FK, src_node_id INT NOT NULL, edges JSONB NOT NULL, updated_at BIGINT)`
  - `edges` is `[[a,b],[b,c],...]` representing the multicast DAG.

Triggers/Notifications:
- On `group_members` INSERT/DELETE → `pg_notify('sync_group_routes', '{"group_id":...}')`.
- Controller listener recomputes routes and pushes new installs.

---

## Tree Computation

- **Union of shortest paths**: For each member `m`, run shortest path `S → m` over the **topology** (existing controller graph). Union all edges; direct edges in both directions (bidirectional graph) as needed for forwarding semantics.
- Store as `group_routes.edges` (directed).
- Build per-node **next_hops** = all outgoing neighbors in the DAG. For a **member** node, include `local_node_id` in `next_hops` to cause local delivery.

---

## Dataplane Behavior

- On `InstallGroupDirectory`: update group IP → id map.
- On `InstallGroupRoutes`: update the `RoutingTable`:
  - Insert/replace `route_id = group_id` next hop sets for all nodes.
  - Map key `Multicast(src, group_id)` → `[route_id]`.
- On packet:
  - If `dst_ip` in map: treat as multicast, key becomes `(S, G)`, replicate to **all** next hops returned.
  - If a hop equals local node id: deliver to TUN (local).

## Observability & Safety

- Existing metrics aggregation works per-hop; aggregate bytes reflect replication.
- No loops: tree is DAG from controller. TTL is relied upon as a safety backstop; we do not generate cycles.

---

## Backward Compatibility

- Unicast messages unchanged; new variants introduced.
- RoutingTable supports both `Unicast` and `Multicast` keys.
- Rollout: deploy controller first; dataplane ignores unknown messages until upgraded.

---

## Test Plan (summary)

- **Unit**: route key selection; group dir lookup; DAG → per-node next_hops; processor fan-out.
- **Integration**: source creates group; N nodes join; controller recompute; verify each member receives; remove member; verify updates; tear down group.
- **Performance**: saturate multicast with 2, 4, 8 branches; ensure no queue starvation or deadlocks.

```

---

File: messages/src/lib.rs
Change: **Add multicast types and messages for group lifecycle, directory, and route installs**

```rs
// --- New: Group identity types ---
pub type GroupId = usize;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct GroupDirectoryEntry {
    pub group_id: GroupId,
    #[serde(with = "ip_ser")]
    pub group_ip: Ipv4Addr,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct GroupRoutingTableEntry {
    /// Use the group_id as the route id for multicast routes.
    pub route_id: usize,
    pub next_hops: Vec<usize>,
    pub src_node_id: usize,
    pub group_id: GroupId,
}
```

```rs
// --- Extend DataplaneToController with group control ---
#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "type")]
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
    UserFlowStart {
        flows: Vec<UserFlowStart>,
    },
    AppFlowStart {
        appflows: Vec<AppFlow>,
    },
    RouteAssigned {
        assignments: Vec<RouteAssignment>,
    },
    // NEW:
    CreateGroup {
        label: String,
    },
    JoinGroup {
        group_id: GroupId,
    },
    LeaveGroup {
        group_id: GroupId,
    },
}
```

```rs
// --- Extend ControllerToDataplane with group directory & routes ---
#[derive(Serialize, Deserialize, PartialEq, Debug)]
#[serde(tag = "type")]
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
    // NEW:
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
```

---

File: controller/src/db.rs
Change: **Create new DB tables for groups, memberships, and multicast routes (and notifications)**

```rs
async fn create_db(pool: &Pool<Postgres>) {
    // ... existing table creations ...

    // NEW: groups table
    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS groups (
            id SERIAL PRIMARY KEY,
            label TEXT UNIQUE NOT NULL,
            src_node_id INTEGER NOT NULL,
            group_ip TEXT UNIQUE NOT NULL,
            created_at BIGINT DEFAULT (EXTRACT(EPOCH FROM NOW())::BIGINT*1000)
        )
        "#
    )
    .execute(pool)
    .await
    .expect("Failed to create groups table");

    // NEW: group_members table
    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS group_members (
            group_id INTEGER NOT NULL REFERENCES groups(id) ON DELETE CASCADE,
            node_id INTEGER NOT NULL,
            joined_at BIGINT DEFAULT (EXTRACT(EPOCH FROM NOW())::BIGINT*1000),
            PRIMARY KEY (group_id, node_id)
        )
        "#
    )
    .execute(pool)
    .await
    .expect("Failed to create group_members table");

    // NEW: group_routes table (stores multicast DAG edges for a group)
    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS group_routes (
            group_id INTEGER NOT NULL REFERENCES groups(id) ON DELETE CASCADE,
            src_node_id INTEGER NOT NULL,
            edges JSONB NOT NULL,
            updated_at BIGINT DEFAULT (EXTRACT(EPOCH FROM NOW())::BIGINT*1000),
            PRIMARY KEY (group_id)
        )
        "#
    )
    .execute(pool)
    .await
    .expect("Failed to create group_routes table");

    // NEW: trigger for membership changes -> recompute routes
    let create_function_sql = r#"
        CREATE OR REPLACE FUNCTION notify_group_membership_change()
        RETURNS TRIGGER AS $$
        DECLARE gid TEXT;
        BEGIN
            IF (TG_OP = 'INSERT') THEN
                gid := NEW.group_id;
            ELSE
                gid := OLD.group_id;
            END IF;
            PERFORM pg_notify('sync_group_routes', '{"group_id":"' || gid || '"}');
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
    "#;

    let create_trigger_sql = r#"
        CREATE TRIGGER group_membership_change_trigger
        AFTER INSERT OR DELETE ON group_members
        FOR EACH ROW
        EXECUTE FUNCTION notify_group_membership_change();
    "#;

    // idempotent trigger creation
    sqlx::query(create_function_sql).execute(pool).await.expect("Failed to create group membership function");
    sqlx::query(create_trigger_sql).execute(pool).await.expect("Failed to create group membership trigger");
}
```

```rs
// NEW: listen and serve group route recomputation + pushes
pub async fn setup_group_notification(
    db_pool: Arc<Pool<Postgres>>,
    node_ws: Arc<RwLock<HashMap<usize, Arc<Mutex<WebSocketWriter>>>>>,
) {
    let mut listener = PgListener::connect_with(&db_pool)
        .await
        .expect("Failed to connect listener");
    listener
        .listen("sync_group_routes")
        .await
        .expect("Failed to listen to sync_group_routes");

    tokio::spawn(async move {
        let mut stream = listener.into_stream();
        while let Some(notification) = stream.next().await {
            match notification {
                Ok(notif) => {
                    let payload = notif.payload();
                    // parse {"group_id":"..."}
                    let gid_opt = serde_json::from_str::<serde_json::Value>(payload)
                        .ok()
                        .and_then(|v| v.get("group_id").and_then(|x| x.as_str()).and_then(|s| s.parse::<i32>().ok()));

                    if let Some(group_id) = gid_opt {
                        // recompute group DAG; persist; build next_hops; push to nodes
                        if let Err(e) = recompute_and_push_group_routes(group_id, &db_pool, &node_ws).await {
                            error!("Failed to recompute/push group {} routes: {}", group_id, e);
                        }
                    } else {
                        error!("Malformed sync_group_routes payload: {}", payload);
                    }
                }
                Err(e) => error!("Error receiving group notification: {}", e),
            }
        }
    });
}

// Skeleton of recompute function; full impl ties into utils helpers.
async fn recompute_and_push_group_routes(
    group_id: i32,
    db_pool: &Pool<Postgres>,
    node_ws: &Arc<RwLock<HashMap<usize, Arc<Mutex<WebSocketWriter>>>>>,
) -> Result<(), anyhow::Error> {
    // 1) Load group, members, topology
    // 2) Compute union-of-shortest-paths DAG edges
    // 3) Upsert group_routes (edges JSONB)
    // 4) For each node, compute next_hops and send InstallGroupRoutes
    // (See controller/src/utils.rs additions.)
    Ok(())
}
```

---

File: controller/src/models.rs
Change: **Add models for groups, memberships, and group routes**

```rs
#[derive(FromRow, Debug)]
pub struct Group {
    pub id: i32,
    pub label: String,
    pub src_node_id: i32,
    pub group_ip: String,
}

#[derive(FromRow, Debug)]
pub struct GroupMember {
    pub group_id: i32,
    pub node_id: i32,
}

#[derive(Clone, FromRow, Debug)]
pub struct DbGroupRoute {
    pub group_id: i32,
    pub src_node_id: i32,
    pub edges: serde_json::Value,
}
```

---

File: controller/src/utils.rs
Change: **Helpers to compute group DAG and per-node next hops; build InstallGroupRoutes**

```rs
use petgraph::graph::DiGraph;
use petgraph::{algo::astar, graph::NodeIndex};
use std::collections::{HashMap, HashSet};

use crate::models::Route as UniRoute;
use crate::models::DbGroupRoute;
use nextmini_messages::{ControllerToDataplane, SchedulingDiscipline};
use super::config;
use super::routing::RoutingProtocol;

// NEW: Compute union of shortest paths S->members over given undirected edges to form a DAG.
pub fn compute_group_tree_edges(
    src_node_id: u32,
    member_node_ids: &[u32],
    undirected_edges: &[(u32, u32)],
) -> Vec<(u32, u32)> {
    // Build bidirectional DiGraph for shortest paths
    let mut edges = Vec::with_capacity(undirected_edges.len() * 2);
    for &(a, b) in undirected_edges {
        edges.push((a, b));
        edges.push((b, a));
    }
    let (node_ids, node_map, graph) = create_graph_with_mapping(&edges);

    let mut seen: HashSet<(u32, u32)> = HashSet::new();
    let mut dag: Vec<(u32, u32)> = Vec::new();

    if let Some(&src_idx) = node_map.get(&src_node_id) {
        let mut sp = crate::routing::ShortestPath::new(graph.clone());
        for &m in member_node_ids {
            if let Some(&dst_idx) = node_map.get(&m) {
                let path = sp.compute_route(src_idx, dst_idx);
                for win in path.windows(2) {
                    let u = node_ids[win[0].index()];
                    let v = node_ids[win[1].index()];
                    if seen.insert((u, v)) {
                        dag.push((u, v));
                    }
                }
            }
        }
    }

    dag
}

// NEW: Build next_hops for a group DAG, for a specific node.
pub fn build_group_routes_for_node(
    group_id: usize,
    src_node_id: u32,
    dag_edges: &[(u32, u32)],
    node_id: u32,
    member_node_ids: &HashSet<u32>,
) -> Option<nextmini_messages::ControllerToDataplane> {
    let (_node_ids, node_map, graph) = create_graph_with_mapping(dag_edges);

    // compute outgoing neighbors from node_id in DAG
    let mut next_hops: Vec<usize> = Vec::new();
    if let Some(&node_idx) = node_map.get(&node_id) {
        for n in graph.neighbors_directed(node_idx, petgraph::Direction::Outgoing) {
            next_hops.push(graph[n] as usize);
        }
    }

    // If this node is a MEMBER, include local delivery by adding itself.
    if member_node_ids.contains(&node_id) {
        next_hops.push(node_id as usize);
    }

    if next_hops.is_empty() {
        return None;
    }

    use nextmini_messages::GroupRoutingTableEntry;
    let entry = GroupRoutingTableEntry {
        route_id: group_id,
        next_hops,
        src_node_id: src_node_id as usize,
        group_id,
    };

    Some(ControllerToDataplane::InstallGroupRoutes {
        group_id,
        src_node_id: src_node_id as usize,
        routes: vec![entry],
    })
}
```

---

File: dataplane/src/node/route.rs
Change: **Introduce `RouteKey` (Unicast/Multicast), group directory & installers; unify next-hop lookup**

```rs
use ahash::AHashMap;
use std::net::Ipv4Addr;
use nextmini_messages::{INVALID, RoutingTableEntry, GroupDirectoryEntry, GroupRoutingTableEntry, GroupId};
use crate::node::config::LocalConfig;
use crate::node::controller::flowstats::FlowStatsReporterHandle;
use crate::node::flow;
use crate::node::{FlowId, FlowIdExt, NodeId};

// NEW: Routing key supports unicast and multicast
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum RouteKey {
    Unicast(NodeId, NodeId),
    Multicast(NodeId, GroupId),
}

#[derive(Clone)]
pub struct RoutingTable {
    pub local_id: NodeId,
    config: LocalConfig,

    // --- Unicast + Multicast ---
    available_routes: AHashMap<RouteKey, Vec<usize>>, // key -> route ids
    route_next_hop: AHashMap<usize, Vec<NodeId>>,      // route id -> next hops

    // Group directory: group_ip -> group_id
    group_dir: AHashMap<Ipv4Addr, GroupId>,

    jump_hasher: jumphash::JumpHasher,
    cache: AHashMap<FlowId, usize>,
}

impl RoutingTable {
    pub fn new(config: LocalConfig) -> Self {
        Self {
            route_next_hop: AHashMap::default(),
            available_routes: AHashMap::default(),
            local_id: config.node_id,
            config,
            jump_hasher: jumphash::JumpHasher::new_with_keys(0x1234567890ABCDEF, 0xFEDCBA0987654321),
            cache: AHashMap::default(),
            group_dir: AHashMap::default(),
        }
    }

    // --- Installers ---

    /// Unicast installer (existing behavior)
    pub fn install_routes(&mut self, routes: Vec<RoutingTableEntry>) {
        // clear unicast cache entries; keep group_dir and multicast state
        self.cache.clear();

        for route in routes {
            self.route_next_hop.insert(route.route_id, route.next_hops.clone());
            let key = RouteKey::Unicast(route.src_node_id, route.dst_node_id);
            self.available_routes.entry(key).or_default().push(route.route_id);
        }
    }

    /// NEW: Install/refresh the group directory (ip->group_id).
    pub fn install_group_directory(&mut self, groups: Vec<GroupDirectoryEntry>) {
        self.group_dir.clear();
        for g in groups {
            self.group_dir.insert(g.group_ip, g.group_id);
        }
    }

    /// NEW: Install multicast routes for a specific group.
    pub fn install_group_routes(
        &mut self,
        group_id: GroupId,
        src_node_id: NodeId,
        routes: Vec<GroupRoutingTableEntry>,
    ) {
        // Replace entries for this group & source
        // Remove old route ids for (src,group) key
        let key = RouteKey::Multicast(src_node_id, group_id);
        self.available_routes.remove(&key);

        for r in routes {
            self.route_next_hop.insert(r.route_id, r.next_hops.clone());
            self.available_routes.entry(key).or_default().push(r.route_id);
        }

        // Clear per-flow selection cache so flows can pick updated route ids
        self.cache.clear();
    }

    // --- Lookups ---

    /// Determine routing key from a flow id.
    fn key_for_flow(&self, flow_id: FlowId) -> Option<RouteKey> {
        if flow_id == flow::INVALID_FLOW_ID {
            return None;
        }
        let (src_node, _dst_node) = self.config.extract_node_ids_from_flow(flow_id);
        let dst_ip = flow_id.dst_ip();

        if let Some(&gid) = self.group_dir.get(&dst_ip) {
            Some(RouteKey::Multicast(src_node, gid))
        } else {
            let dst_node = self.config.ip_to_node_id(dst_ip);
            Some(RouteKey::Unicast(src_node, dst_node))
        }
    }

    /// Select a route id for the given key (consistent hashing when multiple).
    fn select_route_for_key(&mut self, key: &RouteKey) -> Option<usize> {
        let ids = self.available_routes.get(key)?;
        if ids.is_empty() {
            return None;
        }
        if ids.len() == 1 {
            return Some(ids[0]);
        }
        let slot = self.jump_hasher.slot(key, ids.len() as u32);
        Some(ids[slot as usize])
    }

    /// Returns *all* next hops for the selected route at this node (multicast-aware).
    pub fn get_next_hops_by_flow(
        &mut self,
        flow_id: FlowId,
        flowstats_reporter: Option<&FlowStatsReporterHandle>,
    ) -> Result<Vec<NodeId>, String> {
        if flow_id == flow::INVALID_FLOW_ID {
            return Err("No route can be selected.".to_string());
        }

        // Cache by flow id to keep consistent selection
        if let Some(route_id) = self.cache.get(&flow_id) {
            if let Some(hops) = self.route_next_hop.get(route_id) {
                if hops.contains(&INVALID) {
                    return Err(format!("Route {} invalid at this node", route_id));
                }
                return Ok(hops.clone());
            }
            // cache miss fallthrough
        }

        let key = self
            .key_for_flow(flow_id)
            .ok_or_else(|| "Unable to build route key for flow".to_string())?;

        let route_id = self
            .select_route_for_key(&key)
            .ok_or_else(|| "No route ids available for route key".to_string())?;

        // Record/report selection for app-flows from source node (reuses existing logic)
        if let Some(flowstats_reporter) = flowstats_reporter {
            let (src_node_id, _) = self.config.extract_node_ids_from_flow(flow_id);
            let is_app_flow = flow_id.dst_port() != self.config.user_space_server_port
                && flow_id.src_port() != self.config.user_space_server_port;
            if src_node_id == self.local_id && is_app_flow {
                flowstats_reporter.report_route_assigned(flow_id, route_id);
            }
        }

        // Cache and return hops
        self.cache.insert(flow_id, route_id);

        let hops = self
            .route_next_hop
            .get(&route_id)
            .ok_or_else(|| format!("No next hops found for route {}", route_id))?;

        if hops.contains(&INVALID) {
            return Err(format!("Route {} invalid at this node", route_id));
        }

        Ok(hops.clone())
    }
}
```

---

File: dataplane/src/node/processor.rs
Change: **Multicast-aware fan-out (uses `get_next_hops_by_flow`)**

```rs
impl Processor {
    /// Processes inbound packets for outbound delivery (supports multicast fan-out).
    async fn process_packet(&mut self, packet: Packet) {
        let packet_flow_id = packet.flow_id;

        match self
            .routing_table
            .get_next_hops_by_flow(packet_flow_id, self.flowstats_reporter.as_ref())
        {
            Ok(next_hops) => {
                if next_hops.is_empty() {
                    error!("No next hops available for flow {}.", packet_flow_id);
                    return;
                }

                let last = next_hops.len() - 1;
                for (i, next_hop_id) in next_hops.into_iter().enumerate() {
                    let pkt = if i == last { packet.clone() } else { packet.clone() };
                    self.send_packet(pkt, next_hop_id).await;
                }
            }
            Err(e) => error!("Error getting the next hops: {}", e),
        }
    }
}
```

---


File: controller/src/config.rs
Change: **(Plan hook) Optionally reserve a multicast address pool in controller config**

```rs
#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct Config {
    // ...
    // NEW: Optional multicast pool to allocate group IPs (inside virtual network space)
    #[serde(default = "default_multicast_pool_base")]
    pub multicast_pool_base: Ipv4Addr,
    #[serde(default = "default_multicast_pool_mask")]
    pub multicast_pool_mask: Ipv4Addr,
    // ...
}

fn default_multicast_pool_base() -> Ipv4Addr { Ipv4Addr::new(239, 255, 0, 0) }
fn default_multicast_pool_mask() -> Ipv4Addr { Ipv4Addr::new(255, 255, 0, 0) }
```

---

File: docs/docs/examples/multicast-flow.md
Change: **New example workflow (CLI/API level)**

```md
# Example: Multicast Flow

1. **Source** (node 1) → Controller:
```

CreateGroup { label: "job-42" }

```
Controller replies to node 1:
```

GroupCreated { group_id: 7, group_ip: 239.255.0.10, src_node_id: 1 }
InstallGroupDirectory { groups: [{group_id:7, group_ip:239.255.0.10}] } // broadcast to all nodes

```

2. **Destinations** (e.g., nodes 3 and 6) → Controller:
```

JoinGroup { group_id: 7 }

```
Controller recomputes group DAG for (S=1,G=7) and pushes:
```

InstallGroupRoutes { group_id:7, src_node_id:1, routes:[... per-node entries ...] }

```

3. **Source app** sends TCP/UDP to **239.255.0.10**.  
Dataplane recognizes the IP → group_id=7, performs multicast fan-out on each branching node.

4. **Node 6 leaves**:
```

LeaveGroup { group_id:7 }

```
Controller recomputes DAG and pushes updated `InstallGroupRoutes`.
```

---

### Notes & Next Steps

* **Message handling wiring** (not shown here) updates:

  * Dataplane conductor should route:

    * `InstallGroupDirectory` → broadcast to processors (`UpdateGroupDirectory(groups)`).
    * `InstallGroupRoutes` → broadcast (`UpdateGroupRoutes(group_id, src_node_id, entries)`).
  * Controller WS layer to accept `CreateGroup/JoinGroup/LeaveGroup`, allocate `group_ip`, persist, and trigger recompute.

* **Security**: For now, permit any node to join a group. Future: add ACLs on `group_members`.

* **Garbage collection**: If a group has 0 members for X minutes, optionally delete it.

* **Incremental installs**: The `InstallGroupRoutes` is designed to be idempotent and can be sent only to nodes affected by a change to reduce churn.

This plan is designed so that the **controller owns the multicast DAG** and the **dataplane merely follows next_hops**, keeping the fast-path simple and consistent with the existing unicast pipeline.
