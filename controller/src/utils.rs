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
        return false;
    };

    let mut reachable = HashSet::new();
    let mut stack = vec![src_idx];
    while let Some(node_idx) = stack.pop() {
        if !reachable.insert(node_idx) {
            continue;
        }
        for neighbor in graph.neighbors_directed(node_idx, Direction::Outgoing) {
            stack.push(neighbor);
        }
    }

    let mut dst_count = 0;
    for node_idx in reachable {
        let outgoing = graph
            .neighbors_directed(node_idx, Direction::Outgoing)
            .count();
        let incoming = graph
            .neighbors_directed(node_idx, Direction::Incoming)
            .count();
        if outgoing == 0 && incoming > 0 {
            dst_count += 1;
            if dst_count > 1 {
                return true;
            }
        }
    }

    false
}

/// Builds routes from topology edges using the specified routing protocol.
pub fn build_routes_from_topology(
    edges: &[(u32, u32)],
    protocol: &Option<config::RoutingProtocol>,
) -> RouteCollection {
    match protocol {
        None => Vec::new(),
        Some(config::RoutingProtocol::ShortestPath) => {
            // Convert undirected edges to bidirectional directed edges
            let mut bidirectional_edges = Vec::new();
            for &(a, b) in edges {
                bidirectional_edges.push((a, b));
                bidirectional_edges.push((b, a));
            }

            let (node_ids, _node_map, graph) = create_graph_with_mapping(&bidirectional_edges);

            let mut shortest_path = routing::ShortestPath::new(graph.clone());
            let mut routes: RouteCollection = Vec::new();

            // generates shortest path for every node pair
            for src_idx in graph.node_indices() {
                for dst_idx in graph.node_indices() {
                    if src_idx == dst_idx {
                        continue;
                    }

                    let path = shortest_path.compute_route(src_idx, dst_idx);
                    if path.len() >= 2 {
                        // uses node_idx.index() directly as array index
                        let path_edges = path
                            .windows(2)
                            .map(|win| {
                                let src_id = node_ids[win[0].index()];
                                let dst_id = node_ids[win[1].index()];
                                (src_id, dst_id)
                            })
                            .collect::<Vec<_>>();

                        // returns (src_node_id, dst_node_id, edges)
                        // since we the db now needs to know the src and dst node ids(struct Route in model.rs)
                        let src_id = node_ids[src_idx.index()];
                        let dst_id = node_ids[dst_idx.index()];
                        routes.push((src_id, dst_id, path_edges));
                    }
                }
            }

            routes
        }
    }
}

/// Allocates a deterministic multicast IP address within the configured pool.
pub fn allocate_multicast_ip(base_addr: Ipv4Addr, mask: Ipv4Addr, ordinal: u32) -> Ipv4Addr {
    let network = u32::from(base_addr) & u32::from(mask);
    let host_mask = !u32::from(mask);

    if host_mask == 0 {
        warn!(
            "Multicast mask {} does not provide host space; reusing base {}",
            mask, base_addr
        );
        return base_addr;
    }

    let offset = (ordinal.saturating_sub(1)) & host_mask;
    Ipv4Addr::from(network | offset)
}

/// Computes a deterministic multicast route ID for a `(group_id, tree_id)` pair.
///
/// The result is bounded so it can be safely namespaced under the dataplane's
/// multicast route flag (`MULTICAST_ROUTE_FLAG`).
pub fn compute_multitree_route_id(group_id: GroupId, tree_id: usize) -> Result<usize, String> {
    if tree_id >= MULTITREE_STRIDE {
        return Err(format!(
            "tree_id {} exceeds stride {}",
            tree_id, MULTITREE_STRIDE
        ));
    }

    let base = group_id
        .checked_mul(MULTITREE_STRIDE)
        .ok_or_else(|| format!("group_id {} overflows route-id computation", group_id))?;
    let route_id = base
        .checked_add(tree_id)
        .ok_or_else(|| "route-id addition overflowed".to_string())?;

    if route_id >= MULTICAST_ROUTE_FLAG {
        return Err(format!(
            "route_id {} must stay below {}",
            route_id, MULTICAST_ROUTE_FLAG
        ));
    }

    Ok(route_id)
}

/// Returns a canonicalized copy of group trees: sorted by tree_id ascending and validated.
pub fn canonicalize_group_route_trees(
    group_id: GroupId,
    trees: &[GroupRouteTree],
) -> Result<Vec<GroupRouteTree>, String> {
    let mut ordered = trees.to_vec();
    ordered.sort_by_key(|tree| tree.tree_id);

    let mut previous_tree_id = None;
    for tree in &ordered {
        if previous_tree_id == Some(tree.tree_id) {
            return Err(format!(
                "duplicate tree_id {} for group {}",
                tree.tree_id, group_id
            ));
        }
        compute_multitree_route_id(group_id, tree.tree_id)?;
        previous_tree_id = Some(tree.tree_id);
    }

    Ok(ordered)
}

/// Build one per-node multicast routing entry for a specific precomputed route ID.
pub fn build_group_routes_for_node_with_route_id(
    group_id: GroupId,
    route_id: usize,
    src_node_id: u32,
    dag_edges: &[(u32, u32)],
    node_id: u32,
    member_node_ids: &HashSet<u32>,
) -> Option<GroupRoutingTableEntry> {
    if dag_edges.is_empty() && !member_node_ids.contains(&node_id) {
        return None;
    }

    let (_node_ids, node_map, graph) = create_graph_with_mapping(dag_edges);

    let mut next_hops = Vec::new();
    if let Some(&node_idx) = node_map.get(&node_id) {
        for neighbor in graph.neighbors_directed(node_idx, Direction::Outgoing) {
            next_hops.push(graph[neighbor] as usize);
        }
    }

    if member_node_ids.contains(&node_id) {
        next_hops.push(node_id as usize);
    }

    if next_hops.is_empty() {
        return None;
    }

    Some(GroupRoutingTableEntry {
        route_id,
        next_hops,
        src_node_id: src_node_id as usize,
        group_id,
    })
}

/// Build all per-node multicast routing entries for a multi-tree group update.
///
/// Output routes are canonically sorted by `tree_id` ascending.
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
            max_server_port: 8081,
            protocol: Protocol::Tcp,
            scheduler_type: SchedulingDiscipline::Fifo,
            node_spec: None,
        };

        let message = build_startup_response(params);
        match message {
            ControllerToDataplane::StartUp {
                node_id, node_spec, ..
            } => {
                assert_eq!(node_id, 3);
                assert_eq!(
                    node_spec,
                    NodeSpec {
                        node_id: 3,
                        operating_mode: OperatingMode::Normal,
                    }
                );
            }
            _ => panic!("Expected StartUp message."),
        }
    }

    #[test]
    fn test_build_startup_response_uses_node_spec() {
        let params = StartupResponseParams {
            node_id: 5,
            net_mask: Ipv4Addr::new(255, 255, 0, 0),
            virtual_base_addr: Ipv4Addr::new(10, 0, 0, 0),
            user_space_base_addr: Ipv4Addr::new(192, 168, 0, 0),
            external_base_addr: Ipv4Addr::new(172, 16, 0, 0),
            max_server_port: 8081,
            protocol: Protocol::Tcp,
            scheduler_type: SchedulingDiscipline::Fifo,
            node_spec: Some(NodeSpec {
                node_id: 5,
                operating_mode: OperatingMode::Max,
            }),
        };

        let message = build_startup_response(params);
        match message {
            ControllerToDataplane::StartUp { node_spec, .. } => {
                assert_eq!(
                    node_spec,
                    NodeSpec {
                        node_id: 5,
                        operating_mode: OperatingMode::Max,
                    }
                );
            }
            _ => panic!("Expected StartUp message."),
        }
    }

    #[test]
    fn test_build_flows_for_node_skips_invalid_and_missing_routes() {
        let flows = vec![
            DbFlow {
                id: 1,
                src_node_id: 1,
                dst_node_id: 2,
                flow_len_type: "bytes".to_string(),
                flow_len_bytes: Some(128),
                flow_len_duration: None,
                flow_rate: Some(64),
                flow_weight: Some(1),
                is_finished: false,
                is_probe: false,
            },
            DbFlow {
                id: 2,
                src_node_id: 1,
                dst_node_id: 3,
                flow_len_type: "duration".to_string(),
                flow_len_bytes: None,
                flow_len_duration: Some(1.0),
                flow_rate: None,
                flow_weight: None,
                is_finished: false,
                is_probe: false,
            },
            DbFlow {
                id: 3,
                src_node_id: 1,
                dst_node_id: 4,
                flow_len_type: "unknown".to_string(),
                flow_len_bytes: None,
                flow_len_duration: None,
                flow_rate: None,
                flow_weight: None,
                is_finished: false,
                is_probe: false,
            },
            DbFlow {
                id: 4,
                src_node_id: 2,
                dst_node_id: 5,
                flow_len_type: "bytes".to_string(),
                flow_len_bytes: Some(32),
                flow_len_duration: None,
                flow_rate: None,
                flow_weight: None,
                is_finished: false,
                is_probe: false,
            },
        ];
        let flow_routes = vec![DbFlowRoute {
            flow_id: 1,
            route_id: 99,
        }];

        let message = build_flows_for_node(flows, &flow_routes, FlowTransport::LosslessUnicast);
        match message {
            ControllerToDataplane::AddFlows { flows } => {
                assert_eq!(flows.len(), 2);
                let flow_one = flows.iter().find(|f| f.controller_id == Some(1)).unwrap();
                let flow_four = flows.iter().find(|f| f.controller_id == Some(4)).unwrap();

                assert_eq!(flow_one.route_id, Some(99));
                assert_eq!(flow_one.flow_spec.flow_len, FlowLen::Bytes(128));
                assert_eq!(flow_four.route_id, None);
                assert_eq!(flow_four.flow_spec.flow_len, FlowLen::Bytes(32));
            }
            _ => panic!("Expected AddFlows message."),
        }
    }

    #[test]
    fn test_build_flows_for_node_accepts_duration_without_rate_for_tcp() {
        let flows = vec![
            DbFlow {
                id: 1,
                src_node_id: 1,
                dst_node_id: 2,
                flow_len_type: "duration".to_string(),
                flow_len_bytes: None,
                flow_len_duration: Some(2.5),
                flow_rate: None,
                flow_weight: None,
                is_finished: false,
                is_probe: false,
            },
            DbFlow {
                id: 2,
                src_node_id: 2,
                dst_node_id: 3,
                flow_len_type: "bytes".to_string(),
                flow_len_bytes: None,
                flow_len_duration: None,
                flow_rate: None,
                flow_weight: None,
                is_finished: false,
                is_probe: false,
            },
        ];

        let message = build_flows_for_node(flows, &[], FlowTransport::Tcp);
        match message {
            ControllerToDataplane::AddFlows { flows } => {
                assert_eq!(flows.len(), 2);
                let duration_flow = flows.iter().find(|f| f.controller_id == Some(1)).unwrap();
                let bytes_flow = flows.iter().find(|f| f.controller_id == Some(2)).unwrap();

                assert_eq!(duration_flow.flow_spec.flow_len, FlowLen::Duration(2.5));
                assert_eq!(bytes_flow.flow_spec.flow_len, FlowLen::Bytes(0));
                assert_eq!(duration_flow.route_id, None);
                assert_eq!(bytes_flow.route_id, None);
            }
            _ => panic!("Expected AddFlows message."),
        }
    }

    #[test]
    fn test_route_has_multiple_destinations_unicast_linear() {
        // Linear path: 1 -> 2 -> 3
        let route = Route {
            route_id: 0,
            src_node_id: 1,
            dst_node_id: 3,
            edges: vec![(1, 2), (2, 3)],
        };

        assert!(!route_has_multiple_destinations(&route));
    }

    #[test]
    fn test_route_has_multiple_destinations_missing_source() {
        let route = Route {
            route_id: 0,
            src_node_id: 9,
            dst_node_id: 3,
            edges: vec![(1, 2), (2, 3)],
        };

        assert!(!route_has_multiple_destinations(&route));
    }

    #[test]
    fn test_route_has_multiple_destinations_unreachable_sink_ignored() {
        let route = Route {
            route_id: 0,
            src_node_id: 1,
            dst_node_id: 3,
            edges: vec![(1, 2), (2, 3), (4, 5)],
        };

        assert!(!route_has_multiple_destinations(&route));
    }

    #[test]
    fn test_route_has_multiple_destinations_multicast_simple() {
        // Multicast: 1 -> 2, then 2 -> 3 and 2 -> 4
        let route = Route {
            route_id: 0,
            src_node_id: 1,
            dst_node_id: 3, // Primary destination
            edges: vec![(1, 2), (2, 3), (2, 4)],
        };

        assert!(route_has_multiple_destinations(&route));
    }

    #[test]
    fn test_route_has_multiple_destinations_empty_edges() {
        // Empty route
        let route = Route {
            route_id: 0,
            src_node_id: 1,
            dst_node_id: 2,
            edges: vec![],
        };

        assert!(!route_has_multiple_destinations(&route));
    }

    #[test]
    fn test_route_has_multiple_destinations_dag_with_reconvergence() {
        // DAG with reconvergence (still unicast since only one final destination)
        let route = Route {
            route_id: 0,
            src_node_id: 1,
            dst_node_id: 4,
            edges: vec![(1, 2), (1, 3), (2, 4), (3, 4)],
        };

        assert!(!route_has_multiple_destinations(&route));
    }

    #[test]
    fn test_route_has_multiple_destinations_diamond_with_multiple_dsts() {
        // Diamond with multiple destinations
        let route = Route {
            route_id: 0,
            src_node_id: 1,
            dst_node_id: 4,
            edges: vec![(1, 2), (1, 3), (2, 4), (3, 5)],
        };

        assert!(route_has_multiple_destinations(&route));
    }

    #[test]
    fn test_build_routes_from_topology_none() {
        let routes = build_routes_from_topology(&[(1, 2)], &None);
        assert!(routes.is_empty());
    }

    #[test]
    fn test_build_routes_from_topology_empty_edges() {
        let protocol = Some(config::RoutingProtocol::ShortestPath);
        let routes = build_routes_from_topology(&[], &protocol);
        assert!(routes.is_empty());
    }

    #[test]
    fn test_build_routes_from_topology_shortest_path() {
        let protocol = Some(config::RoutingProtocol::ShortestPath);
        let routes = build_routes_from_topology(&[(1, 2)], &protocol);

        assert_eq!(routes.len(), 2);
        assert!(
            routes
                .iter()
                .any(|(src, dst, edges)| *src == 1 && *dst == 2 && *edges == vec![(1, 2)])
        );
        assert!(
            routes
                .iter()
                .any(|(src, dst, edges)| *src == 2 && *dst == 1 && *edges == vec![(2, 1)])
        );
    }

    #[test]
    fn test_build_routes_from_topology_multihop_path() {
        let protocol = Some(config::RoutingProtocol::ShortestPath);
        let routes = build_routes_from_topology(&[(1, 2), (2, 3)], &protocol);

        assert!(
            routes
                .iter()
                .any(|(src, dst, edges)| *src == 1 && *dst == 3 && *edges == vec![(1, 2), (2, 3)])
        );
        assert!(
            routes
                .iter()
                .any(|(src, dst, edges)| *src == 3 && *dst == 1 && *edges == vec![(3, 2), (2, 1)])
        );
    }

    #[test]
    fn test_merge_all_routes_prefers_non_empty_custom_routes() {
        let mut config = config::Config::default();
        config.routes = vec![
            config::Route { route: Vec::new() },
            config::Route {
                route: vec![(0, 1), (1, 2)],
            },
        ];

        let routes = merge_all_routes(&config);
        assert_eq!(routes.len(), 1);
        assert_eq!(routes[0].0, 0);
        assert_eq!(routes[0].1, 2);
        assert_eq!(routes[0].2, vec![(0, 1), (1, 2)]);
    }

    #[test]
    fn test_merge_all_routes_non_contiguous_node_ids() {
        let mut config = config::Config::default();
        config.routes = vec![config::Route {
            route: vec![(10, 20), (20, 30)],
        }];

        let routes = merge_all_routes(&config);
        assert_eq!(routes.len(), 1);
        assert_eq!(routes[0].0, 10);
        assert_eq!(routes[0].1, 30);
        assert_eq!(routes[0].2, vec![(10, 20), (20, 30)]);
    }

    #[test]
    fn test_merge_all_routes_includes_topology_routes() {
        let mut config = config::Config::default();
        config.routing.protocol = Some(config::RoutingProtocol::ShortestPath);
        config.topology.edges = Some(vec![(1, 2)]);

        let routes = merge_all_routes(&config);
        assert_eq!(routes.len(), 2);
        assert!(
            routes
                .iter()
                .any(|(src, dst, edges)| *src == 1 && *dst == 2 && *edges == vec![(1, 2)])
        );
        assert!(
            routes
                .iter()
                .any(|(src, dst, edges)| *src == 2 && *dst == 1 && *edges == vec![(2, 1)])
        );
    }

    #[test]
    fn test_build_routes_for_node_empty_routes() {
        let result = build_routes_for_node(Vec::new(), 1);
        assert!(result.is_none());
    }

    #[test]
    fn test_build_routes_for_node_empty_edges_skipped() {
        let routes = vec![Route {
            route_id: 1,
            src_node_id: 1,
            dst_node_id: 2,
            edges: Vec::new(),
        }];

        let result = build_routes_for_node(routes, 1);
        assert!(result.is_none());
    }

    #[test]
    fn test_build_routes_for_node_unicast_entries() {
        let routes = vec![Route {
            route_id: 1,
            src_node_id: 1,
            dst_node_id: 3,
            edges: vec![(1, 2), (2, 3)],
        }];

        let cases = [(1, vec![2]), (3, vec![3])];
        for (node_id, expected_next_hops) in cases {
            let entries = expect_install_routes(build_routes_for_node(routes.clone(), node_id));
            assert_eq!(entries.len(), 1);
            let entry = &entries[0];
            assert_eq!(entry.route_id, 1);
            assert_eq!(entry.src_node_id, 1);
            assert_eq!(entry.dst_node_id, 3);
            assert!(matches!(entry.forward_mode, RouteForwardingMode::Unicast));
            assert_eq!(entry.next_hops, expected_next_hops);
        }

        let result = build_routes_for_node(routes, 5);
        assert!(result.is_none());
    }

    #[test]
    fn test_build_routes_for_node_deduplicates_next_hops() {
        let routes = vec![Route {
            route_id: 1,
            src_node_id: 1,
            dst_node_id: 2,
            edges: vec![(1, 2), (1, 2)],
        }];

        let entries = expect_install_routes(build_routes_for_node(routes, 1));
        assert_eq!(entries[0].next_hops, vec![2]);
    }

    #[test]
    fn test_build_routes_for_node_multicast_entries() {
        let routes = vec![Route {
            route_id: 1,
            src_node_id: 1,
            dst_node_id: 3,
            edges: vec![(1, 2), (2, 3), (2, 4)],
        }];

        let cases = [(1, vec![2]), (2, vec![3, 4]), (3, vec![3]), (4, vec![4])];
        for (node_id, mut expected_next_hops) in cases {
            let entries = expect_install_routes(build_routes_for_node(routes.clone(), node_id));
            assert_eq!(entries.len(), 1);
            let entry = &entries[0];
            assert_eq!(entry.route_id, 1);
            assert!(matches!(entry.forward_mode, RouteForwardingMode::Multicast));
            expected_next_hops.sort();
            let mut actual_next_hops = entry.next_hops.clone();
            actual_next_hops.sort();
            assert_eq!(actual_next_hops, expected_next_hops);
        }
    }

    #[test]
    fn test_build_routes_for_node_multiple_routes_mixed() {
        // Test with both unicast and multicast routes
        let routes = vec![
            Route {
                route_id: 1,
                src_node_id: 1,
                dst_node_id: 3,
                edges: vec![(1, 2), (2, 3)], // Unicast
            },
            Route {
                route_id: 2,
                src_node_id: 1,
                dst_node_id: 4,
                edges: vec![(1, 2), (2, 4), (2, 5)], // Multicast
            },
        ];

        let result = build_routes_for_node(routes, 2);
        assert!(result.is_some());

        if let Some(ControllerToDataplane::InstallRoutes { routes: entries }) = result {
            assert_eq!(entries.len(), 2);

            // Check first route (unicast)
            let unicast_entry = &entries[0];
            assert_eq!(unicast_entry.route_id, 1);
            assert!(matches!(
                unicast_entry.forward_mode,
                RouteForwardingMode::Unicast
            ));
            assert_eq!(unicast_entry.next_hops, vec![3]);

            // Check second route (multicast)
            let multicast_entry = &entries[1];
            assert_eq!(multicast_entry.route_id, 2);
            assert!(matches!(
                multicast_entry.forward_mode,
                RouteForwardingMode::Multicast
            ));
            let mut next_hops = multicast_entry.next_hops.clone();
            next_hops.sort();
            assert_eq!(next_hops, vec![4, 5]);
        } else {
            panic!("Expected InstallRoutes message.");
        }
    }

    #[test]
    fn test_create_graph_with_mapping() {
        // Test the internal graph creation function
        let edges = vec![(3, 1), (1, 2), (2, 5)];
        let (node_ids, node_map, graph) = create_graph_with_mapping(&edges);

        // Node IDs should be sorted
        assert_eq!(node_ids, vec![1, 2, 3, 5]);

        // Each node should be in the map
        assert!(node_map.contains_key(&1));
        assert!(node_map.contains_key(&2));
        assert!(node_map.contains_key(&3));
        assert!(node_map.contains_key(&5));

        // Graph should have correct number of nodes and edges
        assert_eq!(graph.node_count(), 4);
        assert_eq!(graph.edge_count(), 3);
    }

    #[test]
    fn test_allocate_multicast_ip_advances_within_pool() {
        let base = Ipv4Addr::new(239, 255, 0, 0);
        let mask = Ipv4Addr::new(255, 255, 0, 0);

        let first = allocate_multicast_ip(base, mask, 1);
        let second = allocate_multicast_ip(base, mask, 2);
        let wrap = allocate_multicast_ip(base, mask, 256 * 2);

        assert_eq!(first, Ipv4Addr::new(239, 255, 0, 0));
        assert_eq!(second, Ipv4Addr::new(239, 255, 0, 1));
        assert_eq!(wrap, Ipv4Addr::new(239, 255, 1, 255));
    }

    #[test]
    fn test_allocate_multicast_ip_uses_base_when_mask_is_full() {
        let base = Ipv4Addr::new(239, 255, 0, 0);
        let mask = Ipv4Addr::new(255, 255, 255, 255);

        let addr = allocate_multicast_ip(base, mask, 42);
        assert_eq!(addr, base);
    }

    #[test]
    fn test_allocate_multicast_ip_zero_ordinal_returns_network_base() {
        let base = Ipv4Addr::new(239, 255, 0, 0);
        let mask = Ipv4Addr::new(255, 255, 0, 0);

        let addr = allocate_multicast_ip(base, mask, 0);
        assert_eq!(addr, base);
    }

    #[test]
    fn test_build_group_routes_for_node_includes_local_delivery() {
        let dag_edges = vec![(1, 2), (2, 3), (2, 4)];
        let member_nodes = HashSet::from_iter([3u32]);

        let src_entry = build_group_routes_for_node(7, 1, &dag_edges, 1, &member_nodes).unwrap();
        assert_eq!(src_entry.next_hops, vec![2]);

        let member_entry = build_group_routes_for_node(7, 1, &dag_edges, 3, &member_nodes).unwrap();
        assert_eq!(member_entry.next_hops, vec![3]);

        assert!(
            build_group_routes_for_node(7, 1, &dag_edges, 5, &member_nodes).is_none(),
            "Non-participants should not receive route entries"
        );
    }

    #[test]
    fn test_build_group_routes_for_node_member_with_forwarding() {
        let dag_edges = vec![(1, 2), (2, 3)];
        let member_nodes = HashSet::from_iter([2u32]);

        let entry = build_group_routes_for_node(7, 1, &dag_edges, 2, &member_nodes).unwrap();
        let mut next_hops = entry.next_hops.clone();
        next_hops.sort();
        assert_eq!(next_hops, vec![2, 3]);
    }

    #[test]
    fn test_build_group_routes_for_node_member_not_in_graph() {
        let member_nodes = HashSet::from_iter([9u32]);
        let cases = [Vec::new(), vec![(1, 2)]];

        for dag_edges in cases {
            let entry = build_group_routes_for_node(7, 1, &dag_edges, 9, &member_nodes).unwrap();
            assert_eq!(entry.next_hops, vec![9]);
        }
    }

    #[test]
    fn test_compute_multitree_route_id_bounds() {
        let route_id = compute_multitree_route_id(7, 3).unwrap();
        assert_eq!(route_id, 7 * MULTITREE_STRIDE + 3);
        assert!(route_id < MULTICAST_ROUTE_FLAG);

        let too_large_tree = compute_multitree_route_id(7, MULTITREE_STRIDE);
        assert!(too_large_tree.is_err());

        let max_group = (MULTICAST_ROUTE_FLAG / MULTITREE_STRIDE) - 1;
        assert!(compute_multitree_route_id(max_group, MULTITREE_STRIDE - 1).is_ok());
        assert!(compute_multitree_route_id(max_group + 1, 0).is_err());
    }

    #[test]
    fn test_build_group_routes_for_node_multitree_orders_by_tree_id() {
        let trees = vec![
            GroupRouteTree {
                tree_id: 4,
                weight: Some(0.7),
                edges: vec![(1, 2), (2, 5)],
            },
            GroupRouteTree {
                tree_id: 1,
                weight: Some(0.3),
                edges: vec![(1, 3), (3, 5)],
            },
        ];
        let members = HashSet::from_iter([5u32]);

        let routes = build_group_routes_for_node_multitree(11, 1, &trees, 1, &members).unwrap();
        assert_eq!(routes.len(), 2);

        let expected_first = compute_multitree_route_id(11, 1).unwrap();
        let expected_second = compute_multitree_route_id(11, 4).unwrap();
        assert_eq!(routes[0].route_id, expected_first);
        assert_eq!(routes[0].next_hops, vec![3]);
        assert_eq!(routes[1].route_id, expected_second);
        assert_eq!(routes[1].next_hops, vec![2]);
    }

    #[test]
    fn test_build_group_routes_for_node_multitree_rejects_duplicate_tree_id() {
        let trees = vec![
            GroupRouteTree {
                tree_id: 2,
                weight: None,
                edges: vec![(1, 2)],
            },
            GroupRouteTree {
                tree_id: 2,
                weight: None,
                edges: vec![(1, 3)],
            },
        ];
        let members = HashSet::new();

        let err = build_group_routes_for_node_multitree(9, 1, &trees, 1, &members).unwrap_err();
        assert!(err.contains("duplicate tree_id"));
    }
}
