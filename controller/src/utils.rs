/// Implements utility functions for the controller.
use std::collections::{HashMap, HashSet};
use std::net::Ipv4Addr;

use petgraph::Direction;
use petgraph::graph::DiGraph;
use tracing::{debug, info, warn};

use nextmini_messages::{
    ControllerToDataplane, Flow, FlowLen, FlowSpec, GroupId, GroupRoutingTableEntry, INVALID,
    NodeSpec, OperatingMode, Protocol, RouteForwardingMode, RoutingTableEntry,
    SchedulingDiscipline,
};

use crate::config;
use crate::models::{DbFlow, Route};
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
    pub multicast_pool_base: Ipv4Addr,
    pub multicast_pool_mask: Ipv4Addr,
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
        multicast_pool_base,
        multicast_pool_mask,
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
        multicast_pool_base,
        multicast_pool_mask,
        max_server_port,
        protocol,
        scheduler_type,
        node_spec,
    }
}

/// Builds an AddFlow message for flows.
pub fn build_flows_for_node(flows: Vec<DbFlow>) -> ControllerToDataplane {
    let flows: Vec<Flow> = flows
        .into_iter()
        .map(|flow| {
            debug!("Building an AddFlow message for flow id {}", flow.id);

            // converts database Flow to message Flow.
            let flow_len = match flow.flow_len_type.as_str() {
                "bytes" => FlowLen::Bytes(flow.flow_len_bytes.unwrap_or(0) as usize),
                "duration" => FlowLen::Duration(flow.flow_len_duration.unwrap_or(0.0)),
                _ => FlowLen::Bytes(0), // default fallback
            };

            Flow {
                controller_id: Some(flow.id),
                src_node_id: flow.src_node_id as usize,
                dst_node_id: flow.dst_node_id as usize,
                flow_spec: FlowSpec {
                    flow_len,
                    flow_rate: flow.flow_rate.map(|r| r as usize),
                    flow_weight: flow.flow_weight.map(|w| w as usize),
                },
            }
        })
        .collect();

    ControllerToDataplane::AddFlows { flows }
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

/// Compute a multicast DAG by unioning shortest paths from src to each member.
#[allow(dead_code)]
pub fn compute_group_tree_edges(
    src_node_id: u32,
    member_node_ids: &[u32],
    undirected_edges: &[(u32, u32)],
) -> Vec<(u32, u32)> {
    if member_node_ids.is_empty() || undirected_edges.is_empty() {
        return Vec::new();
    }

    let mut bidirectional = Vec::with_capacity(undirected_edges.len() * 2);
    for &(a, b) in undirected_edges {
        bidirectional.push((a, b));
        bidirectional.push((b, a));
    }

    let (_node_ids, node_map, graph) = create_graph_with_mapping(&bidirectional);
    let Some(&src_idx) = node_map.get(&src_node_id) else {
        warn!(
            "Source node {} missing from topology, unable to compute multicast DAG.",
            src_node_id
        );
        return Vec::new();
    };

    let mut seen = HashSet::new();
    let mut dag = Vec::new();
    let mut shortest_path = routing::ShortestPath::new(graph.clone());

    for &member in member_node_ids {
        if member == src_node_id {
            continue;
        }

        let Some(&dst_idx) = node_map.get(&member) else {
            warn!(
                "Member node {} missing from topology; skipping in multicast tree.",
                member
            );
            continue;
        };

        let path = shortest_path.compute_route(src_idx, dst_idx);
        for window in path.windows(2) {
            let from = graph[window[0]];
            let to = graph[window[1]];
            if seen.insert((from, to)) {
                dag.push((from, to));
            }
        }
    }

    dag
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
        route_id: group_id,
        next_hops,
        src_node_id: src_node_id as usize,
        group_id,
    })
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
                // this node does not belong to this route, but we still need to create
                // a routing table entry with INVALID to maintain consistency across all
                // nodes regarding the size of the routing tables
                next_hops = vec![INVALID];
            }
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
    use crate::models::Route;
    use std::collections::HashSet;

    #[test]
    fn test_compute_group_tree_edges_union_shortest_paths() {
        let dag =
            compute_group_tree_edges(1, &[3, 4, 5], &[(1, 2), (2, 3), (2, 4), (4, 5), (5, 6)]);

        let expected: HashSet<(u32, u32)> = [(1, 2), (2, 3), (2, 4), (4, 5)].into_iter().collect();
        let actual: HashSet<(u32, u32)> = dag.into_iter().collect();

        assert_eq!(actual, expected);
    }

    #[test]
    fn test_multicast_group_routes_include_member_delivery() {
        let dag_edges = vec![(1, 2), (2, 3), (2, 4)];
        let mut members = HashSet::new();
        members.insert(3);
        members.insert(4);

        let branch_entry =
            build_group_routes_for_node(7, 1, &dag_edges, 2, &members).expect("branch node");
        let mut next_hops = branch_entry.next_hops.clone();
        next_hops.sort();
        assert_eq!(next_hops, vec![3, 4]);

        let leaf_entry =
            build_group_routes_for_node(7, 1, &dag_edges, 3, &members).expect("member leaf");
        assert_eq!(leaf_entry.next_hops, vec![3]);

        let non_member = build_group_routes_for_node(7, 1, &dag_edges, 5, &members);
        assert!(non_member.is_none());
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
    fn test_route_has_multiple_destinations_unicast_single_hop() {
        // Direct path: 1 -> 2
        let route = Route {
            route_id: 0,
            src_node_id: 1,
            dst_node_id: 2,
            edges: vec![(1, 2)],
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
    fn test_route_has_multiple_destinations_multicast_complex() {
        // Complex multicast tree
        let route = Route {
            route_id: 0,
            src_node_id: 1,
            dst_node_id: 5,
            edges: vec![(1, 2), (2, 3), (2, 4), (3, 5), (4, 6)],
        };

        assert!(route_has_multiple_destinations(&route));
    }

    #[test]
    fn test_route_has_multiple_destinations_multicast_three_branches() {
        let route = Route {
            route_id: 0,
            src_node_id: 1,
            dst_node_id: 3,
            edges: vec![(1, 2), (2, 3), (2, 4), (2, 5)],
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
    fn test_route_has_multiple_destinations_dag_different_edge_order() {
        let route = Route {
            route_id: 0,
            src_node_id: 1,
            dst_node_id: 4,
            edges: vec![(1, 2), (2, 4), (1, 3), (3, 4)],
        };

        // Same result: still unicast (only one final destination)
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
    fn test_build_routes_for_node_unicast_mode() {
        // Test that unicast routes get RouteForwardingMode::Unicast
        let routes = vec![Route {
            route_id: 1,
            src_node_id: 1,
            dst_node_id: 3,
            edges: vec![(1, 2), (2, 3)],
        }];

        let result = build_routes_for_node(routes, 1);
        assert!(result.is_some());

        if let Some(ControllerToDataplane::InstallRoutes { routes: entries }) = result {
            assert_eq!(entries.len(), 1);
            let entry = &entries[0];
            assert_eq!(entry.route_id, 1);
            assert_eq!(entry.src_node_id, 1);
            assert_eq!(entry.dst_node_id, 3);
            assert!(matches!(entry.forward_mode, RouteForwardingMode::Unicast));
            // Node 1 is the source, should have next_hop to node 2
            assert_eq!(entry.next_hops, vec![2]);
        } else {
            panic!("Expected InstallRoutes message.");
        }
    }

    #[test]
    fn test_build_routes_for_node_multicast_mode() {
        // Test that multicast routes get RouteForwardingMode::Multicast
        let routes = vec![Route {
            route_id: 1,
            src_node_id: 1,
            dst_node_id: 3,
            edges: vec![(1, 2), (2, 3), (2, 4)], // Multicast to both 3 and 4
        }];

        let result = build_routes_for_node(routes, 1);
        assert!(result.is_some());

        if let Some(ControllerToDataplane::InstallRoutes { routes: entries }) = result {
            assert_eq!(entries.len(), 1);
            let entry = &entries[0];
            assert_eq!(entry.route_id, 1);
            assert!(matches!(entry.forward_mode, RouteForwardingMode::Multicast));
            // Node 1 should have next_hop to node 2
            assert_eq!(entry.next_hops, vec![2]);
        } else {
            panic!("Expected InstallRoutes message.");
        }
    }

    #[test]
    fn test_build_routes_for_node_multicast_intermediate_node() {
        // Test multicast routing table for intermediate node with multiple next hops
        let routes = vec![Route {
            route_id: 1,
            src_node_id: 1,
            dst_node_id: 3,
            edges: vec![(1, 2), (2, 3), (2, 4)],
        }];

        let result = build_routes_for_node(routes, 2);
        assert!(result.is_some());

        if let Some(ControllerToDataplane::InstallRoutes { routes: entries }) = result {
            assert_eq!(entries.len(), 1);
            let entry = &entries[0];
            assert_eq!(entry.route_id, 1);
            assert!(matches!(entry.forward_mode, RouteForwardingMode::Multicast));
            // Node 2 is the branch point, should have next_hops to both 3 and 4
            let mut next_hops = entry.next_hops.clone();
            next_hops.sort();
            assert_eq!(next_hops, vec![3, 4]);
        } else {
            panic!("Expected InstallRoutes message.");
        }
    }

    #[test]
    fn test_build_routes_for_node_multicast_destination() {
        // Test multicast routing table for a destination node
        let routes = vec![Route {
            route_id: 1,
            src_node_id: 1,
            dst_node_id: 3,
            edges: vec![(1, 2), (2, 3), (2, 4)],
        }];

        let result = build_routes_for_node(routes, 3);
        assert!(result.is_some());

        if let Some(ControllerToDataplane::InstallRoutes { routes: entries }) = result {
            assert_eq!(entries.len(), 1);
            let entry = &entries[0];
            assert_eq!(entry.route_id, 1);
            assert!(matches!(entry.forward_mode, RouteForwardingMode::Multicast));
            // Node 3 is a destination, should have next_hop to itself
            assert_eq!(entry.next_hops, vec![3]);
        } else {
            panic!("Expected InstallRoutes message.");
        }
    }

    #[test]
    fn test_build_routes_for_node_multicast_second_destination() {
        // Test multicast routing table for the second destination node (not in dst_node_id)
        // This test verifies the fix for the multicast bug where only the first destination
        let routes = vec![Route {
            route_id: 1,
            src_node_id: 1,
            dst_node_id: 3, // dst_node_id only records the first destination
            edges: vec![(1, 2), (2, 3), (2, 4)], // but edges include path to node 4
        }];

        let result = build_routes_for_node(routes, 4); // Test node 4
        assert!(result.is_some());

        if let Some(ControllerToDataplane::InstallRoutes { routes: entries }) = result {
            assert_eq!(entries.len(), 1);
            let entry = &entries[0];
            assert_eq!(entry.route_id, 1);
            assert!(matches!(entry.forward_mode, RouteForwardingMode::Multicast));
            assert_eq!(entry.next_hops, vec![4]);
        } else {
            panic!("Expected InstallRoutes message.");
        }
    }

    #[test]
    fn test_build_routes_for_node_not_in_route() {
        // Test routing table for a node not in the route
        let routes = vec![Route {
            route_id: 1,
            src_node_id: 1,
            dst_node_id: 3,
            edges: vec![(1, 2), (2, 3)],
        }];

        let result = build_routes_for_node(routes, 5); // Node 5 is not in the route
        assert!(result.is_some());

        if let Some(ControllerToDataplane::InstallRoutes { routes: entries }) = result {
            assert_eq!(entries.len(), 1);
            let entry = &entries[0];
            assert_eq!(entry.route_id, 1);
            // Node 5 is not in the route, should have INVALID next_hop
            assert_eq!(entry.next_hops, vec![INVALID]);
        } else {
            panic!("Expected InstallRoutes message.");
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
    fn test_group_id_to_ip_calculation() {
        // New simplified design: group_ip = base + group_id
        let base = Ipv4Addr::new(239, 255, 0, 0);
        let base_u32 = u32::from(base);

        let group_ip_1 = Ipv4Addr::from(base_u32 + 1);
        let group_ip_2 = Ipv4Addr::from(base_u32 + 2);
        let group_ip_520 = Ipv4Addr::from(base_u32 + 520);

        assert_eq!(group_ip_1, Ipv4Addr::new(239, 255, 0, 1));
        assert_eq!(group_ip_2, Ipv4Addr::new(239, 255, 0, 2));
        assert_eq!(group_ip_520, Ipv4Addr::new(239, 255, 2, 8));
    }

    #[test]
    fn test_compute_group_tree_edges_handles_multiple_branches() {
        let edges = vec![(1, 2), (2, 3), (2, 4), (4, 5)];
        let dag = compute_group_tree_edges(1, &[3, 5], &edges);
        let dag_set: HashSet<(u32, u32)> = dag.into_iter().collect();

        let expected = HashSet::from_iter([(1, 2), (2, 3), (2, 4), (4, 5)]);
        assert_eq!(dag_set, expected);
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
}
