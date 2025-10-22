/// Implements utility functions for the controller.
use std::collections::{HashMap, HashSet};
use std::net::Ipv4Addr;

use petgraph::Direction;
use petgraph::graph::DiGraph;
use tracing::{debug, info};

use nextmini_messages::{
    ControllerToDataplane, Flow, FlowLen, FlowSpec, INVALID, NodeSpec, OperatingMode, Protocol,
    RoutingTableEntry, SchedulingDiscipline,
};

use crate::config;
use crate::models::{DbFlow, Route};
use crate::routing;
use crate::routing::RoutingProtocol;
use crate::topology::topo;

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

/// Builds routes from topology edges using the specified routing protocol.
pub fn build_routes_from_topology(
    edges: &[(u32, u32)],
    protocol: &Option<config::RoutingProtocol>,
) -> Vec<(u32, u32, Vec<(u32, u32)>)> {
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
            let mut routes = Vec::new();

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

/// Merge all routes from configuration (both custom and topology-generated).
pub fn merge_all_routes(config: &config::Config) -> Vec<(u32, u32, Vec<(u32, u32)>)> {
    let mut routes = Vec::new();

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
            // if the node is the destination, we add it to the next hops
            if route.dst_node_id == node_id {
                next_hops = vec![node_id as usize]; // local delivery
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
                "RoutingTableEntry node {}: route_id={} src={} dst={} next_hops={:?}",
                node_id, e.route_id, e.src_node_id, e.dst_node_id, e.next_hops
            );
        }

        Some(ControllerToDataplane::InstallRoutes {
            routes: route_entries,
        })
    }
}
