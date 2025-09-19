/// Implements utility functions for the controller.
use std::net::Ipv4Addr;

use tracing::{debug, info};

use nextmini_messages::{
    ControllerToDataplane, Flow, FlowLen, FlowSpec, NodeSpec, OperatingMode, Protocol,
    RoutingTableEntry, SchedulingDiscipline,
};

use crate::models::{DbFlow, Route};
use crate::route::RoutingProtocol;
use crate::{config, route, topo};

use petgraph::graph::DiGraph;

/// Builds a startup message for the dataplane, which includes basic information about the node.
pub fn build_startup_response(
    node_id: usize,
    net_mask: Ipv4Addr,
    virtual_base_addr: Ipv4Addr,
    user_space_base_addr: Ipv4Addr,
    external_base_addr: Ipv4Addr,
    max_server_port: u16,
    protocol: Protocol,
    scheduler_type: SchedulingDiscipline,
    nodes: Option<NodeSpec>,
) -> ControllerToDataplane {
    // Set default node specification if None
    let node_spec = nodes.unwrap_or(NodeSpec {
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
    let message_flows: Vec<Flow> = flows
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

    ControllerToDataplane::AddFlows {
        flows: message_flows,
    }
}

/// Builds routes from topology edges using the specified routing protocol
pub fn build_routes_from_topology(
    edges: &[(u32, u32)],
    protocol: &config::RoutingProtocol,
) -> Vec<(u32, u32, Vec<(u32, u32)>)> {
    match protocol {
        config::RoutingProtocol::ShortestPath => {
            let mut nodes: Vec<u32> = edges.iter().flat_map(|(a, b)| [*a, *b]).collect();
            nodes.sort_unstable();
            nodes.dedup();

            // builds graph as we only need the paths
            let graph = petgraph::graph::UnGraph::<u32, ()>::from_edges(edges);
            let mut shortest_path = route::ShortestPath::new(graph.clone());
            let mut routes = Vec::new();

            // generates shortest path for every (src, dst) pair
            for &src_node_id in &nodes {
                for &dst_node_id in &nodes {
                    if src_node_id == dst_node_id {
                        continue;
                    }

                    // finds node indices in the graph
                    let src_idx = graph
                        .node_indices()
                        .find(|&i| graph[i] == src_node_id)
                        .expect("Src node should exist in graph");
                    let dst_idx = graph
                        .node_indices()
                        .find(|&i| graph[i] == dst_node_id)
                        .expect("Dst node should exist in graph");

                    let path = shortest_path.compute_route(src_idx, dst_idx);
                    if path.len() >= 2 {
                        // converts path to edges
                        let path_edges = path
                            .windows(2)
                            .map(|win| (graph[win[0]], graph[win[1]]))
                            .collect::<Vec<_>>();

                        // returns (src_node_id, dst_node_id, edges)
                        // since we the db now needs to know the src and dst node ids(struct Route in model.rs)
                        routes.push((src_node_id, dst_node_id, path_edges));
                    }
                }
            }

            routes
        }
    }
}

/// Merges all routes from configuration (both custom and topology-generated).
pub fn merge_all_routes(config: &config::Config) -> Vec<(u32, u32, Vec<(u32, u32)>)> {
    let mut routes = Vec::new();

    // adds custom routes (infer src/dst from DiGraph)
    for route in &config.routes {
        if !route.route.is_empty() {
            let graph = DiGraph::<u32, ()>::from_edges(&route.route);
            let dst_node_id = graph
                .node_indices()
                .find(|&id| graph.neighbors_directed(id, petgraph::Outgoing).count() == 0)
                .unwrap();
            let src_node_id = graph
                .node_indices()
                .find(|&id| graph.neighbors_directed(id, petgraph::Incoming).count() == 0)
                .unwrap();
            routes.push((graph[src_node_id], graph[dst_node_id], route.route.clone()));
        }
    }

    // adds topology routes (with explicit src/dst)
    if let Some(edges) = topo::build_topology_edges_from_config(config) {
        let protocol = config
            .routing
            .protocol
            .clone()
            .unwrap_or(config::RoutingProtocol::ShortestPath);

        let topology_routes = build_routes_from_topology(&edges, &protocol);
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
        "Building routes for node {}, total routes: {}",
        node_id,
        routes.len()
    );

    for route in &routes {
        // finds next hop for current node in the path
        let next_hops = route
            .edges
            .iter()
            .find(|&&(src, _)| src == node_id)
            .map(|&(_, dst)| vec![dst as usize])
            .unwrap_or_else(|| {
                if route.dst_node_id == node_id {
                    vec![node_id as usize] // local delivery
                } else {
                    info!(
                        "Node {} is not in route {}, setting next_hop to 0.",
                        node_id, route.route_id
                    );
                    vec![0]
                }
            });

        route_entries.push(RoutingTableEntry {
            route_id: route.route_id,
            next_hops,
            src_node_id: route.src_node_id as usize,
            dst_node_id: route.dst_node_id as usize,
        });
    }

    if route_entries.is_empty() {
        info!("No routes for node {}", node_id);
        None
    } else {
        info!(
            "Finished building routes for node {}, total route entries: {}",
            node_id,
            route_entries.len()
        );
        Some(ControllerToDataplane::InstallRoutes {
            routes: route_entries,
        })
    }
}
