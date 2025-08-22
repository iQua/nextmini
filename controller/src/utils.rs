/// Implements utility functions for the controller.
use std::net::Ipv4Addr;
use std::collections::hash_map::RandomState;

use tracing::{debug, info};
use petgraph::graphmap::DiGraphMap;
use petgraph::graph::{UnGraph, NodeIndex};
use petgraph::Direction::{Outgoing, Incoming};
use petgraph::algo::simple_paths::all_simple_paths;

use nextmini_messages::{
    ControllerToDataplane, Flow, FlowLen, FlowSpec, NodeSpec, OperatingMode, Protocol,
    RoutingTableEntry, SchedulingDiscipline,
};

use crate::models::{DbFlow, Route};
use crate::route::{ShortestPath, RoutingProtocol};

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

/// Builds route-level next-hop information for a specific node.
pub fn build_routes_for_node(routes: Vec<Route>, node_id: u32) -> Option<ControllerToDataplane> {
    let mut route_entries: Vec<RoutingTableEntry> = Vec::new();

    debug!(
        "Building routes for node {}, total routes to process: {}",
        node_id,
        routes.len()
    );

    for route in &routes {
        match route.directed {
            true => {
                // DiGraph is not used because it will always start NodeIndex from 0 no matter if it is in the edges.
                // If multicast is needed in the future, DiGraphMap can be switched to DiGraph easily so as to 
                // leverage petgraph::algo instead of current src_node, dst_node, next_hops search pattern.
                let graph = DiGraphMap::<u32, ()>::from_edges(&route.edges);

                // assumes single-source, single-destination routes
                let dst_node_id = graph.nodes().find(|id| graph.neighbors_directed(*id, Outgoing).count() == 0).unwrap();
                let src_node_id = graph.nodes().find(|id| graph.neighbors_directed(*id, Incoming).count() == 0).unwrap();
                
                debug!(
                    "Using src_node_id: {} (first hop), dst_node_id: {} (last hop)",
                    src_node_id, dst_node_id
                );
                
                // finds next hops
                let next_hops = if graph.contains_node(node_id) {
                    if graph.neighbors_directed(node_id, Outgoing).count() == 0 {
                        // The node is the destination - next hop is itself (local delivery)
                        vec![node_id as usize]
                    } else {
                        // The node is in the middle of the route - next hops are the neighbors
                        graph.neighbors(node_id).map(|id| id as usize).collect::<Vec<_>>()
                    }
                } else {
                    // The node is not in the route - setting next_hop to 0
                    debug!(
                        "Node {} is not in DAG {:?}, setting next_hop to 0.",
                        node_id, graph
                    );

                    vec![0]
                };

                debug!(
                    "Added customized route entry: route_id={}, next_hops={:?}, src_node_id={}, dst_node_id={}.",
                    route.route_id, next_hops, src_node_id, dst_node_id
                );

                route_entries.push(RoutingTableEntry {
                    route_id: route.route_id,
                    next_hops,
                    src_node_id: src_node_id as usize,
                    dst_node_id: dst_node_id as usize,
                });

            }
            false => {
                let current_node = NodeIndex::from(node_id);
                let graph = UnGraph::<u32, ()>::from_edges(&route.edges);
                let mut shortest_path = ShortestPath::new(graph.clone());

                // assumes there is only one topology, the route_id for topologies starts after the last route
                let mut route_id = routes.len();

                // finds the next hop in the shortest path for each (src_node_id, dst_node_id) pair
                for src_node_id in 1..graph.node_count() {
                    for dst_node_id in 1..graph.node_count() {
                        if src_node_id == dst_node_id {
                            continue;
                        }

                        // increments the route_id
                        route_id += 1;
                        
                        let path = shortest_path.compute_route(NodeIndex::from(src_node_id as u32), NodeIndex::from(dst_node_id as u32));

                        // finds next hop for current node in the path
                        let next_hop = if let Some(idx) = path.iter().position(|idx| idx == &current_node) {
                            if idx == path.len() - 1 {
                                // The node is the destination – next hop is itself (local delivery)
                                path[idx].index() as usize
                            } else {
                                // The node is in the middle of the path – next hop is the next node
                                path[idx + 1].index() as usize
                            }
                        } else {
                            // The node is not in the path - setting next_hop to 0
                            debug!(
                                "Node {} is not in the shortest path {:?} from {} to {}, setting next_hop to 0.",
                                node_id, path, src_node_id, dst_node_id
                            );

                            0
                        };

                        debug!(
                            "Added topology route entry: route_id={}, next_hops={:?}, src_node_id={}, dst_node_id={}.",
                            route_id, next_hop, src_node_id, dst_node_id
                        );

                        route_entries.push(RoutingTableEntry {
                            route_id,
                            next_hops: vec![next_hop],
                            src_node_id,
                            dst_node_id,
                        });
                    }
                }
            }
        }
    }

    if route_entries.is_empty() {
        debug!("No routes for node {}", node_id);

        None
    } else {
        debug!(
            "Finished building routes for node {}, total route entries: {}",
            node_id,
            route_entries.len()
        );

        Some(ControllerToDataplane::InstallRoutes { routes: route_entries })
    }
}

/// This function is intensionally kept unused for future needs.
/// Calculates all paths between a (src_node_id, dst_node_id) pair in a UnGraph and finds all next hops for a certain node.
/// Uses petgraph::algo::simple_paths::all_simple_paths and is inefficient for large graphs.
#[allow(dead_code)]
fn find_next_hops_for_pair(
    graph: &UnGraph<u32, ()>,
    current_node: NodeIndex,
    src_node_id: u32,
    dst_node_id: u32,
) -> Vec<usize> {
    let mut next_hops = Vec::new();

    for path in all_simple_paths::<Vec<_>, _, RandomState>(
        graph,
        NodeIndex::from(src_node_id),
        NodeIndex::from(dst_node_id),
        0,
        None,
    ) {
        if let Some(idx) = path.iter().position(|idx| idx == &current_node) {
            if idx == path.len() - 1 {
                // The node is the destination – next hop is itself (local delivery)
                next_hops.push(path[idx].index() as usize);
                break;
            } else {
                // The node is in the middle of the path – next hop is the next node
                next_hops.push(path[idx + 1].index() as usize);
            }
        }
    }

    if next_hops.is_empty() {
        // if no next hops are found - set next hop to 0
        info!(
            "No next hops found for node {} from {} to {}.",
            current_node.index(),
            src_node_id,
            dst_node_id
        );

        next_hops.push(0);
    }

    next_hops
}
