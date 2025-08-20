/// Implements utility functions for the controller.
use std::net::Ipv4Addr;

use tracing::debug;
use petgraph::Direction;
use petgraph::graph::DiGraph;

use nextmini_messages::{
    ControllerToDataplane, Flow, FlowLen, FlowSpec, NodeSpec, OperatingMode, Protocol,
    RoutingTableEntry, SchedulingDiscipline,
};

use crate::models::{DbFlow, Route};

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

    for route in routes {
        let route_id = route.route_id;
        let directed = route.directed;
        let mut edges = route.edges;

        // adds the reverse edges for undirected routes
        if !directed{
            for (a, b) in edges.clone(){
                edges.push((b, a));
            }
        }

        debug!(
            "Processing route_id: {}, directed: {}, edges: {:?}, node_id: {}",
            route_id, directed, edges, node_id
        );

        // converts edges to u32
        let edges: Vec<(u32, u32)> = edges.into_iter().map(|(a, b)| (a as u32, b as u32)).collect();
        
        // builds the DiGraph
        let graph = DiGraph::<u32, ()>::from_edges(edges);

        // Locate the current node within the graph.
        let current_node_index = match graph
            .node_indices()
            .find(|&idx| graph[idx] == node_id)
        {
            Some(idx) => idx,
            None => {
                debug!("Node {} not found in graph, skipping route", node_id);
                continue;
            }
        };

        // determines the source and destination nodes.
        // assumes single-source, single-destination routes.
        let src_node_index = graph
            .node_indices()
            .find(|&idx| graph.neighbors_directed(idx, Direction::Incoming).count() == 0)
            .expect("Route must have a source node");
        let src_node_id = graph[src_node_index] as usize;

        let dst_node_index = graph
            .node_indices()
            .find(|&idx| graph.neighbors_directed(idx, Direction::Outgoing).count() == 0)
            .expect("Route must have a destination node");
        let dst_node_id = graph[dst_node_index] as usize;

        // finds the next hops for the current node
        let mut next_hops: Vec<usize> = graph
            .neighbors(current_node_index)
            .map(|idx| graph[idx] as usize)
            .collect();

        // accounts if at destination node
        if next_hops.is_empty() {
            next_hops.push(node_id as usize);
        }

        debug!(
            "Added route entry: route_id={}, next_hops={:?}, src_node_id={}, dst_node_id={}",
            route_id, next_hops, src_node_id, dst_node_id
        );

        route_entries.push(RoutingTableEntry {
            route_id,
            next_hops,
            src_node_id,
            dst_node_id,
        });
    }

    debug!(
        "Finished building routes for node {}, total route entries: {}",
        node_id,
        route_entries.len()
    );

    if route_entries.is_empty() {
        debug!("No routes for node {}", node_id);
        None
    } else {
        Some(ControllerToDataplane::InstallRoutes { routes: route_entries })
    }
}
