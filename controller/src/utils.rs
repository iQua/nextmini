/// Implements utility functions for the controller.
use nextmini_messages::{
    ControllerToDataplane, Flow, FlowLen, FlowSpec, NodeSpec, OperatingMode, Protocol,
    RoutingTableEntry, SchedulingDiscipline,
};

use crate::models::{DbFlow, Route};
use tracing::debug;

/// Builds a startup message for the dataplane, which includes basic information about the node.
pub fn build_startup_response(
    node_id: usize,
    net_mask: std::net::Ipv4Addr,
    virtual_base_addr: std::net::Ipv4Addr,
    user_space_base_addr: std::net::Ipv4Addr,
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
pub fn build_routes_for_node(routes: Vec<Route>, node_id: i32) -> Option<ControllerToDataplane> {
    let mut route_entries: Vec<RoutingTableEntry> = Vec::new();

    debug!(
        "Building routes for node {}, total routes to process: {}",
        node_id,
        routes.len()
    );

    for route in routes {
        debug!(
            "Processing route_id: {}, path: {:?}, src_node_id: {}, dst_node_id: {}",
            route.route_id, route.route, route.src_node_id, route.dst_node_id
        );

        // finds the position of this node in the route path
        let idx = route.route.iter().position(|&x| x == node_id);

        let next_hop = if let Some(idx) = idx {
            if idx == route.route.len() - 1 {
                // The node is the destination - next hop is itself (local delivery)
                route.route[idx] as usize
            } else {
                // The node is in the middle of the path - next hop is the next node
                route.route[idx + 1] as usize
            }
        } else {
            // Node not in route path — set next_hop to 0
            debug!(
                "Node {} is not in route {:?}, setting next_hop to 0.",
                node_id, route.route
            );

            0
        };

        debug!(
            "Node {} is found in the route {:?}, setting the next_hop to {}.",
            node_id, route.route, next_hop
        );

        // sends route endpoints for dataplane's direction indexing
        let src_node_id = route.route[0] as usize; // Route source
        let dst_node_id = route.route[route.route.len() - 1] as usize; // Route destination

        debug!(
            "Using src_node_id: {} (first hop), dst_node_id: {} (last hop)",
            src_node_id, dst_node_id
        );

        route_entries.push(RoutingTableEntry {
            route_id: route.route_id as usize,
            next_hop,
            src_node_id,
            dst_node_id,
        });

        debug!(
            "Added route entry: route_id={}, next_hop={}, src_node_id={}, dst_node_id={}",
            route.route_id, next_hop, src_node_id, dst_node_id
        );
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
        Some(ControllerToDataplane::InstallRoutes {
            routes: route_entries,
        })
    }
}
