/// Implements utility functions for the controller.
use nextmini_messages::{ControllerToDataplane, Protocol, SimpleRouteEntry};

use crate::models::Route;
use tracing::{debug, info};

/// Creates a new virtual address by adding the node ID to the base address.
pub fn create_new_virtual_addr(
    base_ipv4_addr: [u8; 4],
    ipv4_net_mask: [u8; 4],
    node_id: usize,
) -> Option<[u8; 4]> {
    // makes a copy of the base address
    let base_ip = u32::from_be_bytes(base_ipv4_addr);
    let new_ip = base_ip.wrapping_add(node_id as u32);
    let new_virtual_addr = new_ip.to_be_bytes();
    let net_mask = u32::from_be_bytes(ipv4_net_mask);
    let network = base_ip & net_mask;
    let new_network = new_ip & net_mask;

    // checks if the new virtual address is outside the subnet
    if network != new_network {
        info!("No more nodes can be added to this subnet");
        None
    } else {
        Some(new_virtual_addr)
    }
}

pub fn build_startup_message(
    node_id: usize,
    virtual_addr: [u8; 4],
    net_mask: [u8; 4],
    num_interfaces: usize,
    protocol: Protocol,
) -> ControllerToDataplane {
    ControllerToDataplane::StartUp {
        node_id,
        addr: virtual_addr,
        net_mask,
        num_interfaces,
        protocol,
    }
}

pub fn build_add_node_message(
    protocol: Protocol,
    node_id: usize,
    addr: String,
) -> ControllerToDataplane {
    ControllerToDataplane::AddNode {
        protocol,
        node_id,
        addr,
    }
}

/// Builds route-level next-hop information for a specific node.
/// Controller only computes route_id -> next_hop mappings.
/// Dataplane handles flow_id -> route_id mapping autonomously.
/// Only processes routes that include the current node.
pub fn build_routes_for_node(routes: Vec<Route>, node_id: i32) -> Option<ControllerToDataplane> {
    let mut route_entries: Vec<SimpleRouteEntry> = Vec::new();

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

        // Find the position of this node in the route path
        let idx = route.route.iter().position(|&x| x == node_id);

        // Skip routes that don't include this node
        if idx.is_none() {
            debug!(
                "Node {} not in route path {:?}, skipping",
                node_id, route.route
            );
            continue;
        }

        let idx = idx.unwrap();
        let next_hop = if idx == route.route.len() - 1 {
            // The node is the destination - next hop is itself (local delivery)
            route.route[idx] as usize
        } else {
            // The node is in the middle of the path - next hop is the next node
            route.route[idx + 1] as usize
        };

        debug!(
            "Node {} found at position {} in route, next_hop: {}",
            node_id, idx, next_hop
        );

        // Send route endpoints for dataplane's direction indexing
        let src_node_id = route.route[0] as usize; // Route source
        let dst_node_id = route.route[route.route.len() - 1] as usize; // Route destination

        debug!(
            "Using src_node_id: {} (first hop), dst_node_id: {} (last hop)",
            src_node_id, dst_node_id
        );

        route_entries.push(SimpleRouteEntry {
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
