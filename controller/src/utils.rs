/// Implements utility functions for the controller.
use nextmini_messages::{ControllerToDataplane, Protocol, SimpleRouteEntry};

use crate::models::Route;

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
        println!("No more nodes can be added to this subnet");
        None
    } else {
        Some(new_virtual_addr)
    }
}

pub fn build_startup_message(
    node_id: usize,
    virtual_addr: [u8; 4],
    net_mask: [u8; 4],
    session_id: [u8; 4],
    num_interfaces: usize,
    protocol: Protocol,
) -> ControllerToDataplane {
    ControllerToDataplane::StartUp {
        node_id,
        addr: virtual_addr,
        net_mask,
        session_id,
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

/// Builds an enhanced routing table for a specific node.
/// Now includes src/dst node information for proper flow matching.
/// Computes route_id -> next_hop mapping with flow matching context.
pub fn build_routes_for_node(routes: Vec<Route>, node_id: i32) -> Option<ControllerToDataplane> {
    let mut route_entries: Vec<SimpleRouteEntry> = Vec::new();

    for route in routes {
        // Find the position of this node in the route path
        let idx = route.route.iter().position(|&x| x == node_id);

        let next_hop = match idx {
            // The node is the destination - next hop is itself (local delivery)
            Some(i) if i == route.route.len() - 1 => route.route[i] as usize,
            // The node is in the middle of the path - next hop is the next node
            Some(i) => route.route[i + 1] as usize,
            // The route doesn't pass through this node - mark as inactive
            None => 0,
        };

        route_entries.push(SimpleRouteEntry {
            route_id: route.route_id as usize,
            next_hop,
            src_node_id: route.src_node_id as usize,
            dst_node_id: route.dst_node_id as usize,
        });
    }

    if route_entries.is_empty() {
        None
    } else {
        Some(ControllerToDataplane::InstallRoutes {
            routes: route_entries,
        })
    }
}
