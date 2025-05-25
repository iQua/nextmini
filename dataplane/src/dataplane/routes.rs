use crate::dataplane::FlowId;
use crate::dataplane::NodeId;
use jumphash::JumpHasher;
use nextmini_messages::RoutingTableEntry;
use std::collections::HashMap;
use tracing::{debug, info};

/// The routing table in the dataplane.
#[derive(Clone)]
pub struct RoutingTable {
    /// Local node ID
    pub local_id: NodeId,

    /// Base IPv4 address for node ID calculation (e.g., [10, 0, 0, 0])
    base_ipv4_addr: [u8; 4],

    /// Source-destination pair -> available route IDs
    available_routes: HashMap<(NodeId, NodeId), Vec<usize>>,

    /// Route ID -> next hop
    route_next_hop: HashMap<usize, NodeId>,

    /// Jump consistent hasher (Lamping and Veach, Google 2014)
    jump_hasher: JumpHasher,
}

impl RoutingTable {
    pub fn new(local_id: NodeId) -> Self {
        Self {
            route_next_hop: HashMap::new(),
            available_routes: HashMap::new(),
            local_id,
            base_ipv4_addr: [10, 0, 0, 0],
            // rather than using the default jump hasher with randomized keys, use fixed keys instead
            jump_hasher: JumpHasher::new_with_keys(0x1234567890ABCDEF, 0xFEDCBA0987654321),
        }
    }

    /// Set the base IPv4 address for node ID calculation
    pub fn set_base_ipv4_addr(&mut self, base_addr: [u8; 4]) {
        self.base_ipv4_addr = base_addr;
    }

    /// Install all the routes received from the controller.
    pub fn install_routes(&mut self, routes: Vec<RoutingTableEntry>) {
        info!(
            "RoutingTable: Installing {} routes for local_id {}",
            routes.len(),
            self.local_id
        );

        // Clear existing data
        self.route_next_hop.clear();
        self.available_routes.clear();

        // Build the routing table from routes
        for route in routes {
            // source-destination pair → available route IDs
            let src_dst_pair = (route.src_node_id, route.dst_node_id);

            // route ID → next hop
            self.route_next_hop.insert(route.route_id, route.next_hop);

            self.available_routes
                .entry(src_dst_pair)
                .or_insert_with(Vec::new)
                .push(route.route_id);

            info!(
                "RoutingTable: Installed route {} ({} → {}): the next hop is {}.",
                route.route_id, route.src_node_id, route.dst_node_id, route.next_hop
            );
        }
    }

    /// Extracts source and destination node IDs from the flow ID.
    fn extract_src_dst_from_flow(&self, flow_id: FlowId) -> (NodeId, NodeId) {
        let src_ip = ((flow_id >> 96) & 0xFFFFFFFF) as u32;
        let dst_ip = ((flow_id >> 64) & 0xFFFFFFFF) as u32;

        println!(
            "Before ip_to_node_id: source IP: {}, destination IP: {}",
            src_ip, dst_ip
        );

        // converts IP addresses to node IDs
        let src_node_id = self.ip_to_node_id(src_ip);
        let dst_node_id = self.ip_to_node_id(dst_ip);

        println!(
            "After ip_to_node_id: source IP: {}, destination IP: {}",
            src_ip, dst_ip
        );

        (src_node_id, dst_node_id)
    }

    /// Converts an IP address to its node ID based on the base address.
    fn ip_to_node_id(&self, ip: u32) -> usize {
        let base_ip = u32::from_be_bytes(self.base_ipv4_addr);
        let node_id = ip - base_ip;

        node_id as usize
    }

    /// Applies a deterministic consistent hash function using jump hash for load balancing.
    /// The same flow ID always maps to the same route ID.
    fn jump_hash(&self, flow_id: FlowId, num_buckets: usize) -> usize {
        self.jump_hasher.slot(&flow_id, num_buckets as u32) as usize
    }

    /// Selects a route ID for a flow at each node, performing load balancing using a consistent hash
    /// when multiple routes are available between the same source and destination nodes.
    pub fn select_route_for_flow(&mut self, flow_id: FlowId) -> Option<usize> {
        // obtains the source-destination pair as the key for the available routes
        let src_dst_pair = self.extract_src_dst_from_flow(flow_id);

        // gets the available routes for this source-destination pair
        let available_routes = self.available_routes.get(&src_dst_pair)?;

        // Use jump hash to select among the available routes
        let selected_route_id = if available_routes.len() == 1 {
            available_routes[0]
        } else {
            let hash_result = self.jump_hash(flow_id, available_routes.len());
            available_routes[hash_result]
        };

        debug!(
            "Route ID {} is selected for flow {} from {} available routes.",
            selected_route_id,
            flow_id,
            available_routes.len()
        );

        Some(selected_route_id)
    }

    /// Get next_hop by route ID
    pub fn get_next_hop_by_route(&self, route_id: usize) -> Option<NodeId> {
        self.route_next_hop.get(&route_id).copied()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Assumes that the base IP address is 10.0.0.0: 10.0.0.src_octet and 10.0.0.dst_octet
    fn create_flow_id(
        src_ip_last_octet: u8,
        dst_ip_last_octet: u8,
        sport: u16,
        dport: u16,
        reserved: u8,
    ) -> FlowId {
        let src_ip: u32 = 0x0A000000 | (src_ip_last_octet as u32); // 10.0.0.X
        let dst_ip: u32 = 0x0A000000 | (dst_ip_last_octet as u32); // 10.0.0.Y

        ((src_ip as u128) << 96)
            | ((dst_ip as u128) << 64)
            | ((sport as u128) << 48)
            | ((dport as u128) << 32)
            | (reserved as u128)
    }

    #[test]
    fn test_install_multiple_routes() {
        let mut table = RoutingTable::new(1);
        let routes = vec![
            RoutingTableEntry {
                route_id: 1,
                next_hop: 2,
                src_node_id: 1,
                dst_node_id: 3,
            },
            RoutingTableEntry {
                route_id: 2,
                next_hop: 4,
                src_node_id: 1,
                dst_node_id: 5,
            },
        ];
        table.install_routes(routes);

        assert_eq!(table.route_next_hop.len(), 2);
        assert_eq!(table.available_routes.len(), 2);
        assert_eq!(table.route_next_hop.get(&1), Some(&2));
        assert_eq!(table.route_next_hop.get(&2), Some(&4));
        assert_eq!(table.available_routes.get(&(1, 3)), Some(&vec![1]));
        assert_eq!(table.available_routes.get(&(1, 5)), Some(&vec![2]));
    }

    #[test]
    fn test_install_multiple_routes_alternative() {
        let mut table = RoutingTable::new(1);
        let routes = vec![
            RoutingTableEntry {
                route_id: 1,
                next_hop: 2,
                src_node_id: 1,
                dst_node_id: 3,
            },
            RoutingTableEntry {
                route_id: 2,
                next_hop: 4,
                src_node_id: 1,
                dst_node_id: 3,
            },
        ];
        table.install_routes(routes);

        assert_eq!(table.route_next_hop.len(), 2);
        assert_eq!(table.available_routes.len(), 1);
        let dir_routes = table.available_routes.get(&(1, 3)).unwrap();
        assert!(dir_routes.contains(&1));
        assert!(dir_routes.contains(&2));
    }

    #[test]
    fn test_install_route_with_zero_next_hop() {
        let mut table = RoutingTable::new(1);
        let routes = vec![RoutingTableEntry {
            route_id: 1,
            next_hop: 0,
            src_node_id: 1,
            dst_node_id: 3,
        }];
        table.install_routes(routes);

        // should still be installed
        assert_eq!(table.route_next_hop.len(), 1);
        assert_eq!(table.available_routes.len(), 1);
    }

    #[test]
    fn test_extract_src_dst_from_flow() {
        let table = RoutingTable::new(1); // base_ipv4_addr is [10,0,0,0] by default
        let flow_id = create_flow_id(5, 7, 12345, 80, 6); // 10.0.0.5 -> 10.0.0.7
        let (src_node, dst_node) = table.extract_src_dst_from_flow(flow_id);

        assert_eq!(src_node, 5, "Source node ID mismatch");
        assert_eq!(dst_node, 7, "Destination node ID mismatch");
    }
}
