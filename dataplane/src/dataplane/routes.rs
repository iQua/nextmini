use crate::dataplane::FlowId;
use crate::dataplane::NodeId;
use ahash::AHashMap;
use jumphash::JumpHasher;
use nextmini_messages::RoutingTableEntry;
use std::collections::HashMap;
use tracing::debug;

/// Enhanced route entry that includes src/dst node information for proper flow matching
#[derive(Clone, Debug)]
pub struct EnhancedRouteEntry {
    pub route_id: usize,
    pub next_hop: NodeId,
    pub src_node_id: NodeId,
    pub dst_node_id: NodeId,
}

/// Optimized routing table using direct route_id mapping
/// Controller only sends route-level next-hop info, dataplane manages flow->route mapping
#[derive(Clone)]
pub struct RoutingTable {
    /// Direct route_id -> next_hop mapping (O(1) lookup)
    route_next_hop: HashMap<usize, NodeId>,

    /// Direction -> available route_ids mapping for flow routing
    direction_routes: HashMap<(NodeId, NodeId), Vec<usize>>,

    /// Flow-to-route cache for consistent routing (O(1) after first lookup)
    flow_route_cache: AHashMap<FlowId, usize>,

    /// Local node ID
    pub local_id: NodeId,

    /// Base IPv4 address for node ID calculation (e.g., [10, 0, 0, 0])
    base_ipv4_addr: [u8; 4],

    /// Jump hash hasher for consistent routing
    jump_hasher: JumpHasher,
}

impl RoutingTable {
    pub fn new(local_id: NodeId) -> Self {
        Self {
            route_next_hop: HashMap::new(),
            direction_routes: HashMap::new(),
            flow_route_cache: AHashMap::new(),
            local_id,
            base_ipv4_addr: [10, 0, 0, 0], // Default, should be configured
            jump_hasher: JumpHasher::new_with_keys(0x1234567890ABCDEF, 0xFEDCBA0987654321),
        }
    }

    /// Set the base IPv4 address for node ID calculation
    pub fn set_base_ipv4_addr(&mut self, base_addr: [u8; 4]) {
        self.base_ipv4_addr = base_addr;
    }

    /// Install routes using route_id -> next_hop mapping with direction indexing
    /// Controller only needs to send route-level next-hop info
    pub fn install_routes(&mut self, routes: Vec<RoutingTableEntry>) {
        debug!(
            "RoutingTable: Installing {} routes for local_id {}",
            routes.len(),
            self.local_id
        );

        // Clear existing data
        self.route_next_hop.clear();
        self.direction_routes.clear();
        self.flow_route_cache.clear();

        // Build routing tables directly from routes
        for route in routes {
            if route.next_hop != 0 {
                // 1. Direct route_id -> next_hop mapping
                self.route_next_hop.insert(route.route_id, route.next_hop);

                // 2. Build reverse index: direction -> available route_ids
                let direction = (route.src_node_id, route.dst_node_id);
                self.direction_routes
                    .entry(direction)
                    .or_insert_with(Vec::new)
                    .push(route.route_id);

                debug!(
                    "RoutingTable: Installed route {} ({}→{}) -> next_hop {}",
                    route.route_id, route.src_node_id, route.dst_node_id, route.next_hop
                );
            }
        }

        debug!(
            "RoutingTable: Route installation complete. {} direct routes, {} directions",
            self.route_next_hop.len(),
            self.direction_routes.len()
        );
    }

    /// Legacy method for enhanced routes - now just calls install_routes
    #[allow(dead_code)]
    pub fn install_routes_enhanced(&mut self, routes: Vec<EnhancedRouteEntry>) {
        // Convert EnhancedRouteEntry to RoutingTableEntry for consistency
        let simple_routes: Vec<RoutingTableEntry> = routes
            .into_iter()
            .map(|route| RoutingTableEntry {
                route_id: route.route_id,
                next_hop: route.next_hop,
                src_node_id: route.src_node_id,
                dst_node_id: route.dst_node_id,
            })
            .collect();

        self.install_routes(simple_routes);
    }

    /// Extract src and dst node IDs from flow_id
    fn extract_src_dst_from_flow(&self, flow_id: FlowId) -> (NodeId, NodeId) {
        // Extract src_ip and dst_ip from flow_id
        let src_ip = ((flow_id >> 96) & 0xFFFFFFFF) as u32;
        let dst_ip = ((flow_id >> 64) & 0xFFFFFFFF) as u32;

        // Convert IP addresses to node IDs
        let src_node_id = self.ip_to_node_id(src_ip);
        let dst_node_id = self.ip_to_node_id(dst_ip);

        (src_node_id, dst_node_id)
    }

    /// Convert IP address to node ID based on base address
    fn ip_to_node_id(&self, ip: u32) -> usize {
        let base_ip = u32::from_be_bytes(self.base_ipv4_addr);
        let node_id = ip - base_ip;
        node_id as usize
    }

    /// Jump hash implementation for consistent load balancing using jumphash library
    /// Ensures the same flow_id always maps to the same route_id
    fn jump_hash(&self, flow_id: FlowId, num_buckets: usize) -> usize {
        self.jump_hasher.slot(&flow_id, num_buckets as u32) as usize
    }

    /// Select route_id for a new flow at each node, with load balanced using a consistent hash
    ///  when multiple routes are available between the same source and destination nodes.
    pub fn select_route_for_flow(&mut self, flow_id: FlowId) -> Option<usize> {
        // Extract source and destination nodes
        let (src_node, dst_node) = self.extract_src_dst_from_flow(flow_id);
        let src_dst_pair = (src_node, dst_node);

        debug!(
            "Source node selecting route for direction ({}, {})",
            src_node, dst_node
        );

        // Get available routes for this direction
        let available_routes = self.direction_routes.get(&src_dst_pair)?;

        // Simple route selection: use jump hash among available routes
        let selected_route_id = if available_routes.len() == 1 {
            available_routes[0]
        } else {
            let hash_result = self.jump_hash(flow_id, available_routes.len());
            available_routes[hash_result]
        };

        debug!(
            "Source node selected route_id {} for flow {} from {} available routes",
            selected_route_id,
            flow_id,
            available_routes.len()
        );

        // Cache result for later use
        self.flow_route_cache.insert(flow_id, selected_route_id);

        Some(selected_route_id)
    }

    /// Get next_hop by route_id (for packets with determined route)
    pub fn get_next_hop_by_route(&self, route_id: usize) -> Option<NodeId> {
        self.route_next_hop.get(&route_id).copied()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Assumes base IP 10.0.0.0 for IPs like 10.0.0.src_octet and 10.0.0.dst_octet
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
    fn test_install_multiple_routes_different_directions() {
        let mut table = RoutingTable::new(1);
        let routes = vec![
            RoutingTableEntry {
                route_id: 100,
                next_hop: 2,
                src_node_id: 1,
                dst_node_id: 3,
            },
            RoutingTableEntry {
                route_id: 101,
                next_hop: 4,
                src_node_id: 1,
                dst_node_id: 5,
            },
        ];
        table.install_routes(routes);

        assert_eq!(table.route_next_hop.len(), 2);
        assert_eq!(table.direction_routes.len(), 2);
        assert_eq!(table.route_next_hop.get(&100), Some(&2));
        assert_eq!(table.route_next_hop.get(&101), Some(&4));
        assert_eq!(table.direction_routes.get(&(1, 3)), Some(&vec![100]));
        assert_eq!(table.direction_routes.get(&(1, 5)), Some(&vec![101]));
    }

    #[test]
    fn test_install_multiple_routes_same_direction() {
        let mut table = RoutingTable::new(1);
        let routes = vec![
            RoutingTableEntry {
                route_id: 100,
                next_hop: 2,
                src_node_id: 1,
                dst_node_id: 3,
            },
            RoutingTableEntry {
                route_id: 101,
                next_hop: 4,
                src_node_id: 1,
                dst_node_id: 3,
            },
        ];
        table.install_routes(routes);

        assert_eq!(table.route_next_hop.len(), 2); // Both routes stored
        assert_eq!(table.direction_routes.len(), 1); // One direction
        let dir_routes = table.direction_routes.get(&(1, 3)).unwrap();
        assert!(dir_routes.contains(&100));
        assert!(dir_routes.contains(&101));
    }

    #[test]
    fn test_install_route_with_zero_next_hop() {
        let mut table = RoutingTable::new(1);
        let routes = vec![RoutingTableEntry {
            route_id: 100,
            next_hop: 0, // Invalid next hop
            src_node_id: 1,
            dst_node_id: 3,
        }];
        table.install_routes(routes);

        assert_eq!(table.route_next_hop.len(), 0); // Should not be installed
        assert_eq!(table.direction_routes.len(), 0); // Direction should not be added if no valid route
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
