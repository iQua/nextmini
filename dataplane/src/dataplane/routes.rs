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

    /// Select route_id for a new flow at the source node (load balancing)
    pub fn select_route_for_flow(&mut self, flow_id: FlowId) -> Option<usize> {
        // Extract source and destination nodes
        let (src_node, dst_node) = self.extract_src_dst_from_flow(flow_id);
        let direction_key = (src_node, dst_node);

        debug!(
            "Source node selecting route for direction ({}, {})",
            src_node, dst_node
        );

        // Get available routes for this direction
        let available_routes = self.direction_routes.get(&direction_key)?;

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

    /// Simplified version: compatible with existing code
    /// Note: It is recommended to use the new split method (select_route_for_flow + get_next_hop_by_route)
    #[allow(dead_code)]
    pub fn next_hop_for_flow(&mut self, flow_id: FlowId) -> Option<NodeId> {
        // Fast path: Check flow_id -> route_id cache
        if let Some(&route_id) = self.flow_route_cache.get(&flow_id) {
            return self.route_next_hop.get(&route_id).copied();
        }

        // Slow path: Use the new simplified route selection method
        let route_id = self.select_route_for_flow(flow_id)?;

        // Return next_hop for the selected route
        self.route_next_hop.get(&route_id).copied()
    }

    /// Get number of active routes
    #[allow(dead_code)]
    pub fn num_routes(&self) -> usize {
        self.route_next_hop.len()
    }

    /// Check if any routes are installed
    #[allow(dead_code)]
    pub fn has_routes(&self) -> bool {
        !self.route_next_hop.is_empty()
    }

    /// Debug function to print optimized routing table
    #[allow(dead_code)]
    pub fn debug_print_routing_table(&self) {
        debug!(
            "=== Optimized Route-ID Routing Table Debug for Node {} ===",
            self.local_id
        );
        debug!(
            "Direct routes: {}, Directions: {}, Cached flows: {}",
            self.route_next_hop.len(),
            self.direction_routes.len(),
            self.flow_route_cache.len()
        );

        for (&route_id, &next_hop) in &self.route_next_hop {
            debug!("  Route {}: -> next_hop {}", route_id, next_hop);
        }

        for ((src, dst), route_ids) in &self.direction_routes {
            debug!(
                "  Direction {}→{}: routes {:?} ({} options)",
                src,
                dst,
                route_ids,
                route_ids.len()
            );
        }
        debug!("=== End Routing Table Debug ===");
    }
}

#[cfg(test)]
mod tests {
    use super::*; // Imports RoutingTable, RoutingTableEntry, NodeId, FlowId

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

    #[test]
    fn test_jump_hash_consistency() {
        let table = RoutingTable::new(1);
        let flow_id: FlowId = 12345678901234567890;
        let num_buckets = 10;
        let hash1 = table.jump_hash(flow_id, num_buckets);
        let hash2 = table.jump_hash(flow_id, num_buckets);
        assert_eq!(hash1, hash2, "Jump hash not consistent for same input");
    }

    #[test]
    fn test_jump_hash_range() {
        let table = RoutingTable::new(1);
        let flow_id: FlowId = 9876543210987654321;
        let num_buckets = 5;
        for i in 0..100 {
            // Test with slightly varying flow_ids
            let current_flow_id = flow_id + i as u128;
            let hash_val = table.jump_hash(current_flow_id, num_buckets);
            assert!(
                hash_val < num_buckets,
                "Hash value {} out of range for {} buckets",
                hash_val,
                num_buckets
            );
        }
    }

    #[test]
    fn test_jump_hash_single_bucket() {
        let table = RoutingTable::new(1);
        let flow_id: FlowId = 11112222333344445555;
        let num_buckets = 1;
        let hash_val = table.jump_hash(flow_id, num_buckets);
        assert_eq!(hash_val, 0, "Hash value is not 0 for a single bucket");
    }

    // Basic distribution check - non-exhaustive sanity check
    #[test]
    fn test_jump_hash_distribution_basic() {
        let table = RoutingTable::new(1);
        let num_buckets = 10;
        let mut results = std::collections::HashSet::new();
        // Expect that with a few different flow_ids, we might hit different buckets.
        // This is not a guarantee but likely for a reasonable hash.
        for i in 0..100 {
            let flow_id = 1000 + i as u128;
            results.insert(table.jump_hash(flow_id, num_buckets));
        }
        // We expect at least half of the buckets to be hit.
        assert!(
            results.len() > num_buckets / 2,
            "Jump hash seems to map all test flows to a single bucket, or only one bucket exists. Results: {:?}",
            results
        );
    }

    // This test may be changed if reintallation will not clear the cache.
    #[test]
    fn test_next_hop_cache_reintall_routes() {
        let mut table = RoutingTable::new(1);
        table.set_base_ipv4_addr([10, 0, 0, 0]);

        // add two routes for the same direction [5,7] -> 2 and [5,7] -> 3
        let route1 = vec![RoutingTableEntry {
            route_id: 100,
            next_hop: 2,
            src_node_id: 5,
            dst_node_id: 7,
        }];
        let route2 = vec![RoutingTableEntry {
            route_id: 101,
            next_hop: 3,
            src_node_id: 5,
            dst_node_id: 7,
        }];
        table.install_routes(route1);
        table.install_routes(route2);

        let flow_id = create_flow_id(5, 7, 1234, 80, 6); // 10.0.0.5 -> 10.0.0.7

        // First call
        let next_hop1 = table.next_hop_for_flow(flow_id);
        assert_eq!(next_hop1, Some(3));
        assert_eq!(table.flow_route_cache.len(), 1);
        assert_eq!(table.flow_route_cache.get(&flow_id), Some(&101));
    }

    #[test]
    fn test_next_hop_cache_hit() {
        let mut table = RoutingTable::new(1);
        table.set_base_ipv4_addr([10, 0, 0, 0]);
        let routes = vec![RoutingTableEntry {
            route_id: 100,
            next_hop: 2,
            src_node_id: 5,
            dst_node_id: 7,
        }];
        table.install_routes(routes);

        let flow_id = create_flow_id(5, 7, 1234, 80, 6);
        assert!(table.flow_route_cache.is_empty());

        let next_hop = table.next_hop_for_flow(flow_id);
        assert_eq!(next_hop, Some(2));
        assert_eq!(table.next_hop_for_flow(flow_id), Some(2)); // check cache hit
    }

    #[test]
    fn test_next_hop_cache_miss_with_multiple_routes() {
        let mut table = RoutingTable::new(1);
        table.set_base_ipv4_addr([10, 0, 0, 0]);
        let routes = vec![
            RoutingTableEntry {
                route_id: 100,
                next_hop: 2,
                src_node_id: 5,
                dst_node_id: 7,
            },
            RoutingTableEntry {
                route_id: 101,
                next_hop: 3,
                src_node_id: 5,
                dst_node_id: 7,
            },
        ];
        table.install_routes(routes);

        // Create two different flow_ids that map to the same direction
        let flow_id1 = create_flow_id(5, 7, 1000, 80, 6); // 10.0.0.5 -> 10.0.0.7
        let flow_id2 = create_flow_id(5, 7, 2000, 80, 6); // 10.0.0.5 -> 10.0.0.7, different sport

        let next_hop1 = table.next_hop_for_flow(flow_id1);
        let route_id1 = *table.flow_route_cache.get(&flow_id1).unwrap();

        let next_hop2 = table.next_hop_for_flow(flow_id2);
        let route_id2 = *table.flow_route_cache.get(&flow_id2).unwrap();

        // With jump hash, these should be consistent. They might be the same or different
        // depending on the hash outcomes. We mainly check that *a* valid route is chosen.
        assert!(next_hop1 == Some(2) || next_hop1 == Some(3));
        assert!(next_hop2 == Some(2) || next_hop2 == Some(3));
        assert!(route_id1 == 100 || route_id1 == 101);
        assert!(route_id2 == 100 || route_id2 == 101);

        // Ensure consistency for the same flow_id
        assert_eq!(table.next_hop_for_flow(flow_id1), next_hop1);
        assert_eq!(table.next_hop_for_flow(flow_id2), next_hop2);
    }

    #[test]
    fn test_next_hop_cache_no_route() {
        let mut table = RoutingTable::new(1);
        table.set_base_ipv4_addr([10, 0, 0, 0]);
        let routes = vec![RoutingTableEntry {
            route_id: 100,
            next_hop: 2,
            src_node_id: 5,
            dst_node_id: 7,
        }];
        table.install_routes(routes); // Route for 5->7

        let flow_id = create_flow_id(5, 8, 1234, 80, 6);
        let next_hop = table.next_hop_for_flow(flow_id);
        assert_eq!(next_hop, None);
        assert!(table.flow_route_cache.get(&flow_id).is_none());
    }
}
