use crate::dataplane::FlowId;
use crate::dataplane::NodeId;
use ahash::AHashMap;
use nextmini-messages::SimpleRouteEntry;
use std::collections::HashMap;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use tracing::debug;

/// Enhanced route entry that includes src/dst node information for proper flow matching
#[derive(Clone, Debug)]
pub struct EnhancedRouteEntry {
    pub route_id: usize,
    pub next_hop: NodeId,
    pub src_node_id: NodeId,
    pub dst_node_id: NodeId,
}

/// Ultra-simplified routing table using direct HashMap lookup
/// Single paths use direct mapping, multi-paths use Vec + jump hash
#[derive(Clone)]
pub struct SimpleRoutingTable {
    /// Single-path routes: (src_node, dst_node) -> next_hop
    single_routes: HashMap<(NodeId, NodeId), NodeId>,

    /// Multi-path routes: (src_node, dst_node) -> Vec<next_hop>
    multi_routes: HashMap<(NodeId, NodeId), Vec<NodeId>>,

    /// Local node ID
    pub local_id: NodeId,

    /// Direct cache for flow_id to next_hop mappings (only for multi-path)
    flow_next_hop_cache: AHashMap<FlowId, NodeId>,

    /// Base IPv4 address for node ID calculation (e.g., [10, 0, 0, 0])
    base_ipv4_addr: [u8; 4],
}

impl SimpleRoutingTable {
    pub fn new(local_id: NodeId) -> Self {
        Self {
            single_routes: HashMap::new(),
            multi_routes: HashMap::new(),
            local_id,
            flow_next_hop_cache: AHashMap::new(),
            base_ipv4_addr: [10, 0, 0, 0], // Default, should be configured
        }
    }

    /// Set the base IPv4 address for node ID calculation
    pub fn set_base_ipv4_addr(&mut self, base_addr: [u8; 4]) {
        self.base_ipv4_addr = base_addr;
    }

    /// Install routes using SimpleRouteEntry (now includes src/dst node information)
    pub fn install_routes(&mut self, routes: Vec<SimpleRouteEntry>) {
        debug!(
            "RoutingTable: Installing {} routes for local_id {}",
            routes.len(),
            self.local_id
        );

        // Clear existing routes and cache
        self.single_routes.clear();
        self.multi_routes.clear();
        self.flow_next_hop_cache.clear();

        // First pass: group routes by direction
        let mut temp_routes: HashMap<(NodeId, NodeId), Vec<NodeId>> = HashMap::new();
        for route in routes {
            if route.next_hop != 0 {
                let direction_key = (route.src_node_id, route.dst_node_id);
                temp_routes
                    .entry(direction_key)
                    .or_insert_with(Vec::new)
                    .push(route.next_hop);

                debug!(
                    "RoutingTable: Installed route {} ({}→{}) -> next_hop {}",
                    route.route_id, route.src_node_id, route.dst_node_id, route.next_hop
                );
            }
        }

        // Second pass: optimize single vs multi-path routes
        for (direction, next_hops) in temp_routes {
            if next_hops.len() == 1 {
                // Single path: direct mapping
                self.single_routes.insert(direction, next_hops[0]);
                debug!("Single-path route: {:?} -> {}", direction, next_hops[0]);
            } else {
                // Multi-path: Vec + jump hash
                self.multi_routes.insert(direction, next_hops);
                debug!(
                    "Multi-path route: {:?} -> {:?}",
                    direction,
                    self.multi_routes.get(&direction).unwrap()
                );
            }
        }

        debug!(
            "RoutingTable: Route installation complete. {} single routes, {} multi routes",
            self.single_routes.len(),
            self.multi_routes.len()
        );
    }

    /// Legacy method for enhanced routes - now just calls install_routes
    #[allow(dead_code)]
    pub fn install_routes_enhanced(&mut self, routes: Vec<EnhancedRouteEntry>) {
        // Convert EnhancedRouteEntry to SimpleRouteEntry for consistency
        let simple_routes: Vec<SimpleRouteEntry> = routes
            .into_iter()
            .map(|route| {
                SimpleRouteEntry {
                    route_id: route.route_id,
                    next_hop: route.next_hop,
                    src_node_id: route.src_node_id,
                    dst_node_id: route.dst_node_id,
                }
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

    /// Jump hash implementation for consistent hashing
    /// This is the core algorithm that ensures the same flow_id
    /// always maps to the same route_id across all nodes
    fn jump_hash(&self, flow_id: FlowId, num_buckets: usize) -> usize {
        // Convert flow_id to a hash value
        let mut hasher = DefaultHasher::new();
        flow_id.hash(&mut hasher);
        let mut key = hasher.finish();

        let mut b: i64 = -1;
        let mut j: i64 = 0;

        while j < num_buckets as i64 {
            b = j;
            key = key.wrapping_mul(2862933555777941757_u64).wrapping_add(1);
            j = ((b + 1) as f64 * (1u64 << 31) as f64 / ((key >> 33) + 1) as f64) as i64;
        }

        b as usize
    }

    /// Get next hop from flow_id using optimized lookup (O(1) for all cases)
    pub fn next_hop_for_flow(&mut self, flow_id: FlowId) -> Option<NodeId> {
        let (src_node, dst_node) = self.extract_src_dst_from_flow(flow_id);
        let direction_key = (src_node, dst_node);

        // Ultra-fast path: Single-path routes (no cache needed - already O(1))
        if let Some(&next_hop) = self.single_routes.get(&direction_key) {
            return Some(next_hop);
        }

        // Multi-path routes: Check cache first
        if let Some(&next_hop) = self.flow_next_hop_cache.get(&flow_id) {
            return Some(next_hop);
        }

        // Multi-path routes: Jump hash for load balancing
        if let Some(available_routes) = self.multi_routes.get(&direction_key) {
            let route_index = self.jump_hash(flow_id, available_routes.len());
            let next_hop = available_routes[route_index];

            // Cache result for multi-path routes
            self.flow_next_hop_cache.insert(flow_id, next_hop);
            return Some(next_hop);
        }

        // No route found
        None
    }

    /// Get number of active routes
    #[allow(dead_code)]
    pub fn num_routes(&self) -> usize {
        let single_count = self.single_routes.len();
        let multi_count: usize = self.multi_routes.values().map(|v| v.len()).sum();
        single_count + multi_count
    }

    /// Check if any routes are installed
    #[allow(dead_code)]
    pub fn has_routes(&self) -> bool {
        !self.single_routes.is_empty() || !self.multi_routes.is_empty()
    }

    /// Debug function to print simplified routing table
    #[allow(dead_code)]
    pub fn debug_print_routing_table(&self) {
        debug!(
            "=== HashMap + Jump Hash Routing Table Debug for Node {} ===",
            self.local_id
        );
        debug!(
            "Single routes: {}, Multi routes: {}, Total routes: {}",
            self.single_routes.len(),
            self.multi_routes.len(),
            self.num_routes()
        );

        for ((src, dst), &next_hop) in &self.single_routes {
            debug!("  Single route: {}→{} -> {}", src, dst, next_hop);
        }

        for ((src, dst), next_hops) in &self.multi_routes {
            debug!("  Multi route: {}→{} -> {:?} ({} paths)", src, dst, next_hops, next_hops.len());
        }
        debug!("=== End Routing Table Debug ===");
    }
}
