use crate::dataplane::FlowId;
use crate::dataplane::NodeId;
use ahash::AHashMap;
use nextmini_messages::SimpleRouteEntry;
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

/// Optimized routing table using direct route_id mapping
/// Controller only sends route-level next-hop info, dataplane manages flow->route mapping
#[derive(Clone)]
pub struct SimpleRoutingTable {
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
}

impl SimpleRoutingTable {
    pub fn new(local_id: NodeId) -> Self {
        Self {
            route_next_hop: HashMap::new(),
            direction_routes: HashMap::new(),
            flow_route_cache: AHashMap::new(),
            local_id,
            base_ipv4_addr: [10, 0, 0, 0], // Default, should be configured
        }
    }

    /// Set the base IPv4 address for node ID calculation
    pub fn set_base_ipv4_addr(&mut self, base_addr: [u8; 4]) {
        self.base_ipv4_addr = base_addr;
    }

    /// Install routes using route_id -> next_hop mapping with direction indexing
    /// Controller only needs to send route-level next-hop info
    pub fn install_routes(&mut self, routes: Vec<SimpleRouteEntry>) {
        debug!(
            "RoutingTable: Installing {} routes for local_id {}",
            routes.len(),
            self.local_id
        );

        // Clear existing data
        self.route_next_hop.clear();
        self.direction_routes.clear();
        self.flow_route_cache.clear();

        // Build routing tables
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
        // Convert EnhancedRouteEntry to SimpleRouteEntry for consistency
        let simple_routes: Vec<SimpleRouteEntry> = routes
            .into_iter()
            .map(|route| SimpleRouteEntry {
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

        // Debug info
        println!("DEBUG: Flow ID: {:#x}", flow_id);
        println!("DEBUG: Extracted src_ip: {}.{}.{}.{} ({}), dst_ip: {}.{}.{}.{} ({})",
            (src_ip >> 24) & 0xFF, (src_ip >> 16) & 0xFF, (src_ip >> 8) & 0xFF, src_ip & 0xFF, src_ip,
            (dst_ip >> 24) & 0xFF, (dst_ip >> 16) & 0xFF, (dst_ip >> 8) & 0xFF, dst_ip & 0xFF, dst_ip);
        println!("DEBUG: Converted to src_node_id: {}, dst_node_id: {}", src_node_id, dst_node_id);

        (src_node_id, dst_node_id)
    }

    /// Convert IP address to node ID based on base address
    fn ip_to_node_id(&self, ip: u32) -> usize {
        let base_ip = u32::from_be_bytes(self.base_ipv4_addr);
        let node_id = ip - base_ip;
        
        // Debug info
        println!("DEBUG: IP to node_id conversion - IP: {}, Base IP: {}, Node ID: {}", 
            ip, base_ip, node_id as usize);
        
        node_id as usize
    }

    /// Jump hash implementation for consistent load balancing
    /// Ensures the same flow_id always maps to the same route_id
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

    /// Optimized next hop lookup: flow_id -> route_id -> next_hop
    /// Fast path: O(1) cached lookup
    /// Slow path: extract direction, select route_id, cache result
    pub fn next_hop_for_flow(&mut self, flow_id: FlowId) -> Option<NodeId> {
        // Fast path: Check flow_id -> route_id cache
        if let Some(&route_id) = self.flow_route_cache.get(&flow_id) {
            let next_hop = self.route_next_hop.get(&route_id).copied();
            if let Some(hop) = next_hop {
                println!("DEBUG: Cache hit for flow {:#x}: route_id {} -> next_hop {}", 
                    flow_id, route_id, hop);
            }
            return next_hop;
        }

        // Slow path: New flow processing
        let (src_node, dst_node) = self.extract_src_dst_from_flow(flow_id);
        let direction_key = (src_node, dst_node);

        println!("DEBUG: Looking up route for direction ({}, {})", src_node, dst_node);

        // Find available routes for this direction
        let available_routes = self.direction_routes.get(&direction_key)?;

        // Select route_id using load balancing strategy
        let route_id = if available_routes.len() == 1 {
            // Single route: direct selection
            available_routes[0]
        } else {
            // Multiple routes: use jump hash for consistent load balancing
            let route_index = self.jump_hash(flow_id, available_routes.len());
            available_routes[route_index]
        };

        println!("DEBUG: Selected route_id {} for direction ({}, {}) from {} available routes", 
            route_id, src_node, dst_node, available_routes.len());

        // Cache the flow -> route mapping
        self.flow_route_cache.insert(flow_id, route_id);

        // Return next_hop for the selected route
        let next_hop = self.route_next_hop.get(&route_id).copied();
        if let Some(hop) = next_hop {
            println!("DEBUG: New flow {:#x}: route_id {} -> next_hop {}", 
                flow_id, route_id, hop);
        } else {
            println!("DEBUG: No next_hop found for route_id {}", route_id);
        }

        next_hop
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