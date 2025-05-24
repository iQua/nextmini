use crate::dataplane::FlowId;
use crate::dataplane::NodeId;
use ahash::{AHashMap, AHasher};
use nextmini_messages::RouteMapping;
use std::hash::{Hash, Hasher};
use tracing::debug;

/// Enhanced routing table entry that stores complete route information
#[derive(Clone, Debug)]
pub struct RouteEntry {
    pub next_hop: NodeId,
    pub src_addr: [u8; 4],
    pub dst_addr: [u8; 4],
}

/// Simplified routing table that maps flow_id to globally consistent route_id
/// Each flow_id maps to a fixed route_id across all nodes
#[derive(Clone)]
pub struct SimpleRoutingTable {
    /// Vector of route entries indexed by route_id
    /// None indicates no route installed for that route_id
    route_table: Vec<Option<RouteEntry>>,
    /// Local node ID
    pub local_id: NodeId,
    /// Cache for flow_id to route_id mappings to avoid repeated computation
    flow_route_cache: AHashMap<FlowId, usize>,
    /// Precomputed routes grouped by (src_ip, dst_ip) for faster lookup
    route_index: AHashMap<(u32, u32), Vec<usize>>,
}

impl SimpleRoutingTable {
    pub fn new(local_id: NodeId) -> Self {
        Self {
            route_table: Vec::new(),
            local_id,
            flow_route_cache: AHashMap::new(),
            route_index: AHashMap::new(),
        }
    }

    /// Install routes from controller's RouteMapping messages
    /// Store ALL routes for consistent hashing, but mark inactive routes (next_hop == 0)
    pub fn install_routes(&mut self, routes: Vec<RouteMapping>) {
        debug!(
            "RoutingTable: Installing {} routes for local_id {}",
            routes.len(),
            self.local_id
        );

        // Find the maximum route_id to resize the vector
        let max_route_id = routes.iter().map(|r| r.route_id).max().unwrap_or(0);

        // Resize the vector to accommodate all route_ids
        if self.route_table.len() <= max_route_id {
            self.route_table.resize(max_route_id + 1, None);
        }

        // Install ALL routes for consistent global view
        // Install each route
        for route in routes {
            if route.route_id < self.route_table.len() {
                let route_entry = RouteEntry {
                    next_hop: route.next_hop,
                    src_addr: route.src_addr,
                    dst_addr: route.dst_addr,
                };
                self.route_table[route.route_id] = Some(route_entry);

                if route.next_hop != 0 {
                    debug!(
                        "RoutingTable: Installed active route {} -> next_hop {} for src_addr {:?} and dst_addr {:?}",
                        route.route_id, route.next_hop, route.src_addr, route.dst_addr
                    );
                } else {
                    debug!(
                        "RoutingTable: Installed inactive route {} (doesn't pass through node {})",
                        route.route_id, self.local_id
                    );
                }
            }
        }

        // Rebuild the route index for faster lookups
        self.rebuild_route_index();

        // Clear cache since routes have changed
        self.flow_route_cache.clear();

        debug!(
            "RoutingTable: Route installation complete. Total active routes: {}",
            self.num_routes()
        );

        // Debug print the complete routing table
        self.debug_print_routing_table();
    }

    /// Rebuild the route index for O(1) lookup by (src_ip, dst_ip)
    fn rebuild_route_index(&mut self) {
        self.route_index.clear();

        for (route_id, route_entry_opt) in self.route_table.iter().enumerate() {
            if let Some(route_entry) = route_entry_opt {
                let src_ip = u32::from_be_bytes(route_entry.src_addr);
                let dst_ip = u32::from_be_bytes(route_entry.dst_addr);

                self.route_index
                    .entry((src_ip, dst_ip))
                    .or_insert_with(Vec::new)
                    .push(route_id);
            }
        }

        // CRITICAL: Sort all route lists to ensure deterministic order
        for route_list in self.route_index.values_mut() {
            route_list.sort();
        }

        debug!(
            "RoutingTable: Route index rebuilt with {} src-dst pairs (sorted)",
            self.route_index.len()
        );
    }

    /// Map flow_id to a globally consistent route_id
    /// The same flow_id always maps to the same route_id across all nodes
    pub fn flow_id_to_route_id(&mut self, flow_id: FlowId) -> Option<usize> {
        // Check cache first - O(1) lookup
        if let Some(&cached_route_id) = self.flow_route_cache.get(&flow_id) {
            // Cache hit - removed debug for performance
            return Some(cached_route_id);
        }

        // Extract source and destination IPs from flow_id
        let src_ip = ((flow_id >> 96) & 0xFFFFFFFF) as u32;
        let dst_ip = ((flow_id >> 64) & 0xFFFFFFFF) as u32;
        let src_port = ((flow_id >> 48) & 0xFFFF) as u16;
        let dst_port = ((flow_id >> 32) & 0xFFFF) as u16;

        // Removed high-frequency debug logging for performance

        // Fast lookup using precomputed index - O(1) instead of O(N)
        let mut available_routes = match self.route_index.get(&(src_ip, dst_ip)) {
            Some(routes) => routes.clone(),
            None => {
                debug!("RoutingTable: No routes available for flow direction");
                return None;
            }
        };

        // CRITICAL: Sort routes to ensure deterministic order across all nodes
        // HashMap iteration order is non-deterministic, so we must sort
        available_routes.sort();

        if available_routes.is_empty() {
            debug!("RoutingTable: No routes available for flow direction");
            return None;
        }

        // Use consistent hashing based on the complete 4-tuple to select route_id
        // This ensures the same flow always gets the same route_id globally
        let mut hasher = AHasher::default();
        src_ip.hash(&mut hasher);
        dst_ip.hash(&mut hasher);
        src_port.hash(&mut hasher);
        dst_port.hash(&mut hasher);

        let hash_value = hasher.finish() as usize;
        let selected_index = hash_value % available_routes.len();
        let route_id = available_routes[selected_index];

        // Cache the result for future lookups
        self.flow_route_cache.insert(flow_id, route_id);

        // Removed high-frequency debug logging for performance

        Some(route_id)
    }

    /// Get next hop for a given route_id
    pub fn get_next_hop(&self, route_id: usize) -> Option<NodeId> {
        self.route_table
            .get(route_id)
            .and_then(|route_entry| route_entry.as_ref().map(|entry| entry.next_hop))
    }

    /// Get next hop from flow_id (maps flow to route_id, then gets next_hop)
    pub fn next_hop_for_flow(&mut self, flow_id: FlowId) -> Option<NodeId> {
        // Step 1: Map flow_id to globally consistent route_id
        let route_id = self.flow_id_to_route_id(flow_id)?;

        // Step 2: Get next_hop for this route_id at current node
        let next_hop = self.get_next_hop(route_id);

        match next_hop {
            Some(hop) => {
                // Removed high-frequency debug logging for performance
                Some(hop)
            }
            None => {
                // No next_hop - removed debug for performance
                None
            }
        }
    }

    /// Get number of installed routes
    pub fn num_routes(&self) -> usize {
        self.route_table.iter().filter(|r| r.is_some()).count()
    }

    /// Check if any routes are installed
    #[allow(dead_code)]
    pub fn has_routes(&self) -> bool {
        self.route_table.iter().any(|r| r.is_some())
    }

    /// Debug function to print complete routing table
    pub fn debug_print_routing_table(&self) {
        debug!("=== Routing Table Debug for Node {} ===", self.local_id);
        debug!("Total routes installed: {}", self.num_routes());

        for (route_id, route_entry_opt) in self.route_table.iter().enumerate() {
            if let Some(route_entry) = route_entry_opt {
                let src_ip = u32::from_be_bytes(route_entry.src_addr);
                let dst_ip = u32::from_be_bytes(route_entry.dst_addr);
                debug!(
                    "  Route {}: {}.{}.{}.{} -> {}.{}.{}.{}, next_hop: {}",
                    route_id,
                    (src_ip >> 24) & 0xFF,
                    (src_ip >> 16) & 0xFF,
                    (src_ip >> 8) & 0xFF,
                    src_ip & 0xFF,
                    (dst_ip >> 24) & 0xFF,
                    (dst_ip >> 16) & 0xFF,
                    (dst_ip >> 8) & 0xFF,
                    dst_ip & 0xFF,
                    route_entry.next_hop
                );
            }
        }

        debug!("=== Route Index Debug ===");
        for ((src_ip, dst_ip), route_ids) in &self.route_index {
            debug!(
                "  {}.{}.{}.{} -> {}.{}.{}.{}: routes {:?}",
                (src_ip >> 24) & 0xFF,
                (src_ip >> 16) & 0xFF,
                (src_ip >> 8) & 0xFF,
                src_ip & 0xFF,
                (dst_ip >> 24) & 0xFF,
                (dst_ip >> 16) & 0xFF,
                (dst_ip >> 8) & 0xFF,
                dst_ip & 0xFF,
                route_ids
            );
        }
        debug!("=== End Routing Table Debug ===");
    }
}
