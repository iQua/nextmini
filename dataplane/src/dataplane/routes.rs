use crate::dataplane::FlowId;
use crate::dataplane::NodeId;
use nextmini_messages::RouteMapping;
use tracing::debug;

/// Enhanced routing table entry that stores complete route information
#[derive(Clone, Debug)]
pub struct RouteEntry {
    pub next_hop: NodeId,
    pub src_addr: [u8; 4],
    pub dst_addr: [u8; 4],
}

/// Simplified routing table that uses consistent hashing from flow_id to route_id
/// and stores complete route information indexed by route_id
#[derive(Clone)]
pub struct SimpleRoutingTable {
    /// Vector of route entries indexed by route_id
    /// None indicates no route installed for that route_id
    route_table: Vec<Option<RouteEntry>>,
    /// Local node ID
    pub local_id: NodeId,
}

impl SimpleRoutingTable {
    pub fn new(local_id: NodeId) -> Self {
        Self {
            route_table: Vec::new(),
            local_id,
        }
    }

    /// Install routes from controller's RouteMapping messages
    /// Storing complete route information in a vector indexed by route_id
    pub fn install_routes(&mut self, routes: Vec<RouteMapping>) {
        debug!("RoutingTable: Installing {} routes for local_id {}", routes.len(), self.local_id);
        
        // Find the maximum route_id to resize the vector
        let max_route_id = routes.iter().map(|r| r.route_id).max().unwrap_or(0);

        // Resize the vector to accommodate all route_ids
        if self.route_table.len() <= max_route_id {
            self.route_table.resize(max_route_id + 1, None);
        }

        // Install each route
        for route in routes {
            if route.route_id < self.route_table.len() {
                let route_entry = RouteEntry {
                    next_hop: route.next_hop,
                    src_addr: route.src_addr,
                    dst_addr: route.dst_addr,
                };
                self.route_table[route.route_id] = Some(route_entry);
                debug!(
                    "RoutingTable: Installed route {} -> next_hop {} for src_addr {:?} and dst_addr {:?}",
                    route.route_id, route.next_hop, route.src_addr, route.dst_addr
                );
            }
        }
        
        debug!("RoutingTable: Route installation complete. Total routes: {}", self.num_routes());
    }

    /// Apply consistent hashing to flow_id to get route_id, properly handling flow direction
    pub fn hash_flow_to_route(&self, flow_id: FlowId) -> Option<usize> {
        if self.route_table.is_empty() {
            debug!("RoutingTable: No routes available for flow_id {}", flow_id);
            return None;
        }

        // Extract source and destination IPs from flow_id
        let src_ip = ((flow_id >> 96) & 0xFFFFFFFF) as u32;
        let dst_ip = ((flow_id >> 64) & 0xFFFFFFFF) as u32;
        
        debug!("RoutingTable: Flow {} from IP {}.{}.{}.{} to IP {}.{}.{}.{}", 
               flow_id,
               (src_ip >> 24) & 0xFF, (src_ip >> 16) & 0xFF, (src_ip >> 8) & 0xFF, src_ip & 0xFF,
               (dst_ip >> 24) & 0xFF, (dst_ip >> 16) & 0xFF, (dst_ip >> 8) & 0xFF, dst_ip & 0xFF);
        
        // Find routes that match this flow's source and destination
        let mut matching_routes = Vec::new();
        
        for (route_id, route_entry_opt) in self.route_table.iter().enumerate() {
            if let Some(route_entry) = route_entry_opt {
                // Convert stored addresses to u32 for comparison
                let route_src_ip = u32::from_be_bytes(route_entry.src_addr);
                let route_dst_ip = u32::from_be_bytes(route_entry.dst_addr);
                
                // Check if this route matches the flow's source and destination
                if route_src_ip == src_ip && route_dst_ip == dst_ip {
                    matching_routes.push(route_id);
                    debug!("RoutingTable: Route {} matches flow direction", route_id);
                }
            }
        }
        
        if matching_routes.is_empty() {
            debug!("RoutingTable: No routes found matching flow direction, using fallback");
            // Fallback: find any available route (for flows not explicitly configured)
            for (i, route_entry) in self.route_table.iter().enumerate() {
                if route_entry.is_some() {
                    matching_routes.push(i);
                }
            }
        }
        
        if matching_routes.is_empty() {
            debug!("RoutingTable: No valid routes found for flow_id {}", flow_id);
            return None;
        }
        
        // Apply consistent hash within matching routes
        // Use the full flow_id for consistent selection across all nodes
        let hash_input = flow_id as usize;
        let selected_index = hash_input % matching_routes.len();
        let route_id = matching_routes[selected_index];
        
        debug!("RoutingTable: Consistent hash: flow_id {} -> route_id {} (from {} matching routes: {:?})", 
               flow_id, route_id, matching_routes.len(), matching_routes);
        
        Some(route_id)
    }

    /// Get next hop for a given route_id
    pub fn get_next_hop(&self, route_id: usize) -> Option<NodeId> {
        self.route_table
            .get(route_id)
            .and_then(|route_entry| route_entry.as_ref().map(|entry| entry.next_hop))
    }

    /// Get next hop from flow_id (combines hashing and get_next_hop)
    pub fn next_hop_for_flow(&self, flow_id: FlowId) -> Option<NodeId> {
        let result = self.hash_flow_to_route(flow_id)
            .and_then(|route_id| self.get_next_hop(route_id));
        
        match result {
            Some(next_hop) => {
                debug!("RoutingTable: Flow {} routes to next_hop {}", flow_id, next_hop);
                Some(next_hop)
            }
            None => {
                debug!("RoutingTable: No next_hop found for flow {}", flow_id);
                None
            }
        }
    }

    /// Get number of installed routes
    pub fn num_routes(&self) -> usize {
        self.route_table.iter().filter(|r| r.is_some()).count()
    }

    /// Check if any routes are installed
    pub fn has_routes(&self) -> bool {
        self.route_table.iter().any(|r| r.is_some())
    }
}
