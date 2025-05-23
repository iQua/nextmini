use crate::dataplane::FlowId;
use crate::dataplane::NodeId;
use nextmini_messages::RouteMapping;

/// Simplified routing table that uses consistent hashing from flow_id to route_id
/// and stores next_hop in a vector indexed by route_id
#[derive(Clone)]
pub struct SimpleRoutingTable {
    /// Vector of next hops indexed by route_id
    /// None indicates no route installed for that route_id
    route_table: Vec<Option<NodeId>>,
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
    pub fn install_routes(&mut self, routes: Vec<RouteMapping>) {
        // Find the maximum route_id to resize the vector
        let max_route_id = routes.iter().map(|r| r.route_id).max().unwrap_or(0);

        // Resize the vector to accommodate all route_ids
        if self.route_table.len() <= max_route_id {
            self.route_table.resize(max_route_id + 1, None);
        }

        // Install each route
        for route in routes {
            if route.route_id < self.route_table.len() {
                self.route_table[route.route_id] = Some(route.next_hop);
                println!(
                    "Installed route {} -> next_hop {}",
                    route.route_id, route.next_hop
                );
            }
        }
    }

    /// Apply consistent hashing to flow_id to get route_id
    pub fn hash_flow_to_route(&self, flow_id: FlowId) -> Option<usize> {
        if self.route_table.is_empty() {
            return None;
        }

        // Simple consistent hashing: use flow_id modulo number of routes
        // This ensures packets with the same flow_id always go to the same route
        let route_id = (flow_id as usize) % self.route_table.len();

        // Check if this route_id has a valid next_hop
        if self.route_table[route_id].is_some() {
            Some(route_id)
        } else {
            // Fallback: find the first available route
            for (idx, next_hop) in self.route_table.iter().enumerate() {
                if next_hop.is_some() {
                    return Some(idx);
                }
            }
            None
        }
    }

    /// Get next hop for a given route_id
    pub fn get_next_hop(&self, route_id: usize) -> Option<NodeId> {
        self.route_table
            .get(route_id)
            .and_then(|&next_hop| next_hop)
    }

    /// Get next hop directly from flow_id (combines hashing and lookup)
    pub fn next_hop_for_flow(&self, flow_id: FlowId) -> Option<NodeId> {
        self.hash_flow_to_route(flow_id)
            .and_then(|route_id| self.get_next_hop(route_id))
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
