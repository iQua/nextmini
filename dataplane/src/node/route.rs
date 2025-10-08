use ahash::AHashMap;
use jumphash::JumpHasher;
use rand::Rng;
use tracing::debug;

use nextmini_messages::{INVALID, RoutingTableEntry};

use crate::node::config::LocalConfig;
use crate::node::controller::flowstats::FlowStatsReporterHandle;
use crate::node::flow;
use crate::node::{FlowId, FlowIdExt, NodeId};

/// The routing table in the dataplane.
#[derive(Clone)]
pub struct RoutingTable {
    /// Local node ID
    pub local_id: NodeId,

    config: LocalConfig,

    /// Source-destination node ID pair -> available route IDs
    available_routes: AHashMap<(NodeId, NodeId), Vec<usize>>,

    /// Route ID -> candidate next hops
    route_next_hop: AHashMap<usize, Vec<NodeId>>,

    /// Jump consistent hasher (Lamping and Veach, Google 2014)
    jump_hasher: JumpHasher,

    /// Cache for flow to route ID mappings
    cache: AHashMap<FlowId, usize>,
}

impl RoutingTable {
    pub fn new(config: LocalConfig) -> Self {
        Self {
            route_next_hop: AHashMap::default(),
            available_routes: AHashMap::default(),
            local_id: config.node_id,
            config,
            // rather than using the default jump hasher with randomized keys, use fixed keys instead
            jump_hasher: JumpHasher::new_with_keys(0x1234567890ABCDEF, 0xFEDCBA0987654321),
            cache: AHashMap::default(),
        }
    }

    /// Installs all the routes received from the controller.
    pub fn install_routes(&mut self, routes: Vec<RoutingTableEntry>) {
        // clears existing data
        self.route_next_hop.clear();
        self.available_routes.clear();
        self.cache.clear();

        // builds the routing table from routes
        for route in routes {
            // route ID → next hops
            self.route_next_hop
                .insert(route.route_id, route.next_hops.clone());

            // installs route based on node IDs for both TUN and user space
            let node_id_pair = (route.src_node_id, route.dst_node_id);

            self.available_routes
                .entry(node_id_pair)
                .or_default()
                .push(route.route_id);

            debug!(
                "RoutingTable: Installed route {} (node {} → node {}), next hops: {:?}.",
                route.route_id, route.src_node_id, route.dst_node_id, route.next_hops
            );
        }
    }

    /// Extracts source and destination node IDs from the flow ID.
    pub fn extract_node_ids_from_flow(&self, flow_id: FlowId) -> (NodeId, NodeId) {
        self.config.extract_node_ids_from_flow(flow_id)
    }

    /// Selects a route ID for a flow at each node, performing load balancing using a consistent hash
    /// when multiple routes are available between the same source and destination nodes.
    pub fn select_route_for_flow(
        &mut self,
        flow_id: FlowId,
        flowstats_reporter: Option<&FlowStatsReporterHandle>,
    ) -> Option<usize> {
        if flow_id == flow::INVALID_FLOW_ID {
            // the flow ID cannot be successfully extracted, no routing is possible
            return Some(INVALID);
        }

        // checks the cache first
        if let Some(route_id) = self.cache.get(&flow_id) {
            return Some(*route_id);
        }

        // obtains the source-destination node ID pair as the key for the available routes
        let node_id_pair = self.extract_node_ids_from_flow(flow_id);

        // gets the available routes for this source-destination pair
        let available_routes = self.available_routes.get(&node_id_pair)?;

        // uses jump hash to select among the available routes
        let selected_route_id = if available_routes.len() == 1 {
            // if there is only one available route, it will be selected
            available_routes[0]
        } else {
            // applies a deterministic consistent hash function using jump hash for load balancing;
            // the same flow ID always maps to the same route ID
            let hash_result = self
                .jump_hasher
                .slot(&flow_id, available_routes.len() as u32);

            debug!(
                "Selected route ID (using consistent hashing): {}",
                available_routes[hash_result as usize]
            );

            available_routes[hash_result as usize]
        };

        // stores the selected route into the cache
        self.cache.insert(flow_id, selected_route_id);

        // reports route assignment to the controller only from the source node
        if let Some(flowstats_reporter) = flowstats_reporter {
            let (src_node_id, _) = self.extract_node_ids_from_flow(flow_id);
            if src_node_id == self.local_id {
                flowstats_reporter.report_route_assigned(flow_id, selected_route_id);
            }
        }

        debug!(
            "Route ID {} is selected for source {}:{} → destination {}:{} from {} available routes.",
            selected_route_id,
            flow_id.src_ip(),
            flow_id.src_port(),
            flow_id.dst_ip(),
            flow_id.dst_port(),
            available_routes.len()
        );

        Some(selected_route_id)
    }

    /// Obtains the next hop by the flow ID.
    pub fn get_next_hop_by_flow(
        &mut self,
        flow_id: FlowId,
        flowstats_reporter: Option<&FlowStatsReporterHandle>,
    ) -> Result<NodeId, String> {
        // selects the route ID for a new flow
        if let Some(route_id) = self.select_route_for_flow(flow_id, flowstats_reporter) {
            if route_id == INVALID {
                // No route can be possible as the flow ID is not valid.
                // perhaps a non-IPv4 packet? Drops the packet without forwarding it.
                return Err("No route can be selected.".to_string());
            }

            // gets the next hop by route ID
            if let Some(next_hops) = self.route_next_hop.get(&route_id) {
                if next_hops.contains(&INVALID) {
                    // unless the routing table changes dynamically at runtime, the next hop should
                    // never be invalid, as the source only sends out packets via valid routes only,
                    // and the same flow ID always hashes to the same route ID with consistent hashing
                    panic!(
                        "Route {} is not available on this node (next_hop = INVALID).",
                        route_id
                    );
                }

                if next_hops.len() > 1 {
                    // randomizes the choice between all possible next hops
                    let idx = rand::rng().random_range(0..next_hops.len());
                    return Ok(next_hops[idx]);
                } else {
                    // selects the only choice as the next hop
                    return Ok(next_hops[0]);
                }
            } else {
                return Err(format!(
                    "No next hop is found for route id {} on flow {}: routing inconsistency detected.",
                    route_id, flow_id
                ));
            }
        } else {
            return Err(format!(
                "No route is found for flow {}: the routing table may be misconfigured.",
                flow_id
            ));
        }
    }
}
