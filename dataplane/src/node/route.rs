use ahash::AHashMap;
use jumphash::JumpHasher;
use rand::Rng;
use tracing::debug;

use nextmini_messages::{INVALID, RoutingTableEntry};

use crate::node::config::LocalConfig;
use crate::node::controller::flowstats::FlowStatsReporterHandle;
use crate::node::flow;
use crate::node::{FlowId, FlowIdExt, NodeId};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RouteForwardingMode {
    Unicast,
    Multicast,
}

impl RouteForwardingMode {
    /// Indicates whether this route is multicast-capable.
    fn is_multicast(self) -> bool {
        matches!(self, RouteForwardingMode::Multicast)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RouteDecision {
    Unicast(NodeId),
    Multicast(Vec<NodeId>),
}

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

    /// Route ID -> forwarding mode (unicast vs multicast)
    route_forward_mode: AHashMap<usize, RouteForwardingMode>,

    /// Jump consistent hasher (Lamping and Veach, Google 2014)
    jump_hasher: JumpHasher,

    /// Cache for flow to route ID mappings
    cache: AHashMap<FlowId, usize>,
}

impl RoutingTable {
    /// Creates a routing table for the provided local configuration.
    pub fn new(config: LocalConfig) -> Self {
        Self {
            route_next_hop: AHashMap::default(),
            route_forward_mode: AHashMap::default(),
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
        self.route_forward_mode.clear();
        self.available_routes.clear();
        self.cache.clear();

        // builds the routing table from routes
        for route in routes {
            // route ID → next hops
            self.route_next_hop
                .insert(route.route_id, route.next_hops.clone());
            self.route_forward_mode.insert(
                route.route_id,
                if route.multicast {
                    RouteForwardingMode::Multicast
                } else {
                    RouteForwardingMode::Unicast
                },
            );

            // installs route based on node IDs for both TUN and user space
            let node_id_pair = (route.src_node_id, route.dst_node_id);

            self.available_routes
                .entry(node_id_pair)
                .or_default()
                .push(route.route_id);

            debug!(
                "RoutingTable: Installed route {} (node {} → node {}), next hops: {:?}, multicast: {}.",
                route.route_id,
                route.src_node_id,
                route.dst_node_id,
                route.next_hops,
                route.multicast
            );
        }
    }

    /// Extracts source and destination node IDs from the flow ID.
    pub fn extract_node_ids_from_flow(&self, flow_id: FlowId) -> (NodeId, NodeId) {
        self.config.extract_node_ids_from_flow(flow_id)
    }

    /// Makes a routing decision: multicast or unicast, and the actual next hop(s).
    pub fn route_decision_for_flow(
        &mut self,
        flow_id: FlowId,
        flowstats_reporter: Option<&FlowStatsReporterHandle>,
    ) -> Result<RouteDecision, String> {
        let route_id = self.select_route_for_flow(flow_id, flowstats_reporter)?;
        let hops = self.next_hops_for_route(route_id)?;

        if self.route_forward_mode(route_id).is_multicast() {
            if hops.is_empty() {
                return Err("No next hop(s) available.".to_string());
            }

            Ok(RouteDecision::Multicast(hops.to_vec()))
        } else {
            let hop = Self::pick_single_hop(&hops)?;

            Ok(RouteDecision::Unicast(hop))
        }
    }

    /// Selects a route ID for a flow at each node, performing load balancing using a consistent hash
    /// when multiple routes are available between the same source and destination nodes.
    pub fn select_route_for_flow(
        &mut self,
        flow_id: FlowId,
        flowstats_reporter: Option<&FlowStatsReporterHandle>,
    ) -> Result<usize, String> {
        if flow_id == flow::INVALID_FLOW_ID {
            // the flow ID cannot be successfully extracted, no routing is possible
            return Err("No route can be selected.".to_string());
        }

        // checks the cache first
        if let Some(route_id) = self.cache.get(&flow_id) {
            return Ok(*route_id);
        }

        // obtains the source-destination node ID pair as the key for the available routes
        let node_id_pair = self.extract_node_ids_from_flow(flow_id);

        // gets the available routes for this source-destination pair
        let available_routes = self.available_routes.get(&node_id_pair).ok_or_else(|| {
            format!(
                "No route is found for flow {}: the routing table may be misconfigured.",
                flow_id
            )
        })?;

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

        self.report_route_assignment(flow_id, selected_route_id, flowstats_reporter);

        debug!(
            "Route ID {} is selected for source {}:{} → destination {}:{} from {} available routes.",
            selected_route_id,
            flow_id.src_ip(),
            flow_id.src_port(),
            flow_id.dst_ip(),
            flow_id.dst_port(),
            available_routes.len()
        );

        Ok(selected_route_id)
    }

    /// Retrieves the next-hop candidates for the given route identifier.
    fn next_hops_for_route(&self, route_id: usize) -> Result<&[NodeId], String> {
        let Some(next_hops) = self.route_next_hop.get(&route_id) else {
            return Err(format!(
                "No next hop is found for route id {}: routing inconsistency detected.",
                route_id
            ));
        };

        if next_hops.contains(&INVALID) {
            return Err(format!(
                "Route {} is not available on this node (next_hop = INVALID).",
                route_id
            ));
        }

        Ok(next_hops.as_slice())
    }

    /// Looks up whether the given route forwards via unicast or multicast.
    fn route_forward_mode(&self, route_id: usize) -> RouteForwardingMode {
        self.route_forward_mode
            .get(&route_id)
            .copied()
            .unwrap_or(RouteForwardingMode::Unicast)
    }

    /// Picks a single next hop from a candidate list (random when multiple options exist).
    fn pick_single_hop(next_hops: &[NodeId]) -> Result<NodeId, String> {
        match next_hops.len() {
            0 => Err("No next hop(s) available.".to_string()),
            1 => Ok(next_hops[0]),
            len => Ok(next_hops[rand::rng().random_range(0..len)]),
        }
    }

    /// Emits a route-assignment notification to the controller when appropriate.
    fn report_route_assignment(
        &self,
        flow_id: FlowId,
        route_id: usize,
        flowstats_reporter: Option<&FlowStatsReporterHandle>,
    ) {
        let Some(flowstats_reporter) = flowstats_reporter else {
            return;
        };

        let (src_node_id, _) = self.extract_node_ids_from_flow(flow_id);

        // only reports for app flows (not user space flows)
        // user space flows use a dedicated server port (check both directions)
        let is_app_flow = flow_id.dst_port() != self.config.user_space_server_port
            && flow_id.src_port() != self.config.user_space_server_port;

        debug!(
            "Route selection: flow_id={:?}, src_node={}, local_id={}, is_app_flow={}, route={}",
            flow_id, src_node_id, self.local_id, is_app_flow, route_id
        );

        if src_node_id == self.local_id && is_app_flow {
            flowstats_reporter.report_route_assigned(flow_id, route_id);
            debug!(
                "Reported route assignment: flow_id={:?}, route_id={}",
                flow_id, route_id
            );
        }
    }

    /// Pick a single next hop (random if > 1) in TCP max mode, used by the Connector.
    pub fn get_next_hop_by_flow(
        &mut self,
        flow_id: FlowId,
        flowstats_reporter: Option<&FlowStatsReporterHandle>,
    ) -> Result<NodeId, String> {
        match self.route_decision_for_flow(flow_id, flowstats_reporter)? {
            RouteDecision::Unicast(hop) => Ok(hop),
            RouteDecision::Multicast(hops) => Self::pick_single_hop(&hops),
        }
    }
}
