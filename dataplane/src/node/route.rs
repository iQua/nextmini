use std::net::Ipv4Addr;

use ahash::AHashMap;
use jumphash::JumpHasher;
use nextmini_messages::RoutingTableEntry;
use tracing::{debug, info};

use crate::node::config::LocalConfig;
use crate::node::{FlowId, FlowIdExt, NodeId, NodeIdExt};

/// The routing table in the dataplane.
#[derive(Clone)]
pub struct RoutingTable {
    /// Local node ID
    pub local_id: NodeId,

    config: LocalConfig,

    /// Source-destination node ID pair -> available route IDs
    available_routes: AHashMap<(NodeId, NodeId), Vec<usize>>,

    /// Route ID -> next hop
    route_next_hop: AHashMap<usize, NodeId>,

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

    /// Install all the routes received from the controller.
    pub fn install_routes(&mut self, routes: Vec<RoutingTableEntry>) {
        // clears existing data
        self.route_next_hop.clear();
        self.available_routes.clear();
        self.cache.clear();

        // builds the routing table from routes
        for route in routes {
            // route ID → next hop
            self.route_next_hop.insert(route.route_id, route.next_hop);

            // installs route based on node IDs for both TUN and user space
            let node_id_pair = (route.src_node_id, route.dst_node_id);

            self.available_routes
                .entry(node_id_pair)
                .or_default()
                .push(route.route_id);

            debug!(
                "RoutingTable: Installed route {} (node {} → node {}), next hop is node {}.",
                route.route_id, route.src_node_id, route.dst_node_id, route.next_hop
            );
        }
    }

    /// Extracts source and destination node IDs from the flow ID.
    fn extract_node_ids_from_flow(&self, flow_id: FlowId) -> (NodeId, NodeId) {
        let src_ip = flow_id.src_ip();
        let dst_ip = flow_id.dst_ip();

        let src_node_id = self.ip_to_node_id(src_ip);
        let dst_node_id = self.ip_to_node_id(dst_ip);

        (src_node_id, dst_node_id)
    }

    /// Converts a node ID to its TUN IP address.
    pub fn node_id_to_ip(&self, node_id: NodeId) -> Ipv4Addr {
        node_id.ip_addr(
            self.config.virtual_base_addr,
            self.config.local_netmask,
        )
    }

    /// Converts a node ID to its user space IP address.
    pub fn node_id_to_user_space_ip(&self, node_id: NodeId) -> Ipv4Addr {
        node_id.ip_addr(
            self.config.user_space_base_addr,
            self.config.local_netmask,
        )
    }

    /// Converts IP address to node ID, supporting both TUN and user space networks.
    fn ip_to_node_id(&self, ip: Ipv4Addr) -> NodeId {
        let ip_addr = u32::from(ip);
        let netmask = u32::from(self.config.local_netmask);

        let tun_base = u32::from(self.config.virtual_base_addr);
        let user_space_base = u32::from(self.config.user_space_base_addr);

        match ip_addr & netmask {
            subnet if subnet == (tun_base & netmask) => (ip_addr - tun_base) as NodeId,
            subnet if subnet == (user_space_base & netmask) => {
                (ip_addr - user_space_base) as NodeId
            }
            _ => {
                panic!("Detected unknown IP {}.", ip);
            }
        }
    }

    /// Selects a route ID for a flow at each node, performing load balancing using a consistent hash
    /// when multiple routes are available between the same source and destination nodes.
    pub fn select_route_for_flow(&mut self, flow_id: FlowId) -> Option<usize> {
        if flow_id == 0 {
            // the flow ID cannot be successfully extracted, no routing is possible
            return Some(0);
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
            available_routes[0]
        } else {
            // applies a deterministic consistent hash function using jump hash for load balancing;
            // the same flow ID always maps to the same route ID
            let hash_result = self
                .jump_hasher
                .slot(&flow_id, available_routes.len() as u32);
            info!(
                "Jump hash selected route ID: {}",
                available_routes[hash_result as usize]
            );
            available_routes[hash_result as usize]
        };

        // stores the selected route into the cache
        self.cache.insert(flow_id, selected_route_id);

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

    /// Get next_hop by route ID
    pub fn get_next_hop_by_route(&self, route_id: usize) -> Option<NodeId> {
        self.route_next_hop.get(&route_id).copied()
    }
}
