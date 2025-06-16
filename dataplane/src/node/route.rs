use std::net::Ipv4Addr;

use ahash::AHashMap;
use jumphash::JumpHasher;
use nextmini_messages::RoutingTableEntry;
use tracing::{debug, info};

use crate::node::{FlowId, FlowIdExt, NodeId};

/// The routing table in the dataplane.
#[derive(Clone)]
pub struct RoutingTable {
    /// Local node ID
    pub local_id: NodeId,

    /// Base IPv4 address for node ID calculation (e.g., [10, 0, 0, 0])
    base_ipv4_addr: [u8; 4],

    /// Base IPv4 address for Smoltcp network (e.g., [192, 168, 0, 0])
    smoltcp_base_addr: [u8; 4],

    /// Source-destination pair -> available route IDs
    available_routes: AHashMap<(Ipv4Addr, Ipv4Addr), Vec<usize>>,

    /// Route ID -> next hop
    route_next_hop: AHashMap<usize, NodeId>,

    /// Jump consistent hasher (Lamping and Veach, Google 2014)
    jump_hasher: JumpHasher,

    /// Cache for flow to route ID mappings
    cache: AHashMap<FlowId, usize>,
}

impl RoutingTable {
    pub fn new(local_id: NodeId) -> Self {
        Self {
            route_next_hop: AHashMap::default(),
            available_routes: AHashMap::default(),
            local_id,
            base_ipv4_addr: [10, 0, 0, 0],
            smoltcp_base_addr: [192, 168, 0, 0], // keep the hardcoded base address for smoltcp as tun does
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

            // Two kinds of routes: tun and smoltcp

            // installs TUN network routes (10.0.0.x)
            let tun_src_ip = self.node_id_to_ip(route.src_node_id);
            let tun_dst_ip = self.node_id_to_ip(route.dst_node_id);
            let tun_src_dst_pair = (tun_src_ip, tun_dst_ip);

            self.available_routes
                .entry(tun_src_dst_pair)
                .or_default()
                .push(route.route_id);

            // install smoltcp network routes (192.168.0.x)
            let smoltcp_src_ip = self.node_id_to_smoltcp_ip(route.src_node_id);
            let smoltcp_dst_ip = self.node_id_to_smoltcp_ip(route.dst_node_id);
            let smoltcp_src_dst_pair = (smoltcp_src_ip, smoltcp_dst_ip);

            self.available_routes
                .entry(smoltcp_src_dst_pair)
                .or_default()
                .push(route.route_id);

            debug!(
                "RoutingTable: Installed route {} ({} → {}): tun ({} → {}), smoltcp ({} → {}), next hop is {}.",
                route.route_id,
                route.src_node_id,
                route.dst_node_id,
                tun_src_ip,
                tun_dst_ip,
                smoltcp_src_ip,
                smoltcp_dst_ip,
                route.next_hop
            );
        }
    }

    /// Extracts source and destination node IDs from the flow ID.
    fn extract_src_dst_from_flow(&self, flow_id: FlowId) -> (Ipv4Addr, Ipv4Addr) {
        let src_ip = flow_id.src_ip();
        let dst_ip = flow_id.dst_ip();

        (src_ip, dst_ip)
    }

    /// Converts a node ID to its IP address based on the tun base address.
    pub fn node_id_to_ip(&self, node_id: usize) -> Ipv4Addr {
        let base_ip = u32::from_be_bytes(self.base_ipv4_addr);
        let ip_addr = base_ip + node_id as u32;

        Ipv4Addr::from(ip_addr)
    }

    /// Converts node ID to SmolTCP IP address based on the smoltcp base address.
    pub fn node_id_to_smoltcp_ip(&self, node_id: usize) -> Ipv4Addr {
        let base_ip = u32::from_be_bytes(self.smoltcp_base_addr);
        let ip_addr = base_ip + node_id as u32;
        Ipv4Addr::from(ip_addr)
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

        // obtains the source-destination pair as the key for the available routes
        let src_dst_pair = self.extract_src_dst_from_flow(flow_id);

        // gets the available routes for this source-destination pair
        let available_routes = self.available_routes.get(&src_dst_pair)?;

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
