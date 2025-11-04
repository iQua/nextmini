use ahash::AHashMap;
use jumphash::JumpHasher;
use rand::Rng;
use std::net::Ipv4Addr;
use tracing::debug;

use nextmini_messages::{
    GroupDirectoryEntry, GroupId, GroupRoutingTableEntry, INVALID, RoutingTableEntry,
};

use crate::node::config::LocalConfig;
use crate::node::controller::flowstats::FlowStatsReporterHandle;
use crate::node::flow;
use crate::node::{FlowId, FlowIdExt, NodeId};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum RouteKey {
    Unicast(NodeId, NodeId),
    Multicast(NodeId, GroupId),
}

/// The routing table in the dataplane.
#[derive(Clone)]
pub struct RoutingTable {
    /// Local node ID
    pub local_id: NodeId,

    config: LocalConfig,

    /// Route key (unicast/multicast) -> available route IDs
    available_routes: AHashMap<RouteKey, Vec<usize>>,

    /// Route ID -> next hops
    route_next_hop: AHashMap<usize, Vec<NodeId>>,

    /// Multicast group directory (group_ip -> group_id)
    group_dir: AHashMap<Ipv4Addr, GroupId>,

    /// Jump consistent hasher (Lamping and Veach, Google 2014)
    jump_hasher: JumpHasher,

    /// Cache for flow to route ID mappings
    cache: AHashMap<FlowId, usize>,
}

impl RoutingTable {
    /// Creates a routing table for the provided local configuration.
    pub fn new(config: LocalConfig) -> Self {
        Self {
            available_routes: AHashMap::default(),
            route_next_hop: AHashMap::default(),
            group_dir: AHashMap::default(),
            local_id: config.node_id,
            config,
            // rather than using the default jump hasher with randomized keys, use fixed keys instead
            jump_hasher: JumpHasher::new_with_keys(0x1234567890ABCDEF, 0xFEDCBA0987654321),
            cache: AHashMap::default(),
        }
    }

    /// Installs all the unicast routes received from the controller.
    pub fn install_routes(&mut self, routes: Vec<RoutingTableEntry>) {
        // drop cached selections so flows can pick up the refreshed routing set
        self.cache.clear();

        // purge existing unicast entries before installing replacements
        let existing_unicast_keys: Vec<RouteKey> = self
            .available_routes
            .keys()
            .filter(|key| matches!(key, RouteKey::Unicast(_, _)))
            .copied()
            .collect();

        for key in existing_unicast_keys {
            if let Some(route_ids) = self.available_routes.remove(&key) {
                for route_id in route_ids {
                    self.route_next_hop.remove(&route_id);
                }
            }
        }

        // builds the routing table from routes
        for route in routes {
            self.route_next_hop
                .insert(route.route_id, route.next_hops.clone());

            let key = RouteKey::Unicast(route.src_node_id, route.dst_node_id);
            self.available_routes
                .entry(key)
                .or_default()
                .push(route.route_id);

            debug!(
                "RoutingTable: Installed route {} (node {} → node {}), next hops: {:?}, mode: {:?}.",
                route.route_id,
                route.src_node_id,
                route.dst_node_id,
                route.next_hops,
                route.forward_mode
            );
        }
    }

    /// Extracts source and destination node IDs from the flow ID.
    pub fn extract_node_ids_from_flow(&self, flow_id: FlowId) -> (NodeId, NodeId) {
        self.config.extract_node_ids_from_flow(flow_id)
    }

    /// Installs or refreshes the multicast group directory.
    pub fn install_group_directory(&mut self, groups: Vec<GroupDirectoryEntry>) {
        self.group_dir.clear();
        for entry in groups {
            self.group_dir.insert(entry.group_ip, entry.group_id);
        }
    }

    /// Installs multicast routing information for a specific (src, group) pair.
    pub fn install_group_routes(
        &mut self,
        group_id: GroupId,
        src_node_id: NodeId,
        routes: Vec<GroupRoutingTableEntry>,
    ) {
        let key = RouteKey::Multicast(src_node_id, group_id);

        if let Some(old_route_ids) = self.available_routes.remove(&key) {
            for route_id in old_route_ids {
                self.route_next_hop.remove(&route_id);
            }
        }

        for route in routes {
            self.route_next_hop
                .insert(route.route_id, route.next_hops.clone());
            self.available_routes
                .entry(key)
                .or_default()
                .push(route.route_id);
        }

        // clear cache so flows pick up the refreshed routes immediately
        self.cache.clear();
    }

    /// Builds the routing key for a flow, incorporating multicast groups when applicable.
    fn key_for_flow(&self, flow_id: FlowId) -> Option<RouteKey> {
        if flow_id == flow::INVALID_FLOW_ID {
            return None;
        }

        let (src_node, dst_node) = self.config.extract_node_ids_from_flow(flow_id);
        let dst_ip = flow_id.dst_ip();

        if let Some(&group_id) = self.group_dir.get(&dst_ip) {
            Some(RouteKey::Multicast(src_node, group_id))
        } else {
            Some(RouteKey::Unicast(src_node, dst_node))
        }
    }

    /// Selects a route id for the provided route key.
    fn select_route_for_key(&mut self, key: &RouteKey) -> Option<usize> {
        let ids = self.available_routes.get(key)?;
        if ids.is_empty() {
            return None;
        }

        if ids.len() == 1 {
            return Some(ids[0]);
        }

        let slot = self.jump_hasher.slot(key, ids.len() as u32);
        Some(ids[slot as usize])
    }

    /// Returns the next hops for a flow, supporting multicast fan-out.
    pub fn get_next_hops_by_flow(
        &mut self,
        flow_id: FlowId,
        flowstats_reporter: Option<&FlowStatsReporterHandle>,
    ) -> Result<Vec<NodeId>, String> {
        if flow_id == flow::INVALID_FLOW_ID {
            // the flow ID cannot be successfully extracted, no routing is possible
            return Err("No route can be selected.".to_string());
        }

        // checks the cache first
        if let Some(route_id) = self.cache.get(&flow_id)
            && let Some(next_hops) = self.route_next_hop.get(route_id)
        {
            if next_hops.contains(&INVALID) {
                return Err(format!("Route {} invalid at this node", route_id));
            }

            return Ok(next_hops.clone());
        }

        let key = self
            .key_for_flow(flow_id)
            .ok_or_else(|| "Unable to build route key for flow".to_string())?;

        let route_id = self
            .select_route_for_key(&key)
            .ok_or_else(|| "No route ids available for route key".to_string())?;

        self.report_route_assignment(flow_id, route_id, flowstats_reporter);

        debug!(
            "Route ID {} is selected for source {}:{} → destination {}:{} from {} available routes.",
            route_id,
            flow_id.src_ip(),
            flow_id.src_port(),
            flow_id.dst_ip(),
            flow_id.dst_port(),
            self.available_routes
                .get(&key)
                .map(|entries| entries.len())
                .unwrap_or(0)
        );

        self.cache.insert(flow_id, route_id);

        let hops = self
            .route_next_hop
            .get(&route_id)
            .ok_or_else(|| format!("No next hops found for route {}", route_id))?;

        if hops.contains(&INVALID) {
            return Err(format!("Route {} invalid at this node", route_id));
        }

        Ok(hops.clone())
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
        let hops = self.get_next_hops_by_flow(flow_id, flowstats_reporter)?;
        Self::pick_single_hop(&hops)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node::config::LocalConfig;

    fn make_flow_id(src_ip: Ipv4Addr, dst_ip: Ipv4Addr, src_port: u16, dst_port: u16) -> FlowId {
        ((u32::from(src_ip) as u128) << 96)
            | ((u32::from(dst_ip) as u128) << 64)
            | ((src_port as u128) << 48)
            | ((dst_port as u128) << 32)
    }

    fn make_config(node_id: NodeId) -> LocalConfig {
        LocalConfig {
            node_id,
            ..Default::default()
        }
    }

    #[test]
    fn returns_multicast_next_hops_from_group_routes() {
        let config = make_config(2);
        let mut table = RoutingTable::new(config.clone());

        let group_ip = Ipv4Addr::new(10, 0, 0, 200);
        table.install_group_directory(vec![GroupDirectoryEntry {
            group_id: 7,
            group_ip,
        }]);

        table.install_group_routes(
            7,
            1,
            vec![GroupRoutingTableEntry {
                route_id: 7,
                next_hops: vec![3, 4],
                src_node_id: 1,
                group_id: 7,
            }],
        );

        let flow_id = make_flow_id(
            Ipv4Addr::new(10, 0, 0, 1),
            group_ip,
            1234,
            config.user_space_server_port,
        );

        let hops = table
            .get_next_hops_by_flow(flow_id, None)
            .expect("multicast next hops should be available");

        assert_eq!(hops, vec![3, 4]);
    }

    #[test]
    fn reinstalls_group_routes_clears_cached_selection() {
        let config = make_config(2);
        let mut table = RoutingTable::new(config.clone());

        let group_ip = Ipv4Addr::new(10, 0, 0, 180);
        table.install_group_directory(vec![GroupDirectoryEntry {
            group_id: 9,
            group_ip,
        }]);

        let flow_id = make_flow_id(
            Ipv4Addr::new(10, 0, 0, 1),
            group_ip,
            4321,
            config.user_space_server_port,
        );

        table.install_group_routes(
            9,
            1,
            vec![GroupRoutingTableEntry {
                route_id: 9,
                next_hops: vec![5],
                src_node_id: 1,
                group_id: 9,
            }],
        );

        let first = table
            .get_next_hops_by_flow(flow_id, None)
            .expect("initial multicast hop");
        assert_eq!(first, vec![5]);

        table.install_group_routes(
            9,
            1,
            vec![GroupRoutingTableEntry {
                route_id: 9,
                next_hops: vec![6],
                src_node_id: 1,
                group_id: 9,
            }],
        );

        let updated = table
            .get_next_hops_by_flow(flow_id, None)
            .expect("updated multicast hop");
        assert_eq!(updated, vec![6]);
    }
}
