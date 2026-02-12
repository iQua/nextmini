use ahash::AHashMap;
use jumphash::JumpHasher;
use rand::Rng;
use smallvec::SmallVec;
use std::net::Ipv4Addr;
use tracing::debug;

use nextmini_messages::{
    GroupDirectoryEntry, GroupId, GroupRoutingTableEntry, INVALID, MULTICAST_ROUTE_FLAG,
    MULTITREE_STRIDE, RoutingTableEntry,
};

use crate::node::config::LocalConfig;
use crate::node::controller::flowstats::FlowStatsReporterHandle;
use crate::node::flow;
use crate::node::{FlowId, FlowIdExt, NodeId};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum RouteKey {
    Unicast(NodeId, NodeId),
    Multicast {
        src_node_id: NodeId,
        group_id: GroupId,
        tree_id: u16,
    },
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

    /// Fast path cache for multicast tree selections keyed by `(src, group, tree)`.
    multicast_tree_cache: AHashMap<(NodeId, GroupId, u16), usize>,
}

const INLINE_HOPS: usize = 4;
pub type HopBuffer = SmallVec<[NodeId; INLINE_HOPS]>;

fn encode_multicast_route_id(route_id: usize) -> usize {
    MULTICAST_ROUTE_FLAG | route_id
}

fn deterministic_multicast_route_id(group_id: GroupId, tree_id: u16) -> Option<usize> {
    let tree = usize::from(tree_id);
    if tree >= MULTITREE_STRIDE {
        return None;
    }
    group_id
        .checked_mul(MULTITREE_STRIDE)?
        .checked_add(tree)
        .filter(|route_id| *route_id < MULTICAST_ROUTE_FLAG)
}

fn tree_id_from_route_id(group_id: GroupId, route_id: usize) -> Option<u16> {
    let base = group_id.checked_mul(MULTITREE_STRIDE)?;
    if route_id < base {
        return None;
    }
    let tree = route_id - base;
    if tree >= MULTITREE_STRIDE {
        return None;
    }
    u16::try_from(tree).ok()
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
            multicast_tree_cache: AHashMap::default(),
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
        let existing_multicast_keys: Vec<RouteKey> = self
            .available_routes
            .keys()
            .filter(|key| {
                matches!(
                    key,
                    RouteKey::Multicast {
                        src_node_id: existing_src,
                        group_id: existing_group,
                        ..
                    } if *existing_src == src_node_id && *existing_group == group_id
                )
            })
            .copied()
            .collect();

        for key in existing_multicast_keys {
            if let Some(old_route_ids) = self.available_routes.remove(&key) {
                for route_id in old_route_ids {
                    self.route_next_hop.remove(&route_id);
                }
            }
        }
        self.multicast_tree_cache
            .retain(|(cached_src, cached_group, _), _| {
                *cached_src != src_node_id || *cached_group != group_id
            });

        for route in routes {
            let Some(tree_id) = tree_id_from_route_id(group_id, route.route_id) else {
                debug!(
                    "RoutingTable: skipping multicast route {} for group {} due to invalid deterministic tree mapping",
                    route.route_id, group_id
                );
                continue;
            };
            let Some(expected_route_id) = deterministic_multicast_route_id(group_id, tree_id)
            else {
                debug!(
                    "RoutingTable: skipping multicast route {} for group {} tree {} due to deterministic route-id overflow",
                    route.route_id, group_id, tree_id
                );
                continue;
            };
            if expected_route_id != route.route_id {
                debug!(
                    "RoutingTable: skipping multicast route {} for group {} tree {} (expected deterministic route id {})",
                    route.route_id, group_id, tree_id, expected_route_id
                );
                continue;
            }

            let key = RouteKey::Multicast {
                src_node_id,
                group_id,
                tree_id,
            };
            let encoded_id = encode_multicast_route_id(route.route_id);

            self.route_next_hop
                .insert(encoded_id, route.next_hops.clone());
            self.available_routes.insert(key, vec![encoded_id]);
        }

        // clear cache so flows pick up the refreshed routes immediately
        self.cache.clear();
    }

    /// Builds the routing key for a flow, incorporating multicast groups when applicable.
    fn key_for_flow(&self, flow_id: FlowId, tree_id: Option<u16>) -> Option<RouteKey> {
        if flow_id == flow::INVALID_FLOW_ID {
            return None;
        }

        let dst_ip = flow_id.dst_ip();
        if let Some(&group_id) = self.group_dir.get(&dst_ip) {
            let src_node = self.config.ip_to_node_id(flow_id.src_ip());
            if src_node == INVALID {
                return None;
            }
            return Some(RouteKey::Multicast {
                src_node_id: src_node,
                group_id,
                tree_id: tree_id.unwrap_or(0),
            });
        }

        self.config
            .try_extract_node_ids_from_flow(flow_id)
            .map(|(src_node, dst_node)| RouteKey::Unicast(src_node, dst_node))
    }

    /// Selects a route id for the provided route key.
    fn select_route_for_key(&mut self, key: &RouteKey) -> Option<usize> {
        match key {
            RouteKey::Unicast(_, _) => {
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
            RouteKey::Multicast {
                src_node_id,
                group_id,
                tree_id,
            } => {
                let cache_key = (*src_node_id, *group_id, *tree_id);
                if let Some(cached_route_id) = self.multicast_tree_cache.get(&cache_key)
                    && self.route_next_hop.contains_key(cached_route_id)
                {
                    return Some(*cached_route_id);
                }

                let route_id = deterministic_multicast_route_id(*group_id, *tree_id)?;
                let encoded_id = encode_multicast_route_id(route_id);
                if self.route_next_hop.contains_key(&encoded_id) {
                    self.multicast_tree_cache.insert(cache_key, encoded_id);
                    Some(encoded_id)
                } else {
                    None
                }
            }
        }
    }

    /// Returns the next hops for a flow, supporting multicast fan-out.
    pub fn get_next_hops_by_flow(
        &mut self,
        flow_id: FlowId,
        flowstats_reporter: Option<&FlowStatsReporterHandle>,
    ) -> Result<HopBuffer, String> {
        self.get_next_hops_by_flow_and_tree(flow_id, None, flowstats_reporter)
    }

    /// Returns the next hops for a flow, optionally forcing a multicast tree id.
    pub fn get_next_hops_by_flow_and_tree(
        &mut self,
        flow_id: FlowId,
        tree_id: Option<u16>,
        flowstats_reporter: Option<&FlowStatsReporterHandle>,
    ) -> Result<HopBuffer, String> {
        if flow_id == flow::INVALID_FLOW_ID {
            // the flow ID cannot be successfully extracted, no routing is possible
            return Err("No route can be selected.".to_string());
        }

        let key = self
            .key_for_flow(flow_id, tree_id)
            .ok_or_else(|| "Unable to build route key for flow".to_string())?;

        if matches!(key, RouteKey::Unicast(_, _))
            && let Some(route_id) = self.cache.get(&flow_id)
            && let Some(next_hops) = self.route_next_hop.get(route_id)
        {
            return Self::copy_next_hops(*route_id, next_hops);
        }

        let route_id = self.select_route_for_key(&key).ok_or_else(|| match key {
            RouteKey::Multicast {
                src_node_id,
                group_id,
                tree_id,
            } => format!(
                "Unknown multicast tree route for src_node_id={}, group_id={}, tree_id={}",
                src_node_id, group_id, tree_id
            ),
            RouteKey::Unicast(_, _) => "No route ids available for route key".to_string(),
        })?;

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

        if matches!(key, RouteKey::Unicast(_, _)) {
            self.cache.insert(flow_id, route_id);
        }

        let hops = self
            .route_next_hop
            .get(&route_id)
            .ok_or_else(|| format!("No next hops found for route {}", route_id))?;

        Self::copy_next_hops(route_id, hops)
    }

    /// Pins a specific route for a flow by pre-populating the cache.
    ///
    /// This allows controller-assigned flows to use a specific route rather than
    /// going through normal route selection. The pinned route takes effect on the
    /// flow's first packet (cache hit).
    ///
    /// Note: Pinned routes are cleared when routes are reinstalled (topology changes).
    /// The controller should re-pin routes after topology stabilizes if needed.
    pub fn pin_route_for_flow(&mut self, flow_id: FlowId, route_id: usize) -> Result<(), String> {
        // Validate route exists
        let next_hops = self
            .route_next_hop
            .get(&route_id)
            .ok_or_else(|| format!("Route {} not found in routing table", route_id))?;

        if next_hops.is_empty() {
            return Err(format!("Route {} has no next hops", route_id));
        }

        if next_hops.contains(&INVALID) {
            return Err(format!("Route {} contains invalid next hops", route_id));
        }

        // Pre-populate cache - flow will hit cache on first packet
        self.cache.insert(flow_id, route_id);
        debug!("Pinned route {} for flow {:032x}", route_id, flow_id);

        Ok(())
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

        let src_node_id = self.config.ip_to_node_id(flow_id.src_ip());
        if src_node_id == INVALID {
            return;
        }

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

    fn copy_next_hops(route_id: usize, source: &[NodeId]) -> Result<HopBuffer, String> {
        if source.contains(&INVALID) {
            return Err(format!("Route {} invalid at this node", route_id));
        }

        Ok(HopBuffer::from_slice(source))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node::config::LocalConfig;
    use nextmini_messages::RouteForwardingMode;

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

    fn multicast_route_id(group_id: GroupId, tree_id: u16) -> usize {
        deterministic_multicast_route_id(group_id, tree_id).expect("deterministic multicast route")
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
                route_id: multicast_route_id(7, 0),
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

        assert_eq!(&hops[..], &[3, 4]);
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
                route_id: multicast_route_id(9, 0),
                next_hops: vec![5],
                src_node_id: 1,
                group_id: 9,
            }],
        );

        let first = table
            .get_next_hops_by_flow(flow_id, None)
            .expect("initial multicast hop");
        assert_eq!(&first[..], &[5]);

        table.install_group_routes(
            9,
            1,
            vec![GroupRoutingTableEntry {
                route_id: multicast_route_id(9, 0),
                next_hops: vec![6],
                src_node_id: 1,
                group_id: 9,
            }],
        );

        let updated = table
            .get_next_hops_by_flow(flow_id, None)
            .expect("updated multicast hop");
        assert_eq!(&updated[..], &[6]);
    }

    #[test]
    fn multicast_lookup_respects_explicit_tree_id() {
        let config = make_config(2);
        let mut table = RoutingTable::new(config.clone());

        let group_ip = Ipv4Addr::new(10, 0, 0, 181);
        table.install_group_directory(vec![GroupDirectoryEntry {
            group_id: 12,
            group_ip,
        }]);

        table.install_group_routes(
            12,
            1,
            vec![
                GroupRoutingTableEntry {
                    route_id: multicast_route_id(12, 0),
                    next_hops: vec![5],
                    src_node_id: 1,
                    group_id: 12,
                },
                GroupRoutingTableEntry {
                    route_id: multicast_route_id(12, 1),
                    next_hops: vec![6, 7],
                    src_node_id: 1,
                    group_id: 12,
                },
            ],
        );

        let flow_id = make_flow_id(
            Ipv4Addr::new(10, 0, 0, 1),
            group_ip,
            4322,
            config.user_space_server_port,
        );

        let default_tree_hops = table
            .get_next_hops_by_flow(flow_id, None)
            .expect("default tree should resolve");
        assert_eq!(&default_tree_hops[..], &[5]);

        let explicit_tree_hops = table
            .get_next_hops_by_flow_and_tree(flow_id, Some(1), None)
            .expect("explicit tree should resolve");
        assert_eq!(&explicit_tree_hops[..], &[6, 7]);
    }

    #[test]
    fn multicast_lookup_hard_fails_on_unknown_tree() {
        let config = make_config(2);
        let mut table = RoutingTable::new(config.clone());

        let group_ip = Ipv4Addr::new(10, 0, 0, 182);
        table.install_group_directory(vec![GroupDirectoryEntry {
            group_id: 13,
            group_ip,
        }]);

        table.install_group_routes(
            13,
            1,
            vec![GroupRoutingTableEntry {
                route_id: multicast_route_id(13, 0),
                next_hops: vec![9],
                src_node_id: 1,
                group_id: 13,
            }],
        );

        let flow_id = make_flow_id(
            Ipv4Addr::new(10, 0, 0, 1),
            group_ip,
            4323,
            config.user_space_server_port,
        );

        let err = table
            .get_next_hops_by_flow_and_tree(flow_id, Some(3), None)
            .expect_err("unknown tree must hard fail");
        assert!(
            err.contains("Unknown multicast tree route"),
            "expected hard drop reason, got {err}"
        );
    }

    #[test]
    fn unicast_lookup_stays_inline() {
        let config = make_config(2);
        let mut table = RoutingTable::new(config.clone());

        table.install_routes(vec![RoutingTableEntry {
            route_id: 10,
            src_node_id: 2,
            dst_node_id: 3,
            next_hops: vec![7],
            forward_mode: RouteForwardingMode::Unicast,
        }]);

        let flow_id = make_flow_id(
            Ipv4Addr::new(10, 0, 0, 2),
            Ipv4Addr::new(10, 0, 0, 3),
            1234,
            config.user_space_server_port + 1,
        );

        let hops = table
            .get_next_hops_by_flow(flow_id, None)
            .expect("unicast hop should resolve");

        assert_eq!(&hops[..], &[7]);
        assert!(
            !hops.spilled(),
            "unicast hop allocation escaped inline buffer"
        );

        // ensure cached path also stays inline
        let hops_cached = table
            .get_next_hops_by_flow(flow_id, None)
            .expect("cached unicast hop should resolve");

        assert_eq!(&hops_cached[..], &[7]);
        assert!(
            !hops_cached.spilled(),
            "cached unicast hop allocation escaped inline buffer"
        );
    }

    mod multicast_tests {
        use super::*;

        #[test]
        fn install_group_routes_adds_members() {
            let config = make_config(1);
            let mut table = RoutingTable::new(config.clone());

            // Install group directory first
            let group_ip = Ipv4Addr::new(239, 0, 0, 1);
            table.install_group_directory(vec![GroupDirectoryEntry {
                group_id: 1,
                group_ip,
            }]);

            // Install routes for group 1 with two next-hop members
            table.install_group_routes(
                1,
                1,
                vec![GroupRoutingTableEntry {
                    route_id: multicast_route_id(1, 0),
                    next_hops: vec![2, 3],
                    src_node_id: 1,
                    group_id: 1,
                }],
            );

            // Verify the route was installed
            let key = RouteKey::Multicast {
                src_node_id: 1,
                group_id: 1,
                tree_id: 0,
            };
            assert!(table.available_routes.contains_key(&key));
            assert_eq!(table.available_routes.get(&key).unwrap().len(), 1);
            let encoded = encode_multicast_route_id(multicast_route_id(1, 0));
            assert_eq!(table.route_next_hop.get(&encoded).unwrap(), &vec![2, 3]);
        }

        #[test]
        fn install_group_routes_replaces_existing_membership() {
            let config = make_config(1);
            let mut table = RoutingTable::new(config.clone());

            let group_ip = Ipv4Addr::new(239, 0, 0, 2);
            table.install_group_directory(vec![GroupDirectoryEntry {
                group_id: 2,
                group_ip,
            }]);

            // Install initial routes
            table.install_group_routes(
                2,
                1,
                vec![GroupRoutingTableEntry {
                    route_id: multicast_route_id(2, 0),
                    next_hops: vec![2, 3],
                    src_node_id: 1,
                    group_id: 2,
                }],
            );

            // Update routes with different members
            table.install_group_routes(
                2,
                1,
                vec![GroupRoutingTableEntry {
                    route_id: multicast_route_id(2, 1),
                    next_hops: vec![2, 4],
                    src_node_id: 1,
                    group_id: 2,
                }],
            );

            // Old route should be removed
            let old_encoded = encode_multicast_route_id(multicast_route_id(2, 0));
            assert!(table.route_next_hop.get(&old_encoded).is_none());

            // New route should be active
            let encoded = encode_multicast_route_id(multicast_route_id(2, 1));
            assert_eq!(table.route_next_hop.get(&encoded).unwrap(), &vec![2, 4]);

            let key = RouteKey::Multicast {
                src_node_id: 1,
                group_id: 2,
                tree_id: 1,
            };
            let route_ids = table.available_routes.get(&key).unwrap();
            assert_eq!(route_ids.len(), 1);
            assert_eq!(route_ids[0], encoded);
        }

        #[test]
        fn install_group_routes_with_empty_next_hops() {
            let config = make_config(1);
            let mut table = RoutingTable::new(config.clone());

            let group_ip = Ipv4Addr::new(239, 0, 0, 4);
            table.install_group_directory(vec![GroupDirectoryEntry {
                group_id: 4,
                group_ip,
            }]);

            // Install route with no members
            table.install_group_routes(
                4,
                1,
                vec![GroupRoutingTableEntry {
                    route_id: multicast_route_id(4, 0),
                    next_hops: vec![],
                    src_node_id: 1,
                    group_id: 4,
                }],
            );

            let flow_id = make_flow_id(
                Ipv4Addr::new(10, 0, 0, 1),
                group_ip,
                6000,
                config.user_space_server_port,
            );

            let result = table.get_next_hops_by_flow(flow_id, None);

            // Should succeed but return empty hop list
            assert!(result.is_ok());
            assert_eq!(result.unwrap().len(), 0);
        }

        #[test]
        fn install_group_routes_multiple_groups() {
            let config = make_config(1);
            let mut table = RoutingTable::new(config.clone());

            let group_ip_1 = Ipv4Addr::new(239, 0, 0, 10);
            let group_ip_2 = Ipv4Addr::new(239, 0, 0, 20);

            table.install_group_directory(vec![
                GroupDirectoryEntry {
                    group_id: 10,
                    group_ip: group_ip_1,
                },
                GroupDirectoryEntry {
                    group_id: 20,
                    group_ip: group_ip_2,
                },
            ]);

            // Install routes for group 10
            table.install_group_routes(
                10,
                1,
                vec![GroupRoutingTableEntry {
                    route_id: multicast_route_id(10, 0),
                    next_hops: vec![2, 3],
                    src_node_id: 1,
                    group_id: 10,
                }],
            );

            // Install routes for group 20
            table.install_group_routes(
                20,
                1,
                vec![GroupRoutingTableEntry {
                    route_id: multicast_route_id(20, 0),
                    next_hops: vec![4, 5],
                    src_node_id: 1,
                    group_id: 20,
                }],
            );

            // Verify both groups are independently routable
            let flow_1 = make_flow_id(
                Ipv4Addr::new(10, 0, 0, 1),
                group_ip_1,
                7000,
                config.user_space_server_port,
            );
            let flow_2 = make_flow_id(
                Ipv4Addr::new(10, 0, 0, 1),
                group_ip_2,
                7001,
                config.user_space_server_port,
            );

            let hops_1 = table.get_next_hops_by_flow(flow_1, None).unwrap();
            let hops_2 = table.get_next_hops_by_flow(flow_2, None).unwrap();

            assert_eq!(&hops_1[..], &[2, 3]);
            assert_eq!(&hops_2[..], &[4, 5]);
        }

        #[test]
        fn install_group_directory_replaces_existing() {
            let config = make_config(1);
            let mut table = RoutingTable::new(config);

            let group_ip_old = Ipv4Addr::new(239, 0, 0, 50);
            let group_ip_new = Ipv4Addr::new(239, 0, 0, 51);

            // Install initial directory
            table.install_group_directory(vec![GroupDirectoryEntry {
                group_id: 50,
                group_ip: group_ip_old,
            }]);

            assert_eq!(table.group_dir.get(&group_ip_old), Some(&50));

            // Replace with new directory
            table.install_group_directory(vec![GroupDirectoryEntry {
                group_id: 51,
                group_ip: group_ip_new,
            }]);

            // Old mapping should be gone
            assert_eq!(table.group_dir.get(&group_ip_old), None);
            // New mapping should be present
            assert_eq!(table.group_dir.get(&group_ip_new), Some(&51));
        }

        #[test]
        fn multicast_flow_without_group_directory_returns_error() {
            let config = make_config(1);
            let mut table = RoutingTable::new(config.clone());

            // No group directory installed

            let flow_id = make_flow_id(
                Ipv4Addr::new(10, 0, 0, 1),
                Ipv4Addr::new(239, 0, 0, 99), // Unknown multicast IP
                8000,
                config.user_space_server_port,
            );

            let result = table.get_next_hops_by_flow(flow_id, None);
            assert!(
                matches!(result, Err(ref msg) if msg.contains("Unable to build route key")),
                "Expected error about missing route key, got {:?}",
                result
            );
        }

        #[test]
        fn multicast_route_key_uses_source_node_and_group_id() {
            let config = make_config(2);
            let mut table = RoutingTable::new(config.clone());

            let group_ip = Ipv4Addr::new(239, 1, 1, 1);
            table.install_group_directory(vec![GroupDirectoryEntry {
                group_id: 100,
                group_ip,
            }]);

            let flow_id = make_flow_id(
                Ipv4Addr::new(10, 0, 0, 1), // Node 1 as source
                group_ip,
                9000,
                config.user_space_server_port,
            );

            let key = table.key_for_flow(flow_id, None);

            assert_eq!(
                key,
                Some(RouteKey::Multicast {
                    src_node_id: 1,
                    group_id: 100,
                    tree_id: 0
                })
            );
        }

        #[test]
        fn install_group_routes_multiple_routes_same_group() {
            let config = make_config(1);
            let mut table = RoutingTable::new(config.clone());

            let group_ip = Ipv4Addr::new(239, 2, 2, 2);
            table.install_group_directory(vec![GroupDirectoryEntry {
                group_id: 200,
                group_ip,
            }]);

            // Install multiple routes for the same (src, group) pair
            table.install_group_routes(
                200,
                1,
                vec![
                    GroupRoutingTableEntry {
                        route_id: multicast_route_id(200, 1),
                        next_hops: vec![3],
                        src_node_id: 1,
                        group_id: 200,
                    },
                    GroupRoutingTableEntry {
                        route_id: multicast_route_id(200, 2),
                        next_hops: vec![4],
                        src_node_id: 1,
                        group_id: 200,
                    },
                ],
            );

            let key = RouteKey::Multicast {
                src_node_id: 1,
                group_id: 200,
                tree_id: 1,
            };
            let route_ids = table.available_routes.get(&key).unwrap();
            assert_eq!(route_ids.len(), 1);
            let enc1 = encode_multicast_route_id(multicast_route_id(200, 1));
            assert_eq!(route_ids[0], enc1);
            let key_tree_2 = RouteKey::Multicast {
                src_node_id: 1,
                group_id: 200,
                tree_id: 2,
            };
            let route_ids_tree_2 = table.available_routes.get(&key_tree_2).unwrap();
            assert_eq!(route_ids_tree_2.len(), 1);
            let enc2 = encode_multicast_route_id(multicast_route_id(200, 2));
            assert_eq!(route_ids_tree_2[0], enc2);
        }

        #[test]
        fn multicast_and_unicast_routes_coexist() {
            let config = make_config(1);
            let mut table = RoutingTable::new(config.clone());

            // Install unicast route
            table.install_routes(vec![RoutingTableEntry {
                route_id: 5000,
                src_node_id: 1,
                dst_node_id: 2,
                next_hops: vec![7],
                forward_mode: RouteForwardingMode::Unicast,
            }]);

            // Install multicast route
            let group_ip = Ipv4Addr::new(239, 5, 5, 5);
            table.install_group_directory(vec![GroupDirectoryEntry {
                group_id: 500,
                group_ip,
            }]);

            table.install_group_routes(
                500,
                1,
                vec![GroupRoutingTableEntry {
                    route_id: multicast_route_id(500, 0),
                    next_hops: vec![8, 9],
                    src_node_id: 1,
                    group_id: 500,
                }],
            );

            // Verify unicast flow
            let unicast_flow = make_flow_id(
                Ipv4Addr::new(10, 0, 0, 1),
                Ipv4Addr::new(10, 0, 0, 2),
                10000,
                config.user_space_server_port + 1,
            );
            let unicast_hops = table.get_next_hops_by_flow(unicast_flow, None).unwrap();
            assert_eq!(&unicast_hops[..], &[7]);

            // Verify multicast flow
            let multicast_flow = make_flow_id(
                Ipv4Addr::new(10, 0, 0, 1),
                group_ip,
                10001,
                config.user_space_server_port,
            );
            let multicast_hops = table.get_next_hops_by_flow(multicast_flow, None).unwrap();
            assert_eq!(&multicast_hops[..], &[8, 9]);
        }

        #[test]
        fn get_next_hop_by_flow_picks_single_from_multicast() {
            let config = make_config(1);
            let mut table = RoutingTable::new(config.clone());

            let group_ip = Ipv4Addr::new(239, 9, 9, 9);
            table.install_group_directory(vec![GroupDirectoryEntry {
                group_id: 999,
                group_ip,
            }]);

            table.install_group_routes(
                999,
                1,
                vec![GroupRoutingTableEntry {
                    route_id: multicast_route_id(999, 0),
                    next_hops: vec![10, 11, 12],
                    src_node_id: 1,
                    group_id: 999,
                }],
            );

            let flow_id = make_flow_id(
                Ipv4Addr::new(10, 0, 0, 1),
                group_ip,
                9999,
                config.user_space_server_port,
            );

            let hop = table.get_next_hop_by_flow(flow_id, None).unwrap();

            // Should pick one of the three
            assert!([10, 11, 12].contains(&hop));
        }

        #[test]
        fn copy_next_hops_rejects_invalid_nodes() {
            let result = RoutingTable::copy_next_hops(123, &[1, 2, INVALID, 4]);

            assert!(result.is_err());
            assert!(result.unwrap_err().contains("Route 123 invalid"));
        }

        #[test]
        fn select_route_for_multicast_tree_is_deterministic_and_cached() {
            let config = make_config(1);
            let mut table = RoutingTable::new(config);

            let key = RouteKey::Multicast {
                src_node_id: 1,
                group_id: 200,
                tree_id: 3,
            };
            let encoded = encode_multicast_route_id(multicast_route_id(200, 3));
            table.route_next_hop.insert(encoded, vec![42]);

            // Call multiple times to verify deterministic selection
            let first = table.select_route_for_key(&key);
            let second = table.select_route_for_key(&key);

            assert_eq!(first, second, "Same key should select same route");
            assert_eq!(first, Some(encoded));
            assert_eq!(table.multicast_tree_cache.get(&(1, 200, 3)), Some(&encoded));
        }
        #[test]
        fn pick_single_hop_chooses_from_multiple() {
            let hops = vec![10, 20, 30, 40, 50];

            // Sample multiple times to verify randomness works
            let mut seen = std::collections::HashSet::new();
            for _ in 0..100 {
                let hop = RoutingTable::pick_single_hop(&hops).unwrap();
                assert!(hops.contains(&hop));
                seen.insert(hop);
            }

            // With 100 samples, should see at least 3 different values
            assert!(seen.len() >= 3, "Random selection should vary");
        }
    }
}
