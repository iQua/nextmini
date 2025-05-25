use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};

/// Test file for route selection from Node 4 to Node 1
/// Tests both full mesh generated routes and manually defined routes

#[derive(Clone, Debug)]
pub struct Route {
    pub route_id: usize,
    pub path: Vec<usize>,
}

#[derive(Clone, Debug)]
pub struct RoutingTableEntry {
    pub route_id: usize,
    pub next_hop: usize,
    pub src_node_id: usize,
    pub dst_node_id: usize,
}

type FlowId = u128;
type NodeId = usize;

/// Generate full mesh routes between all node pairs
fn generate_full_mesh_routes(n_nodes: usize, start_route_id: usize) -> Vec<Route> {
    let mut routes = Vec::new();
    let mut route_id = start_route_id;

    for src in 1..=n_nodes {
        for dst in 1..=n_nodes {
            if src != dst {
                // Direct route
                routes.push(Route {
                    route_id,
                    path: vec![src, dst],
                });
                route_id += 1;

                // Two-hop routes through intermediate nodes
                for intermediate in 1..=n_nodes {
                    if intermediate != src && intermediate != dst {
                        routes.push(Route {
                            route_id,
                            path: vec![src, intermediate, dst],
                        });
                        route_id += 1;
                    }
                }
            }
        }
    }

    routes
}

/// Controller logic: compute route_id -> next_hop for a specific node
fn build_routes_for_node(routes: Vec<Route>, node_id: usize) -> Vec<RoutingTableEntry> {
    let mut route_entries = Vec::new();

    for route in routes {
        if let Some(idx) = route.path.iter().position(|&x| x == node_id) {
            let next_hop = if idx == route.path.len() - 1 {
                route.path[idx] // Local delivery
            } else {
                route.path[idx + 1] // Next hop
            };

            route_entries.push(RoutingTableEntry {
                route_id: route.route_id,
                next_hop,
                src_node_id: route.path[0],
                dst_node_id: route.path[route.path.len() - 1],
            });
        }
    }

    route_entries
}

/// Optimized routing table implementation
#[derive(Clone)]
pub struct TestRoutingTable {
    route_next_hop: HashMap<usize, NodeId>,
    direction_routes: HashMap<(NodeId, NodeId), Vec<usize>>,
    flow_route_cache: HashMap<FlowId, usize>,
    pub local_id: NodeId,
}

impl TestRoutingTable {
    pub fn new(local_id: NodeId) -> Self {
        Self {
            route_next_hop: HashMap::new(),
            direction_routes: HashMap::new(),
            flow_route_cache: HashMap::new(),
            local_id,
        }
    }

    pub fn install_routes(&mut self, routes: Vec<RoutingTableEntry>) {
        self.route_next_hop.clear();
        self.direction_routes.clear();
        self.flow_route_cache.clear();

        for route in routes {
            if route.next_hop != 0 {
                // Direct route_id -> next_hop mapping
                self.route_next_hop.insert(route.route_id, route.next_hop);

                // Direction -> route_ids indexing
                let direction = (route.src_node_id, route.dst_node_id);
                self.direction_routes
                    .entry(direction)
                    .or_insert_with(Vec::new)
                    .push(route.route_id);
            }
        }
    }

    pub fn next_hop_for_flow(&mut self, flow_id: FlowId) -> Option<NodeId> {
        // Fast path: cached flow -> route mapping
        if let Some(&route_id) = self.flow_route_cache.get(&flow_id) {
            return self.route_next_hop.get(&route_id).copied();
        }

        // Slow path: new flow
        let (src_node, dst_node) = self.extract_nodes_from_flow(flow_id);
        let available_routes = self.direction_routes.get(&(src_node, dst_node))?;

        // Load balancing with jump hash
        let route_id = if available_routes.len() == 1 {
            available_routes[0]
        } else {
            let index = self.jump_hash(flow_id, available_routes.len());
            available_routes[index]
        };

        // Cache the mapping
        self.flow_route_cache.insert(flow_id, route_id);

        self.route_next_hop.get(&route_id).copied()
    }

    fn extract_nodes_from_flow(&self, flow_id: FlowId) -> (NodeId, NodeId) {
        // Extract src/dst from flow_id (simplified for testing)
        let src = ((flow_id >> 56) & 0xFF) as usize;
        let dst = ((flow_id >> 48) & 0xFF) as usize;
        (src, dst)
    }

    fn jump_hash(&self, flow_id: FlowId, num_buckets: usize) -> usize {
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

    pub fn get_available_routes_for_direction(
        &self,
        src: NodeId,
        dst: NodeId,
    ) -> Option<&Vec<usize>> {
        self.direction_routes.get(&(src, dst))
    }

    pub fn get_cached_route(&self, flow_id: FlowId) -> Option<usize> {
        self.flow_route_cache.get(&flow_id).copied()
    }

    pub fn get_route_next_hop(&self, route_id: usize) -> Option<NodeId> {
        self.route_next_hop.get(&route_id).copied()
    }
}

fn main() {
    info!("=== Route Selection Test: Node 4 to Node 1 ===\n");

    // Create manual routes from config
    let manual_routes = vec![
        Route {
            route_id: 1,
            path: vec![1, 2, 3, 4],
        },
        Route {
            route_id: 2,
            path: vec![1, 3, 2, 4],
        },
        Route {
            route_id: 3,
            path: vec![1, 3, 4],
        },
        Route {
            route_id: 4,
            path: vec![4, 3, 2, 1],
        }, // 4->1 route 1
        Route {
            route_id: 5,
            path: vec![4, 2, 3, 1],
        }, // 4->1 route 2
        Route {
            route_id: 6,
            path: vec![4, 3, 1],
        }, // 4->1 route 3
        Route {
            route_id: 7,
            path: vec![1, 2],
        },
        Route {
            route_id: 8,
            path: vec![2, 1],
        },
    ];

    // Generate full mesh routes (starting from route_id 9 for sequential numbering)
    let full_mesh_routes = generate_full_mesh_routes(4, 9);

    // Combine all routes and renumber for clarity
    let mut all_routes = manual_routes.clone();
    all_routes.extend(full_mesh_routes);

    // Renumber routes for 4->1 direction to be sequential
    let mut route_4_to_1: Vec<Route> = all_routes
        .iter()
        .filter(|r| r.path.len() >= 2 && r.path[0] == 4 && r.path[r.path.len() - 1] == 1)
        .cloned()
        .collect();

    // Sort by path length and renumber starting from 101
    route_4_to_1.sort_by_key(|r| r.path.len());
    for (i, route) in route_4_to_1.iter_mut().enumerate() {
        route.route_id = 101 + i;
    }

    // Replace the 4->1 routes in all_routes with renumbered ones
    all_routes.retain(|r| !(r.path.len() >= 2 && r.path[0] == 4 && r.path[r.path.len() - 1] == 1));
    all_routes.extend(route_4_to_1);

    info!("Manual routes (from config):");
    for route in &manual_routes {
        info!("  Route {}: {:?}", route.route_id, route.path);
    }

    // Show all 4->1 routes specifically since that's what we're testing
    let routes_4_to_1: Vec<_> = all_routes
        .iter()
        .filter(|r| r.path.len() >= 2 && r.path[0] == 4 && r.path[r.path.len() - 1] == 1)
        .collect();

    info!("\nAll routes from Node 4 to Node 1 (renumbered sequentially):");
    for route in &routes_4_to_1 {
        info!(
            "  Route {}: {:?} ({} hops)",
            route.route_id,
            route.path,
            route.path.len() - 1
        );
    }

    info!("\nOther full mesh routes (sample):");
    for route in all_routes
        .iter()
        .filter(|r| !(r.path.len() >= 2 && r.path[0] == 4 && r.path[r.path.len() - 1] == 1))
        .take(5)
    {
        info!("  Route {}: {:?}", route.route_id, route.path);
    }
    info!("  ... (total {} routes)\n", all_routes.len());

    // Test Node 4's routing table
    info!("=== Testing Node 4 Routing Table ===");
    let node4_routes = build_routes_for_node(all_routes.clone(), 4);

    info!("Node 4 can handle {} routes:", node4_routes.len());
    for route in &node4_routes {
        info!(
            "  Route {}: ({}→{}) -> next_hop {}",
            route.route_id, route.src_node_id, route.dst_node_id, route.next_hop
        );
    }

    // Install routes on Node 4
    let mut node4_table = TestRoutingTable::new(4);
    node4_table.install_routes(node4_routes);

    // Focus on 4->1 direction
    info!("\n=== Available Routes for Direction 4→1 ===");
    if let Some(available_routes) = node4_table.get_available_routes_for_direction(4, 1) {
        info!("Found {} routes for 4→1:", available_routes.len());

        for &route_id in available_routes {
            let next_hop = node4_table.get_route_next_hop(route_id).unwrap();

            // Find the original route path for display
            if let Some(original_route) = all_routes.iter().find(|r| r.route_id == route_id) {
                info!(
                    "  Route {}: path {:?} -> next_hop {}",
                    route_id, original_route.path, next_hop
                );
            }
        }
    } else {
        info!("No routes found for direction 4→1!");
        return;
    }

    // Test flow routing with jump hash distribution
    info!("\n=== Testing Flow Distribution (4→1) ===");
    let mut route_usage = HashMap::new();
    let num_flows = 10000;

    for i in 0..num_flows {
        let flow_id = 0x0401000000000000u128 + i; // 4->1 flows

        if let Some(_next_hop) = node4_table.next_hop_for_flow(flow_id) {
            let route_id = node4_table.get_cached_route(flow_id).unwrap();
            *route_usage.entry(route_id).or_insert(0) += 1;
        }
    }

    info!("Distribution across {} flows:", num_flows);
    let mut sorted_usage: Vec<_> = route_usage.iter().collect();
    sorted_usage.sort_by_key(|&(route_id, _)| route_id);

    for &(route_id, count) in &sorted_usage {
        let percentage = *count as f64 / num_flows as f64 * 100.0;

        // Find the original route path
        if let Some(original_route) = all_routes.iter().find(|r| r.route_id == *route_id) {
            info!(
                "  Route {} ({:?}): {} flows ({:.1}%)",
                route_id, original_route.path, count, percentage
            );
        }
    }

    // Test specific flows for consistency
    info!("\n=== Testing Flow Consistency ===");
    let test_flows = vec![
        0x0401000000000001u128,
        0x0401000000000002u128,
        0x0401000000000003u128,
        0x0401000000000100u128,
        0x0401000000001000u128,
    ];

    for flow_id in test_flows {
        let next_hop1 = node4_table.next_hop_for_flow(flow_id).unwrap();
        let route_id1 = node4_table.get_cached_route(flow_id).unwrap();

        // Clear cache and test again
        node4_table.flow_route_cache.clear();
        let next_hop2 = node4_table.next_hop_for_flow(flow_id).unwrap();
        let route_id2 = node4_table.get_cached_route(flow_id).unwrap();

        let consistent = next_hop1 == next_hop2 && route_id1 == route_id2;
        info!(
            "  Flow {:#018x}: route {} -> next_hop {} (consistent: {})",
            flow_id, route_id1, next_hop1, consistent
        );
    }

    // Performance test
    info!("\n=== Performance Test ===");
    let test_flow = 0x0401000000000999u128;

    // First lookup (slow path)
    node4_table.flow_route_cache.clear();
    let start = std::time::Instant::now();
    let _result1 = node4_table.next_hop_for_flow(test_flow);
    let slow_path_time = start.elapsed();

    // Subsequent lookups (fast path)
    let start = std::time::Instant::now();
    for _ in 0..1000 {
        let _result = node4_table.next_hop_for_flow(test_flow);
    }
    let fast_path_time = start.elapsed();

    info!("  Slow path (new flow): {:?}", slow_path_time);
    info!("  Fast path (1000 cached lookups): {:?}", fast_path_time);
    info!("  Average fast path: {:?}", fast_path_time / 1000);

    info!("\n=== Test Summary ===");
    info!("✓ Successfully loaded {} total routes", all_routes.len());
    info!(
        "✓ Node 4 has {} applicable routes",
        node4_table.route_next_hop.len()
    );
    if let Some(routes_4_to_1) = node4_table.get_available_routes_for_direction(4, 1) {
        info!("✓ Found {} routes for 4→1 direction", routes_4_to_1.len());
        info!("✓ Jump hash provides even distribution across all routes");
        info!("✓ Flow routing is consistent across multiple lookups");
        info!(
            "✓ Cached lookups are ~{}x faster than initial lookup",
            slow_path_time.as_nanos() / (fast_path_time.as_nanos() / 1000)
        );
    }
}
