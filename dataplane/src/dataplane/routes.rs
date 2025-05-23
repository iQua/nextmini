use fxhash::FxHashMap;
use serde_json::Value;

use crate::dataplane::FLOW_ID_PATH_MASK;
use crate::dataplane::FlowId;
use crate::dataplane::NodeId;

use crate::dataplane::packet::json_byte_array_to_flow_id;

#[derive(Clone)]
pub struct Route {
    next_hop: NodeId,
    id: u8,
    // streams: Vec<SocketId>,
}

#[allow(unused)]
impl Route {
    pub fn new(next_hop: NodeId, id: u8) -> Self {
        Self {
            next_hop,
            id,
            // streams,
        }
    }

    pub fn from_json(object: &Value) -> Self {
        let mut streams = vec![];
        if let Some(streams_array) = object["streams"].as_array() {
            for stream in streams_array {
                if let Some(stream_str) = stream.as_str() {
                    let parts: Vec<&str> = stream_str.split(':').collect();
                    if parts.len() == 2 {
                        if let (Ok(port), Ok(id)) =
                            (parts[0].parse::<u16>(), parts[1].parse::<u16>())
                        {
                            streams.push((port, id));
                            continue;
                        }
                    }
                    eprintln!("Warning: Invalid stream string format: {}", stream_str);
                }
            }
        }
        Self {
            next_hop: object["next_hop"]
                .as_u64()
                .expect("Invalid next_hop field in JSON object, expected usize")
                as usize,
            id: object["id"]
                .as_u64()
                .expect("Invalid route id field in JSON object, expected usize")
                as u8,
            // streams,
        }
    }

    pub fn get_next_hop(&self) -> NodeId {
        self.next_hop
    }
}

#[derive(Clone)]
#[allow(unused)]
pub struct Flow {
    flow_id: FlowId,
    routes: Vec<Route>,
}

impl Flow {
    pub fn new(flow_id: FlowId, routes: Vec<Route>) -> Self {
        // let scheduler = StrideScheduler::from_routes(& mut routes);
        Self { flow_id, routes }
    }

    pub fn from_json(object: &Value) -> Self {
        let mut routes = Vec::<Route>::new();
        if let Some(routes_array) = object["routes"].as_array() {
            for route in routes_array {
                routes.push(Route::from_json(route));
            }
        }

        let flow_id = json_byte_array_to_flow_id(&object["flow_id"]);

        Self::new(flow_id, routes)
    }
}

#[derive(Clone)]
pub struct RoutingTable {
    // stream_mapping: FxHashMap<(FlowId, SocketId), u8>,
    next_hop: FxHashMap<FlowId, NodeId>,
    n_routes: FxHashMap<FlowId, usize>,
    pub local_id: NodeId,
}

impl RoutingTable {
    pub fn new(local_id: NodeId) -> RoutingTable {
        RoutingTable {
            // stream_mapping: FxHashMap::default(),
            next_hop: FxHashMap::default(),
            n_routes: FxHashMap::default(),
            local_id,
        }
    }

    pub fn merge(&mut self, routing_table: RoutingTable) {
        // Updates the routing table with the new one
        // Keeps residual path fragments from the old routing table
        // to avoid dropping all packets in the queue.
        for (flow_id, next_hop) in routing_table.next_hop {
            self.next_hop.insert(flow_id, next_hop);
        }
        for (flow_id, n_routes) in routing_table.n_routes {
            self.n_routes.insert(flow_id, n_routes);
        }
        // for (flow_id, route_id) in routing_table.stream_mapping {
        //     self.stream_mapping.insert(flow_id, route_id);
        // }
    }

    pub fn add_flow(&mut self, flow: Flow) {
        // adds a new flow to the routing table, and replaces if it already exists
        let flow_id = flow.flow_id;

        for route in flow.routes.clone() {
            let route_id = route.id as u64;

            // adds the route id to the third component of the sender ipv4 address
            let flow_route_id = flow_id + (route_id << 40);

            self.next_hop
                .insert(flow_route_id & FLOW_ID_PATH_MASK, route.next_hop);
            self.n_routes
                .insert(flow_id & FLOW_ID_PATH_MASK, flow.routes.len());
            // for stream in route.streams {
            //     self.stream_mapping.insert((flow_id, stream), route.id);
            // }
        }
    }

    pub fn get_num_paths(&self, flow_id: &FlowId) -> usize {
        self.n_routes
            .get(&(*flow_id & FLOW_ID_PATH_MASK))
            .copied()
            .unwrap_or(1)
    }

    // pub fn get_path_id(&self, flow_id: &FlowId, stream_id: &SocketId) -> Option<u8> {
    //     self.stream_mapping.get(&(*flow_id, *stream_id)).copied()
    // }

    // pub fn insert_stream_mapping(&mut self, flow_id: FlowId, stream_id: SocketId, route_id: u8) {
    //     self.stream_mapping.insert((flow_id, stream_id), route_id);
    // }

    pub fn next_hop(&self, flow_id: &FlowId) -> Option<&NodeId> {
        self.next_hop.get(&(*flow_id & FLOW_ID_PATH_MASK))
    }
}
