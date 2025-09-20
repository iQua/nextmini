## Configuration File Example

```toml
protocol = "quic"

[topology]
type = "full_mesh"
full_mesh_config = { n_nodes = 4 }

# Custom routes
[[routes]]
route = [1, 2, 3, 4]

[[routes]]
route = [1, 3, 2, 4]

[[routes]]
route = [1, 3, 4]

[[routes]]
route = [4, 3, 2, 1]

[[routes]]
route = [4, 2, 3, 1]

[[routes]]
route = [4, 3, 1]

[[routes]]
route = [1, 2]

[[routes]]
route = [2, 1]
```

## Routes Table in Database

Routes stored in database after controller automatically assigns route_id:

| route_id | src_node_id | dst_node_id | route         |
|----------|-------------|-------------|---------------|
| 0        | 1           | 2           | [1, 2]        |
| 1        | 1           | 3           | [1, 3]        |
| 2        | 1           | 4           | [1, 4]        |
| 3        | 2           | 1           | [2, 1]        |
| 4        | 2           | 3           | [2, 3]        |
| 5        | 2           | 4           | [2, 4]        |
| 6        | 3           | 1           | [3, 1]        |
| 7        | 3           | 2           | [3, 2]        |
| 8        | 3           | 4           | [3, 4]        |
| 9        | 4           | 1           | [4, 1]        |
| 10       | 4           | 2           | [4, 2]        |
| 11       | 4           | 3           | [4, 3]        |
| 12       | 1           | 4           | [1, 2, 3, 4]  |
| 13       | 1           | 4           | [1, 3, 2, 4]  |
| 14       | 1           | 4           | [1, 3, 4]     |
| 15       | 4           | 1           | [4, 3, 2, 1]  |
| 16       | 4           | 1           | [4, 2, 3, 1]  |
| 17       | 4           | 1           | [4, 3, 1]     |

Note: Custom routes `[1, 2]` and `[2, 1]` overlap with preset topology routes, so they won't be added to the database again.

## Routes Distributed to Each Node

### Routes for Node 1 (MessagePack Format)

```rust
ControllerToDataplane::InstallRoutes {
    routes: vec![
        // Preset routes (route_id 0-11)
        RoutingTableEntry { route_id: 0,  next_hop: 2, src_node_id: 1, dst_node_id: 2 },  // [1,2]
        RoutingTableEntry { route_id: 1,  next_hop: 3, src_node_id: 1, dst_node_id: 3 },  // [1,3]
        RoutingTableEntry { route_id: 2,  next_hop: 4, src_node_id: 1, dst_node_id: 4 },  // [1,4]
        RoutingTableEntry { route_id: 3,  next_hop: 1, src_node_id: 2, dst_node_id: 1 },  // [2,1] - local delivery
        RoutingTableEntry { route_id: 4,  next_hop: 0, src_node_id: 2, dst_node_id: 3 },  // [2,3] - not via node 1
        RoutingTableEntry { route_id: 5,  next_hop: 0, src_node_id: 2, dst_node_id: 4 },  // [2,4] - not via node 1
        RoutingTableEntry { route_id: 6,  next_hop: 1, src_node_id: 3, dst_node_id: 1 },  // [3,1] - local delivery
        RoutingTableEntry { route_id: 7,  next_hop: 0, src_node_id: 3, dst_node_id: 2 },  // [3,2] - not via node 1
        RoutingTableEntry { route_id: 8,  next_hop: 0, src_node_id: 3, dst_node_id: 4 },  // [3,4] - not via node 1
        RoutingTableEntry { route_id: 9,  next_hop: 1, src_node_id: 4, dst_node_id: 1 },  // [4,1] - local delivery
        RoutingTableEntry { route_id: 10, next_hop: 0, src_node_id: 4, dst_node_id: 2 },  // [4,2] - not via node 1
        RoutingTableEntry { route_id: 11, next_hop: 0, src_node_id: 4, dst_node_id: 3 },  // [4,3] - not via node 1

        // Custom routes (route_id 12-17)
        RoutingTableEntry { route_id: 12, next_hop: 2, src_node_id: 1, dst_node_id: 4 },  // [1,2,3,4]
        RoutingTableEntry { route_id: 13, next_hop: 3, src_node_id: 1, dst_node_id: 4 },  // [1,3,2,4]
        RoutingTableEntry { route_id: 14, next_hop: 3, src_node_id: 1, dst_node_id: 4 },  // [1,3,4]
        RoutingTableEntry { route_id: 15, next_hop: 1, src_node_id: 4, dst_node_id: 1 },  // [4,3,2,1] - local delivery
        RoutingTableEntry { route_id: 16, next_hop: 1, src_node_id: 4, dst_node_id: 1 },  // [4,2,3,1] - local delivery
        RoutingTableEntry { route_id: 17, next_hop: 1, src_node_id: 4, dst_node_id: 1 },  // [4,3,1] - local delivery
    ]
}
```

## Key Logic

   - Default routes in the topology are direct one-hop links, and they are assigned before custom route IDs
   - If the current node is the destination: next_hop = own node ID (local delivery)
   - If current node is in the middle of the path: next_hop = next node ID in the path
   - If current node is not in the path: next_hop = 0 (invalid route)
   - Duplicate routes are automatically skipped without assigning new route_id
   - Uses MessagePack binary format for serialization
   - Sent to dataplane nodes via WebSocket
   - Implements automatic route table synchronization through the PostgreSQL notification mechanism
   - Automatically distributes the latest route table to all available nodes when route table changes
