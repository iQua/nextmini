## Configuration File Example

```toml
reset_db = true
protocol = "quic"

# Preset topology (optional)
[routes_preset]
topology = "full_mesh"  # or "ring"
n_nodes = 4

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

## Route Table in Database

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

## Route Tables Distributed to Each Node

### Route Table for Node 1 (MessagePack Format)

```rust
ControllerToDataplane::InstallRoutes {
    routes: vec![
        // Preset routes (route_id 0-11)
        SimpleRouteEntry { route_id: 0,  next_hop: 2, src_node_id: 1, dst_node_id: 2 },  // [1,2]
        SimpleRouteEntry { route_id: 1,  next_hop: 3, src_node_id: 1, dst_node_id: 3 },  // [1,3]
        SimpleRouteEntry { route_id: 2,  next_hop: 4, src_node_id: 1, dst_node_id: 4 },  // [1,4]
        SimpleRouteEntry { route_id: 3,  next_hop: 1, src_node_id: 2, dst_node_id: 1 },  // [2,1] - local delivery
        SimpleRouteEntry { route_id: 4,  next_hop: 0, src_node_id: 2, dst_node_id: 3 },  // [2,3] - not via node 1
        SimpleRouteEntry { route_id: 5,  next_hop: 0, src_node_id: 2, dst_node_id: 4 },  // [2,4] - not via node 1
        SimpleRouteEntry { route_id: 6,  next_hop: 1, src_node_id: 3, dst_node_id: 1 },  // [3,1] - local delivery
        SimpleRouteEntry { route_id: 7,  next_hop: 0, src_node_id: 3, dst_node_id: 2 },  // [3,2] - not via node 1
        SimpleRouteEntry { route_id: 8,  next_hop: 0, src_node_id: 3, dst_node_id: 4 },  // [3,4] - not via node 1
        SimpleRouteEntry { route_id: 9,  next_hop: 1, src_node_id: 4, dst_node_id: 1 },  // [4,1] - local delivery
        SimpleRouteEntry { route_id: 10, next_hop: 0, src_node_id: 4, dst_node_id: 2 },  // [4,2] - not via node 1
        SimpleRouteEntry { route_id: 11, next_hop: 0, src_node_id: 4, dst_node_id: 3 },  // [4,3] - not via node 1

        // Custom routes (route_id 12-17)
        SimpleRouteEntry { route_id: 12, next_hop: 2, src_node_id: 1, dst_node_id: 4 },  // [1,2,3,4]
        SimpleRouteEntry { route_id: 13, next_hop: 3, src_node_id: 1, dst_node_id: 4 },  // [1,3,2,4]
        SimpleRouteEntry { route_id: 14, next_hop: 3, src_node_id: 1, dst_node_id: 4 },  // [1,3,4]
        SimpleRouteEntry { route_id: 15, next_hop: 1, src_node_id: 4, dst_node_id: 1 },  // [4,3,2,1] - local delivery
        SimpleRouteEntry { route_id: 16, next_hop: 1, src_node_id: 4, dst_node_id: 1 },  // [4,2,3,1] - local delivery
        SimpleRouteEntry { route_id: 17, next_hop: 1, src_node_id: 4, dst_node_id: 1 },  // [4,3,1] - local delivery
    ]
}
```

### Route Table for Node 2 (MessagePack Format)

```rust
ControllerToDataplane::InstallRoutes {
    routes: vec![
        // Preset routes (route_id 0-11)
        SimpleRouteEntry { route_id: 0,  next_hop: 2, src_node_id: 1, dst_node_id: 2 },  // [1,2] - local delivery
        SimpleRouteEntry { route_id: 1,  next_hop: 0, src_node_id: 1, dst_node_id: 3 },  // [1,3] - not via node 2
        SimpleRouteEntry { route_id: 2,  next_hop: 0, src_node_id: 1, dst_node_id: 4 },  // [1,4] - not via node 2
        SimpleRouteEntry { route_id: 3,  next_hop: 1, src_node_id: 2, dst_node_id: 1 },  // [2,1]
        SimpleRouteEntry { route_id: 4,  next_hop: 3, src_node_id: 2, dst_node_id: 3 },  // [2,3]
        SimpleRouteEntry { route_id: 5,  next_hop: 4, src_node_id: 2, dst_node_id: 4 },  // [2,4]
        SimpleRouteEntry { route_id: 6,  next_hop: 0, src_node_id: 3, dst_node_id: 1 },  // [3,1] - not via node 2
        SimpleRouteEntry { route_id: 7,  next_hop: 2, src_node_id: 3, dst_node_id: 2 },  // [3,2] - local delivery
        SimpleRouteEntry { route_id: 8,  next_hop: 0, src_node_id: 3, dst_node_id: 4 },  // [3,4] - not via node 2
        SimpleRouteEntry { route_id: 9,  next_hop: 0, src_node_id: 4, dst_node_id: 1 },  // [4,1] - not via node 2
        SimpleRouteEntry { route_id: 10, next_hop: 2, src_node_id: 4, dst_node_id: 2 },  // [4,2] - local delivery
        SimpleRouteEntry { route_id: 11, next_hop: 0, src_node_id: 4, dst_node_id: 3 },  // [4,3] - not via node 2

        // Custom routes (route_id 12-17)
        SimpleRouteEntry { route_id: 12, next_hop: 3, src_node_id: 1, dst_node_id: 4 },  // [1,2,3,4]
        SimpleRouteEntry { route_id: 13, next_hop: 4, src_node_id: 1, dst_node_id: 4 },  // [1,3,2,4] - via node 2
        SimpleRouteEntry { route_id: 14, next_hop: 0, src_node_id: 1, dst_node_id: 4 },  // [1,3,4] - not via node 2
        SimpleRouteEntry { route_id: 15, next_hop: 1, src_node_id: 4, dst_node_id: 1 },  // [4,3,2,1]
        SimpleRouteEntry { route_id: 16, next_hop: 3, src_node_id: 4, dst_node_id: 1 },  // [4,2,3,1] - via node 2
        SimpleRouteEntry { route_id: 17, next_hop: 0, src_node_id: 4, dst_node_id: 1 },  // [4,3,1] - not via node 2
    ]
}
```

### Route Table for Node 3 (MessagePack Format)

```rust
ControllerToDataplane::InstallRoutes {
    routes: vec![
        // Preset and custom routes
        SimpleRouteEntry { route_id: 0,  next_hop: 0, src_node_id: 1, dst_node_id: 2 },  // [1,2] - not via node 3
        SimpleRouteEntry { route_id: 1,  next_hop: 3, src_node_id: 1, dst_node_id: 3 },  // [1,3] - local delivery
        SimpleRouteEntry { route_id: 2,  next_hop: 0, src_node_id: 1, dst_node_id: 4 },  // [1,4] - not via node 3
        SimpleRouteEntry { route_id: 3,  next_hop: 0, src_node_id: 2, dst_node_id: 1 },  // [2,1] - not via node 3
        SimpleRouteEntry { route_id: 4,  next_hop: 3, src_node_id: 2, dst_node_id: 3 },  // [2,3] - local delivery
        SimpleRouteEntry { route_id: 5,  next_hop: 0, src_node_id: 2, dst_node_id: 4 },  // [2,4] - not via node 3
        SimpleRouteEntry { route_id: 6,  next_hop: 1, src_node_id: 3, dst_node_id: 1 },  // [3,1]
        SimpleRouteEntry { route_id: 7,  next_hop: 2, src_node_id: 3, dst_node_id: 2 },  // [3,2]
        SimpleRouteEntry { route_id: 8,  next_hop: 4, src_node_id: 3, dst_node_id: 4 },  // [3,4]
        SimpleRouteEntry { route_id: 9,  next_hop: 0, src_node_id: 4, dst_node_id: 1 },  // [4,1] - not via node 3
        SimpleRouteEntry { route_id: 10, next_hop: 0, src_node_id: 4, dst_node_id: 2 },  // [4,2] - not via node 3
        SimpleRouteEntry { route_id: 11, next_hop: 3, src_node_id: 4, dst_node_id: 3 },  // [4,3] - local delivery

        // Custom routes (route_id 12-17)
        SimpleRouteEntry { route_id: 12, next_hop: 4, src_node_id: 1, dst_node_id: 4 },  // [1,2,3,4]
        SimpleRouteEntry { route_id: 13, next_hop: 2, src_node_id: 1, dst_node_id: 4 },  // [1,3,2,4]
        SimpleRouteEntry { route_id: 14, next_hop: 4, src_node_id: 1, dst_node_id: 4 },  // [1,3,4]
        SimpleRouteEntry { route_id: 15, next_hop: 2, src_node_id: 4, dst_node_id: 1 },  // [4,3,2,1]
        SimpleRouteEntry { route_id: 16, next_hop: 1, src_node_id: 4, dst_node_id: 1 },  // [4,2,3,1]
        SimpleRouteEntry { route_id: 17, next_hop: 1, src_node_id: 4, dst_node_id: 1 },  // [4,3,1]
    ]
}
```

### Route Table for Node 4 (MessagePack Format)

```rust
ControllerToDataplane::InstallRoutes {
    routes: vec![
        // Preset and custom routes
        SimpleRouteEntry { route_id: 0,  next_hop: 0, src_node_id: 1, dst_node_id: 2 },  // [1,2] - not via node 4
        SimpleRouteEntry { route_id: 1,  next_hop: 0, src_node_id: 1, dst_node_id: 3 },  // [1,3] - not via node 4
        SimpleRouteEntry { route_id: 2,  next_hop: 4, src_node_id: 1, dst_node_id: 4 },  // [1,4] - local delivery
        SimpleRouteEntry { route_id: 3,  next_hop: 0, src_node_id: 2, dst_node_id: 1 },  // [2,1] - not via node 4
        SimpleRouteEntry { route_id: 4,  next_hop: 0, src_node_id: 2, dst_node_id: 3 },  // [2,3] - not via node 4
        SimpleRouteEntry { route_id: 5,  next_hop: 4, src_node_id: 2, dst_node_id: 4 },  // [2,4] - local delivery
        SimpleRouteEntry { route_id: 6,  next_hop: 0, src_node_id: 3, dst_node_id: 1 },  // [3,1] - not via node 4
        SimpleRouteEntry { route_id: 7,  next_hop: 0, src_node_id: 3, dst_node_id: 2 },  // [3,2] - not via node 4
        SimpleRouteEntry { route_id: 8,  next_hop: 4, src_node_id: 3, dst_node_id: 4 },  // [3,4] - local delivery
        SimpleRouteEntry { route_id: 9,  next_hop: 1, src_node_id: 4, dst_node_id: 1 },  // [4,1]
        SimpleRouteEntry { route_id: 10, next_hop: 2, src_node_id: 4, dst_node_id: 2 },  // [4,2]
        SimpleRouteEntry { route_id: 11, next_hop: 3, src_node_id: 4, dst_node_id: 3 },  // [4,3]

        // Custom routes (route_id 12-17)
        SimpleRouteEntry { route_id: 12, next_hop: 4, src_node_id: 1, dst_node_id: 4 },  // [1,2,3,4] - local delivery
        SimpleRouteEntry { route_id: 13, next_hop: 4, src_node_id: 1, dst_node_id: 4 },  // [1,3,2,4] - local delivery
        SimpleRouteEntry { route_id: 14, next_hop: 4, src_node_id: 1, dst_node_id: 4 },  // [1,3,4] - local delivery
        SimpleRouteEntry { route_id: 15, next_hop: 3, src_node_id: 4, dst_node_id: 1 },  // [4,3,2,1]
        SimpleRouteEntry { route_id: 16, next_hop: 2, src_node_id: 4, dst_node_id: 1 },  // [4,2,3,1]
        SimpleRouteEntry { route_id: 17, next_hop: 3, src_node_id: 4, dst_node_id: 1 },  // [4,3,1]
    ]
}
```

## Route Table Field Description

- **route_id**: Unique identifier for the route, automatically assigned by controller
- **next_hop**: The next hop node ID that this node should forward packets to
- **src_node_id**: Source node ID of the traffic
- **dst_node_id**: Destination node ID of the traffic

## Key Logic

1. **Route ID Assignment**:
   - Preset topology route IDs are assigned first (Full Mesh or Ring), then custom route IDs
   - Preset topology routes start numbering from 0

2. **next_hop Calculation**:
   - If current node is the destination: next_hop = own node ID (local delivery)
   - If current node is in the middle of the path: next_hop = next node ID in the path
   - If current node is not in the path: next_hop = 0 (invalid route)

3. **Route Deduplication**:
   - Uses database UNIQUE constraint `(src_node_id, dst_node_id, route)`
   - Duplicate routes are automatically skipped without assigning new route_id

4. **Message Format**:
   - Uses MessagePack binary format for serialization
   - Sent to dataplane nodes via WebSocket

5. **Auto Synchronization**:
   - Implements automatic route table synchronization through PostgreSQL notification mechanism
   - Automatically distributes latest route table to all available nodes when route table changes
   - Can be controlled via the `auto_db_sync` configuration parameter
