# NextMini Source-Selected Routing Architecture

## Core Concept

The system uses **source-selected routing** where:
- **Source nodes** select route IDs using consistent hashing
- **Intermediate nodes** perform O(1) lookups using the route ID
- **Route IDs** are embedded in packet headers for stateless forwarding

## Packet Journey

```
┌─────────────────────────────────────────────────────────────────────┐
│                         Packet Journey                              │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  1. Packet arrives at SOURCE node without route_id                  │
│     ┌─────────────────────────────────────────────┐                 │
│     │ Packet { flow_id: 0x..., route_id: None }  │                 │
│     └─────────────────────────────────────────────┘                 │
│                           │                                         │
│                           ▼                                         │
│  2. Source node selects route using jump hash                       │
│     ┌─────────────────────────────────────────────┐                 │
│     │ available_routes = [12, 13, 14]            │                 │
│     │ hash_result = jump_hash(flow_id, 3)        │                 │
│     │ selected_route_id = available_routes[hash]  │                 │
│     └─────────────────────────────────────────────┘                 │
│                           │                                         │
│                           ▼                                         │
│  3. Source adds route_id to packet                                  │
│     ┌─────────────────────────────────────────────┐                 │
│     │ Packet { flow_id: 0x..., route_id: Some(12)}│                │
│     └─────────────────────────────────────────────┘                 │
│                           │                                         │
│                           ▼                                         │
│  4. Intermediate nodes use route_id for lookup                      │
│     ┌─────────────────────────────────────────────┐                 │
│     │ next_hop = route_table[route_id]            │                 │
│     │ // No recalculation needed!                 │                 │
│     └─────────────────────────────────────────────┘                 │
└─────────────────────────────────────────────────────────────────────┘
```

## Visual Flow Example: Route 12 [1→2→3→4]

```
┌─────────┐    route_id=12     ┌─────────┐    route_id=12     ┌─────────┐    route_id=12     ┌─────────┐
│ Node 1  │ ─────────────────→ │ Node 2  │ ─────────────────→ │ Node 3  │ ─────────────────→ │ Node 4  │
│ (SRC)   │                    │ (FWD)   │                    │ (FWD)   │                    │ (DST)   │
└─────────┘                    └─────────┘                    └─────────┘                    └─────────┘
     │                              │                              │                              │
     ▼                              ▼                              ▼                              ▼
SELECT route_id              USE route_id               USE route_id                    DELIVER
from available               for O(1) lookup            for O(1) lookup                 locally
routes [12,13,14]           next_hop = 3               next_hop = 4
jump_hash(flow_id,3)
→ route_id = 12
```

## Decision Flow

```
                    📦 Packet Arrives
                           │
                      ┌────▼─────┐
                      │ Has      │
                      │route_id? │◄─── Check IP Options field
                      └────┬─────┘
                      YES  │  NO
                   ┌───────┘  └───────┐
                   ▼                  ▼
            ┌─────────────┐    ┌─────────────┐
            │ USE existing│    │ This is     │
            │ route_id    │    │ SOURCE node │
            │             │    │             │
            │ O(1) lookup │    │ SELECT      │
            │ next_hop =  │    │ route_id    │
            │ table[id]   │    │ using       │
            └─────────────┘    │ jump_hash   │
                   │           └─────────────┘
                   │                  │
                   │                  ▼
                   │           ┌─────────────┐
                   │           │ EMBED       │
                   │           │ route_id    │
                   │           │ in packet   │
                   │           │ options     │
                   │           └─────────────┘
                   │                  │
                   └──────────────────┼─────────── Forward to next_hop
```

## Multi-Path Load Balancing

### Case 1: Few Flows (Flows ≤ Routes)
```
Direction: Node A → Node D
Available Routes: [Route 12, Route 13, Route 14]

Flow 1 ──┐
Flow 2 ──┼─→ jump_hash() ──┐
Flow 3 ──┘                │
                          ▼
         ┌─────────────────────────────────┐
         │  Route Selection Distribution   │
         │                                 │
         │  Flow 1 → Route 12 (A→B→D)     │
         │  Flow 2 → Route 13 (A→C→D)     │
         │  Flow 3 → Route 14 (A→E→D)     │
         └─────────────────────────────────┘
```

### Case 2: Many Flows (Flows > Routes)
```
Direction: Node A → Node D
Available Routes: [Route 12, Route 13, Route 14]

Flow 1 ──┐
Flow 2 ──┤
Flow 3 ──┤
Flow 4 ──┼─→ jump_hash() ──┐
Flow 5 ──┤                │
Flow 6 ──┤                │
Flow 7 ──┘                ▼
         ┌─────────────────────────────────────────┐
         │     Consistent Hash Distribution        │
         │                                         │
         │  Flow 1, 4 → Route 12 (A→B→D)         │
         │  Flow 2, 6 → Route 13 (A→C→D)         │
         │  Flow 3, 5, 7 → Route 14 (A→E→D)      │
         │                                         │
         │  Same flow_id always → same route_id   │
         └─────────────────────────────────────────┘

Key: Jump hash ensures consistent mapping even with many flows
```

## Performance Benefits

| Feature | Source Node | Intermediate Node |
|---------|-------------|-------------------|
| Route Calculation | O(1) after cache | **None needed** |
| Table Lookup | O(1) | **O(1)** |
| State Tracking | Flow cache only | **Stateless** |
| Load Balancing | Jump hash | **N/A** |

## Key Advantages

1. **O(1) Forwarding**: Intermediate nodes perform constant-time lookups
2. **Consistent Routing**: Same flow always uses same route via jump hash
3. **Stateless Design**: No per-flow state at intermediate nodes
4. **Load Balancing**: Automatic distribution across multiple paths
5. **Simple Protocol**: Route ID embedded in standard IP options