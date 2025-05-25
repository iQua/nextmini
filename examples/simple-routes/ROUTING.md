### New Architecture: Source-Selected Routing

The system now uses a **simplified, source-selected routing approach**:

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

### Visual Flow Example: Route 12 [1→2→3→4]

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

### Route Selection Decision Tree

```
                    📦 Packet Arrives
                           │
                      ┌────▼─────┐
                      │ Has      │
                      │route_id? │◄─── Already set by source node
                      └────┬─────┘
                      YES  │  NO
                   ┌───────┘  └───────┐
                   ▼                  ▼
            ┌─────────────┐    ┌─────────────┐
            │ USE existing│    │ This is     │
            │ route_id    │    │ SOURCE node │
            │             │    │             │
            │ O(1) lookup │    │ SELECT      │
            │ in route    │    │ route_id    │
            │ table       │    │ using       │
            └─────────────┘    │ jump_hash   │
                   │           └─────────────┘
                   │                  │
                   └──────────────────┼─────────── Forward to next_hop
                                      │
                                      ▼
                               ┌─────────────┐
                               │ ADD route_id│
                               │ to packet   │
                               │ header      │
                               └─────────────┘
```
