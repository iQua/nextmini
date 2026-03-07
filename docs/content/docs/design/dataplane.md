---
title: "Dataplane Design"
description: "Design-level explanation of packet processing, ingress modes, and routing lookup in the dataplane."
---

This page gives the high-level runtime flow for packet handling in the dataplane and how routes are resolved before forwarding.

The `Conductor` owns one control-plane interface, one or more processor actors, and one connector actor. Which actor handles a packet is decided by `local destination`, `operating_mode`, and transport mode.

## Implementation-level behavior

A packet never jumps directly into forwarding logic. It is first accepted by a `ProcessorHandle` and then either sent to processor lanes or to the connector path.

If the destination is the local node, routing stays inside the processor path. If the destination is remote and `OperatingMode::Max` is enabled, routing moves to the connector path. In all other cases, including normal mode, remote packets stay on the processor path.

How packets are queued depends on `Feature::Sequential` versus `Feature::Concurrent`:

In sequential mode, packets are hashed into one bounded channel per lane and handled by one task per lane. FEC lossless traffic is also lane-hashed by `(flow_id, tree_id)` so different trees do not contend on one lane by default. This is the only current processor mode that exposes a tree-visible ingress surface for multi-tree lossless backpressure.  
In concurrent mode, all packets share one bounded `flume` channel and are processed by multiple workers, with a warning that collaborative FEC trees are not isolated per-worker lane. A full shared queue in this mode only indicates global ingress pressure, not pressure on any specific tree.

In both modes, `channel_backpressure` decides behavior when queues are full: blocking send for backpressure, immediate drop when not. In the current multi-tree FEC sender, that queue pressure is observed asynchronously: a tree worker blocks in `process_packet(...).await`, its sender-local tree lane stops draining, and the sender eventually treats that lane as blocked. That is why collaborative multi-tree lossless currently depends on sequential ingress with a tree-visible queueing surface.

The processor actors all subscribe to the same broadcast channel, so controller-sourced updates to routes, group directory, and other runtime state are observed consistently across lanes.

Both processor and connector tasks consume in short batches: they handle one item and then drain as many already-queued packets as possible before returning to the `select` loop. This reduces scheduling overhead and preserves cache locality.

The connector is a separate actor with its own queue and its own `RoutingTable`. In max mode, once routing chooses connector egress, it establishes or reuses a TCP-max scheduler toward the selected next hop. A route miss in the connector is still treated as a hard miss: the packet is dropped, and the dataplane logs the reason rather than inventing a fallback.

Local packet delivery follows a short chain: route resolution produces next hops, the processor clones packets for local multicast fan-out, then each copy is sent. If the next hop is local, the flow may continue through lossless delivery, local interface write for `local_address`, or user-space TCP send. If the next hop is remote, that hop’s scheduler is invoked.

## Processor Route Resolution

Routing decisions are centralized in `RoutingTable`.

Route keys are derived from two inputs: extracted node IDs from a packet flow and local group-directory state. Unicast keys become `(src_node_id, dst_node_id)`. Group destination IPs are first mapped to `(src_node_id, group_id)` and then either include an explicit tree id or fall back to control-tree selection.

For unicast, the first resolution result for a flow is cached, so subsequent packets from the same flow reuse the same route id.  
For multicast, route selection is tree-aware: if a packet carries an explicit tree, that tree is used; if not, the table chooses tree 0 when available, otherwise the smallest installed tree for that `(src, group)` pair.

The routing table keeps two multicast caches, one for explicit tree selection and one for control-tree fallback. Both caches are invalidated when the corresponding route set is reinstalled, so new controller state becomes effective on the next lookup.

On success, unicast and processor-local multicast can return multiple next hops. The processor sends one copy per hop. In connector mode, `get_next_hop_by_flow` enforces the single-hop behavior needed by flow-aware TCP-max schedulers.

If resolution fails, the packet is intentionally dropped with a clear error path in logs. That is the design intent: forwarding only relies on control-plane-installed route state.

## Route lifecycle and coherence

Controller updates are applied incrementally but treated as authoritative. Route installs clear relevant caches for affected `(src, dst)` or `(src, group)` keys before insertion, and pinned routes are refreshed only through explicit control updates.

Connector and processor tables are updated independently because they are different actors, but both consume the same route sources from the controller stream.

Pinned route updates flow through `ProcessorMessage::PinRouteForFlow` and are just cached pre-seeds for specific flows. They still follow the same invalidation model when controller routes refresh.
