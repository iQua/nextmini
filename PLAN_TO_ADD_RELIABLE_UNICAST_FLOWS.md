## 1. Scope clarification

What we **will** do:

* Implement **controller‑driven reliable unicast flows** as a new flow transport, using:

  * `node/reliable` (RLM) as the engine.
  * One RLM **sender** on the source node, one **receiver** on the destination node.
* Each flow is configured with a **sending rate** by the controller and realized as:

  * An RLM session with a `TokenBucketSpec`/`DataPacer`.

* No Python API, no Python involvement in these flows.

---

## 2. Architecture with the Python piece removed

### 2.1 High-level

```text
Controller
  |
  |  Flow{ src, dst, flow_len, flow_rate, transport = ReliableUnicast }
  v
Dataplane node (src)                  Dataplane node (dst)
  |                                   |
  |-> ReliableUnicastFlowHandle ----->|-> ReliableUnicastFlowHandle
      spawn_sender(flow)                  spawn_receiver(flow)
      (RLM sender)                        (RLM receiver)
```

Under the hood on each node:

```text
ReliableUnicastFlowHandle
    |
    v
ReliableHandle
    |
    v
SessionManager
    |
    v
sender::run / receiver::run
    |
    v
Processor (Packet::build_ipv4_tcp_packet) -> RoutingTable -> Scheduler -> Network
```

No Python involvement at any point in this path.

---

## 3. Modules & main changes (cleaned up)

### 3.1 New module: `node/reliable/unicast.rs`

Same purpose as before, but now **only controller‑facing**:

```rs
#[derive(Clone)]
pub struct ReliableUnicastFlowHandle {
    cfg: LocalConfig,
    processors: ProcessorHandle,
    flowstats: FlowStatsReporterHandle,
    reliable: ReliableHandle,
}

impl ReliableUnicastFlowHandle {
    pub fn new(
        cfg: LocalConfig,
        processors: ProcessorHandle,
        flowstats: FlowStatsReporterHandle,
        reliable: ReliableHandle,
    ) -> Self { ... }

    /// Called by Conductor when controller installs flows.
    pub fn add_flows(&self, flows: Vec<Flow>) {
        for flow in flows {
            if flow.src_node_id == self.cfg.node_id {
                self.spawn_sender(flow.clone());
            }
            if flow.dst_node_id == self.cfg.node_id {
                self.spawn_receiver(flow);
            }
        }
    }

    fn spawn_sender(&self, flow: Flow) { ... }
    fn spawn_receiver(&self, flow: Flow) { ... }
}
```

### 3.2 Changes in existing code

* `dataplane/src/node/reliable/mod.rs`: `pub mod unicast;`
* `Conductor`:

  * Construct a `ReliableUnicastFlowHandle`.
  * When a new `Flow` with `transport = ReliableUnicast` is received, call `unicast_handle.add_flows`.
* `FlowSpec` / controller model:

  * Add `transport: FlowTransport` with `ReliableUnicast` variant.
* `Processor`:

  * Ensure `connect_reliable_handle()` is called once; no other changes.
* **No changes to `python-api` needed** for this feature.

---

## 4. Flow behavior: how a reliable unicast flow is realized

### 4.1 Session ID for controller flows

We still want deterministic, controller‑flow‑specific `SessionId`s, **but only used inside dataplane**.

Helper in `node/reliable/unicast.rs`:

```rs
fn session_id_for_flow(flow: &Flow) -> SessionId {
    use std::hash::{Hash, Hasher};
    let mut h = std::collections::hash_map::DefaultHasher::new();
    flow.controller_id.hash(&mut h);
    flow.src_node_id.hash(&mut h);
    flow.dst_node_id.hash(&mut h);
    flow.flow_spec.flow_len.hash(&mut h);
    let raw = h.finish() & 0x7FFF_FFFF_FFFF_FFFF; // 63 bits
    raw | 0x8000_0000_0000_0000                     // mark as "controller flow"
}
```

* This ensures:

  * Same `Flow` → same `SessionId` on both src and dst.
  * We can optionally keep Python RLM sessions separate by making `SessionManager::allocate_session_id()` stay in `MSB = 0` range, but since you don’t want a Python API for these flows, **they won’t overlap anyway** in practice.

If you don’t plan to mix Python RLM with controller unicast sessions, you can even skip the MSB split and just use a hash; the top‑bit namespacing is just extra safety.

### 4.2 Configuring the sending rate

Each Flow from the controller comes with a **desired sending rate** (as you said: “emulated flows with a sending rate each”).

We map that to RLM’s `data_bucket`:

```rs
fn bucket_from_flow_rate(flow_rate: Option<usize>, default_bucket: &Option<TokenBucketSpec>)
    -> Option<TokenBucketSpec>
{
    if let Some(bps) = flow_rate {
        // Example: simple conversion; you’ll pick actual token sizes
        Some(TokenBucketSpec {
            rate_bytes_per_sec: (bps as u64 / 8).max(1),
            bucket_size: (bps as u64 / 8) * 2, // 2 seconds of burst
        })
    } else {
        default_bucket.clone()
    }
}
```

This bucket feeds into `DataPacer` in `sender.rs`, so the flow’s **sending rate** is enforced entirely inside RLM.

---

## 5. Implementation details for the unicast driver (dataplane only)

### 5.1 Sender side (`spawn_sender`)

We’ll keep the “simple first” version: allocate a synthetic buffer (no Python, no real payload), and let RLM handle reliability + pacing.

```rs
impl ReliableUnicastFlowHandle {
    fn spawn_sender(&self, flow: Flow) {
        let cfg = self.cfg.clone();
        let processors = self.processors.clone();
        let reliable = self.reliable.clone();
        let flowstats = self.flowstats.clone();

        tokio::spawn(async move {
            let sid = session_id_for_flow(&flow);
            let reliable_cfg = cfg.reliable.clone();

            let src_port = cfg.user_space_client_port;
            let dst_port = cfg.user_space_server_port;

            // Determine total bytes for this flow
            let total_bytes: u64 = match flow.flow_spec.flow_len {
                FlowLen::Bytes(n) => n as u64,
                FlowLen::Duration(dur) => match flow.flow_spec.flow_rate {
                    Some(bps) => (bps as u64 * dur.as_secs()) / 8,
                    None => {
                        tracing::warn!("ReliableUnicastFlow: duration-based flow without rate; skipping sender");
                        return;
                    }
                },
            };

            if total_bytes == 0 {
                tracing::warn!("ReliableUnicastFlow: zero-length flow; skipping sender");
                return;
            }

            // Synthetic payload (pattern doesn't matter; we only care about bytes).
            let source_buffer = bytes::Bytes::from(vec![0xAAu8; total_bytes as usize]);

            let dst_ip = (flow.dst_node_id as NodeId)
                .ip_addr(cfg.user_space_base_addr, cfg.local_netmask);

            let data_bucket = bucket_from_flow_rate(
                flow.flow_spec.flow_rate,
                &reliable_cfg.data_bucket,
            );

            let common = CommonConfig {
                session_id: sid,
                group_ip: dst_ip,
                chunk_size: reliable_cfg.chunk_size_unicast.unwrap_or(8500),
                src_port,
                dst_port,
                control_weight: reliable_cfg.control_weight,
                data_bucket,
                local_node_id: cfg.node_id,
                user_space_base_addr: cfg.user_space_base_addr,
                local_netmask: cfg.local_netmask,
            };

            let sender_cfg = SenderConfig {
                common,
                receiver_ids: vec![flow.dst_node_id],
                total_bytes,
                source_buffer,
                ready_grace_ms: reliable_cfg.ready_grace_ms,
                topology_ready: None,
                routes_ready: None,
            };

            // Flow weight for scheduler
            if let Some(weight) = flow.flow_spec.flow_weight {
                let flow_id = flow_id_for_unicast(&cfg, &flow, src_port, dst_port);
                processors.set_flow_weight(flow_id, weight);
            }

            // Ensure RLM sends only once topology & routes are considered “ready”
            reliable.set_topology_ready(true);
            reliable.set_group_routes_ready(dst_ip, cfg.node_id);

            // Stats
            if let Some(controller_id) = flow.controller_id {
                let flow_id = flow_id_for_unicast(&cfg, &flow, src_port, dst_port);
                flowstats.report_user_flow_start(flow_id, controller_id);
            }

            let started_sid = reliable.start_sender(sender_cfg).await;
            let ok = reliable.wait_completion(started_sid).await;

            if let Some(controller_id) = flow.controller_id {
                let flow_id = flow_id_for_unicast(&cfg, &flow, src_port, dst_port);
                flowstats.report_flow_finished(flow_id, Some(controller_id));
            }

            if !ok {
                tracing::warn!(session_id = started_sid, "ReliableUnicastFlow: completion failed or timed out");
            }

            reliable.stop(started_sid);
        });
    }
}
```

### 5.2 Receiver side (`spawn_receiver`)

No buffer, just ACK and accounting.

```rs
impl ReliableUnicastFlowHandle {
    fn spawn_receiver(&self, flow: Flow) {
        let cfg = self.cfg.clone();
        let reliable = self.reliable.clone();

        tokio::spawn(async move {
            let sid = session_id_for_flow(&flow);
            let reliable_cfg = cfg.reliable.clone();

            let expected_bytes: u64 = match flow.flow_spec.flow_len {
                FlowLen::Bytes(n) => n as u64,
                FlowLen::Duration(dur) => match flow.flow_spec.flow_rate {
                    Some(bps) => (bps as u64 * dur.as_secs()) / 8,
                    None => {
                        tracing::warn!("ReliableUnicastFlow: duration-based flow without rate on receiver; skipping");
                        return;
                    }
                },
            };

            if expected_bytes == 0 {
                tracing::warn!("ReliableUnicastFlow: zero expected_bytes on receiver; skipping");
                return;
            }

            let dst_ip = (flow.dst_node_id as NodeId)
                .ip_addr(cfg.user_space_base_addr, cfg.local_netmask);

            let data_bucket = bucket_from_flow_rate(
                flow.flow_spec.flow_rate,
                &reliable_cfg.data_bucket,
            );

            let common = CommonConfig {
                session_id: sid,
                group_ip: dst_ip,
                chunk_size: reliable_cfg.chunk_size_unicast.unwrap_or(8500),
                src_port: cfg.user_space_client_port,
                dst_port: cfg.user_space_server_port,
                control_weight: reliable_cfg.control_weight,
                data_bucket,
                local_node_id: cfg.node_id,
                user_space_base_addr: cfg.user_space_base_addr,
                local_netmask: cfg.local_netmask,
            };

            let receiver_cfg = ReceiverConfig {
                common,
                source_node_id: flow.src_node_id,
                expected_bytes,
                sink_buffer: None,  // we do not keep payload
            };

            let started_sid = reliable.start_receiver(receiver_cfg).await;
            let _ = reliable.wait_completion(started_sid).await;
            reliable.stop(started_sid);
        });
    }
}
```

---

## 6. Controller integration, with no Python in the loop

### 6.1 FlowSpec

In `messages` (and mirrored in `controller`):

```rs
pub enum FlowTransport {
    Tcp,
    ReliableUnicast,
}

pub struct FlowSpec {
    pub flow_len: FlowLen,
    pub flow_rate: Option<usize>,   // bits per second
    pub flow_weight: Option<usize>,
    pub transport: FlowTransport,
}
```

### 6.2 Controller behavior

When the controller decides to instantiate flows, it:

1. Decides on `flow_len` and `flow_rate` per flow.
2. Sets `transport = ReliableUnicast` for flows that should use RLM instead of smoltcp.
3. Sends the `Flow` to both src and dst nodes (whatever mechanism you already use).

On each dataplane node:

```rs
match flow.flow_spec.transport {
    FlowTransport::Tcp => tcp_flow_handle.add_flows(vec![flow]),
    FlowTransport::ReliableUnicast => reliable_unicast_handle.add_flows(vec![flow]),
}
```

No Python, no RLM session configuration from userspace.

---

## 7. Migration / rollout 

1. **Implement unicast driver**:

   * `ReliableUnicastFlowHandle`
   * new `FlowTransport::ReliableUnicast` enum variant.

2. **Add a config knob** to the controller:

   * use TCP flows (smoltcp) by default.
   * allow a test job to request `ReliableUnicast`.

3. **Test**:

   * Basic two‑node integration test: create one RLM unicast flow, verify:

     * correct bytes sent/received.
     * RLM logs make sense (manifest, ready, ack, eot).
   * Throughput comparison vs smoltcp for representative flow sizes and rates.

4. **Gradually switch**:

   * For internal experiments, use `ReliableUnicast` as default transport.
   * Keep smoltcp as an optional transport for fallback / debugging.

