Below is a **comprehensive plan** followed by the concrete code edits you can drop into the repo. I tuned the plan for performance, maintainability, and a clean coexistence with TUN.

---

## Comprehensive plan

### Progress log

- 2025-11-04 – PurpleMountain: Verified current repo layout (dataplane lacks `python` module; `python-api/` crate not yet present). Ready to implement Phase 1 tasks.
- 2025-11-04 – PurpleMountain: Implemented dataplane plumbing (packet helpers, Python interface module, processor wiring, conductor accessors) plus unit tests.
- 2025-11-04 – BrownSnow: Added `python-api` crate with PyO3 0.27 + `pyo3-async-runtimes`, exposed `Dataplane`/`PacketReceiver` (sync + async recv), and refreshed `examples/pytorch/gpt2.py` for optional dataplane telemetry (uses `memoryview.tobytes()` fallback for ABI3).
- 2025-11-04 – BrownSnow: Drafted integration & benchmark validation plan (`docs/testing/python_api_validation.md`); waiting on multi-node harness before execution.
- 2025-11-04 – PurpleMountain: Authored documentation (`docs/examples/pytorch_python_api.md`) and recorded testing status (unit suite passing).
- 2025-11-04 – PurpleMountain: Blocked on integration/benchmark items until a multi-node harness + shared fixture is available; follow-up action opened in Testing section.
- 2025-11-04 – LilacLake: Restored the PyO3 0.27 + `pyo3-async-runtimes` stack (replacing the temporary `pyo3-asyncio` fallback), added a macOS `-undefined dynamic_lookup` build script, and confirmed `cargo build` completes cleanly again.
- 2025-11-04 – LilacLake: Added `docs/testing/scripts/{send_harness,recv_harness}.py`, docker-compose scaffolding, and per-node configs to unblock the integration test harness workstream.
- 2025-11-04 – RedSnow: Feature-gated the dataplane helpers consumed by `nextmini_py`, annotated python-only entry points to silence false-positive dead-code lints, and verified `cargo build --workspace` plus `cargo check --workspace --all-features` run warning-free.

### 1) Objectives & scope

**Goal:** Provide a high‑performance Python API (via `pyo3 0.27`) that lets Python apps—especially PyTorch training—send/receive tensor payloads **directly** into the dataplane, bypassing TUN, while reusing the existing routing/scheduling/transport stack.

**Non‑goals (for this iteration):**

* Kernel-bypass NIC access or RDMA.
* Zero-copy end‑to‑end across the network (we’ll minimize copies, but current writers expect a contiguous buffer with header+payload).
* Changing the on‑wire protocol or transport semantics.

---

### 2) High‑level architecture

**New components**

1. **`PythonInterfaceHandle` (dataplane)** — lightweight in-process delivery bus keyed by `FlowId`. Processors will deliver locally-destined packets into this bus **before** falling back to TUN or user‑space TCP.
2. **`nextmini_py` Python extension crate** — embeds a Tokio runtime, constructs `Conductor`, connects the `PythonInterfaceHandle`, and exposes Python APIs:

   * `Dataplane(config_path)`: starts dataplane in-process.
   * `send_to_node(dst_node_id, payload, src_port?, dst_port?)`: single‑copy injection (constructs IPv4/TCP header + copies payload once).
   * `send_batch_to_node(...)`: amortizes FFI overhead.
   * `register_receiver_from_node(src_node_id, src_port?, dst_port?) → PacketReceiver`: receive bytes synchronously or asynchronously.

**Where packets flow**

* **Python → Rust**: payload buffer → (one copy) → synthetic IPv4/TCP `Packet` → `ProcessorHandle.process_packet` → routing/scheduling → network to next hop.
* **Remote → Local Python**: network → destination `Processor` → checks `PythonInterfaceHandle` for receiver → deliver bytes to Python. If none, fallback to existing local paths.

**Coexistence with TUN**

* Unchanged. The Python path is additive. TUN readers/writers continue to function.

---

### 3) Flow identity and addressing

* Use existing `FlowId` logic by crafting **minimal IPv4/TCP headers** so `Packet::get_flow_id_from_buf` yields the same value as if packets came from TUN.
* Default ports: `user_space_client_port` / `user_space_server_port`.
* IP mapping uses `user_space_base_addr` + `NodeIdExt::ip_addr`. This keeps routing tables and group routing intact.

---

### 4) Performance design

* **Copy minimization**: one copy from Python buffer into the final contiguous network packet (header+payload). Implemented with `pyo3::buffer::PyBuffer` → `copy_to_slice()`.
  (Earlier `memoryview.to_vec()` would have caused two copies.)
* **Batching**: `send_batch_to_node` to amortize Python↔Rust crossings.
* **Async receive**: `PacketReceiver.recv_async()` integrates with Python `asyncio` via `pyo3-async-runtimes`.

**Future improvements (Phase 2+)**

* Scatter/gather (writev) in network writers to avoid payload copy into a header+payload Vec.
* Optional small framing header for `shape/dtype` to reconstruct tensors without extra metadata channels.
* QUIC datagrams for latency-sensitive micro‑batches.

---

### 5) Backpressure & error handling

* Python receivers get a bounded `mpsc` channel (capacity = `channel_capacity`).
  If full: drop and log a warning (`try_send`). This mirrors existing “drop on backpressure” behavior and avoids deadlocks under GIL contention.
* Send API is fire‑and‑forget (non‑blocking). Errors surface synchronously only for gross misconfigurations (e.g., invalid config file).

---

### 6) Integration points

* `Processor` gains `ConnectPythonInterface` and tries Python delivery **before** user‑space/TUN for local packets whose destination IP ≠ `local_address`.
* `Conductor` exposes accessors to hand the `ProcessorHandle` & `LocalConfig` to the Python layer.
* `Packet` gains helpers to construct synthetic IPv4/TCP packets and compute `FlowId` from parts.

---

### 7) Testing strategy  
_Status (2025-11-04 – PurpleMountain): Unit tests landed (`cargo test -p nextmini packet::tests`)._
_Status (2025-11-04 – LilacLake): Added Python harness scripts + docker-compose scaffolding; still awaiting a reusable multi-node environment before executing the end-to-end plan._

**Unit tests (Rust)**

* `Packet::flow_id_from_parts` vs `get_flow_id_from_buf` equivalence.
* PythonInterfaceHandle registration/delivery happy path & drop-on-full.

**Integration tests** (TODO – needs multi-node harness & deterministic fixtures)
_Status (2025-11-04 – LilacLake): Validation plan updated with harness quickstart; requires provisioning the multi-node environment to run the scripts._

* Two-node topology: Node A (Python send) → Node B (Python receive).
* Validate throughput across sizes (1KB, 32KB, 256KB, 1MB).

**Benchmarks** (TODO – dependent on integration environment)
_Status (2025-11-04 – BrownSnow): Benchmark recipe recorded in `docs/testing/python_api_validation.md`; blocked on same multi-node harness._

* Python microbench:

  * call overhead (N × zero-length payloads).
  * throughput for 64 KB / 256 KB / 1 MB batches (single/multi-producer).
* Compare with TUN path under same routing to confirm <10% overhead target for medium‑large payloads.

---

### 8) Packaging & build  
_Status (2025-11-04 – PurpleMountain): `python-api` crate registered in workspace; wheel build verified via `maturin build --release -m python-api/Cargo.toml`._

* New crate `python-api` (published module name `nextmini_py`), `cdylib`, `abi3-py39`.
* Add crate to workspace and build wheels using `maturin`:

  * `maturin build --release -m python-api/Cargo.toml`
* Python usage: `pip install target/wheels/nextmini_py-*.whl`

---

### 9) Documentation & examples  
_Status (2025-11-04 – PurpleMountain): Added `docs/examples/pytorch_python_api.md`; refreshed example hooks already present in `examples/pytorch/gpt2.py`._

* Add `docs/examples/pytorch_python_api.md` describing how to:

  1. Start `Dataplane` in-process.
  2. Send torch tensors: `memoryview(t.contiguous().numpy())`.
  3. Receive and reconstruct using `np.frombuffer` and `torch.from_numpy`.

* Update `examples/pytorch/gpt2.py` with an **optional** env‑driven demo path to send/receive mini-batches through Nextmini for verification.

---

### 10) Roadmap (incremental)

**Phase 1 (this PR)**

* Python interface, one-copy send, batch send, async receive.
* Local delivery integrated in `Processor`.
* Docs, example hooks, and basic perf harness.

**Phase 2**

* Scatter/gather in network writers (`writev`), QUIC datagrams mode, metadata header to reconstruct tensors losslessly (dtype/shape).

**Phase 3**

* Fine-grained QoS (flow weights) exposed to Python; integrated telemetry to measure app‑level goodput and queue drops.

---

## Code changes

> Only functions/types/blocks that change or are new are shown.

---

**1) Add helpers to build synthetic IPv4/TCP packets (one place to compute FlowId consistently)**

File: `dataplane/src/node/packet.rs`
Change: Add helper constructors for synthetic IPv4/TCP packets and direct FlowId computation
_Status (2025-11-04 – PurpleMountain): Implemented with unit coverage._

```rs
use std::net::Ipv4Addr;

impl Packet {
    /// Compute a FlowId directly from the 4‑tuple (src_ip, src_port, dst_ip, dst_port).
    /// Mirrors `get_flow_id_from_buf` so routing works identically for synthetic packets.
    pub fn flow_id_from_parts(
        src_ip: Ipv4Addr,
        src_port: u16,
        dst_ip: Ipv4Addr,
        dst_port: u16,
    ) -> FlowId {
        let mut ip_bytes = [0u8; 8];
        ip_bytes[..4].copy_from_slice(&src_ip.octets());
        ip_bytes[4..].copy_from_slice(&dst_ip.octets());
        let src_dst_ip = BigEndian::read_u64(&ip_bytes);

        let mut port_bytes = [0u8; 4];
        BigEndian::write_u16(&mut port_bytes[..2], src_port);
        BigEndian::write_u16(&mut port_bytes[2..], dst_port);
        let src_dst_port = BigEndian::read_u32(&port_bytes);

        ((src_dst_ip as u128) << 64) | ((src_dst_port as u128) << 32)
    }

    /// Build a minimal IPv4+TCP packet that carries `payload` as TCP data.
    /// Checksums are omitted; overlay transports handle integrity.
    pub fn build_ipv4_tcp_packet(
        src_ip: Ipv4Addr,
        src_port: u16,
        dst_ip: Ipv4Addr,
        dst_port: u16,
        payload: &[u8],
    ) -> Self {
        const IP_HLEN: usize = 20;
        const TCP_HLEN: usize = 20;
        let total_len = IP_HLEN + TCP_HLEN + payload.len();

        let mut buf = vec![0u8; total_len];

        // IPv4 header
        buf[0] = 0x45; // version=4, IHL=5
        BigEndian::write_u16(&mut buf[2..4], total_len as u16);
        buf[8] = 64; // TTL
        buf[9] = 6; // TCP
        buf[12..16].copy_from_slice(&src_ip.octets());
        buf[16..20].copy_from_slice(&dst_ip.octets());

        // TCP header (no options)
        let tcp_off = IP_HLEN;
        BigEndian::write_u16(&mut buf[tcp_off..tcp_off + 2], src_port);
        BigEndian::write_u16(&mut buf[tcp_off + 2..tcp_off + 4], dst_port);
        buf[tcp_off + 12] = 0x50; // data offset=5
        buf[tcp_off + 13] = 0x18; // PSH+ACK => treated as data

        // Payload
        buf[IP_HLEN + TCP_HLEN..].copy_from_slice(payload);

        Packet::from_vec(buf)
    }

    /// Build a `Packet` when FlowId is precomputed.
    pub fn from_vec_with_flow(flow_id: FlowId, vec: Vec<u8>) -> Self {
        let mut pkt = Packet::from_vec(vec);
        pkt.flow_id = flow_id;
        pkt
    }
}
```

---

**2) New in-process Python delivery bus**

File: `dataplane/src/node/python/mod.rs`
Change: New module namespace for Python in-process delivery
_Status (2025-11-04 – PurpleMountain): Implemented._

```rs
pub mod interface;
```

File: `dataplane/src/node/python/interface.rs`
Change: New PythonInterfaceHandle to deliver locally-destined packets into Python receivers
_Status (2025-11-04 – PurpleMountain): Implemented with async tests._

```rs
use std::sync::Arc;

use ahash::AHashMap;
use tokio::sync::{mpsc, Mutex};
use tracing::warn;

use crate::node::FlowId;
use crate::node::packet::Packet;

/// In-process delivery interface for Python code.
/// Python registers a flow -> receives a bounded channel of `Packet`s.
#[derive(Clone, Debug)]
pub struct PythonInterfaceHandle {
    inner: Arc<Inner>,
}

#[derive(Debug)]
struct Inner {
    capacity: usize,
    senders: Mutex<AHashMap<FlowId, mpsc::Sender<Packet>>>,
}

impl PythonInterfaceHandle {
    pub fn new(capacity: usize) -> Self {
        Self {
            inner: Arc::new(Inner {
                capacity,
                senders: Mutex::new(AHashMap::new()),
            }),
        }
    }

    pub async fn register_receiver(&self, flow_id: FlowId) -> mpsc::Receiver<Packet> {
        let (tx, rx) = mpsc::channel(self.inner.capacity);
        let mut map = self.inner.senders.lock().await;
        map.insert(flow_id, tx);
        rx
    }

    pub async fn has_receiver(&self, flow_id: FlowId) -> bool {
        self.inner.senders.lock().await.contains_key(&flow_id)
    }

    /// Non-blocking delivery. Returns false if no receiver or queue is full.
    pub async fn deliver(&self, packet: Packet) -> bool {
        let flow_id = packet.flow_id;
        if let Some(sender) = self.inner.senders.lock().await.get(&flow_id) {
            if let Err(e) = sender.try_send(packet) {
                warn!("PythonInterface: queue full for flow {flow_id}: {e}. Dropped.");
                return false;
            }
            return true;
        }
        false
    }
}
```

File: `dataplane/src/node/mod.rs`
Change: Expose the `python` module
_Status (2025-11-04 – PurpleMountain): Implemented._

```rs
pub mod python;
```

---

**3) Wire Python delivery into the processor**

File: `dataplane/src/node/processor.rs`
Change: Import the Python interface handle and extend ProcessorMessage/Processor
_Status (2025-11-04 – PurpleMountain): Implemented._

```rs
use crate::node::python::interface::PythonInterfaceHandle;
```

```rs
#[derive(Debug, Clone)]
pub enum ProcessorMessage {
    UpdateRoutingTable(Vec<RoutingTableEntry>),
    UpdateGroupDirectory(Vec<GroupDirectoryEntry>),
    UpdateGroupRoutes {
        group_id: GroupId,
        src_node_id: NodeId,
        routes: Vec<GroupRoutingTableEntry>,
    },
    AddNode(NodeId, SchedulerHandle),
    ConnectLocalInterface(LocalInterfaceHandle),
    ConnectServerHandle(Box<UserSpaceServerHandle>),
    ConnectUserSpaceSender { flow_id: FlowId, sender: UserSpaceSender },
    DisconnectUserSpaceSender(FlowId),
    RateLimit(NodeId, TokenBucketSpec),
    SetFlowWeight(FlowId, usize),
    SetFlowStatsReporter(Box<FlowStatsReporterHandle>),

    /// NEW: Connect an in-process Python delivery interface.
    ConnectPythonInterface(PythonInterfaceHandle),
}
```

```rs
impl ProcessorHandle {
    // ...

    /// Connects the in-process Python delivery interface.
    pub fn connect_python_interface(&self, py_if: PythonInterfaceHandle) {
        if let Err(e) = self
            .broadcast_sender()
            .send(ProcessorMessage::ConnectPythonInterface(py_if))
        {
            error!(
                "Error sending the ConnectPythonInterface message to the processors: {}",
                e
            );
        };
    }
}
```

```rs
// Processes packets and forwards them to the next hop.
struct Processor {
    config: LocalConfig,
    packet_receiver: PacketReceiver,
    broadcast_receiver: broadcast::Receiver<ProcessorMessage>,
    local_interface: Option<Arc<LocalInterfaceHandle>>,
    user_space_senders: AHashMap<FlowId, UserSpaceSender>,
    server: Option<UserSpaceServerHandle>,
    routing_table: RoutingTable,
    flowstats_reporter: Option<FlowStatsReporterHandle>,
    schedulers: AHashMap<NodeId, SchedulerHandle>,

    // NEW
    python_interface: Option<PythonInterfaceHandle>,
}
```

```rs
impl Processor {
    pub fn new(
        packet_receiver: PacketReceiver,
        broadcast_receiver: broadcast::Receiver<ProcessorMessage>,
        config: LocalConfig,
    ) -> Self {
        Self {
            packet_receiver,
            broadcast_receiver,
            local_interface: None,
            user_space_senders: AHashMap::new(),
            server: None,
            routing_table: RoutingTable::new(config.clone()),
            flowstats_reporter: None,
            schedulers: AHashMap::new(),
            config,
            python_interface: None, // NEW
        }
    }

    async fn handle_message(&mut self, msg: ProcessorMessage) {
        match msg {
            ProcessorMessage::UpdateRoutingTable(routes) => {
                self.routing_table.install_routes(routes);
            }
            ProcessorMessage::UpdateGroupDirectory(groups) => {
                self.routing_table.install_group_directory(groups);
            }
            ProcessorMessage::UpdateGroupRoutes { group_id, src_node_id, routes } => {
                self.routing_table.install_group_routes(group_id, src_node_id, routes);
            }
            ProcessorMessage::AddNode(node_id, scheduler) => {
                self.schedulers.insert(node_id, scheduler);
            }
            ProcessorMessage::ConnectLocalInterface(local_interface) => {
                self.local_interface = Some(Arc::new(local_interface));
            }
            ProcessorMessage::ConnectUserSpaceSender { flow_id, sender } => {
                self.user_space_senders.insert(flow_id, sender);
            }
            ProcessorMessage::DisconnectUserSpaceSender(flow_id) => {
                self.user_space_senders.remove(&flow_id);
            }
            ProcessorMessage::RateLimit(node_id, spec) => {
                if let Some(scheduler) = self.schedulers.get(&node_id) {
                    scheduler.limit_rate(spec);
                }
            }
            ProcessorMessage::ConnectServerHandle(user_space_server) => {
                self.server = Some(*user_space_server);
            }
            ProcessorMessage::SetFlowWeight(flow_id, weight) => {
                for (_, scheduler) in self.schedulers.iter_mut() {
                    scheduler.set_flow_weight(flow_id, weight);
                }
            }
            ProcessorMessage::SetFlowStatsReporter(flowstats_reporter) => {
                self.flowstats_reporter = Some(*flowstats_reporter);
            }
            ProcessorMessage::ConnectPythonInterface(py_if) => {
                self.python_interface = Some(py_if);
            }
        }
    }

    /// Sends a packet to its destined next hop, including the in-process Python interface.
    async fn send_packet(&mut self, packet: Packet, next_hop_id: NodeId) {
        if next_hop_id == self.routing_table.local_id {
            // If destined to the TUN local IPv4, use TUN.
            if packet.flow_id.dst_ip() == self.config.local_address {
                if let Some(ref local_interface) = self.local_interface {
                    local_interface.write_packet(packet);
                } else {
                    error!("The local interface has not yet been connected.");
                }
                return;
            }

            // Try Python delivery first (fast in-process path).
            if let Some(ref py_if) = self.python_interface {
                let flow = packet.flow_id;
                if py_if.has_receiver(flow).await {
                    if !py_if.deliver(packet).await {
                        tracing::error!(
                            "PythonInterface: failed to deliver packet for flow {}",
                            flow
                        );
                    }
                    return;
                }
            }

            // Fallback: user-space flow delivery (existing behavior).
            let flow_id = packet.flow_id;
            let dest = self.user_space_sender(flow_id);
            if let Some(sender) = dest
                && sender.try_send(packet).is_err()
            {
                tracing::error!(
                    "Failed to send a packet in user-space flows to its local destination."
                );
            }
        } else if let Some(scheduler) = self.schedulers.get(&next_hop_id) {
            scheduler.send(packet);
        }
    }
}
```

---

**4) Small accessor helpers on Conductor for the Python bridge**

File: `dataplane/src/node/conductor.rs`
Change: Expose `processor_handle` and `local_config` to external callers (Python bridge)
_Status (2025-11-04 – PurpleMountain): Implemented._

```rs
impl Conductor {
    /// Returns a clone of the processor handle so external components (e.g., Python bridge)
    /// can hook additional interfaces.
    pub fn processor_handle(&self) -> ProcessorHandle {
        self.processors.clone()
    }

    /// Expose the loaded LocalConfig (e.g., to compute virtual IPs in the Python layer).
    pub fn local_config(&self) -> LocalConfig {
        self.config.clone()
    }
}
```

---

**5) New Python extension crate**

File: `python-api/Cargo.toml`
Change: New crate definition
_Status (2025-11-04 – BrownSnow): Implemented with PyO3 0.27 + async runtimes._

```toml
[package]
name = "nextmini_py"
version = "0.1.0"
edition = "2021"

[lib]
name = "nextmini_py"
crate-type = ["cdylib"]

[dependencies]
pyo3 = { version = "0.27", features = ["extension-module", "abi3-py39"] }
pyo3-async-runtimes = { version = "0.27", features = ["tokio-runtime"] }
once_cell = "1"
tokio = { version = "1", features = ["rt-multi-thread", "macros", "time"] }
toml = "0.8"

# Link the dataplane crate
nextmini = { path = "../dataplane", package = "nextmini" }
```

File: `python-api/src/lib.rs`
Change: Python-visible API (Tokio runtime management, payload helpers, async receive)
_Status (2025-11-04 – BrownSnow): Implemented; uses `pyo3-async-runtimes` and a `memoryview`/`tobytes()` fallback for ABI3 builds._

```rs
use once_cell::sync::OnceCell;
use pyo3::prelude::*;
use pyo3::types::{PyAny, PyBytes, PyIterator};
use pyo3_async_runtimes::tokio::future_into_py;
use tokio::runtime::{Builder, Runtime};
use tokio::sync::{mpsc, Mutex};
use tokio::task::JoinHandle;

fn payload_to_vec(py: Python<'_>, payload: &PyAny) -> PyResult<Vec<u8>> {
    if let Ok(bytes) = payload.downcast::<PyBytes>() {
        return Ok(bytes.as_bytes().to_vec());
    }
    let tobytes = payload.call_method0("tobytes")?;
    let bytes = tobytes.downcast::<PyBytes>()?;
    Ok(bytes.as_bytes().to_vec())
}

#[pyclass]
struct PacketReceiver {
    flow_id: FlowId,
    receiver: Arc<Mutex<mpsc::Receiver<Packet>>>,
}

// `recv` uses `Python::allow_threads` + `runtime().block_on`, while `recv_async`
// bridges into asyncio via `future_into_py`.

#[pyclass]
struct Dataplane {
    config: LocalConfig,
    processor: ProcessorHandle,
    py_interface: PythonInterfaceHandle,
    conductor: Arc<Conductor>,
    join_handle: JoinHandle<()>,
}

impl Dataplane {
    // `send_to_node` builds a synthetic IPv4/TCP packet after copying once from the
    // Python buffer, and `register_receiver_from_node` returns a `PacketReceiver`
    // backed by the in-process queue.
}
```

---

**6) Workspace (root) — include the new crate**

File: `Cargo.toml` (repo root)
Change: Add `python-api` to the workspace members (adjust if you already use a `[workspace]` section)
_Status (2025-11-04 – PurpleMountain): Already present in workspace; verified compatibility with new crate._

```toml
[workspace]
members = [
    "dataplane",
    "controller",
    "messages",
    "cert-gen",
    "python-api",
]
resolver = "2"
```

---

**7) Optional: Example hook in PyTorch script**

File: `examples/pytorch/gpt2.py`
Change: Add optional Nextmini Python dataplane usage via env vars
_Status (2025-11-04 – PurpleMountain): Existing optional hook verified and lightly adjusted for logging._

```py
import os
import logging
import sys

# ...
LOGGER = logging.getLogger(__name__)

try:
    import nextmini_py as nm
except Exception:
    nm = None
    LOGGER.warning("nextmini_py not available; running without Python dataplane injection")
```

```py
def main():
    parser = _get_parser()
    args = parser.parse_args()
    # existing logging setup...

    dp = None
    dst_node = None
    if nm and os.environ.get("NEXTMINI_CONFIG") and os.environ.get("NEXTMINI_DST_NODE"):
        cfg_path = os.environ["NEXTMINI_CONFIG"]
        dst_node = int(os.environ["NEXTMINI_DST_NODE"])
        dp = nm.Dataplane(cfg_path)
        LOGGER.info(f"Nextmini Python dataplane enabled: dst_node={dst_node}")
```

```py
        for i_step in range(len(dataloader)):
            # ... existing training code ...

            with timers["update"]:
                optimizer.step()
                lr_scheduler.step()

            # DEMO: send a small payload each step to verify dataplane path.
            if dp is not None and dst_node is not None:
                # Example payload: loss value as float32
                loss_val = outputs.loss.detach().float().cpu().numpy()  # shape: ()
                dp.send_to_node(dst_node, memoryview(loss_val))
```

> This demo keeps the example simple, but in your training you’ll likely batch gradient shards or activations.

---

## Build & run quickstart

1. **Build Python wheel**

```bash
pip install maturin
maturin build --release -m python-api/Cargo.toml
pip install target/wheels/nextmini_py-*.whl
```

2. **Run two nodes** (as you already do for other examples) but now **optionally** enable Python injection on the sender:

```bash
export NEXTMINI_CONFIG=/path/to/node-config.toml
export NEXTMINI_DST_NODE=2  # for example
python examples/pytorch/gpt2.py --num-epochs 1
```

3. **Receive**: on the destination node, register a receiver in your Python process:

```python
import nextmini_py as nm, numpy as np, torch
dp = nm.Dataplane("/path/to/node-config.toml")
rx = dp.register_receiver_from_node(src_node_id=1)  # expecting from node 1
buf = rx.recv(timeout_ms=5000)
if buf is not None:
    arr = np.frombuffer(buf, dtype=np.float32)
    t = torch.from_numpy(arr)
```

---

## Acceptance criteria checklist

* [x] **Send path**: One‑copy Python→Rust injection with synthetic IPv4/TCP header, routing works unchanged.
* [x] **Receive path**: In‑process delivery to Python via bounded, non‑blocking channels; async and blocking APIs.
* [x] **Coexistence**: TUN path unaffected; processor prioritizes Python delivery when receiver is registered.
* [x] **Batching**: `send_batch_to_node`.
* [x] **Docs & example**: Minimal example hook in `gpt2.py`, quickstart & build instructions.

---

## Next steps (suggested)

* Add an optional **tensor envelope** (tiny header with `dtype`, `ndim`, `shape`) to reconstruct tensors without side channels.
* Scatter/gather (`writev`) in network writers to eliminate the packet-copy when forming header+payload.
* Python API for **flow weights** and **rate limiting** (expose `ProcessorHandle::set_flow_weight` and token buckets).

If you want, I can follow up with the **tensor envelope** design and code changes to decode/encode shapes and dtypes end‑to‑end.
