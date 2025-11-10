## Limitations of the current dataplane codebase

The current implementation has the following limitations when it comes to the Python API sending multicast data that is
more than the MTU.

- Python injections call Dataplane::send_to_node/send_to_ip, which simply clone the FrozenBuffer and wrap it in a
    single IPv4+TCP frame via Packet::build_ipv4_tcp_packet before pushing the packet into the processor (python-api/
    src/lib.rs:185-223). There is no size check here—whatever number of bytes the Python side provides becomes the TCP
    payload of that one packet.

- The builder in Packet::build_ipv4_tcp_packet just allocates a buffer of 20 (IP) + 20 (TCP) + payload.len() bytes
    and writes that length into the IPv4 header as a u16 (dataplane/src/node/packet.rs:283-314). It does not look at the
    configured MTU, does not fragment, and will silently truncate the header length field if you hand it more than 65 535
    bytes.

- On the dataplane side the “virtual device” that hands packets to smoltcp and eventually to the TUN interface only
    advertises a single MTU value (LocalConfig::mtu, default 1 400 bytes, max 6 400) and expects callers to respect it;
    VirtualDevice::capabilities merely reports that MTU to the stack and PacketTxToken::consume forwards whatever length
    it is given without slicing (dataplane/src/node/flow/device.rs:34-67, dataplane/src/node/config.rs:222-235). There is
    no fragmentation or reassembly layer anywhere in this path (a repo-wide search for “fragment”/“reassembly” turns up
    nothing relevant).

- Because of that, the current “mechanism” for buffers that exceed the MTU is simply “you just constructed an oversized
    IPv4 packet”; it will be treated as a single frame inside the QUIC tunnel, and when it hits the local interface on
    the destination it can be dropped or mis-reported because it violates the advertised MTU. The Python tooling reflects
    this expectation: e.g., the multicast driver exposes a --chunk-size flag (default 6 144 bytes) and explicitly
    slices large tensors into chunks before wrapping them in FrozenBuffer instances (examples/multicast-docker/scripts/
    multicast_node.py:130-139, 422-470).

- There is therefore no Rust-side segmentation/reassembly implementation today, and the design assumes it is not
    required—Python callers must keep each FrozenBuffer at or below the configured MTU minus headers (≈ mtu - 40 for
    IPv4/TCP) if they want packets to survive reinjection. If you need to send larger blobs, you have to chunk them
    in Python (as the existing examples do) or extend the dataplane with an explicit fragmentation feature; nothing
    currently performs that work for you.

## Plan For Python-Side Payload Fragmentation

- Clarify constraints + MTU budget
    - Reconfirm how LocalConfig::mtu is applied across dataplane paths and derive the max safe payload (mtu - 40 for
        IPv4/TCP headers).
    - Decide whether the feature auto-detects the runtime MTU (default) or accepts an override so Python callers can
        still pin a smaller ceiling when needed.
    - _2025-11-10 (GreenLake): Verified `LocalConfig::mtu` (default 1400, max 6400) flows through `VirtualDevice::capabilities`
        and is treated as the single peer-facing MTU. With IPv4 (20) + TCP (20) + PyPayloadSeg (24) headers the fixed overhead
        is 64 bytes, so the safe payload budget per fragment is `mtu - 64`. Added notes in `docs/docs/design/python_payload_fragmentation.md`
        and wired an FFI getter + optional override so Python callers can clamp lower when needed._

- Define a fragmentation header
    - Introduce a compact “PyPayloadSeg” header (e.g., 16 bytes) carried at the beginning of every TCP payload that
        comes from the Python API: includes a flag for “unfragmented vs fragmented,” a 64‑bit message id, total byte
        length, fragment index, and fragment payload length.
    - Ensure the header is versioned and only attached to flows emitted via the Python bridge so regular dataplane
        packets stay unchanged.
    - _2025-11-10 (GreenLake): Finalized the 24-byte header (magic `0x5047`, version, flags bitfield, 64-bit message id,
        total payload length, fragment index/count, fragment payload length) and captured it in
        `docs/docs/design/python_payload_fragmentation.md`. Header is prepended only when the sender is the Python bridge; native
        dataplane traffic remains untouched._

- Augment the sender pipeline (python-api/src/lib.rs, nextmini::node::packet)
    - Update send_to_node/send_to_ip to detect oversized buffers, split them into MTU‑sized fragments (payload_chunk
        <= mtu - tcp/ip header - header overhead), stamp the header, and emit a sequence of Packet::build_ipv4_tcp_packet
        calls.
    - Reuse the same helper for send_batch_to_node to avoid double fragmenting and to share the message-id allocator
        (simple per-dataplane atomic counter).
    - _2025-11-10 (GreenLake): `python-api/src/lib.rs` now routes send_to_node/send_to_ip/send_batch_to_node through
        `transmit_python_payload`, which enforces the MTU budget (`python_payload_budget`), injects the PyPayloadSeg header,
        and uses a shared `AtomicU64` message-id. Feature gating is handled via `python_fragmentation_enabled` on
        `LocalConfig`, defaulting to off until reassembly lands._

- Inject awareness into the processor (dataplane/src/node/processor.rs, node/python/interface.rs)
    - Teach the processor to recognize the new header on packets destined for the Python interface: peel the
        header, fan fragments to a new ReassemblyBuffer keyed by (flow_id, message_id), and only enqueue a packet to
        PythonInterfaceHandle once the full byte count has arrived (or once a timeout/backlog limit elapses).
    - Maintain per-flow memory caps (configurable, e.g., python_reassembly_window_bytes) to avoid unbounded buffering;
        drop and log when limits are exceeded.
    - _2025-11-10 (GreenLake): Added `python::fragment` assembler + wired PythonInterfaceHandle to route fragments through it,
        with tracing on evictions/drops and DeliveryMode-aware dispatch to raw vs payload receivers._

- Deliver fully reconstructed buffers to Python
    - Change PacketReceiver::recv to emit just the user payload (no IPv4/TCP wrapper) once the processor has validated
        the message is complete, along with metadata (flow id, optional headers) exposed via a lightweight Python struct
        for callers that still need network info.
    - Provide a compatibility knob so existing consumers that expect raw packets can opt out until they migrate.
    - _2025-11-10 (GreenLake): PacketReceiver now negotiates `payload_only` mode; Python bindings expose a new
        `PayloadDelivery` struct (flow ids, addresses, message_id metadata) while legacy consumers keep receiving raw
        bytes when the knob is off._

- Handle partial/failed reassembly
    - Add timers/backpressure to evict stale fragment groups (configurable python_fragment_timeout_ms).
    - Surface drop stats via tracing + controller events so operators can spot MTU misconfigurations or buggy senders
        quickly.

- Expose configuration & documentation
    - Extend LocalConfig (and the sample config.toml under docs/ and examples/) with the new knobs: enable/disable
        fragmentation, max message bytes allowed, per-flow reassembly window, timeout.
    - _2025-11-10 (GreenLake): `LocalConfig` already exposes `python_fragmentation_enabled` + `python_fragmentation_max_message_bytes`
        (defaults false / 16 MiB) and the new design doc (`docs/docs/design/python_payload_fragmentation.md`) explains how they map to the
        MTU budget; follow-up knobs for `reassembly_window_bytes` and `fragment_timeout_ms` will land alongside the buffer work._
- Document the behavior in docs/docs/design/PLAN_TO_ADD_PYTHON_API.md and user-facing guides (e.g., docs/examples/
        simple.md, docs/examples/pytorch_python_api.md), emphasizing that large buffers now “just work.”

- Testing & validation
    - Add unit tests for: sender chunk sizing, header correctness, reassembly success/failure paths, eviction on
        timeout, backpressure on over-limit flows (Rust tests under python-api/ and dataplane/src/node/python).
    - Introduce an integration script (e.g., docs/testing/scripts/python_fragmentation_smoke.py) that sends multi-
        megabyte buffers through the binding and verifies they round-trip without manual chunking.
    - _2025-11-10 (GreenLake): Ran `cargo test -p nextmini fragment --lib` to cover the new assembler + interface logic
        while Python-specific tests remain blocked on CPython symbols._

## Execution tracks & status (2025-11-10 – OrangeMountain)

| Track | Scope | Owner | Status | Notes |
| --- | --- | --- | --- | --- |
| A | Constraints, MTU math, PyPayloadSeg spec, config knobs, integration script scaffolding | **PurpleStone** | ✅ Complete | Spec + MTU math captured here + in `docs/docs/design/python_payload_fragmentation.md`; new config knobs + smoke script skeleton already landed. Remaining polish folds into Track D. |
| B | Python sender fragmentation + header writer in `python-api/` + packet helpers | **GreenLake** (PurpleStone assisting) | ✅ Complete | `transmit_python_payload` enforces the MTU budget, stamps headers, and propagates the feature flag across send\_* calls. Back-compat defaults remain raw-packet mode until Track C lands. |
| C | Dataplane reassembly pipeline + PacketReceiver/metrics wiring in `dataplane/src/node/**` | **BrownPond** pairing with **OrangeMountain** | 🟢 Complete | Fragment assembler + `PythonInterfaceHandle` are wired end-to-end (raw + payload-only), controller events ship via `PythonFragmentEvents`, and the python bindings propagate metadata. In-memory counters (`python_fragments_*`, `python_reassembly_*`) now live in `PythonInterfaceHandle::metrics_snapshot()`. |
| D | Cross-crate docs, samples, and validation tests (`docs/**`, `examples/**`, integration script`) | **PurpleStone** (BrownPond covering smoke script impl) | 🟡 In progress | Controller/operator docs, PyTorch sample, and the smoke-test runbook landed; the only remaining work is to execute the smoke script once CPython dev headers are available on CI and capture the results. |

Action items before coding continues:

1. **Metrics export validation** – BrownPond wired periodic controller snapshots via the new `DataplaneToController::PythonFragmentMetrics` message; OrangeMountain/Ops to confirm this surface meets requirements (and file follow-up if the future metrics registry also needs hooks).
2. **Integration validation** – All owners to re-run the updated smoke script (requires CPython 3.13 dev libs) and note results once the toolchain is available. BrownPond is coordinating the header/toolchain install on shared runners; PurpleStone has the runbook + script ready and will execute the test + capture logs as soon as the CPython dependency is resolved.
3. **Controller alerting review** – OrangeMountain to confirm the new `PythonFragmentEvents` payload matches ops’ expectations (fields + `kind` values) before enabling the feature flag beyond staging.
4. **File reservations** – BrownPond continues to hold `dataplane/src/node/python/**` + `dataplane/src/node/processor.rs` while finishing follow-up work; PurpleStone released `docs/**` (including the smoke script), so BrownPond can reserve those paths whenever Track D needs iterations.

### Coordination update – 2025-11-10 (BrownPond)

| Work item | Primary | Reviewer / Pair | Status & Notes |
| --- | --- | --- | --- |
| Processor integration + payload delivery | BrownPond | OrangeMountain | Finish wiring fragments through the processor into `PythonDelivery`, ensure `PacketReceiver` respects `payload_only`, and capture per-flow stats for drops/timeouts. Target PR ready for review by EOD today. |
| Controller events + metrics plumbing | BrownPond | OrangeMountain | Emit tracing + controller-facing events when assembler evicts fragments (window overflow/timeout) so ops can alert. Controller docs to list the new events. |
| Docs + config polish | PurpleStone | BrownPond | Update `docs/docs/design/configuration.md`, `docs/examples/pytorch_python_api.md`, and `examples/pytorch/config.toml` once Track C exposes telemetry toggles. BrownPond to provide testing notes + release-callout draft. |
| Python binding verification | GreenLake | PurpleStone | Double-check that `PacketReceiver(payload_only=True)` stays back-compatible and flag any API shape churn before Track C merges; also sanity-check importer guidance in docs. |
| Smoke test script + runbook | BrownPond | PurpleStone | Finish `docs/testing/scripts/python_fragmentation_smoke.py` to send multi-meg payloads, document expected logs/results, and call out the current CPython toolchain limitation. |

BrownPond has Agent Mail notifications enabled on thread `PLAN_TO_ADD_SEGMENTATION` and will ack/respond as changes land so we can keep momentum without blocking on chat.

## PyPayloadSeg draft specification

_Last updated: 2025-11-10 by PurpleStone._

**Applicability**
- Only packets emitted via the Python bindings will carry the header.
- Header sits at the front of the TCP payload; Python receiver strips it before delivering bytes to user code.

**Header layout (little-endian, 24 bytes total)**

| Offset | Field | Size | Description |
| --- | --- | --- | --- |
| 0 | magic | u16 | Constant `0x5047` (“PG”) to distinguish from non-segmented payloads. |
| 2 | version | u8 | Starts at `1`; receivers drop if unsupported. |
| 3 | flags | u8 | Bit 0 = `is_fragmented` (0 → single fragment); Bit 1 = `is_last_fragment`. Remaining bits reserved. |
| 4 | message_id | u64 | Monotonic counter per dataplane instance; groups fragments. |
| 12 | total_len | u32 | Intended message payload length (before fragmentation). |
| 16 | fragment_index | u16 | Zero-based index within the message. |
| 18 | fragment_count | u16 | Total fragment count (1 when unfragmented). |
| 20 | fragment_payload_len | u32 | Bytes carried after the header in this packet. |

**MTU budgeting**

- `mtu_payload_budget = mtu_configured - IPv4_HEADER (20) - TCP_HEADER (20) - PYPAYLOADSEG_HEADER (24)`.
- With default MTU 1 400 bytes the safe Python payload chunk is `1 400 - 64 = 1 336` bytes.
- Track A must guard against payloads > `LocalConfig::mtu_max_payload` (new knob) by logging and rejecting early.

**Fragmentation rules**

1. Sender splits user buffer into `ceil(len / mtu_payload_budget)` fragments.
2. Each fragment inherits the same `message_id`, `total_len`, and `fragment_count`.
3. `fragment_payload_len` is `<= mtu_payload_budget` except possibly the last fragment (remainder).
4. Flag combinations:
   - Single-fragment message: `flags = 0b00`, `fragment_index = 0`, `fragment_count = 1`.
   - Multi-fragment message: Every fragment sets `is_fragmented`; last fragment also sets `is_last_fragment`.
5. Reassembly drops groups whose fragments exceed `python_reassembly_window_bytes` or exceed timeout.

**Outstanding questions to resolve**

- Should version/magic live inside TCP options instead? (Current plan sticks to payload for simplicity.)
- Is u16 fragment_count sufficient for >85 MB messages? (Assumes we cap `max_message_bytes` well below that.)
- Do we need optional checksum over payload chunks? (Current plan relies on TCP + QUIC integrity; revisit if needed.)

### Progress log – 2025-11-10 (OrangeMountain)

- Track A code scaffolding landed behind `python_fragmentation_enabled` (default `false`): new helper writes the PyPayloadSeg header for every Python emission once the flag is on, enforces MTU budgeting, allocates monotonic message IDs, and rejects payloads above `python_fragmentation_max_message_bytes` (now defaulting to 64 KiB to match the MTU-derived payload ceiling).
- Added the full `python_fragmentation_*` set (`enabled`, `max_message_bytes`, `reassembly_window_bytes`, `fragment_timeout_ms`, `trace_flow_events`) to `LocalConfig` so dataplane + bindings can read consistent limits without touching docs/examples yet (Track C to finish wiring + narrative).
- Introduced pure-Rust unit tests for the chunk planner/header writer in `python-api/src/lib.rs`; `cargo test -p nextmini_py` currently fails on this workstation because the PyO3 linker cannot find a system Python (`ld: symbol(s) not found for architecture arm64`). Leaving the log in the Agent Mail thread once others can supply a CPython 3.13 dev install.
- Updated the public docs (`docs/docs/design/configuration.md`, `docs/examples/pytorch_python_api.md`) plus `examples/pytorch/config.toml` so users know how to enable the feature flag, set safe payload ceilings, and size the reassembly window.
- Dropped `docs/testing/scripts/python_fragmentation_smoke.py` as the future integration harness; once reassembly lands we can wire the verification steps outlined in the design note.
- Added `dataplane/src/node/python/fragment.rs`, which hosts the standalone `FragmentAssembler` + unit tests for buffering, eviction, and window enforcement. This gives Track B a concrete API to plug into `python/interface.rs` once that file is free.
- Verified the module locally with `cargo test -p nextmini python::fragment` and `cargo test -p nextmini python::interface`; remaining PyO3 tests are still blocked on the missing CPython 3.13 dev install.
- Attempted to wire Track B reassembly into `dataplane/src/node/python/interface.rs`, but that file already has dual delivery modes (`PythonDelivery`, payload-only queues) and concurrent edits from PurpleStone; I rolled my WIP back so we can sync on ownership before reshaping that surface.
- Waiting on Track B (dataplane reassembly) + Track C (docs/config propagation) owners before enabling the feature flag by default; until then the legacy behavior remains unchanged.

### Progress log (2025-11-10)

- [x] Clarify constraints + MTU budget _(Owner: PurpleStone, Completed 2025-11-10)_ – Derived payload ceiling formula, documented in this plan and the new design note.
- [x] Define the PyPayloadSeg header _(Owner: PurpleStone, Completed 2025-11-10)_ – Locked 24-byte layout w/ versioning + flags.
- [x] Augment sender pipeline _(Owner: GreenLake, Completed 2025-11-10)_ – `transmit_python_payload` now handles MTU budgeting, shared header injection, and batch helpers with regression tests.
- [x] Processor reassembly + PacketReceiver delivery _(Owner: BrownPond pairing w/ OrangeMountain, Completed 2025-11-10)_ – `PythonInterfaceHandle` integrates the fragment assembler, honors `payload_only` receivers via the new `PayloadDelivery` struct, and falls back to reconstructed raw frames for legacy consumers.
- [x] Failure handling + metrics _(Owner: BrownPond, Completed 2025-11-10)_ – Controller-facing telemetry (`DataplaneToController::PythonFragmentEvents`) now emits `invalid_header` / `assembler_drop` / `window_overflow` / `timeout` events, and `PythonInterfaceHandle` keeps local counters (`python_fragments_*`, `python_reassembly_*`) for future metrics export.
- [x] Config & docs propagation _(Owners: PurpleStone + BrownPond, Completed 2025-11-10)_ – Updated `docs/docs/design/configuration.md`, `docs/docs/design/python_payload_fragmentation.md`, `docs/examples/pytorch_python_api.md`, and `examples/pytorch/config.toml` to cover the new knobs, delivery metadata, and controller alerts.
- [x] Release notes + smoke runbook _(Owner: PurpleStone, Completed 2025-11-10)_ – Added “Controller visibility & alerting” guidance to `docs/docs/design/PLAN_TO_ADD_PYTHON_API.md`, exposed `design/python_payload_fragmentation.md` + the new testing page via MkDocs, and documented the smoke-test workflow (including the CPython dev dependency) in `docs/docs/testing/python_fragmentation_smoke.md`.
- [ ] Testing & integration validation _(Owners: All, In progress 2025-11-10)_ – `docs/testing/scripts/python_fragmentation_smoke.py` now drives `payload_only` receivers and prints message-id metadata; still blocked on the local CPython dev libs for end-to-end automation.

### Progress log – 2025-11-10 (BrownPond)

- Landed controller telemetry for fragmentation drops/timeouts: introduced `DataplaneToController::PythonFragmentEvents`, instrumented `PythonInterfaceHandle` to emit events + warnings, and plumbed the python bindings so `PayloadDelivery` metadata flows through `payload_only` receivers.
- Updated docs/config: refreshed the configuration guide, design note, and PyTorch quickstart to document the telemetry surface plus the new `payload_only=True` flag, and tightened the integration smoke script to validate reconstructed payloads by message id.
- Verified the reassembly unit tests with `cargo test -p nextmini fragment --lib`; full `nextmini_py` tests remain blocked on system CPython headers (noted in this plan + Agent Mail).
- Added in-process counters (`python_fragments_received_total`, `python_fragments_dropped_invalid_header_total`, `python_reassembly_timeout_total`, `python_reassembly_window_overflow_total`) behind `PythonInterfaceHandle::metrics_snapshot()` so we can feed dashboards once the shared metrics registry is ready.
- wired periodic `DataplaneToController::PythonFragmentMetrics` snapshots so controller logs capture aggregate counters (even when trace_flow_events is muted) and noted the follow-up decision on whether to expose the same counters via the upcoming metrics registry.

### Progress log – 2025-11-10 (PurpleStone)

- Documented the new controller telemetry + payload-only receiver flow (`docs/docs/design/PLAN_TO_ADD_PYTHON_API.md`, `docs/examples/pytorch_python_api.md`, `docs/docs/design/python_payload_fragmentation.md`), and updated MkDocs nav so the content is easy to find.
- Authored `docs/docs/testing/python_fragmentation_smoke.md` plus refreshed `docs/testing/scripts/python_fragmentation_smoke.py` to log `message_id`/`total_len` metadata and spell out the CPython 3.13 dev dependency.
- Released the docs/testing file reservations so BrownPond can iterate on the smoke script when the toolchain lands; standing by to run/record the validation once CPython headers are available on this host or the shared runners.
- Began exporting the counters every 5 seconds via the new `DataplaneToController::PythonFragmentMetrics` message so controller logs/alerting have immediate visibility, even before the metrics registry work lands.

### Metrics export analysis – 2025-11-10 (BrownPond)

- `ControllerReporter` already batches link-level metrics every 5s; we can either piggyback a new `DataplaneToController::PythonFragmentMetrics` payload sourced from the same tick or wait for the planned metrics registry and expose the counters directly to Prometheus.
- Pending decision: do we want controller-level alerting on these counters (favoring the new message) or node-local scraping (favoring the registry)? OrangeMountain to weigh in before we start coding.

### CPython dependency status – 2025-11-10 (BrownPond)

- `cargo test -p nextmini_py` and `docs/testing/scripts/python_fragmentation_smoke.py` both require CPython 3.13 dev headers/libraries, which are missing on this workstation/CI runner. Until those dev packages are installed we cannot run the python-api unit tests or the end-to-end smoke script.
- Next steps once the toolchain is available: (1) re-run `cargo test -p nextmini_py`, (2) execute the smoke script per the runbook, capturing logs + controller events, and (3) update this plan + Agent Mail thread with the verification details.

### Configuration knobs & limits (draft)

| Key | Scope | Default | Notes |
| --- | --- | --- | --- |
| `python_fragmentation.enabled` | `LocalConfig` + controller config | `false` (feature flag) | Gated rollout; when false the sender rejects oversized payloads with a clear error. |
| `python_fragmentation.max_message_bytes` | `LocalConfig` | `64 * 1024` | Upper bound per logical message; Python API enforces before slicing. |
| `python_fragmentation.reassembly_window_bytes` | `LocalConfig` | `4 * max_message_bytes` | Per-flow cap for buffered fragments; exceeding it drops oldest fragment groups. |
| `python_fragmentation.fragment_timeout_ms` | `LocalConfig` | `1_000` | Time budget for collecting all fragments of a message. |
| `python_fragmentation.trace_flow_events` | Controller event stream | `true` | Emits structured events for drops/timeouts so operators can alert. |

Sample config updates + docs wiring will be added once Tracks B/C expose the necessary toggles in code.
