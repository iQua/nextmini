# Current Lossless Session Design

**Generated**: March 7, 2026  
**Repo**: `/Users/bli/Playground/nextmini`  
**Scope**: current implementation in `dataplane/src/node/session` and `messages/src/lossless_session.rs`

## Purpose

This document describes the current lossless session subsystem as it exists today.

It is primarily a description of the current design. It also records the rewrite
direction discussed on March 7, 2026 so the current implementation and the
desired target do not drift apart in separate notes.

## High-Level Summary

The current subsystem supports two transfer modes:

- Plain lossless mode
- FEC mode

Both modes share the same session runtime and some control-plane behavior, but they do not share a single canonical transfer unit.

The current design is:

- plain mode is chunk-based
- FEC mode is symbol-based, with blocks layered on top of chunks
- completion is chunk-oriented in plain mode
- completion becomes block-oriented inside sender-side FEC retirement logic, but still chunk-oriented at `EOT`

This mixed model is the main conceptual source of complexity.

## Main Code Locations

- Wire protocol: `messages/src/lossless_session.rs`
- Session runtime: `dataplane/src/node/session/runtime.rs`
- Public session API shim: `dataplane/src/node/session/api.rs`
- Sender: `dataplane/src/node/session/sender.rs`
- Receiver: `dataplane/src/node/session/receiver.rs`
- FEC adapter: `dataplane/src/node/session/fec.rs`
- FEC runtime policy: `dataplane/src/node/session/fec_policy.rs`
- Control helpers: `dataplane/src/node/session/control.rs`
- Flow integration: `dataplane/src/node/session/unicast.rs`
- Packet ingress/routing interaction: `dataplane/src/node/processor.rs`, `dataplane/src/node/route.rs`

## Current Terminology

The current implementation uses several different units:

- **Session**: the runtime-scoped transfer instance, keyed by `session_id`
- **Chunk**: the canonical plain-mode payload unit; `LosslessSessionData.index` is a chunk index
- **FEC block**: a group of `symbols_per_block` chunks in FEC mode
- **Symbol**: one source or coded FEC payload within a FEC block
- **Tree**: multicast tree selected by `tree_id` on FEC data packets
- **Tree lane**: a sender-local bounded queue and worker task for a particular `tree_id`

Important: the current code does **not** use a single canonical "block" abstraction across both modes.

## Wire Protocol

The wire protocol is defined in `messages/src/lossless_session.rs`.

### Common Header

Every frame carries:

- magic
- version
- kind (`Data` or `Control`)
- `session_id`
- `body_len`

There are two protocol versions:

- `LOSSLESS_SESSION_BASE_VERSION = 1`
- `LOSSLESS_SESSION_FEC_VERSION = 2`

### Plain Data Frame

Plain-mode data uses:

- `LosslessSessionData { index, payload_len }`
- payload bytes

The sender emits these with `encode_data(session_id, index, payload)`.

This is a chunk-indexed protocol.

### FEC Data Frame

FEC-mode data uses:

- `LosslessSessionFecData { block_id, symbol_id, tree_id, payload_len }`
- payload bytes

The sender emits these with `encode_fec_data(session_id, block_id, symbol_id, tree_id, payload)`.

This is a block/symbol protocol.

### Control Frames

Current control frames are:

- `Manifest { chunk_size, total_bytes }`
- `FecManifest { chunk_size, total_bytes, fec }`
- `Ready { node_id }`
- `FecCapabilities { node_id, capabilities }`
- `Ack { up_to }`
- `FecStatus { block_id, deficit_symbols }`
- `Eot { last_index }`

Notable current properties:

- plain mode uses cumulative `Ack { up_to }`
- FEC mode uses `FecStatus` for per-block progress/deficit signaling
- `Eot` is still chunk-index based in both modes
- control frames do not carry `tree_id`

## Session Runtime

The session runtime in `dataplane/src/node/session/runtime.rs` is an actor that:

- starts sender and receiver tasks
- routes inbound frames by `session_id`
- tracks task handles and per-session input channels
- exposes `start_sender`, `start_receiver`, `deliver`, `wait_completion`, and `stop`
- maintains a topology-ready watch channel for senders

Current runtime behavior:

- sender configuration is post-processed through `fec_policy`
- receiver configuration derives local `fec_capabilities`
- topology readiness is held outside the sender task and exposed through `watch`

The `api.rs` module is not a true standalone boundary. It mostly re-exports runtime-facing pieces and defines `InboundFrame`.

## Sender Design

The sender lives in `dataplane/src/node/session/sender.rs`.

### Main Loop

The top-level sender loop currently does all of the following in one place:

- transfer timeout checks
- control-frame polling
- topology gate release
- ready gate release
- manifest emission and re-emission
- source drain detection
- plain-mode data sending
- FEC-mode scheduling
- `EOT` emission
- blocked-lane waiting
- completion logging

### Shared Sender State

`SenderState` is the central mutable object.

It currently holds:

- plain-mode state
- FEC-mode state
- control/ready/topology state
- pacing state
- completion accounting
- observability counters
- addressing information

This means the sender is implemented as one large state machine with many mode branches rather than as a shared core plus optional mode component.

### Plain Mode

In plain mode, the sender:

1. builds a `ChunkSource`
2. reads the next chunk in strict chunk-index order
3. sends `DATA(index, payload)`
4. tracks `primary_chunks`
5. retires progress via cumulative `Ack { up_to }`
6. sends `Eot { last_index = total_chunks }` once source is drained and in-flight work is retired

Additional plain-mode details:

- a sliding window is computed from `DEFAULT_WINDOW` and the token bucket
- progress is receiver-minimum cumulative chunk progress
- `Ack` handling is cumulative and monotonic

### FEC Mode

In FEC mode, the sender still starts from the chunk stream, but remaps chunks into FEC blocks and symbols.

For each chunk:

- `zero_based = chunk.index - 1`
- `block_id = zero_based / symbols_per_block`
- `symbol_id = zero_based % symbols_per_block`

These are treated as **systematic** symbols.

The sender can also emit extra **coded** symbols using the stride scheduler.

Current FEC behavior:

- manifest is `FecManifest`
- sender waits for `Ready` and `FecCapabilities`
- sender validates receiver compatibility before sending FEC data
- FEC retirement is based on completed blocks, not cumulative chunk `Ack`
- coded symbols are driven by receiver `FecStatus.deficit_symbols`

### Stride Scheduler

The current FEC coded-symbol scheduler is `StrideScheduler`.

It tracks active blocks with:

- current deficit
- stride
- pass
- lazy-created encoder
- next coded ESI
- coded budget

Its design assumes:

- coded work is generated on demand from deficit reports
- higher-deficit blocks should receive more coded symbols
- coded symbols may be prioritized ahead of newly streamed source symbols

### Tree IDs And Tree Lanes

FEC mode supports multiple trees.

Current sender behavior:

- each FEC packet carries a `tree_id`
- the sender creates one local lane per configured tree
- each lane is a bounded `mpsc` queue plus a worker task
- the main sender loop round-robins symbols across those lanes
- if every lane queue is full, the sender enters an all-lanes-blocked wait

Important current distinction:

- `tree_id` is part of the packet/routing model
- tree lanes are only a sender-local implementation strategy

The sender does not observe network-tree backpressure directly. It only observes
whether its own local per-tree lane queue is full. In the current design, that
lane eventually stays full because the tree worker sending through
`ProcessorHandle::process_packet(...).await` stops draining when processor ingress
backpressures.

### Current Sender Completion Semantics

Plain mode:

- completion unit is chunk progress

FEC mode:

- internal retirement unit is contiguous completed FEC blocks
- `EOT` still uses the final chunk index

So FEC sender completion is internally block-based but externally still chunk-anchored.

## Receiver Design

The receiver lives in `dataplane/src/node/session/receiver.rs`.

### Common Receiver Behavior

The receiver:

- emits `Ready` immediately on start
- receives inbound frames through a per-session channel
- decodes plain data, FEC data, or control frames
- writes recovered bytes into the sink buffer if one exists

### Plain Mode Receiver

In plain mode, the receiver:

1. decodes `DATA(index, payload)`
2. stores payloads in a `pending` map keyed by chunk index
3. drains contiguous chunks starting at `expected`
4. appends drained bytes to the sink
5. sends batched cumulative `Ack { up_to }`
6. stops when `Eot.last_index` has been reached contiguously

This is a standard chunk-reorder plus cumulative-ack design.

### FEC Mode Receiver

In FEC mode, the receiver:

1. waits for `FecManifest`
2. validates that local `FecCapabilities` support the manifest
3. groups incoming symbols by `block_id`
4. stores symbols in `FecBlockState`
5. attempts decode when enough symbols appear to be available
6. converts decoded source symbols back into chunk-indexed payloads
7. feeds those decoded chunks into the same chunk-ordering path used by plain mode

This means the current receiver does not use one block delivery path. Instead, FEC decode is translated back into chunks so the plain receiver path can be reused.

### FEC Feedback

The receiver sends `FecStatus { block_id, deficit_symbols }`.

Current meaning:

- `deficit_symbols > 0`: more symbols needed for that block
- `deficit_symbols == 0`: block completed

The receiver also periodically re-sends terminal zero-deficit statuses for recently completed blocks.

So FEC receiver behavior includes:

- bounded/jittered feedback timing
- terminal zero-deficit re-advertisement
- separate decoded-block history tracking

## ACK And Completion Path

### How ACKs Are Sent Today

The receiver uses `ControlEmitter` to:

- encode a control frame
- wrap it in an IPv4/TCP packet
- inject it through `ProcessorHandle::process_packet`

The sender receives it through the normal packet path:

- processor detects a lossless session packet
- extracts `session_id`
- derives `peer_id` from the source node
- delivers the payload into the session runtime

### Current Plain ACK Semantics

Plain mode uses:

- `Ack { up_to }`
- cumulative progress
- monotonic chunk retirement

### Current FEC Completion Semantics

FEC mode does **not** use cumulative chunk `Ack` for retirement.

Instead:

- `FecStatus { block_id, deficit_symbols == 0 }` means a block is complete
- sender advances contiguous per-receiver completed-block progress
- sender retires work by the minimum completed contiguous block across receivers

But receiver-side object completion is still checked against chunk continuity and `Eot.last_index`.

## Routing And Trees

`tree_id` is only present on FEC data frames.

Current lower-layer use:

- `Packet::lossless_fec_tree_id()` parses `tree_id` from FEC payloads
- processor ingress sharding hashes `(flow_id, tree_id)` for FEC packets
- route selection uses `tree_id` to choose the multicast tree route

Control frames do not carry `tree_id`, so they rely on control-tree selection behavior in the routing layer.

## Flow Integration

`dataplane/src/node/session/unicast.rs` currently integrates lossless sessions with controller-assigned flows.

Current behavior:

- computes deterministic `session_id` and client port from the flow
- spawns sender and receiver wrapper tasks
- constructs `SenderRequest` and `ReceiverRequest`
- uses a template source buffer for sender-side data generation
- waits for completion and reports flow-finished state

This module is orchestration around the session runtime rather than part of the core transport protocol.

## Current Design Consequences

The current design has several important consequences:

### 1. Two Different Transfer Models

The subsystem is not implemented as one protocol with an optional FEC encoding layer.

It is currently:

- a chunk protocol in plain mode
- a block/symbol protocol overlaid on top of that chunk model in FEC mode

### 2. FEC Changes More Than Encoding

FEC mode changes:

- wire data shape
- sender retirement semantics
- receiver feedback semantics
- completion accounting
- routing use of `tree_id`
- sender scheduling model

So FEC is not a small optional plugin in the current design.

### 3. Sender And Receiver Carry Mixed Responsibilities

Both sender and receiver currently combine:

- shared session shell behavior
- plain-mode logic
- FEC-specific logic
- control/progress policy
- transport packet emission/parsing

### 4. Blocks Are Not Canonical

The current code uses:

- chunks as the canonical unit for plain transfer
- blocks only inside FEC
- symbols only inside FEC

There is no single transfer unit shared by both modes.

## Short Current-State Summary

The current lossless session subsystem is a hybrid design:

- plain mode sends ordered chunks and uses cumulative ACKs
- FEC mode groups chunks into blocks, sends symbols across trees, and uses per-block FEC status
- receiver-side delivery is still ultimately chunk-based
- tree lanes are a sender-local dispatch mechanism, not a wire-level requirement

This design works, but it is conceptually split between chunk-oriented plain transfer and block/symbol-oriented FEC transfer.

## Rewrite Direction Under Discussion

The intended refactor is a substantial rewrite, not a small cleanup. The goal is
to remove the chunk-first / FEC-overlay split and replace it with one canonical
transfer model.

### Canonical Transfer Unit

The rewrite should make `block` the only top-level transfer unit.

- current plain-mode `chunk` should be renamed to `block`
- plain mode should send whole blocks without coding
- FEC mode should send symbols within a block
- `symbols_per_block` remains configurable, but it applies to one canonical block abstraction rather than to a separate FEC-only block layer

This means the current distinction between "plain chunks" and "FEC blocks" should
disappear.

### Target Wire Shape

The intended protocol shape is:

- plain mode: `BlockData { block_id, payload }`
- FEC mode: `BlockSymbol { block_id, symbol_id, tree_id, payload }`
- completion: `BlockAck { block_id }`
- optional FEC deficit/status feedback remains block-scoped

There should be no separate `BlockRepair` frame. In a fountain-code design,
additional coded output is just more `BlockSymbol` for the same `block_id` with
higher `symbol_id` values.

### Target Completion Model

The target control/completion rules are:

- ACKs are per block, not cumulative
- there is no sliding window
- sender progress is not a moving cumulative watermark
- transfer completion is defined by block completion, not by contiguous chunk retirement

This is intended to fit bulk transfer rather than streaming semantics.

### Target Sender Scheduling

The target sender scheduler is source-first:

- always prioritize the next unsent source symbol before any extra fountain symbol
- do not prioritize additional coded symbols ahead of new source data
- do not use sender-side ACK timeouts
- emit extra fountain symbols only after the initial source pass and only in response to receiver block-scoped feedback

So the sender should not have a separate "repair phase" packet type or a
repair-first priority rule. Extra output is only a scheduling decision over
`BlockSymbol`.

### Trees And Backpressure In The Rewrite

`tree_id` remains part of the FEC packet model because lower layers use it for
ingress sharding and multicast route lookup.

Tree lanes, however, are not protocol elements and should not survive the
rewrite as a required design concept.

The preferred backpressure boundary is processor ingress:

- the sender should send asynchronously via a non-blocking processor API
- if sending a symbol for a selected `tree_id` returns a full queue / `WouldBlock`, that tree should be treated as immediately backpressured for that scheduling attempt
- this backpressure signal is meaningful only when ingress is tree-visible, which today means sequential processor mode with per-lane queues hashed by `(flow_id, tree_id)`
- a shared ingress queue only provides global backpressure, not tree-specific backpressure

### ACK Path In The Rewrite

Block acknowledgements should continue to return over the control-frame path:

- receiver encodes the control frame and injects it through the processor handle
- sender receives it through normal session delivery
- control frames still do not need `tree_id`; they rely on the existing control-tree selection behavior in the routing layer

This keeps control routing separate from the FEC data-tree selection path while
still allowing block-scoped completion.
