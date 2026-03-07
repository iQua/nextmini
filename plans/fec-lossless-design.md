# Lossless Session Rewrite Design

**Generated**: March 7, 2026  
**Repo**: `/Users/bli/Playground/nextmini`  
**Scope**: target design for the rewritten lossless session subsystem

## Purpose

This document describes the intended design for the rewritten lossless session
subsystem.

It is a forward-looking design document. It does not describe the existing
codebase.

## Design Goals

- One canonical transfer unit: `block`.
- Plain lossless mode is the default.
- Plain mode sends on one tree.
- FEC mode is optional and is the only mode that sends across multiple trees.
- Plain and FEC share one session shell and one completion model.
- ACKs are per-block, not cumulative.
- The protocol is for bulk transfer, not streaming, so there is no sliding
  window.
- The sender does not wait on ACK timeouts.
- Fountain-code output uses the same `BlockSymbol` frame shape as source symbols;
  there is no separate repair frame.

## Terminology

- **Session**: one end-to-end transfer identified by `session_id`
- **Block**: the canonical ordered unit of transfer and acknowledgement
- **Symbol**: one FEC payload unit within a block
- **Tree**: a multicast tree chosen by `tree_id`
- **Manifest**: session metadata sent before data transmission begins
- **BlockAck**: receiver confirmation that one block is complete
- **BlockStatus**: receiver feedback that one FEC block needs additional symbols

There is no `chunk` concept in this design.

## Transfer Model

The sender splits the object into ordered blocks:

- `block_id` starts at `0`
- `total_blocks = ceil(total_bytes / block_size)`
- each block maps to an absolute byte range using `block_id * block_size`
- the final block may be shorter than `block_size`

The canonical delivery rule is:

- plain mode delivers one full block directly
- FEC mode delivers one block by transmitting symbols for that block
- completion is always tracked at block granularity

The receiver writes a completed block directly to its final byte offset. There is
no contiguous-drain or cumulative-retirement rule in the protocol.

## Modes

### Plain Mode

Plain mode is the default.

- the sender emits uncoded block payloads
- only one tree is used
- completion is `BlockAck { block_id }`

Plain mode exists to provide the simplest possible lossless bulk-transfer path.

### FEC Mode

FEC mode is optional.

- the sender emits symbols inside each block
- `symbols_per_block` is configurable
- source symbols are sent first
- additional fountain symbols are sent only when a receiver asks for more
- `tree_id` is carried on FEC symbol packets
- FEC mode is the only mode that can stripe traffic across multiple trees

The important point is that FEC changes encoding, not the top-level transfer
unit. The top-level unit remains the block.

## Wire Protocol

### Common Session Metadata

Each session begins with a manifest that defines:

- `session_id`
- `total_bytes`
- `block_size`
- `total_blocks`
- `mode`

When `mode = Fec`, the manifest also defines:

- `symbols_per_block`
- the configured `tree_ids` set, or an equivalent tree-selection policy

The manifest is the contract for the whole transfer. Both plain and FEC modes
use the same block numbering and object geometry.

### Data Frames

Plain mode uses:

- `BlockData { block_id, payload }`

FEC mode uses:

- `BlockSymbol { block_id, symbol_id, tree_id, payload }`

There is no separate `BlockRepair` frame.

In FEC mode:

- `symbol_id < symbols_per_block` means a source symbol
- `symbol_id >= symbols_per_block` means an additional fountain symbol

Extra coded output is just more `BlockSymbol` for the same `block_id`.

### Control Frames

The control surface is:

- `Manifest`
- `Ready`
- `BlockAck { block_id }`
- `BlockStatus { block_id, deficit_symbols }` for FEC mode
- `Eot`

`BlockStatus` is receiver-driven FEC feedback. It does not change the completion
model. Completion is still `BlockAck`.

`Eot` means the sender has emitted the initial source pass for all blocks. It is
not a cumulative completion marker.

## Session Lifecycle

1. The sender creates a session and sends `Manifest`.
2. The receiver validates the manifest and replies `Ready`.
3. If the session uses FEC, the receiver must also confirm that FEC mode is
   supported before FEC transmission begins.
4. The sender emits block data or block symbols.
5. The receiver sends `BlockAck` whenever a block is complete.
6. In FEC mode, the receiver sends `BlockStatus` when additional symbols are
   needed for a block.
7. After the sender has emitted the initial source pass for all blocks, it sends
   `Eot`.
8. The session completes when every receiver has acknowledged every block.

Because there is no sender-side ACK timeout, the receiver should be willing to
re-emit `BlockAck` for a completed block while the session remains open. A
duplicate `BlockData`, duplicate `BlockSymbol`, or post-`Eot` session activity
may all be used as triggers for re-advertising completion.

## Sender Design

### Shared Sender Core

The shared sender core is responsible for:

- manifest emission
- session readiness
- block geometry
- per-block completion tracking
- session completion detection
- control-frame handling

The shared sender core does not care whether a block is encoded directly or via
FEC symbols.

### Plain Sender

The plain sender is minimal:

- choose the next unsent block
- emit `BlockData { block_id, payload }`
- track `BlockAck { block_id }` until each receiver has acknowledged the block
- mark the block complete for each receiver that acknowledges it

Plain mode uses the default single tree and does not carry `tree_id` in its data
frame.

### FEC Sender

The FEC sender owns only:

- symbol generation
- tree selection
- FEC feedback handling

The FEC sender keeps per-block state:

- `next_source_symbol`
- `next_fountain_symbol`
- outstanding extra-symbol demand
- per-receiver acknowledgement status

## Sender Scheduling Rules

The FEC sender scheduler is source-first.

The next block is chosen by this priority:

1. the lowest `block_id` with an unsent source symbol
2. if every source symbol for every block has been emitted, the lowest
   `block_id` with outstanding extra-symbol demand
3. otherwise nothing is ready to send

This implies:

- source symbols are always prioritized ahead of extra fountain symbols
- there is no repair-first policy
- extra symbols are strictly receiver-driven
- there is no sender ACK timeout

There is also no protocol sliding window.

## Receiver Design

### Shared Receiver Core

The shared receiver core is responsible for:

- manifest validation
- block ledger management
- block completion tracking
- writing completed blocks into the destination buffer
- sending `BlockAck`
- final session completion detection

Completion is block-based in both modes.

### Plain Receiver

The plain receiver:

- accepts `BlockData`
- validates `block_id` and payload size
- writes the completed block at its final byte offset
- sends `BlockAck { block_id }`

There is no cumulative acknowledgement and no contiguous-drain path.

### FEC Receiver

The FEC receiver:

- groups symbols by `block_id`
- attempts decode when enough symbols are available
- writes the completed block at its final byte offset once decode succeeds
- sends `BlockAck { block_id }`
- sends `BlockStatus { block_id, deficit_symbols }` when more symbols are needed

The receiver does not acknowledge symbols. It acknowledges completed blocks.

## Tree Selection And Backpressure

`tree_id` is a real protocol field in FEC mode because lower layers use it for
routing and ingress placement.

Tree lanes are not part of this design.

The sender should have one FEC scheduler, not one worker per tree. Tree choice
is a scheduling decision, not a protocol abstraction.

### Tree Choice

For each selected `(block_id, symbol_id)`, the sender chooses a candidate tree
from the configured `tree_ids` set in deterministic round-robin order.

The scheduling rule is:

- pick the next symbol to send
- try the next candidate `tree_id`
- if that tree accepts the symbol, advance sender state
- if that tree is backpressured, try the next tree without advancing symbol state

### Backpressure Boundary

Backpressure should be observed at processor ingress.

The sender uses asynchronous, non-blocking submission to processor ingress and
interprets the result immediately:

- `Queued`: the symbol was accepted
- `WouldBlock`: that tree is backpressured right now
- `Closed`: that tree/path is unavailable

On `WouldBlock`:

- the sender treats that `tree_id` as immediately backpressured for the current
  scheduling pass
- the sender does not advance `symbol_id`
- the sender tries another tree

If every configured tree is backpressured, the sender yields and waits for the
next opportunity to send.

### Requirement For Tree-Visible Ingress

Per-tree backpressure only makes sense if processor ingress is tree-visible.

That means multi-tree FEC requires an ingress mode where:

- the sender can submit work without blocking
- full/not-full is observable immediately
- different trees do not collapse onto one indistinguishable shared queue

If all trees share one queue, the sender can only observe global pressure, not
tree-specific pressure.

## ACK Path

`BlockAck` is a control frame from receiver to sender.

The receiver sends it through the normal control path:

- encode the control frame
- inject it into the dataplane packet path
- route it back to the sender
- deliver it to the session runtime

Control frames do not need to carry `tree_id`.

## Shared Component Boundaries

The rewritten subsystem should be split into small components with clear
ownership:

- `wire`: frame types and encoding/decoding
- `plan`: block geometry and offsets
- `ledger`: shared per-block session state
- `runtime`: session registry and typed frame dispatch
- `sender/plain`: plain block emission
- `sender/fec`: symbol generation, FEC feedback, and tree scheduling
- `receiver/plain`: plain block receive/acknowledge
- `receiver/fec`: symbol collection, decode, and block acknowledgement

The shared pieces own block/session semantics. FEC owns only FEC-specific
encoding and scheduling behavior.

## Invariants

- The canonical unit is always `block`.
- Plain mode is the default.
- Plain mode uses one tree.
- FEC mode is optional.
- FEC mode may use multiple trees.
- ACKs are per-block.
- There are no cumulative ACKs.
- There is no protocol sliding window.
- There is no `BlockRepair` frame.
- Extra fountain output is still `BlockSymbol`.
- Tree lanes are not part of the protocol design.
