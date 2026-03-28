# Lossless Tree Contract Audit

## Summary

Tree identity is not purely local in the current lossless implementation.

Today it is part of the outer transport and scheduling contract in addition to
being validated on the inner lossless FEC symbol wire format. Because of that:

- `manifest.tree_ids` must remain in scope for validation and preflight
- `BlockSymbol.tree_id` must remain on the wire
- control frames must continue to clear `tree_id` and rely on control-tree
  routing (`tree_id=None`)

Any future attempt to remove these fields requires a transport/routing redesign,
not just a codec cleanup.

## Current Dependencies

### 1. Manifest validation depends on advertised FEC trees

`messages/src/lossless_session.rs`

- `LosslessSessionManifest::validate()` requires FEC `tree_ids` to be non-empty,
  sorted, and unique.
- `LosslessSessionManifest::validate_block_symbol()` rejects symbols whose
  `symbol.tree_id` is not advertised by the manifest.

This means tree identity is part of the current decoder/validation contract, not
just sender-local scheduling state.

### 2. The FEC sender uses manifest trees to stripe and patch symbols

`dataplane/src/node/session/sender/fec.rs`

- `FecSender::new()` clones `fec.tree_ids` from the manifest.
- `FecSender::try_send_symbol()` round-robins those tree ids.
- `lossless_session::set_block_symbol_tree_id()` patches the inner
  `BlockSymbol.tree_id` on the already-encoded frame before submission.

The sender therefore depends on both:

- outer transport `tree_id` for processor/routing behavior
- inner `BlockSymbol.tree_id` for validation and test-visible consistency

### 3. Runtime preflight depends on multi-tree ingress capabilities

`dataplane/src/node/session/runtime.rs`
`dataplane/src/node/processor.rs`

- `validate_sender_ingress_contract()` probes with one configured FEC tree and
  rejects collaborative multi-tree FEC unless ingress reports
  `TreeVisibleNonBlocking`.
- `ProcessorHandle::lossless_ingress_contract()` distinguishes
  `TreeVisibleNonBlocking` from `SharedQueueNonBlocking`.
- `ProcessorHandle::select_processor_ingress_lane()` hashes on
  `(flow_id, tree_id)` when a lossless FEC tree id is present.

So tree identity is part of the processor-ingress contract, not just a receiver
detail.

### 4. Routing treats payload trees and control trees differently

`dataplane/src/node/session/control.rs`
`dataplane/src/node/route.rs`

- Payload frames preserve their explicit `tree_id`.
- Control frames intentionally clear `tree_id` and are routed with
  `tree_id=None`.
- Multicast control routing then resolves `tree_id=None` to tree `0` when
  installed, otherwise the smallest installed tree for that multicast group.

This is a distinct routing contract. Removing inner or outer tree identity
without redesigning control routing would be incorrect.

## Existing Coverage

The current tree contract is already protected by tests in multiple layers:

- `messages/src/lossless_session.rs`
  - rejects unknown symbol tree ids
- `dataplane/tests/fec_multitree.rs`
  - proves emitted symbols stay on configured trees and packet outer tree id
    matches inner `BlockSymbol.tree_id`
- `dataplane/tests/fec_handshake.rs`
  - rejects unsorted/duplicate configured trees
  - rejects collaborative multi-tree FEC on shared-queue ingress
- `dataplane/src/node/route.rs`
  - proves explicit tree lookup is honored
  - proves control routing with `tree_id=None` uses tree `0` or the smallest
    installed tree
- `dataplane/src/node/session/control.rs`
  - proves control packets clear payload tree ids before routing

## Scope Decision For Phase 2

Phase 2 cleanup must preserve:

- `manifest.tree_ids`
- `BlockSymbol.tree_id`
- explicit outer transport `tree_id` on payload frames
- `tree_id=None` control routing

Still out of scope:

- removing `manifest.tree_ids`
- removing `BlockSymbol.tree_id`
- removing inner `session_id`
- header simplification

## What A Later Redesign Would Need

Before tree-related wire cleanup becomes safe, a later redesign would need to
prove all of the following:

1. Receiver-side validation can rely only on outer transport metadata or a new
   explicit transport contract.
2. Sender striping no longer depends on patching inner symbol tree ids.
3. Processor ingress and route selection can recover the same tree identity
   everywhere it is currently consumed.
4. Control routing semantics remain explicit after any transport changes.

Until then, tree identity remains a real protocol/transport dependency.
