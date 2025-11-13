Reliable Multicast Configuration (draft)

Overview
- Defines default behavior for the reliable multicast engines when the `reliable` feature is enabled in the dataplane.

Location
- Rust struct: `dataplane/src/node/config.rs` → `ReliableConfig`.

Fields
- `default_chunk_size: u32` — default payload chunk size in bytes.
- `control_weight: u32` — WRR weight for control flows to avoid starvation.
- `data_bucket: Option<TokenBucketSpec>` — pacing for data flows.
- `sack_interval_ms: u32` — upper bound on how often receivers emit SACK updates
  when gaps persist. Receivers send an immediate SACK when a gap first appears and
  then re-emit at this interval until the gap closes. Set to `0` to disable the
  timer (SACKs only fire when new gaps are observed).
- `nack_interval_ms: u32` — how often targeted repairs are requested on timeouts.
- Session coordination happens via explicit session IDs. Senders still allocate via
  `Dataplane.send_file(..., session_id=...)` (or allow the runtime to pick
  one), but receivers can now omit the `session_id`. When `Dataplane.receive_file`
  is invoked without an ID, the dataplane waits for the first inbound manifest, adopts
  the sender's session ID automatically, and only then spawns the reliable receiver.
  Advanced orchestrators may still pre-register IDs with
  `Dataplane.reliable_register_session_id(...)` when they need to short-circuit the wait.
- `ack_policy: String` — sender commit policy: `all`, `k:N`, or `frac:P`.
- `fec_k: Option<u16>` / `fec_p: Option<u16>` — optional FEC parameters per block.
- `ready_grace_ms: u64` — grace window before the sender starts streaming when not all receivers have reported `Ready` (default 1500 ms).

Example (node.toml)
```
[reliable]
default_chunk_size = 4096
control_weight = 10
ack_policy = "all" # or "k:2", "frac:0.75"
sack_interval_ms = 25
nack_interval_ms = 50
# Optional pacing
# [reliable.data_bucket]
# rate = 50_000_000  # bytes/sec
# burst = 200_000    # bytes

# Optional FEC
# fec_k = 8
# fec_p = 2
```

Notes
- Ack policy parser lives in `messages/src/rlm.rs` (unit tested).
- E2E harness and tests are gated until engines are wired; dry-run traces are available under `examples/reliable_multicast/`.
