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
- `ack_policy: String` — sender commit policy: `all`, `k:N`, or `frac:P`.
- `fec_k: Option<u16>` / `fec_p: Option<u16>` — optional FEC parameters per block.

Example (node.toml)
```
[reliable]
default_chunk_size = 32768
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
