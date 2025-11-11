Reliable Multicast Logging Hooks

Overview
- Engines can emit compact hex dumps of control/data frames to aid debugging and E2E traceability.
- This is disabled by default and can be enabled per run via an environment variable.

Controls
- `RELIABLE_HEX_LOG=1` — when set, small control frames (MANIFEST/ACK/SACK/REPAIR/EOT) and short DATA frames are logged with a compact hex dump alongside parsed metadata.

Where it shows up
- Sender baseline (`dataplane/src/node/reliable/sender.rs`) logs hex when the env var is present; receiver and other engines can adopt the same pattern as wiring lands.

Usage
- Example:
  - `RELIABLE_HEX_LOG=1 RUST_LOG=info cargo run -p dataplane --features reliable`
  - Or when running tests/harnesses that start the dataplane.

Notes
- Keep payload hex length limited in logs (e.g., only when small or truncated) to avoid excessive output.
- E2E tests in `examples/reliable_multicast/tests/` include placeholders to display hex sequences and will flip to live capture once engines emit real frames.

