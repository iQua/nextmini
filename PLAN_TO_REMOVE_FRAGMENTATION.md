Here’s how I’d approach this: treat “no fragmentation anywhere” as a *protocol change*, then unwind the implementation in a few deliberate phases.

I’ll split the plan into:

1. Python API (where fragmentation was originally introduced).
2. Reliable dataplane (RLM/PGMCC).
3. Cross‑cutting cleanup: config, docs, tests, rollout.

---

## 1. Python API: remove fragmenter/defragmenter

### 1.1. Take stock of what exists

On the Rust side you currently have:

* `dataplane/src/node/python/payload.rs`

  * `PyPayloadSegHeader`
  * `python_payload_budget`
  * `build_py_payload_segments`
* `dataplane/src/node/python/fragment.rs`

  * `FragmentAssemblerConfig`
  * `FragmentAssembler`
  * `DefragOutcome`
* `dataplane/src/node/python/interface.rs`

  * `FragmentationRuntime`
  * Fragmentation metrics (`PythonFragmentEvent`, `PythonFragmentMetricsSnapshot`, etc.)
  * Send path that optionally segments outbound Python payloads.
  * Receive path that looks for `PyPayloadSegHeader` and uses `FragmentAssembler`.

On the Python side you’ll have mirrored logic:

* Some `Fragmenter` / `Defragmenter` equivalents.
* Python definitions of the header / magic (`"pysg"`), and tests for them.

And in config:

* `python_fragmentation_enabled`
* `python_fragmentation_max_message_bytes`
* `python_fragmentation_reassembly_window_bytes`
* `python_fragmentation_fragment_timeout_ms`
* `python_fragmentation_trace_flow_events`

---

### 1.2. Decide on protocol behavior going forward

Target behavior for Python API:

* **Every “flow packet” from Python to the dataplane is a single, opaque byte buffer.**
* No `PyPayloadSegHeader`, no segment indexes, no reassembly.
* Underlying transport (Unix/TCP/whatever you use between Python and Rust) is responsible for splitting into OS‑level packets; you don’t do app‑level segmentation.

You’ll want to confirm:

* The Python↔Rust channel (length‑prefix framing / protobuf / cap’n proto / etc.) tolerates “large” payloads without needing your own fragmentation.
* Any existing client that had `python_fragmentation_enabled=true` is either nonexistent or ok to break / migrate.

---

### 1.3. Make fragmentation a no‑op at runtime

Before deleting code, make it effectively unused:

**Rust side (Python interface):**

* In `PythonInterface` (or equivalent):

  * In the send path:

    * Replace “if fragmentation enabled → split into segments” with “always send exactly one frame”:

      ```rs
      pub fn send_flow_payload(&mut self, flow_id: FlowId, payload: Bytes) {
          // Old:
          // if let Some(frag) = &mut self.fragmentation {
          //     frag.send(flow_id, payload);
          // } else {
          //     self.send_raw(flow_id, payload);
          // }

          // New:
          self.send_raw(flow_id, payload);
      }
      ```

  * In the receive path:

    * Remove the branch that attempts `PyPayloadSegHeader::decode_from`.
    * Deliver the payload directly to the flow handler.

* Still keep the code for `FragmentationRuntime`, `FragmentAssembler`, etc. compiled, but they’re no longer constructed or used by `PythonInterface`.

**Config:**

* Keep `python_fragmentation_*` fields but:

  * Don’t pass them into any `FragmentAssemblerConfig` for Python.
  * Optionally log once at startup if any of them are set: “python_fragmentation_* is deprecated and has no effect”.

**Python side:**

* Make the Python fragmenter/defragmenter classes never used:

  * Call sites that previously wrapped outgoing payloads in a fragmenter should send the raw bytes instead.
  * Receive side should treat incoming frames as one message, no defrag.

At this point, the system behaves *as if* there were no fragmentation, but all the code still exists. This is a safe place to run tests and staging.

---

### 1.4. Delete the Python fragmentation implementation

Once you’re happy that no deployment relies on it:

**Rust side:**

1. Delete `dataplane/src/node/python/fragment.rs` completely, unless something else uses its types.

2. In `dataplane/src/node/python/payload.rs`:

   * Remove `PyPayloadSegHeader`.
   * Remove `python_payload_budget`.
   * Remove `build_py_payload_segments` and associated error types.
   * If that empties the module, delete the module and its `mod` re‑exports.

3. In `dataplane/src/node/python/interface.rs`:

   * Remove `FragmentationRuntime` and all references to it.
   * Remove any code paths that emit `PythonFragmentEvent::Fragment*` or use `PythonFragmentMetricsSnapshot` specifically for fragmentation.
   * Collapse the send/receive logic into a single, straightforward “one buffer in, one buffer out” pathway.

4. Remove any Python‑fragmentation‑specific logs and metrics:

   * e.g. counters like `python_fragment_message_dropped`, `python_fragment_reassembly_timeout`.

**Config:**

* Mark `python_fragmentation_*` fields as *deprecated but still accepted* for one release:

  ```rs
  #[deprecated(note = "Python fragmentation has been removed; this field is ignored")]
  #[serde(default)]
  pub python_fragmentation_enabled: bool;
  ```

* After you’ve had a release with that deprecation:

  * Remove those fields from `LocalConfig`.
  * Remove CLI flags `--python-fragmentation-*`.
  * Update any config validation, docs, and examples accordingly.

**Python side:**

* Delete the fragmenter/defragmenter classes and any `PyPayloadSegHeader` representations.
* Remove tests that specifically assert fragmentation behavior.
* Simplify docstrings / README for the Python API: no more mention of segmentation, message‑ids, etc.

---

## 2. Reliable dataplane (RLM/PGMCC): remove fragmenter/defragmenter

Right now RLM reuses the same machinery:

* **Sender:**

  * `dataplane/src/node/reliable/sender.rs`:

    * `FrameFragmenter` and `SenderState::fragment_frame`.
    * Uses `python_payload_budget` & `build_py_payload_segments`.

* **Receiver:**

  * `dataplane/src/node/processor.rs`:

    * Fields like `reliable_fragment_enabled: bool` and `reliable_fragment_assembler: FragmentAssembler`.
    * `defragment_reliable` and its use in `try_deliver_reliable`.

* **Config:**

  * `CommonConfig.fragmentation` (or similar) holding `FragmentationConfig`.
  * Construction of `FragmentationConfig` from local config.

---

### 2.1. Decide how you want chunks vs MTU to work

You have two main choices:

1. **Shrink `chunk_size` to ≤ MTU budget.**

   * i.e. enforce at sender setup time:

     ```rs
     let budget = mtu - (IPV4_HEADER_LEN + TCP_HEADER_LEN + some_safety);
     let chunk_size = reliable_cfg.default_chunk_size.min(budget);
     ```

   * This makes each RLM chunk roughly “one MTU‑sized TCP payload”.

   * No RLM fragmentation needed; PGMCC’s notion of “one congestion‑controlled packet” ≈ one on‑wire packet.

2. **Allow chunk_size ≫ MTU and rely on lower layers (TCP/QUIC) to segment.**

   * Simpler config, but you might get large overlay IPv4 packets written to a TUN device and rely on kernel IP fragmentation or a higher‑level transport.
   * If that’s acceptable in your environment, you don’t *need* RLM‑level fragmentation for correctness, but you should be aware you’re no longer enforcing `LocalConfig.mtu` at the RLM layer.

For a clean design, I’d recommend **(1)**: clamp RLM `chunk_size` to an MTU‑sized budget and ditch RLM fragmentation entirely.

---

### 2.2. Stop using RLM fragmentation at runtime

In the “disable first, then delete” spirit:

**Sender (`reliable/sender.rs`):**

* In `SenderState::new` (or wherever `FrameFragmenter::new(common)` is called), stop constructing a `FrameFragmenter`.

* In `SenderState::send_data_chunk`:

  Replace:

  ```rs
  let frame = rlm::encode_data(self.session_id, chunk.index, &chunk.data);
  let fragments = self.fragment_frame(frame);
  for frame in fragments {
      self.send_frame(frame, processors);
  }
  ```

  with:

  ```rs
  let frame = rlm::encode_data(self.session_id, chunk.index, &chunk.data);
  self.send_frame(frame, processors);
  ```

* Same for control frames if you were fragmenting them (e.g. repair or setup frames).

**Receiver (`node/processor.rs`):**

* In the struct:

  * Leave `reliable_fragment_enabled` set to `false` (or remove its initialization), so the defrag path is never taken.

* In `try_deliver_reliable`:

  Replace:

  ```rs
  let payload = packet.payload.as_ref();

  if self.reliable_fragment_enabled {
      match self.defragment_reliable(packet.flow_id, payload) {
          DefragOutcome::Ready(bytes) => { /* pass bytes to RLM */ }
          DefragOutcome::Pending | DefragOutcome::Dropped => return true;
      }
  } else {
      // pass payload directly to RLM
  }
  ```

  with the “else” branch only: always pass `payload` directly to the RLM codec.

At this stage, nothing *uses* `FrameFragmenter` or RLM `FragmentAssembler`; but the code is still present.

---

### 2.3. Delete the RLM fragmentation bits

Once you’ve validated behavior without fragmentation:

**In `dataplane/src/node/reliable/sender.rs`:**

* Remove the entire `FrameFragmenter` struct and its methods.
* Remove `SenderState::fragment_frame` helper.
* Remove any imports of `python_payload_budget` and `build_py_payload_segments`.

**In `dataplane/src/node/processor.rs`:**

* Remove `reliable_fragment_enabled` and `reliable_fragment_assembler` fields.
* Delete `defragment_reliable` and any helper methods for fragment eviction.
* Remove any `FragmentAssemblerConfig` creation guarded by `#[cfg(feature = "reliable")]`.

**In `dataplane/src/node/reliable/session.rs` (or equivalent):**

* Remove `FragmentationConfig` from `CommonConfig`.
* Remove its population from config.
* Remove any remaining references to `common.fragmentation`.

**Shared Python fragment bits:**

* If you already removed the Python fragmentation module (`python/fragment.rs`, `PyPayloadSegHeader`, etc.) in step 1, all those imports should be gone; any leftover references will be caught by the compiler.

---

### 2.4. Enforce sane `chunk_size` (if you want MTU discipline)

If you want RLM chunks to respect the MTU without fragmentation:

* During `SenderConfig`/`CommonConfig` construction, add a clamp:

  ```rs
  let mtu_budget = mtu.saturating_sub(IPV4_HEADER_LEN + TCP_HEADER_LEN + 64); // small safety margin
  let chunk_size = reliable_cfg.default_chunk_size.min(mtu_budget.max(1));

  let common = CommonConfig {
      chunk_size,
      // ... other fields
  };
  ```

* Optionally log a warning if you *had* to clamp:

  > “Reliable chunk size 32768 > MTU budget 1336; clamped to 1336”

That gives you simple, no‑fragmentation behavior while still honoring the MTU conceptually.

---

## 3. Cross‑cutting cleanup: config, docs, tests, rollout

### 3.1. Config & CLI

* Mark all `python_fragmentation_*` fields as deprecated, then remove.
* Remove any RLM `fragmentation` config types (`FragmentationConfig`, `CommonConfig.fragmentation`).
* Update:

  * CLI help.
  * Sample config files.
  * Helm charts / deployment manifests (if they mention these knobs).
  * Any “tuning” docs that talk about fragmentation windows or timeouts.

### 3.2. Docs

* Delete or archive `docs/docs/design/python_payload_fragmentation.md`.

  * If you keep it: add a huge “OBSOLETE – fragmentation removed in version X.Y” header and briefly describe the new behavior.

* Anywhere in the docs that previously promised:

  * “We fragment Python payloads to respect MTU”
  * “RLM uses PyPayloadSegHeader for fragmentation”

  update to:

  * “Large payloads rely on the underlying transport (TCP/QUIC) for segmentation”
  * “RLM chunks are sized to MTU by configuration; no application‑level fragmentation.”

### 3.3. Tests

* Remove unit/property tests for:

  * `PyPayloadSegHeader` encode/decode.
  * `build_py_payload_segments`.
  * `FragmentAssembler` behavior and eviction semantics.
  * RLM fragmentation behavior (e.g., tests that large chunks are split and reassembled).

* Add / update tests that:

  * Send large Python payloads end‑to‑end and assert everything still works with no fragmentation.
  * Use reliable multicast with:

    * Various `chunk_size` values (including ones larger than MTU and verify clamping, if implemented).
    * Random loss + RTT to ensure PGMCC still converges.
  * Regression test around previous fragmentation failure modes:

    * e.g. “send a huge message that used to require multiple fragments; now ensure it is delivered as one logical message and doesn’t cause panics or silent truncation.”

### 3.4. Versioning & rollout

Because this is a protocol‑level change between Python clients and the dataplane binary:

* **Pick a version boundary** where:

  * “Server X.Y + Python client X.Y” expect NO fragmentation.
  * Document that mixing “old clients with fragmentation enabled” and “new dataplane” is unsupported.

* For extra safety, you can:

  * In the new dataplane, if an incoming Python frame starts with `PY_PAYLOAD_SEG_MAGIC` (`0x7079_7367`), log an explicit error:

    > “Received legacy PyPayloadSegHeader; Python fragmentation is no longer supported. Please upgrade the client.”

  * Then drop it (or treat as opaque bytes if you’re comfortable with that).

---

## 4. Quick checklist

**Python API:**

* [ ] Stop using fragmentation at runtime.
* [ ] Delete Rust `python::payload`/`python::fragment` fragmentation code.
* [ ] Delete Python fragmenter/defragmenter.
* [ ] Remove fragmentation metrics/events.
* [ ] Deprecate → remove `python_fragmentation_*` config.

**Reliable dataplane:**

* [ ] Bypass `FrameFragmenter` in sender; delete it.
* [ ] Bypass `reliable_fragment_assembler` in receiver; delete it.
* [ ] Remove `FragmentationConfig` from `CommonConfig`.
* [ ] Decide/enforce `chunk_size` policy vs MTU.

**Global:**

* [ ] Update docs to reflect “no app‑level fragmentation”.
* [ ] Remove/adjust tests.
* [ ] Add regression tests for large payloads and RLM behavior.
* [ ] Roll out with clear versioning + compatibility note.

If you want, I can follow up with a more concrete pseudo‑diff for the Rust bits (e.g. what `Processor::try_deliver_reliable` and `SenderState::send_data_chunk` look like after all this), but structurally this is the full plan.

