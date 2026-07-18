# WR failed-cell triage and performance-work proposal

Date: 2026-07-18

Branch: `perfect-fec-runtime`

Scope: WR harness resilience, one failed-cell causal replay, and a proposal for later simulator
optimization. No WR sweep was rerun, no Task 3 optimization was implemented, and no production
code was changed.

Evidence class: **model-level evidence from wansim's ideal-DoF abstraction, not a WAN
measurement**.

## Executive verdict

The failed cell is case **(c), a simulator artifact**.

The slow receiver's rank really did stop: it was 6,967 at both ends of the fatal 15-second
window, while its cumulative BlockAck watermark and the sender's joined watermark both remained
409. However, this was not evidence that the receiver had reached an irrecoverable WAN stall.
The WR source had already emitted exactly 131,072 frames: 65,536 on each tree, precisely the
harness's silent `K * 8` per-tree ceiling. It stopped emitting at 4.658267507 s even though the
session was incomplete. The last frame reached the slow receiver at 8.825576173 s, rank drained to
6,967 by 8.953643174 s, and then no innovative delivery was possible. The sender correctly fired
its 15-second no-joined-progress clock at 23.970600001 s, but the no-progress condition had been
manufactured by the simulator ceiling.

This result does **not** establish a production liveness-granularity bug and does **not** justify
changing section P. It invalidates this WR cell as performance evidence. The simulator must first
remove the silent emission ceiling (or turn any explicit guard into an immediate, named harness
failure) before the WR sweep can be trusted.

## Task 1: resilient per-cell WR execution

Commit `5db0a49` (`Persist WR cells durably and resume runs.`) changed the WR runner from one
all-or-nothing write at the end of 3,040 tasks to a per-cell durable store:

- each cell writes into a same-filesystem temporary shard, synchronizes its files and directory,
  atomically renames the shard, and synchronizes the parent directory;
- success and failure are both terminal persisted states, so a bad cell cannot erase prior work;
- `--resume` validates the run manifest, skips every complete success or failure shard, removes
  torn temporary shards, and schedules only missing cells;
- final aggregate CSVs are rebuilt in canonical task order from validated shards and are also
  replaced atomically;
- incompatible seed/task/schema manifests fail closed rather than mixing runs.

The gate
`resume_after_kill_preserves_durable_cells_and_discards_torn_shards` starts a real child test,
waits until one shard is durable and a second is deliberately torn, sends `SIGKILL`, and proves
that resume retains the first cell, discards the temporary shard, and finishes exactly the missing
cell. `failed_cell_is_durable_without_replacing_prior_success` pins failure persistence.

At the Task 1 commit, the Boston wansim gate was green: formatting and clippy with warnings denied
passed; nextest reported 109 passed and 1 ignored manual probe.

## Task 2: exact replay definition

Only this requested cell was replayed:

| Axis | Value |
|---|---|
| profile / placement | digitalocean-like / west-origin |
| offered background load | 70% |
| temporal jitter | enabled |
| protocol / admission | carousel / hybrid-drop |
| K / seed | 8,192 / 15 |
| slow receiver | receiver 0, 125x decoder/sink service time |
| BlockAck cadence | production 1x (`c2` in the internal half-step encoding) |
| source pacer | disabled |
| codec | ideal distinct-DoF bucket |
| worker count | one nexosim worker |

The replay retained a canonical causal trace for the source, the slow receiver, BlockAck joins,
and both liveness clocks. It also emitted 250 ms timeline buckets and counted every instrumented
event class. The raw retained trace was deterministically compressed with `gzip -n`; all committed
artifacts are covered by [`SHA256SUMS`](../results/wansim/wr-triage/SHA256SUMS):

- [`summary.csv`](../results/wansim/wr-triage/summary.csv)
- [`timeline-250ms.csv`](../results/wansim/wr-triage/timeline-250ms.csv)
- [`fatal-window-events.csv`](../results/wansim/wr-triage/fatal-window-events.csv)
- [`causal-events.csv.gz`](../results/wansim/wr-triage/causal-events.csv.gz)
- [`event-class-counts.csv`](../results/wansim/wr-triage/event-class-counts.csv)

The one release replay took 25:19.95 wall time, 1,549.62 user CPU seconds, 0.85 system CPU
seconds, and 1,064,940 KiB peak RSS on `xindan@boston.csl.toronto.edu`. No repeat replay was run.
The high cost is itself profiled below; the diagnostic recorder materially slowed the run and its
timing is not a simulator-throughput benchmark.

The first mechanical post-run classifier saw zero rank motion and labeled the surface symptom
`(b)`. Inspection of the complete trace then found the exact two-tree emission ceiling. The final
classifier therefore gives a reached emission guard causal precedence and labels the committed
summary `(c)`. The summary and fatal-window views were derived from the same single trace; the
cell was not rerun to obtain the corrected label.

## Causal timeline

The production-shaped geometry used by this revision has 482 progress units, about 16.996 source
symbols per unit. The final incomplete watermark would be 481. The receiver stopped at watermark
409 with 1,225 DoF still missing, 72 watermark positions short of the final one. Therefore this
was not the proposed "last approximately 17 DoF take longer than 15 seconds" quantization case.

| Simulated time | Causal event | State after event |
|---:|---|---|
| 1.000000001 s | first source frame emitted | carousel begins continuous departure |
| 4.658267507 s | last source frame emitted | exactly 131,072 total; both trees exactly 65,536 |
| 8.825262433 s | last slow-receiver inbox acceptance | rank work still draining |
| 8.825576173 s | last slow-receiver frame arrival | no more data exists behind the transport |
| 8.825581173 s | last slow-receiver application drop | hybrid drop remains after TCP delivery |
| 8.953643174 s | last decoder completion/rank observation | rank 6,967; watermark 409 |
| 8.970563230 s | last strict sender BlockAck join | sender watermark 409; fatal window starts |
| 23.687947474 s | last BlockAck joined before abort | silence age at abort only 282.653 ms |
| 23.967738962 s | receiver submits another watermark-409 BlockAck | reverse path remains active |
| 23.970600001 s | sender no-progress abort | progress age 15.000036771 s |

Across the entire replay, the slow receiver saw all 131,072 emitted frames. It admitted 6,967
(5.3154%), dropped 124,105 (94.6846%) after transport delivery, and completed 6,967 decoder
services. The source emitted 65,536 frames on each tree. This exact equality is what identifies
the guard, rather than an organic congestion equilibrium.

During the inclusive fatal window `[8.970563230 s, 23.970600001 s]`:

- rank advance was 0; receiver and sender watermarks were 409 throughout;
- there were no data arrivals, inbox accepts, drops, or decoder completions;
- the receiver emitted 92 BlockAcks;
- the sender processed 93 joins for the slow peer: one strict-progress join exactly at the left
  boundary and 92 subsequent join no-ops;
- the silence clock stayed healthy, while only the no-progress clock exhausted its budget.

Thus the alternatives resolve as follows:

| Candidate | Observation | Verdict |
|---|---|---|
| (a) rank advanced behind a final-block watermark | rank did not advance; watermark 409 was not the final 481 | rejected |
| (b) no innovative delivery occurred for 15 s | true as a surface symptom | insufficient causal diagnosis |
| (c) ACK path, sender join, or model artifact | ACKs and joins continued; the source had silently exhausted its artificial stream | **accepted: model artifact** |

The BlockAck join-semilattice and both liveness clocks behaved consistently with the independent
wansim implementation of the [normative section-P state machine](perfect-fec-runtime.md#p-protocol-assumptions-and-state-machines-normative-precedes-all-stages).
The defect is below that state machine in the finite test-stream geometry.

## Emission-tail semantic check

The previously observed ratio `94,714 / 8,192 = 11.5618x` is compatible with the **unpaced
work-conserving carousel model**, but it must not be described as a normal ACK-flight tail.

With a 125x slow service center and hybrid drop, TCP can successfully deliver a frame while the
bounded application inbox discards it. Such a frame consumes wire capacity but contributes no
rank. Section-P work conservation tells the source to keep sending fresh repair while any frozen
peer remains incomplete. With no pacer, the only immediate brakes are socket/channel
backpressure and completion feedback. Consequently an intermediate 11.6x emission count is
model-correct for this overload regime: it contains replacement for application drops, queued
work, and feedback flight, not merely the final RTT of packets.

The causal replay ultimately reached `131,072 / 8,192 = 16x`, the exact harness ceiling. That
terminal 16x value is **not** model-correct carousel completion behavior. The cited 94,714 count is
below the ceiling and can be a valid intermediate observation, but it cannot rescue the failed
cell or be treated as its terminal overhead.

Production has an optional token-bucket pacer; the WR cell explicitly has none. When configured,
production paces each carousel symbol while still servicing control frames and timers and
rechecking completion. A suitable rate and burst would reduce inbox overload, application drops,
and redundant queued repair. It would bound the immediate burst, while remaining overshoot would
depend on the pacing rate, feedback delay, and bytes already accepted by processor/tree/socket
queues. Pacing would not change BlockAck granularity, cumulative-join semantics, or the two
liveness clocks. Production also has no `K * 8` per-tree stop: it continues until completion,
liveness abort, or the codec's checked symbol-ID namespace is actually exhausted.

No production pacing or liveness change is warranted from this replay. In particular,
rank-carrying heartbeats, finer progress credit, and tail-scaled stall budgets remain candidate
responses only if a future cap-free replay actually demonstrates case (a).

## Event-class profiling baseline

The counter observed 64 classes and 118,137,607 class occurrences. Classes overlap—a backbone
service can increment a dispatch class and a service-state class—so their sum is not a count of
unique nexosim events. It is nevertheless an exact hotspot baseline for this seed.

| Event class | Count |
|---|---:|
| backbone timestamp resolution dispatch | 27,367,271 |
| backbone resource service started | 14,301,896 |
| backbone resource service finished | 14,301,894 |
| backbone service-deadline dispatch | 14,301,894 |
| backbone propagation-complete dispatch | 14,301,862 |
| backbone packet receive dispatch | 5,479,185 |
| backbone packet exit | 5,470,211 |
| relay child ACK dispatch | 2,584,161 |
| child admission blocked | 2,279,372 |
| child segment emitted | 2,069,537 |
| upstream ACK emitted | 2,025,300 |
| relay upstream-segment dispatch | 1,773,091 |
| relay upstream socket read | 1,610,731 |
| child socket write | 1,390,131 |
| source drive dispatch | 691,025 |
| receiver timer dispatch | 690,000 |
| receiver data-segment dispatch | 625,573 |
| child queue admission | 524,288 |
| source data-ACK dispatch | 459,934 |
| runtime-command dispatch / enqueue | 393,260 / 393,216 |
| relay timer dispatch | 273,412 |
| assembled logical frames | 262,144 |
| source timer dispatch | 230,000 |
| source logical frames emitted | 131,072 |
| background network-packet dispatch | 24,237 |

The retained causal trace has 421,184 records, but the diagnostic counter performed a locked
`BTreeMap` update on every one of the 118 million occurrences. That design intentionally favored
one-run observability over speed and explains much of the 25-minute replay. It must not be used in
the full sweep.

More importantly, the hotspot distribution corrects the initial optimization hypothesis.
Background endpoint packets are not the dominant event class. The foreground's artificial 16x
departure is multiplied through two trees, relay fan-out, per-hop TCP segments and ACKs, and the
regional resource scheduler. The first priorities are therefore fixing the emission semantics,
removing recorder lock/map work, and reducing backbone scheduler wakeups. Blindly replacing
explicit background TCP with a fluid aggregate would risk changing the very coupling WR is meant
to measure while attacking a secondary count.

## Task 3 proposal only: faster WR without changing outcomes

Nothing in this section has been implemented. A full rerun remains blocked on review.

### 0. Correctness prerequisite: remove the silent departure ceiling

`FramedStream` already generates payload bytes lazily from a counter PRF; the `K * 8` count is not
backing payload storage. Separate deterministic byte generation from protocol termination:

1. make the carousel stream logically extend over the checked frame-ID/stream-offset namespace;
2. remove `maximum_frames_per_tree` from the source's writable-lane predicate;
3. retain a configurable diagnostic guard only if reaching it records an immediate named
   `emission_guard_exhausted` failure—never an empty writable set that masquerades as network
   silence;
4. keep completion rechecks, liveness clocks, and all section-P control behavior unchanged.

This is a simulator correctness repair, not a throughput optimization. Before another WR result
run, add a focused regression in which hybrid drops require more than 8K emissions per tree and
the sender either completes or fails loudly, never stalls because the synthetic stream ended.

### 1. Replace diagnostic counting with fixed, local counters

- Assign stable enum indices to event classes.
- Keep fixed `u64` arrays in the owning model/actor and merge once after the simulation. With one
  nexosim worker there is no reason to lock a shared ordered map per event.
- Record per-second rank/ACK/liveness aggregates online instead of reconstructing them from every
  retained event.
- Keep the dynamic full-event recorder as an opt-in forensic mode only.

Expected semantic effect: none. Only observation storage changes. This should remove up to 118
million lock plus tree-lookup operations in the failing-cell diagnostic path.

### 2. Use bounded triggered causal timelines

Maintain exact lifetime summary counters plus a bounded canonical ring covering the most recent
20 simulated seconds. On failure, flush the ring and the summary; on success, discard it unless
full tracing was requested. The 20-second window would include this cell's 4.658 s ceiling event
when the abort fires at 23.971 s. Make the horizon a recorded configuration value, and set an
explicit `history_truncated` flag if the triggering cause predates it.

Expected semantic effect: none. This bounds retained memory and CSV construction without dropping
the state needed for ordinary liveness triage.

### 3. Collapse redundant backbone wakeups, not packet semantics

The highest count is the regional backbone's same-timestamp resolution layer. Replace one
mailbox-scheduled resolution per arrival/finish timestamp with one actor-owned ordered agenda and
at most one nexosim wakeup for its current minimum. At a timestamp, preserve the existing local
rule exactly: sorted service finishes first, then sorted arrivals, then start-next. Preserve every
packet's integer serialization finish, propagation finish, finite-queue admission, flow identity,
and TCP-visible boundary event.

A later optimization may batch adjacent equal-size serialization calculations inside one resource
only when no intervening arrival, finish, queue decision, or control packet can observe the
interior. It is not allowed to turn TCP segments into a fluid byte rate.

Expected semantic effect: none at actor boundaries; only redundant engine messages disappear.

### 4. Aggregate background actors only if it remains exact—and only after profiling

The provisionally suggested background aggregation should first mean **actor aggregation**, not
flow aggregation: one workload model may own an array of the same per-flow application and TCP
states, but it must emit the same individually identified packets at the same timestamps. That can
remove actor/mailbox overhead without changing Reno competition or physical flow count.

Replacing several TCP flows with one fluid or super-flow changes congestion windows, ACK clocks,
queue burstiness, and the emergent A4 coupling measurement. Such a model cannot satisfy exact
semantics preservation and must be introduced, if at all, as a separately labeled sensitivity
abstraction—not as the WR evidence engine. Given only 24,237 background endpoint-packet
dispatches in this replay, this work is deferred unless an A/B profile shows at least a material
wall-time benefit after items 0–3.

### Semantics-preservation proof strategy

Implement optimizations behind a reference/optimized switch and compare a matched deterministic
subset before deleting the reference path.

For recorder and bounded-history changes, require byte-identical ordered digests of all external
protocol/model observables:

```text
(time_ns, component, event, flow_id, sequence, bytes, value)
```

The digest set must include logical frame emission/delivery, post-TCP inbox accept/drop, decoder
rank, every BlockAck/AckProbe/SessionComplete submission and receipt, join results, both liveness
clocks, physical queue drops, link exits, and local/sender completion or abort. Final summary rows,
failure text, failure timestamp, mailbox high-water marks, and ownership/conservation counters
must also be identical.

For the backbone agenda optimization, internal dispatch counters are allowed to shrink, but the
ordered boundary trace above and every per-resource queue admission/drop/service timestamp must
remain byte-identical. The comparison should run both registration orders and at least these
matched cells:

- W0a plateau, loss recovery, and ACK-at-deadline tie;
- W0b sequential/concurrent fan-out and drop-after-transport ordering;
- W1 strict conservation and rounds/carousel overtaking;
- W2 hybrid and blocking straggler cells;
- W3 edge-disjoint, full-overlap, and eight-receiver incast cells;
- one healthy WR cell and this exact digitalocean-like failing cell after the emission fix.

Run K=64/512 screening first and K=8,192 for the two WR cells. A mismatch is a correctness failure,
not a tolerance question. Separately measure release wall time, user CPU, peak RSS, engine dispatch
counts, and retained-record counts. Only after exact outcome equivalence should a resumed WR sweep
be considered. A fluid/background super-flow, if ever evaluated, cannot use this proof claim and
must report sensitivity deltas against the exact engine.

## Reproduction and gates

All cargo commands and the replay ran on Boston; the workstation was used only for editing and
artifact synchronization.

```bash
ssh xindan@boston.csl.toronto.edu
cd /home/xindan/wansim-wr-ed31c2a
source "$HOME/.cargo/env"

CARGO_INCREMENTAL=0 PYO3_PYTHON=/opt/homebrew/bin/python3.13 \
  cargo fmt --all -- --check
CARGO_INCREMENTAL=0 PYO3_PYTHON=/opt/homebrew/bin/python3.13 \
  cargo clippy --all-targets -- -D warnings
CARGO_INCREMENTAL=0 PYO3_PYTHON=/opt/homebrew/bin/python3.13 \
  cargo nextest run

# The only causal replay performed:
/usr/bin/time -v env CARGO_INCREMENTAL=0 \
  PYO3_PYTHON=/opt/homebrew/bin/python3.13 \
  cargo run --release --bin wr_triage -- results/wansim/wr-triage-task2

cd results/wansim/wr-triage-task2
gzip -n causal-events.csv
sha256sum causal-events.csv.gz fatal-window-events.csv \
  timeline-250ms.csv event-class-counts.csv summary.csv > SHA256SUMS
```

Final Task 2 wansim gate:

| Gate | Result |
|---|---|
| `cargo fmt --all -- --check` | pass |
| `cargo clippy --all-targets -- -D warnings` | pass |
| `cargo nextest run` | 111 passed, 1 ignored manual probe |
| committed artifact digests | all five pass `shasum -a 256 -c SHA256SUMS` |

The root workspace excludes `wansim`; it was not rebuilt because this task changed only the
standalone simulator and plans/results. No full WR sweep, optimization implementation, W4 work,
or production change was started.
