# Perfect FEC runtime invariants and evidence

This document is an evidence index for lossless protocol version 10. It does not redefine the
protocol. [Section P of the master plan](../plans/perfect-fec-runtime.md#p-protocol-assumptions-and-state-machines-normative-precedes-all-stages)
remains normative; if this summary and Section P disagree, Section P wins. The wire history and
field layouts live with the implementation in the
[lossless-session version/layout table](../messages/src/lossless_session/mod.rs).

All statements below are conditional on Section P's fair-loss and timeout assumptions and apply
only inside the deployed, manifest-validated envelope:

- RaptorQ uses `1 <= K <= 56,403` and encoding-symbol ids below `2^24`.
- A Carousel+METTLE prefix has at most 65,536 source symbols (`K`/`N`) and checked
  `source_count * symbol_bytes <= 96 MiB`. Prefix count, final-prefix shape, symbol size, and coded
  rate are negotiated in the manifest.
- The dependency-free wire geometry and the codec-specific checks must both accept a manifest.
  Tests outside those caps prove rejection, not codec behavior.
- The configured receiver admission defaults are one 192 MiB reservation per live dense decoder,
  at most four concurrent decoder permits, and sequential prefix-decoder lifetime per session.

The tests cited here establish executable behavior over their test domains. They are not a proof of
all schedules, all platforms, or loss processes outside the normative assumptions.

## L1, L2, and L3 enforcement map

| Invariant | Enforcing test | File | What the assertion pins |
| --- | --- | --- | --- |
| L1 pooling | `eager_decode_is_invariant_to_delivery_order_and_tree_labels` | [`dataplane/tests/fec_carousel_conformance.rs`](../dataplane/tests/fec_carousel_conformance.rs) | The same pooled `(block, ESI, payload)` set produces the same sink bytes and decode metrics after delivery order and tree labels are permuted. |
| L1 pooling | `checkpoint_ages_only_after_reorder_budget_and_classifies_no_loss_as_empty` | [`dataplane/src/node/session/receiver/mettle_carousel.rs`](../dataplane/src/node/session/receiver/mettle_carousel.rs) | A complete METTLE bin set arriving in extreme reverse cross-tree order creates no missing-bin repair demand after the reorder budget. |
| L2 no ownership | `fec_tree_schedule_is_one_slot_per_tree_round_robin` | [`dataplane/src/node/session/sender/fec.rs`](../dataplane/src/node/session/sender/fec.rs) | The unweighted schedule contains one slot for each configured tree, with no per-tree symbol quota. |
| L2 no ownership | `sender_stripes_symbols_across_configured_trees` | [`dataplane/tests/fec_multitree.rs`](../dataplane/tests/fec_multitree.rs) | The integration sender uses every configured tree rather than assigning a block or ESI to one owner. |
| L2 no ownership | `carousel_conformance_preserves_freshness_across_tree_fallback_and_ack_reorder` | [`dataplane/src/node/session/sender/fec.rs`](../dataplane/src/node/session/sender/fec.rs) | When the first tree is full, the same pending fresh symbol can queue on another tree; fallback does not duplicate an ESI or reopen work after a stale acknowledgement. |
| L3 work conservation | `carousel_source_phase_is_block_sequential_and_exactly_k` | [`dataplane/src/node/session/sender/fec.rs`](../dataplane/src/node/session/sender/fec.rs) | The RaptorQ carousel emits each exact source prefix, then selects fresh repair work round-robin from globally incomplete blocks. |
| L3 work conservation | `send_symbol_returns_after_one_all_blocked_tree_sweep` | [`dataplane/src/node/session/sender/fec.rs`](../dataplane/src/node/session/sender/fec.rs) | A submission attempt performs one finite tree sweep and returns `AllWouldBlock`; it does not hide a wait loop from control and timer servicing. |
| L3 work conservation | `final_ack_terminates_sender_while_every_tree_remains_backpressured` | [`dataplane/tests/fec_carousel_conformance.rs`](../dataplane/tests/fec_carousel_conformance.rs) | Backpressure is an enumerated wait state, controls remain serviceable, and the final joined acknowledgement ends work with `queued_after_final_ack_processed == 0`. |
| L3 work conservation | `ack_wins_the_pacing_race_before_the_next_frame_submission` | [`dataplane/tests/fec_carousel_conformance.rs`](../dataplane/tests/fec_carousel_conformance.rs) | Completion is rechecked after pacing and before submission; a final acknowledgement prevents the next queue operation. |
| L3 work conservation | `ack_loss_liveness_accepts_first_ack_heartbeats_and_final_ack` | [`dataplane/tests/fec_carousel_conformance.rs`](../dataplane/tests/fec_carousel_conformance.rs) | While completion is missing, the sender probes and accepts valid unchanged heartbeats without inventing progress, then stops on the final cumulative acknowledgement. |
| L3 work conservation | `sender_esi_observer_enforces_checked_monotone_increments` and `snapshot_exposes_wait_and_receiver_event_boundaries` | [`dataplane/src/node/session/metrics.rs`](../dataplane/src/node/session/metrics.rs) | Fresh ESI sequences and the finite wait-state vocabulary are observable at the event boundaries used by the conformance tests. |

L3 does not promise that already queued frames disappear when a final acknowledgement is generated.
That feedback-latency tail is measured at the receiver as
`symbols_received_after_local_block_complete`. The enforceable sender boundary is that no new frame
is queued after the final acknowledgement has been processed.

## Protocol state-machine index

The following is a navigation summary derived from normative Section P, not a second specification.
Timeout values, validation rules, and edge cases must be taken from
[P1–P9](../plans/perfect-fec-runtime.md#p-protocol-assumptions-and-state-machines-normative-precedes-all-stages).

### Receiver

| State | Operational summary | Transition | Principal CI witnesses |
| --- | --- | --- | --- |
| `Active` | Decode eagerly; advertise cumulative progress after debounce, on heartbeat, and when targeted by `AckProbe`. | Object decoded **and every configured sink accepts it** -> `LocallyComplete`. | `carousel_receiver_debounces_progress_then_sends_heartbeats`, `receiver_ack_timer_is_fair_under_a_continuously_ready_data_inbox`, and `carousel_receiver_eagerly_decodes_acks_and_commits_before_completion` in [`receiver/mod.rs`](../dataplane/src/node/session/receiver/mod.rs) and [`fec_carousel_conformance.rs`](../dataplane/tests/fec_carousel_conformance.rs). |
| `LocallyComplete` (passive) | Replay the final cumulative acknowledgement on heartbeat/probe while the live receiver hands off to the bounded runtime replay cache. | `SessionComplete` or passive-window expiry -> `Finished`. | `dropped_completion_recovers_through_probe_during_passive_handoff`, `completed_carousel_receiver_answers_probe_while_replay_install_is_pending`, `carousel_conformance_full_data_inbox_cannot_deadlock_completion_handoff`, and `completed_carousel_replay_expires_at_configured_deadline` in [`fec_carousel_conformance.rs`](../dataplane/tests/fec_carousel_conformance.rs), [`receiver/mod.rs`](../dataplane/src/node/session/receiver/mod.rs), and [`runtime.rs`](../dataplane/src/node/session/runtime.rs). |
| `Finished` | Receiver task and any live handoff are done; only an unexpired runtime replay entry may answer a targeted probe. | Terminal. | `passive_receiver_finishes_when_session_complete_is_dropped_forever` and `completed_replay_insert_sweeps_expired_entries_in_every_mode` in [`fec_carousel_conformance.rs`](../dataplane/tests/fec_carousel_conformance.rs) and [`runtime.rs`](../dataplane/src/node/session/runtime.rs). |

### Sender

| State | Operational summary | Transition | Principal CI witnesses |
| --- | --- | --- | --- |
| `Sending` | Select fresh payload for a globally incomplete block/stream, perform one tree sweep, or service control, pacing, backpressure, and liveness timers. | No immediately emittable incomplete work while cumulative peer completion is missing -> `Probing`; complete quorum may go directly to `Finished`. | The L3 suite above plus `carousel_requires_every_peer_ack_before_skipping_a_block` in [`sender/fec.rs`](../dataplane/src/node/session/sender/fec.rs). |
| `Probing` | Target `AckProbe` only to frozen peers whose cumulative completion is missing. Payload work may continue when it becomes available. | Joined completion from every frozen peer -> `Finished`; silence/stall deadline -> aborted session. | `frozen_quorum_requires_every_peer_and_targets_only_the_missing_peer`, `empty_carousel_probes_the_missing_peer_before_success`, and `carousel_liveness_distinguishes_silence_from_stall` in [`fec_carousel_conformance.rs`](../dataplane/tests/fec_carousel_conformance.rs), [`fec_carousel_sender.rs`](../dataplane/tests/fec_carousel_sender.rs), and [`sender/state.rs`](../dataplane/src/node/session/sender/state.rs). |
| `Finished` | Best-effort repeated `SessionComplete`, then report `SessionOutcome::Completed`; receiver confirmation of `SessionComplete` is not required. | Terminal. | `carousel_sender_finishes_only_after_cumulative_ack_and_repeats_completion` and `queued_final_ack_beats_control_channel_disconnect` in [`fec_carousel_sender.rs`](../dataplane/tests/fec_carousel_sender.rs) and [`sender/fec.rs`](../dataplane/src/node/session/sender/fec.rs). |

### Cross-cutting P1–P9 contracts

- **P1/P4 control loss, duplication, and reorder:** acknowledgements join monotonically, canonical
  snapshots converge after deterministic wire truncation, and probes/replay recover lost completion
  controls. See `ack_join_is_permutation_duplicate_and_reorder_invariant`,
  `range_scaling_is_wire_bounded_and_eventually_converges`, and
  `dropped_completion_recovers_through_probe_during_passive_handoff` in
  [`fec_carousel_conformance.rs`](../dataplane/tests/fec_carousel_conformance.rs).
- **P2 identity:** nonzero issued session ids are retained for the process lifetime and stale
  incarnations do not mutate a successor. See
  `lossless_session_id_allocator_never_reuses_an_id_for_transfer_reuse` in
  [`controller/src/utils.rs`](../controller/src/utils.rs) and
  `successor_transfers_reject_stale_payload_and_ack_incarnations` in
  [`fec_carousel_conformance.rs`](../dataplane/tests/fec_carousel_conformance.rs). CI checks nonreuse
  and rejection; it does not statistically certify operating-system entropy quality.
- **P3 quorum:** the Ready quorum freezes, every member is required, and an empty frozen quorum is
  trivial success. See `quorum_freeze_preserves_only_pre_freeze_ready_peers` in
  [`sender/state.rs`](../dataplane/src/node/session/sender/state.rs) and
  `empty_frozen_quorum_is_trivial_success_without_payload_emission` in
  [`fec_carousel_conformance.rs`](../dataplane/tests/fec_carousel_conformance.rs).
- **P7 liveness:** `last_ack_seen` and `last_ack_progress` are distinct clocks. Unchanged valid
  heartbeats refresh silence but not progress. See `carousel_liveness_distinguishes_silence_from_stall`,
  `carousel_progress_extends_only_the_progressing_peers_clock`, and
  `healthy_long_transfer_refreshes_both_liveness_clocks` in
  [`sender/state.rs`](../dataplane/src/node/session/sender/state.rs).
- **P8 completion:** sink failure returns the distinct aborted outcome and is never acknowledged as
  completion. See `sink_write_error_aborts_receiver_with_distinct_outcome` and
  `carousel_receiver_eagerly_decodes_acks_and_commits_before_completion` in
  [`receiver/mod.rs`](../dataplane/src/node/session/receiver/mod.rs).
- **P9 versioning:** version 10 layout and mode combinations are validated before dispatch. See
  `manifest_decode_rejects_unknown_or_plain_feedback_mode` in
  [`messages/src/lossless_session/control_frames.rs`](../messages/src/lossless_session/control_frames.rs)
  and `deliver_frame_drops_unsupported_version_before_dispatch` in
  [`runtime.rs`](../dataplane/src/node/session/runtime.rs).

## Measured versus guaranteed claims

Evidence classes used below:

- **A — CI-asserted:** the named test asserts the contract. The claim is still limited to the
  validated envelope and Section P assumptions.
- **B — measured evidence:** a deterministic benchmark or manual release spike recorded the number.
  It is not a CI performance gate or a platform-independent guarantee.
- **C — paper only:** the claim comes from the METTLE paper and has **not** been independently
  verified by this repository.

| Load-bearing claim | Class | Evidence and boundary |
| --- | --- | --- |
| L1 pooled decoding is independent of tree identity. | A | `eager_decode_is_invariant_to_delivery_order_and_tree_labels`; see the L1 map above. |
| L2 has no tree ownership or per-tree quota, and a blocked tree permits fallback. | A | `fec_tree_schedule_is_one_slot_per_tree_round_robin`, `sender_stripes_symbols_across_configured_trees`, and `carousel_conformance_preserves_freshness_across_tree_fallback_and_ack_reorder`. |
| L3 queues no new frame after processing final cumulative completion and exposes only enumerated waits. | A | `final_ack_terminates_sender_while_every_tree_remains_backpressured`, `ack_wins_the_pacing_race_before_the_next_frame_submission`, and the metrics tests listed above. This excludes frames already queued before acknowledgement processing. |
| Block acknowledgements form a monotone join under loss, duplication, reorder, and bounded wire snapshots. | A | `ack_join_is_permutation_duplicate_and_reorder_invariant`, `block_ack_validation_enforces_manifest_bounds_and_canonical_ranges`, `block_ack_truncates_to_lowest_wire_ranges`, and `range_scaling_is_wire_bounded_and_eventually_converges` in the conformance and [`messages`](../messages/src/lossless_session/) tests. |
| Completion handoff remains live when `SessionComplete`, probes, or the first replay handoff race is lost/backpressured. | A | `dropped_completion_recovers_through_probe_during_passive_handoff`, `carousel_conformance_full_data_inbox_cannot_deadlock_completion_handoff`, and `deliver_frame_replays_final_carousel_ack_for_probe`. Permanent control failure still aborts by timeout per P1. |
| Silence and no-progress are separate per-peer failure modes; the frozen quorum cannot silently shrink. | A | `carousel_liveness_distinguishes_silence_from_stall`, `carousel_progress_extends_only_the_progressing_peers_clock`, `carousel_sender_aborts_when_frozen_peer_is_silent`, and `carousel_sender_aborts_live_peer_without_ack_progress` in [`sender/state.rs`](../dataplane/src/node/session/sender/state.rs) and [`sender/mod.rs`](../dataplane/src/node/session/sender/mod.rs). |
| Completion means decoded data was accepted by every configured sink; a sink error aborts distinctly. | A | `carousel_receiver_eagerly_decodes_acks_and_commits_before_completion` and `sink_write_error_aborts_receiver_with_distinct_outcome`. |
| Peer-controlled wire geometry, symbol ids, versions, modes, and METTLE prefix geometry are rejected outside checked bounds. | A | `symbol_payload_boundaries_accept_min_and_max_and_reject_max_plus_one`, `raptorq_geometry_accepts_boundary_k_and_rejects_outside_it`, `raptorq_esi_boundary_reaches_codec_only_when_representable`, `mettle_stream_geometry_enforces_source_and_payload_caps`, and `deliver_frame_drops_unsupported_version_before_dispatch` in [`messages`](../messages/src/lossless_session/), [`fec.rs`](../dataplane/src/node/session/fec.rs), and [`runtime.rs`](../dataplane/src/node/session/runtime.rs). |
| `Rounds + METTLE` remains the finite-block adaptation; paper-native object/prefix streaming is selected only by `Carousel + METTLE`. | A | `sender_policy_defaults_to_rounds_feedback`, `sender_policy_negotiates_mettle_carousel_object_stream`, `mettle_carousel_requires_exact_checked_object_stream_geometry`, plus the frozen `mettle_lossless_session_completes_full_terminated_zero_overhead_stream` fixture in [`fec_policy.rs`](../dataplane/src/node/session/fec_policy.rs), [`validation.rs`](../messages/src/lossless_session/validation.rs), and [`fec_mettle_session.rs`](../dataplane/tests/fec_mettle_session.rs). |
| Object-stream source/offset/sink mapping is exact across empty, partial, and multi-prefix objects, with no padding leakage. | A | `object_symbol_plan_validates_empty_exact_and_partial_objects`, `object_symbol_mapping_roundtrips_across_prefix_boundaries`, and `mettle_carousel_writes_global_sources_across_prefix_boundaries` in [`plan.rs`](../dataplane/src/node/session/plan.rs) and [`receiver/mod.rs`](../dataplane/src/node/session/receiver/mod.rs). |
| Receiver decoder admission enforces the configured logical reservation, four default permits, fifth-session rejection, and sequential prefix lifetime. | A | `mettle_decoder_budget_defaults_and_fifth_permit_rejection_are_checked`, `fifth_concurrent_dense_decoder_is_rejected_at_permit_exhaustion`, `decoder_admission_charge_tracks_negotiated_coded_rate`, and the multi-prefix mapping tests. These assert admission policy, not that 192 MiB is sufficient on every host. |
| METTLE repair checkpoints do not overtake queued epoch payload; reorder within budget causes no retransmission; known near/far loss recovers by targeted ranges; checkpoint loss is retried. | A | `mettle_checkpoint_cannot_overtake_backpressured_epoch_payload`, `checkpoint_ages_only_after_reorder_budget_and_classifies_no_loss_as_empty`, `known_near_and_far_losses_recover_from_only_epoch_targeted_bins`, and `dropped_first_checkpoint_recovers_on_cadence_retransmission` in [`sender/fec.rs`](../dataplane/src/node/session/sender/fec.rs), [`receiver/mettle_carousel.rs`](../dataplane/src/node/session/receiver/mettle_carousel.rs), and [`fec_mettle_carousel_recovery.rs`](../dataplane/tests/fec_mettle_carousel_recovery.rs). |
| Finite METTLE overhead means `terminal_symbol_count / K - 1`, including the compressed tail; unattainable small-`K` targets are rejected. | A | `table_iv_targets_are_solved_against_actual_finite_transmission_counts`, `object_stream_sweep_avoids_repeated_k256_termination_tails`, and `small_prefix_target_below_tail_floor_is_explicitly_rejected` in [`mettle/tests/paper_coding_efficiency.rs`](../mettle/tests/paper_coding_efficiency.rs). |
| A 4,096-trial zero-failure run can only establish a one-sided 95% upper bound of `0.000731113` (about `7.3e-4`). | A | `table_iv_default_trials_can_support_the_stated_failure_probability` checks the exact bound. This is resolution arithmetic, not independent evidence that the codec has that failure probability. |
| In the matched deterministic trace model, Carousel emits about 2% more symbols and completes in 6–10% fewer logical ticks than Rounds. | B | [`docs/perfect-runtime-benchmark-evidence.md`](perfect-runtime-benchmark-evidence.md) and [`plans/stage1-rounds-carousel-benchmark-evidence.md`](../plans/stage1-rounds-carousel-benchmark-evidence.md). Exact seed reproducibility is asserted by `rounds_vs_carousel_matched_seed_trace_benchmark_evidence`; relative performance is evidence, not a CI gate. |
| Dense decoder construction and prefix-stall memory fit the selected 25 ms / 192 MiB manual budgets on the measured host. | B | Gate 3 remeasurement: 9.991 ms maximum construction and 116.109 MiB maximum RSS in [`plans/stage3-sim-report.md`](../plans/stage3-sim-report.md). The mandatory remeasurement policy and why it is not a CI threshold are recorded under [Automated performance thresholds](../plans/perfect-fec-runtime-questions.md#automated-performance-thresholds-review-item-9). |
| The fixed reservoir extension is unsuitable for integration under the tested burst traces. | B | At 11.0107% actual total overhead, 4,096-trial completion was 91.040% for short GE bursts and 64.868% for long GE bursts. See [`plans/stage3-sim-report.md`](../plans/stage3-sim-report.md) and the accepted ruling in [`plans/stage3-review-claude.md`](../plans/stage3-review-claude.md). Reservoir wire behavior was not integrated. |
| The paper's non-systematic four-edge, time-coupled construction and its qualitative TLE/tail semantics are represented by this implementation. | A for implementation shape; C for paper validity | `mettle_defaults_match_paper_profile`, `tle_bin_id_matches_floor_of_one_plus_c_times_source_id`, `non_tle_trials_follow_the_paper_constant_width`, and tail-compression tests in [`mettle/src/params.rs`](../mettle/src/params.rs) pin our translation. They do not independently validate the paper's analysis or experiments. |
| METTLE effectiveness, latency distribution, and very small decoding-failure probabilities reported by the paper generalize to this deployment. | C | **Not independently verified.** In particular, any paper claim below the 4,096-trial resolution of about `7.3e-4` is unverified here. The deployed implementation also terminates at negotiated prefixes no larger than 65,536 sources, so claims for much larger streams are not established by our tests. |

## Explicit non-claims and open bounds

- The current branch does not guarantee a process-wide Carousel+METTLE **sender** cache bound.
  Operators must bound concurrent senders using per-session encoded-prefix retention; see
  [Process-wide sender-cache admission](../plans/perfect-fec-runtime-questions.md#process-wide-sender-cache-admission-review-item-7).
- The 192 MiB receiver reservation and 25 ms construction threshold are manually measured policy
  budgets, not cross-platform CI guarantees.
- RaptorQ is not claimed to decode at exactly `K`; backend overhead is measured on deterministic
  fixtures. Receiver completion-tail symbols are expected under feedback latency.
- Reservoir repair remains a rejected extension **BEYOND the METTLE paper**. Stage 3.2 added no wire
  or manifest fields.
- Multi-pass reseeding is outside this branch and lives in
  [`plans/rateless-mettle-vnext.md`](../plans/rateless-mettle-vnext.md).
