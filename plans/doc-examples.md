# Documentation Plan: Examples Coverage and Migration

## Dependencies First

### Task Dependency Graph

- `T1 -> T2 -> T3 -> T4 -> T5`
- `T5 -> T6, T7, T8, T9, T10, T11`
- `T6, T7, T8, T9, T10, T11 -> T12`
- `T12 -> T13 -> T14 -> T15 -> T16 -> T17 -> T18`

### Task Matrix

| Task ID | Summary | depends_on |
|---|---|---|
| `T1` | Snapshot current docs/examples state and guardrails | `[]` |
| `T2` | Build complete list of top-level `examples/` folders | `[T1]` |
| `T3` | Compute docs coverage gap vs `docs/content/docs/examples/_meta.json` | `[T2]` |
| `T4` | Decide canonical page filenames/slugs for undocumented folders | `[T3]` |
| `T5` | Define shared page structure and writing constraints | `[T4]` |
| `T6` | Draft pages for local single-host examples | `[T5]` |
| `T7` | Draft pages for namespace examples | `[T5]` |
| `T8` | Draft pages for host-network compose examples | `[T5]` |
| `T9` | Draft pages for swarm/multi-node deployment examples | `[T5]` |
| `T10` | Draft pages for cloud deployment examples | `[T5]` |
| `T11` | Draft pages for workload/algorithm examples | `[T5]` |
| `T12` | Update docs navigation/index for all new pages | `[T6, T7, T8, T9, T10, T11]` |
| `T13` | Replace stale references to in-folder markdown files | `[T12]` |
| `T14` | Add cross-links between related example pages where needed | `[T13]` |
| `T15` | Remove migrated markdown docs inside `examples/**` | `[T14]` |
| `T16` | Run consistency checks (coverage + dead references) | `[T15]` |
| `T17` | Run docs validation commands (best effort) | `[T16]` |
| `T18` | Final review and change summary | `[T17]` |

## Objective

Create dedicated, high-level but reproducible documentation pages under `docs/content/docs/examples/` for each currently undocumented top-level example folder, then remove duplicated markdown documentation from the `examples/` tree once coverage is complete.

## Scope

In scope:
- Add new pages for undocumented top-level directories in `examples/`.
- Use source files in each example folder (`docker-compose*`, `*.toml`, scripts, existing README/run notes) as ground truth.
- Update docs navigation metadata.
- Fix references in docs that point to removed in-folder markdown docs.
- Remove migrated markdown documentation from `examples/**`.

Out of scope:
- Runtime code changes in dataplane/controller.
- Behavior changes to example scripts.
- Refactoring docs outside examples/design/python-api unless needed for broken links.

## Detailed Execution Plan

### `T1` Snapshot current state and constraints
`depends_on: []`
`status: completed`

Actions:
- Capture `git status --short` and keep unrelated workspace changes untouched.
- Confirm current docs index files and existing pages.
- Confirm requested style constraints: natural prose first, bullets only when useful, reproducible steps.

Deliverable:
- Baseline inventory to compare before/after.

Work log:
- Captured dirty workspace state and confirmed unrelated docs/design changes are present and must be preserved.
- Confirmed examples docs index and top-level docs index locations.
- Confirmed user style constraints from conversation history for prose-first, reproducible docs.

### `T2` Build top-level examples inventory
`depends_on: [T1]`
`status: completed`

Actions:
- Enumerate immediate children of `examples/`.
- Exclude non-folder docs entry (`examples/README.md`) from folder coverage list.

Deliverable:
- Canonical folder list for coverage check.

Work log:
- Enumerated top-level folders under `examples/`.
- Excluded `examples/README.md` from folder coverage target list.

### `T3` Compute docs coverage gap
`depends_on: [T2]`
`status: completed`

Actions:
- Enumerate existing pages in `docs/content/docs/examples/`.
- Compare with top-level folder list.
- Mark already-covered folders and uncovered folders.

Deliverable:
- Explicit undocumented-folder list.

Work log:
- Compared top-level `examples/` folders with `docs/content/docs/examples/*.md`.
- Confirmed the uncovered folder set for new page creation.

### `T4` Decide canonical slugs and page mapping
`depends_on: [T3]`
`status: completed`

Actions:
- Map each undocumented folder to `docs/content/docs/examples/<slug>.md`.
- Prefer slug parity with folder names (`multi-dc` -> `multi-dc.md`, etc.).
- Capture any deliberate exceptions.

Deliverable:
- Final page filename mapping for writing.

Work log:
- Chose folder-parity slug policy (`<folder>.md`) for all uncovered top-level folders.
- Reserved existing page names where already documented (for example `simple`, `routes`, `namespace`).

### `T5` Define writing template and quality bar
`depends_on: [T4]`
`status: completed`

Actions:
- Standardize page layout:
  - frontmatter (`title`, `description`)
  - high-level architecture intent
  - prerequisites
  - reproducible run sequence
  - verification commands/log expectations
  - cleanup
- Keep prose narrative primary, use bullets only for compact checklists.

Deliverable:
- Reusable authoring template for all pages.

Work log:
- Locked page structure to: intent, prerequisites, reproducible run steps, validation, cleanup.
- Set prose-first style with limited bullet usage for checklists and command blocks for reproducibility.

### `T6` Draft local single-host example pages
`depends_on: [T5]`
`status: completed`

Target folders:
- `simple-flow`
- `simple-max`
- `simple-scheduler`
- `smoltcp-test`
- `splice-test`
- `wget`

Actions:
- Extract commands and expected behavior from compose/config/script files.
- Include customization knobs where scripts support them (for example `nodes.py` in `splice-test`).

Deliverable:
- New docs pages for all local single-host examples.

Work log:
- Added `docs/content/docs/examples/simple-flow.md`.
- Added `docs/content/docs/examples/simple-max.md`.
- Added `docs/content/docs/examples/simple-scheduler.md`.
- Added `docs/content/docs/examples/smoltcp-test.md`.
- Added `docs/content/docs/examples/splice-test.md`.
- Added `docs/content/docs/examples/wget.md`.

### `T7` Draft namespace example pages
`depends_on: [T5]`
`status: completed`

Target folders:
- `ns-flow`
- `ns-public`

Actions:
- Document Linux prerequisites and namespace-specific startup/cleanup steps.
- Preserve generated-config workflow (`generate.py`) and run orchestration (`run.sh`).

Deliverable:
- New docs pages covering local namespace and multi-host namespace setups.

Work log:
- Added `docs/content/docs/examples/ns-flow.md` with `generate.py` + `run.sh` workflow and cleanup guidance.
- Added `docs/content/docs/examples/ns-public.md` with multi-host controller/VM1/VM2 startup and verification steps.

### `T8` Draft host-network compose deployment pages
`depends_on: [T5]`
`status: completed`

Target folders:
- `public-network`
- `sba-compose`

Actions:
- Document per-VM split, host network requirement, and controller address wiring.
- Capture reproducible startup order by compose file.

Deliverable:
- New docs pages for non-swarm multi-VM deployments.

Work log:
- Added `docs/content/docs/examples/public-network.md`.
- Added `docs/content/docs/examples/sba-compose.md`.
- Documented host-network requirement and per-VM startup order.

### `T9` Draft swarm and large-scale deployment pages
`depends_on: [T5]`
`status: completed`

Target folders:
- `simple-swarm`
- `multi-dc`
- `multi-nodes`
- `sba-swarm`
- `swarm-curl`

Actions:
- Describe manager/worker responsibilities and deployment order.
- Capture route insertion step for `swarm-curl`.
- Document generated compose behavior for `multi-nodes`.

Deliverable:
- New docs pages for swarm-scale examples.

Work log:
- Added `docs/content/docs/examples/simple-swarm.md`.
- Added `docs/content/docs/examples/multi-dc.md`.
- Added `docs/content/docs/examples/multi-nodes.md`.
- Added `docs/content/docs/examples/sba-swarm.md`.
- Added `docs/content/docs/examples/swarm-curl.md`.

### `T10` Draft cloud deployment pages
`depends_on: [T5]`
`status: completed`

Target folders:
- `arbutus`
- `flyio`

Actions:
- Convert long in-folder setup notes to concise docs-site runbooks.
- Preserve critical operational prerequisites (network interface, Docker root relocation, Fly app sequencing).

Deliverable:
- New docs pages for cloud-hosted examples.

Work log:
- Added `docs/content/docs/examples/arbutus.md`.
- Added `docs/content/docs/examples/flyio.md`.
- Preserved operational caveats around Arbutus storage/networking and Fly deployment order.

### `T11` Draft workload and algorithm example pages
`depends_on: [T5]`
`status: completed`

Target folders:
- `lp`
- `rl`
- `routing`
- `multicast-docker`

Actions:
- Document runtime flow for LP, RL trainer/worker, routing generation, and multicast verification.
- Keep explanations high-level while making command steps reproducible.

Deliverable:
- New docs pages for workload-oriented examples.

Work log:
- Added `docs/content/docs/examples/lp.md`.
- Added `docs/content/docs/examples/rl.md`.
- Added `docs/content/docs/examples/routing.md`.
- Added `docs/content/docs/examples/multicast-docker.md`.

### `T12` Update examples docs index
`depends_on: [T6, T7, T8, T9, T10, T11]`
`status: completed`

Actions:
- Update `docs/content/docs/examples/_meta.json` with all new pages.
- Ensure ordering is intentional and stable.

Deliverable:
- Navigation includes full examples coverage.

Work log:
- Updated `docs/content/docs/examples/_meta.json` to include all newly documented example pages.
- Removed `pytorch_python_api` from examples navigation because Python API now lives under top-level docs.

### `T13` Replace stale markdown-source references
`depends_on: [T12]`
`status: completed`

Actions:
- Search docs for links/references to `examples/**/README.md`, `run.md`, `ROUTING.md`, `exp.md`.
- Replace with links to canonical docs pages.

Deliverable:
- No docs-site dependency on soon-to-be-removed in-folder markdown files.

Work log:
- Replaced stale `examples/.../README.md` and `examples/arbutus/readme.md` references in existing docs pages.
- Updated Python API links in `pytorch.md` and `pytorch-sba.md` to `/docs/python-api`.

### `T14` Add cross-links between related pages
`depends_on: [T13]`
`status: completed`

Actions:
- Add targeted links between related deployments (for example `multi-dc` and `swarm-curl`, `multicast-docker` and multicast design/testing pages).

Deliverable:
- Better discoverability without duplication.

Work log:
- Added cross-links between:
  - `multi-dc` and `swarm-curl`
  - `multicast-docker` and multicast lifecycle/testing docs
  - `sba-swarm` and SBA workload walkthroughs
  - `routing` and `routes`

### `T15` Remove migrated in-folder markdown docs
`depends_on: [T14]`
`status: completed`

Actions:
- Delete migrated markdown docs under `examples/**` including:
  - `README.md`/`readme.md`
  - `run.md`
  - `ROUTING.md`
  - `exp.md`
- Do not delete non-markdown assets/scripts/configs.

Deliverable:
- Canonical docs now live in `docs/` only.

Work log:
- Deleted migrated markdown docs from `examples/**`, including top-level and nested `README.md`, `run.md`, `ROUTING.md`, and `exp.md` files.

### `T16` Run consistency checks
`depends_on: [T15]`
`status: completed`

Actions:
- Verify every top-level `examples/<folder>` has a corresponding docs page or explicit exclusion note.
- Verify no docs references remain to deleted markdown files.

Deliverable:
- Coverage and reference integrity report.

Work log:
- Verified no top-level example folders remain undocumented.
- Verified no markdown docs remain under `examples/`.
- Verified no docs references remain to removed in-folder markdown files.

### `T17` Run docs validation commands
`depends_on: [T16]`
`status: completed`

Actions:
- Run docs checks available in repo (at minimum `cd docs && bun run types:check` if environment is ready).
- Record failures if toolchain is unavailable.

Deliverable:
- Validation status and any actionable errors.

Work log:
- Ran `cd docs && bun run types:check`.
- Result: pass (`fumadocs-mdx` generation + `tsc --noEmit` succeeded).

### `T18` Final review and handoff
`depends_on: [T17]`
`status: completed`

Actions:
- Summarize created pages, removed files, index updates, and residual risks.
- Provide short next-step options only if needed.

Deliverable:
- Completion report suitable for review/merge.

Work log:
- Prepared final summary of added pages, index updates, link migrations, deletions, and validation outcome.

## Acceptance Criteria

- Every undocumented top-level example folder is documented under `docs/content/docs/examples/`.
- Existing in-folder markdown docs used as source material are removed after migration.
- `docs/content/docs/examples/_meta.json` includes all new pages.
- No docs content points to removed `examples/**` markdown files.
- New pages are high-level in prose and operationally reproducible.

## Risks and Mitigations

- Risk: accidental overlap with unrelated dirty workspace changes.
  - Mitigation: touch only docs/example markdown and metadata files needed by this plan.
- Risk: stale runtime commands in old README content.
  - Mitigation: prioritize compose/scripts/config files over prose as source of truth.
- Risk: docs navigation clutter.
  - Mitigation: use consistent titles/descriptions and intentional `_meta` ordering.

## Estimated Execution Order

1. `T1` through `T5`
2. `T6` through `T11` in parallel writing batches
3. `T12` through `T14`
4. `T15` through `T18`
