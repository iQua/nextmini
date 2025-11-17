# Repository Guidelines

## Project Structure & Module Organization

- The workspace root (`Cargo.toml`) ties together the Rust crates in `messages/`, `dataplane/`, `controller/`, and `cert-gen/`; run commands from the root unless you are iterating on a single crate.
- Control-plane logic and database integration live under `controller/src`, while dataplane actors, flow handling, and smoltcp code live in `dataplane/src`.
- Shared message formats sit in `messages/src`, and TLS helper tooling is in `cert-gen/`.
- The `python-api/` crate builds the `nextmini_py` PyO3 extension that embeds the dataplane; Python drivers in `examples/**` and `tools/**` assume the bindings are installed before they run.
- Operational helpers (Terraform plans, routing calculators, monitoring UIs) live under `tools/`; most are Python-based wrappers over the bindings above.
- Supplemental material is kept in `docs/` (design notes) and `examples/` (scenario templates); reference these before introducing new configuration knobs or demos.

## Build, Test, and Development Commands

- `cargo build --workspace` compiles every crate and is the best pre-push smoke check.
- `cargo check` gives the fastest edit/compile loop; prefer it while iterating on dataplane logic.
- `cargo fmt --all` and `cargo clippy --workspace -- -D warnings` enforce formatting and linting; run both before publishing a branch.
- `cargo test --workspace` executes unit and integration tests. The controller tests expect a Postgres instance—boot one locally with `./start-database.sh` before running the suite.
- Use `RUST_LOG=info cargo run -p controller` and `cargo run -p dataplane` to launch the control and data components in dev mode; logs are critical for diagnosing flow-state issues.
- Build the Python bindings with `pip install maturin` followed by `maturin build --release -m python-api/Cargo.toml` (or `maturin develop --release -m python-api/Cargo.toml` when iterating); install the produced `nextmini_py` wheel so Python-side tooling can import it.
- Python projects in `examples/`, `tools/`, and `docs/` pin to CPython 3.13 via `pyproject.toml`; create virtualenvs with `uv venv` and install deps via `uv pip install ...` to stay consistent with the checked-in metadata.

## Coding Style & Naming Conventions

- Follow `rustfmt` defaults (4-space indentation, trailing commas, alphabetical imports); never hand-edit formatting that the formatter will revert.
- Modules, files, and functions use `snake_case`; types and traits use `UpperCamelCase`. Constants stay `SCREAMING_SNAKE_CASE`.
- Add tracing via `tracing::{debug, info, warn, error}` instead of `println!`, and keep messages actionable (flow IDs, node IDs, controller IDs).

## Testing Guidelines

- Co-locate tests in the owning module with `#[cfg(test)] mod tests` and use `#[tokio::test]` for async paths such as controller I/O.
- Name tests after the behavior under test (e.g., `reports_flow_finished_for_user_space`) and mirror the crate path when adding files in `tests/`.
- Keep fast-running unit tests in-tree; integration tests that require Postgres or network setup should document prerequisites in comments or the `docs/` tree.
- The command to run all tests is: `cargo nextest run --no-default-features --features python-extension --features dev-tests`

## Python Bindings & Tooling

- The `python-api/src` crate exposes `nextmini_py::Dataplane`, `PacketReceiver`, and frozen buffer helpers so Python workloads can inject or tap flows in-process. Follow `docs/docs/examples/pytorch_python_api.md` for end-to-end guidance.
- Wheels are compiled with `pyo3`'s `abi3-py313` feature, so ensure CPython 3.13.* is active when running the `maturin build --release -m python-api/Cargo.toml` or `maturin develop --release -m python-api/Cargo.toml` commands listed above.
- Most automation (`examples/pytorch`, `examples/multicast-*`, `tools/monitor`, routing utilities) dynamically import the module and expect a valid node config path plus the `NEXTMINI_CONFIG` / `NEXTMINI_DST_NODE` environment variables noted in the docs; keep those hooks intact when extending the scripts.
- When writing new Python helpers, reuse the buffer adapters in `python-api/src/buffer.rs` and register flows via `Dataplane::register_receiver_*` instead of rolling bespoke socket glue—this keeps behavior aligned with the Rust dataplane.

## Documentation Tooling

- `docs/` hosts the MkDocs site. Bootstrap its environment with `uv venv && source .venv/bin/activate && uv pip install mkdocs-material`.
- Run `mkdocs serve` for a live preview and `mkdocs build` to generate the static `site/` output; commit doc changes alongside the features they cover.

## Commit & Pull Request Guidelines

- Commits follow short, descriptive titles in sentence-style messages with initial capitals and closing periods (e.g., “Fixed user-space flow finished reporting.”); summarize scope, not the workflow.

## MCP Agent Mail — coordination for multi-agent workflows

What it is:

- A mail-like layer that lets coding agents coordinate asynchronously via MCP tools and resources.
- Provides identities, inbox/outbox, searchable threads, and advisory file reservations, with human-auditable artifacts in Git.

Why it's useful:

- Prevents agents from stepping on each other with explicit file reservations (leases) for files/globs.
- Keeps communication out of your token budget by storing messages in a per-project archive.
- Offers quick reads (`resource://inbox/...`, `resource://thread/...`) and macros that bundle common flows.

How to use it effectively:

1) Same repository
   - Register an identity: call `ensure_project`, then `register_agent` using this repo's absolute path as `project_key`.
   - Reserve files before you edit: `file_reservation_paths(project_key, agent_name, ["src/**"], ttl_seconds=3600, exclusive=true)` to signal intent and avoid conflict.
   - Communicate with threads: use `send_message(..., thread_id="FEAT-123")`; check inbox with `fetch_inbox` and acknowledge with `acknowledge_message`.
   - Read fast: `resource://inbox/{Agent}?project=<abs-path>&limit=20` or `resource://thread/{id}?project=<abs-path>&include_bodies=true`.
   - Tip: set `AGENT_NAME` in your environment so the pre-commit guard can block commits that conflict with others' active exclusive file reservations.

2) Across different repos in one project (e.g., Next.js frontend + FastAPI backend)
   - Option A (single project bus): register both sides under the same `project_key` (shared key/path). Keep reservation patterns specific (e.g., `frontend/**` vs `backend/**`).
   - Option B (separate projects): each repo has its own `project_key`; use `macro_contact_handshake` or `request_contact`/`respond_contact` to link agents, then message directly. Keep a shared `thread_id` (e.g., ticket key) across repos for clean summaries/audits.

Macros vs. granular tools:

- Prefer macros when you want speed or are on a smaller model: `macro_start_session`, `macro_prepare_thread`, `macro_file_reservation_cycle`, `macro_contact_handshake`.
- Use granular tools when you need control: `register_agent`, `file_reservation_paths`, `send_message`, `fetch_inbox`, `acknowledge_message`.

Common pitfalls:

- "from_agent not registered": always `register_agent` in the correct `project_key` first.
- "FILE_RESERVATION_CONFLICT": adjust patterns, wait for expiry, or use a non-exclusive reservation when appropriate.
- Auth errors: if JWT+JWKS is enabled, include a bearer token with a `kid` that matches server JWKS; static bearer is used only when JWT is disabled.
