# Repository Guidelines

## Project Structure & Module Organization

- The workspace root (`Cargo.toml`) ties together the Rust crates in `messages/`, `dataplane/`, `controller/`, and `cert-gen/`; run commands from the root unless you are iterating on a single crate.
- Control-plane logic and database integration live under `controller/src`, while dataplane actors, flow handling, and smoltcp code live in `dataplane/src`.
- Shared message formats sit in `messages/src`, and TLS helper tooling is in `cert-gen/`.
- Supplemental material is kept in `docs/` (design notes) and `examples/` (scenario templates); reference these before introducing new configuration knobs or demos.

## Build, Test, and Development Commands

- `cargo build --workspace` compiles every crate and is the best pre-push smoke check.
- `cargo check` gives the fastest edit/compile loop; prefer it while iterating on dataplane logic.
- `cargo fmt --all` and `cargo clippy --workspace -- -D warnings` enforce formatting and linting; run both before publishing a branch.
- `cargo test --workspace` executes unit and integration tests. The controller tests expect a Postgres instance—boot one locally with `./start-database.sh` before running the suite.
- Use `RUST_LOG=info cargo run -p controller` and `cargo run -p dataplane` to launch the control and data components in dev mode; logs are critical for diagnosing flow-state issues.

## Coding Style & Naming Conventions

- Follow `rustfmt` defaults (4-space indentation, trailing commas, alphabetical imports); never hand-edit formatting that the formatter will revert.
- Modules, files, and functions use `snake_case`; types and traits use `UpperCamelCase`. Constants stay `SCREAMING_SNAKE_CASE`.
- Add tracing via `tracing::{debug, info, warn, error}` instead of `println!`, and keep messages actionable (flow IDs, node IDs, controller IDs).

## Testing Guidelines

- Co-locate tests in the owning module with `#[cfg(test)] mod tests` and use `#[tokio::test]` for async paths such as controller I/O.
- Name tests after the behavior under test (e.g., `reports_flow_finished_for_user_space`) and mirror the crate path when adding files in `tests/`.
- Keep fast-running unit tests in-tree; integration tests that require Postgres or network setup should document prerequisites in comments or the `docs/` tree.

## Commit & Pull Request Guidelines

- Commits follow short, descriptive titles in sentence-style messages with initial capitals and closing periods (e.g., “Fixed user-space flow finished reporting.”); summarize scope, not the workflow.
- Each pull request should link to any relevant issue, describe functional impact, and call out verification steps (`cargo test`, manual flow replay, etc.).

## MCP Agent Mail — coordination for multi-agent workflows

What it is
- A mail-like layer that lets coding agents coordinate asynchronously via MCP tools and resources.
- Provides identities, inbox/outbox, searchable threads, and advisory file reservations, with human-auditable artifacts in Git.

Why it's useful
- Prevents agents from stepping on each other with explicit file reservations (leases) for files/globs.
- Keeps communication out of your token budget by storing messages in a per-project archive.
- Offers quick reads (`resource://inbox/...`, `resource://thread/...`) and macros that bundle common flows.

How to use effectively
1) Same repository
   - Register an identity: call `ensure_project`, then `register_agent` using this repo's absolute path as `project_key`.
   - Reserve files before you edit: `file_reservation_paths(project_key, agent_name, ["src/**"], ttl_seconds=3600, exclusive=true)` to signal intent and avoid conflict.
   - Communicate with threads: use `send_message(..., thread_id="FEAT-123")`; check inbox with `fetch_inbox` and acknowledge with `acknowledge_message`.
   - Read fast: `resource://inbox/{Agent}?project=<abs-path>&limit=20` or `resource://thread/{id}?project=<abs-path>&include_bodies=true`.
   - Tip: set `AGENT_NAME` in your environment so the pre-commit guard can block commits that conflict with others' active exclusive file reservations.

2) Across different repos in one project (e.g., Next.js frontend + FastAPI backend)
   - Option A (single project bus): register both sides under the same `project_key` (shared key/path). Keep reservation patterns specific (e.g., `frontend/**` vs `backend/**`).
   - Option B (separate projects): each repo has its own `project_key`; use `macro_contact_handshake` or `request_contact`/`respond_contact` to link agents, then message directly. Keep a shared `thread_id` (e.g., ticket key) across repos for clean summaries/audits.

Macros vs granular tools
- Prefer macros when you want speed or are on a smaller model: `macro_start_session`, `macro_prepare_thread`, `macro_file_reservation_cycle`, `macro_contact_handshake`.
- Use granular tools when you need control: `register_agent`, `file_reservation_paths`, `send_message`, `fetch_inbox`, `acknowledge_message`.

Common pitfalls
- "from_agent not registered": always `register_agent` in the correct `project_key` first.
- "FILE_RESERVATION_CONFLICT": adjust patterns, wait for expiry, or use a non-exclusive reservation when appropriate.
- Auth errors: if JWT+JWKS is enabled, include a bearer token with a `kid` that matches server JWKS; static bearer is used only when JWT is disabled.
