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

- The `python-api/src` crate exposes `nextmini_py::Dataplane`, `PacketReceiver`, and frozen buffer helpers so Python workloads can inject or tap flows in-process. Follow `docs/content/docs/examples/pytorch_python_api.md` for end-to-end guidance.
- Wheels are compiled with `pyo3`'s `abi3-py313` feature, so ensure CPython 3.13.* is active when running the `maturin build --release -m python-api/Cargo.toml` or `maturin develop --release -m python-api/Cargo.toml` commands listed above.
- Most automation (`examples/pytorch`, `examples/multicast-*`, `tools/monitor`, routing utilities) dynamically import the module and expect a valid node config path plus the `NEXTMINI_CONFIG` / `NEXTMINI_DST_NODE` environment variables noted in the docs; keep those hooks intact when extending the scripts.
- When writing new Python helpers, reuse the buffer adapters in `python-api/src/buffer.rs` and register flows via `Dataplane::register_receiver_*` instead of rolling bespoke socket glue—this keeps behavior aligned with the Rust dataplane.

## Documentation Tooling

- `docs/` hosts the Fuma docs app. Bootstrap tooling in `docs/` (`bun`, `bun run`, and dependencies in `docs/package.json`) before editing.
- Run `cd docs && bun dev` for a live docs preview; run `bun run types:check` before publishing documentation updates.

## Commit & Pull Request Guidelines

- Commits follow short, descriptive titles in sentence-style messages with initial capitals and closing periods (e.g., “Fixed user-space flow finished reporting.”); summarize scope, not the workflow.

## Additional Agent Operating Rules

### Context7

- ALWAYS proactively use Context7 when I need library/API documentation, code generation, setup or configuration steps without me having to explicitly ask.
- External libraries/docs/frameworks should be guided by Context7.

### Planning
- All plans MUST include a dependency graph.
- Every task in a plan must declare `depends_on: []` using explicit task IDs such as `T1`, `T2`.

### Execution
- Complete all tasks from a plan without stopping for permission between steps. Use best judgment, keep moving.
- Only stop to ask when a step is destructive/irreversible or there is a genuine blocker.

### Subagents

- Spawn subagents automatically when:
  - Parallelizable work (e.g., install + verify, npm test + typecheck, unblocked tasks from plan)
  - Long‑running or blocking tasks where a worker can run independently.
  - Isolation for risky changes or checks
  - Code review would be helpful
- If you're launching subagents for parallelization, add this robust context to your prompt:
  - **Context**: Share plan file location and info if available
  - **Dependencies**: What work/files are completed? Any dependencies?
  - **Related tasks**: Any adjacent tasks, files, or agents?
  - **Exact task**: Description, file paths/names, acceptance criteria
  - **Validation**: How to validate work if possible.
  - **Constraints**: Risks, gotchas, things to avoid
  - **Be thorough**: Provide ANY/ALL context that will aid success.
- ALWAYS wait for all subagents to complete before yielding.

### Bugs
- Add a regression test when it is appropriate for bug-related changes.
