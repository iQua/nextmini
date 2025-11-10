## nextmini_py crate guide

This crate builds the `nextmini_py` PyO3 extension that embeds the dataplane. By default it enables the `python-extension` feature so `maturin build --release -m python-api/Cargo.toml` (or `maturin develop --release`) keeps emitting a CPython extension compatible with the docs and tooling in `examples/**` and `tools/**`.

### Running the Rust unit tests

The PyO3 `extension-module` feature asks the Apple linker to leave all Python symbols unresolved until the interpreter loads the cdylib. That is exactly what we want for the wheel, but it breaks `cargo test` because the harness links an executable and therefore **must** link against `libpython`.

To run the tests, disable the default `python-extension` feature and opt into the small `dev-tests` helper feature:

```bash
PYO3_PYTHON=/opt/homebrew/opt/python@3.13/bin/python3.13 \
cargo nextest run --no-default-features --features dev-tests
```

`dev-tests` only enables `pyo3/auto-initialize` so each test case gets a ready-to-use interpreter without manual `Python::with_gil` boilerplate.
