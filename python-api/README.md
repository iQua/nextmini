## nextmini_py crate guide

This crate builds the `nextmini_py` PyO3 extension that embeds the dataplane. By default it enables the `python-extension` feature so `maturin build --release -m python-api/Cargo.toml` (or `maturin develop --release`) keeps emitting a CPython extension compatible with the docs and tooling in `examples/**` and `tools/**`.

### Running the Rust unit tests

PyO3 0.28 deprecates the old `extension-module` Cargo feature. The supported path is to link `libpython` by default so binaries and tests work, then let `maturin >= 1.9.4` set `PYO3_BUILD_EXTENSION_MODULE` when producing the wheel.

That means plain workspace test commands now work:

```bash
cargo nextest run
```
