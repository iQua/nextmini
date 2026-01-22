# Docs

Nextmini documentation lives in two places:

- This MkDocs site (`docs/docs/`)
- Code-level docs via rustdoc (`cargo doc`)

## Preview this site locally

```bash
cd docs
uv venv --python 3.13
source .venv/bin/activate
uv pip install -r requirements.txt
mkdocs serve
```

## Generate Rust API docs

Open rustdoc directly:

```bash
cargo doc --workspace --no-deps --open
```

Or generate rustdoc into the MkDocs site:

```bash
bash docs/generate-rustdoc.sh
```

See: [Rust API reference (rustdoc)](../design/rust-api.md).
