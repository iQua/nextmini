# Docs & API

Nextmini has two kinds of documentation:

- **Guides / concepts**: this MkDocs site (`docs/docs/`)
- **Code-level API**: Rustdoc (`cargo doc`)

## Preview this site locally

```bash
python3 -m venv docs/.venv
source docs/.venv/bin/activate
pip install -r docs/requirements.txt
cd docs
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

## Build the Python extension

See: [Python dataplane API](../design/python-api.md).

