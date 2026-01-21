# Nextmini Documentation (MkDocs)

This directory contains the Nextmini documentation built with [MkDocs](https://www.mkdocs.org/) and [Material for MkDocs](https://squidfunk.github.io/mkdocs-material/).

## Installation

To install Material for MkDocs, run:

```bash
cd docs
uv venv --python 3.13
source .venv/bin/activate
uv pip install -r requirements.txt
```

## Usage

### Development Server

To serve the website for development, run:

```bash
mkdocs serve
```

Then open your browser to `http://127.0.0.1:8000/`

### Build Static Site

To compile it to a static website, run:

```bash
mkdocs build
```

The static website will be available in the `site/` directory.

## Rust API docs (rustdoc)

To publish the Rust API reference alongside the MkDocs site:

```bash
# From the repo root:
bash docs/generate-rustdoc.sh

# Or, from inside the docs directory:
# bash generate-rustdoc.sh
```

This generates `cargo doc` output and copies it into `docs/docs/api/rustdoc/`, which MkDocs will include as static files.
