---
title: Documentation Migration Notes
description: Migration guide from the old MkDocs documentation layout to the Fumadocs-based docs system.
---

# Nextmini Documentation (MkDocs)

This directory contains the Nextmini documentation built with [MkDocs](https://www.mkdocs.org/) and [Material for MkDocs](https://squidfunk.github.io/mkdocs-material/).

## Installation

To install Material for MkDocs, run:

```bash
uv venv
source .venv/bin/activate
uv pip install mkdocs-material
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
