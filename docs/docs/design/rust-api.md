# Rust API reference (rustdoc)

Nextmini uses standard Rust doc comments. You can generate an HTML API reference with `cargo doc` and publish it alongside the MkDocs site as static files.

## Generate rustdoc into the documentation site

From the repo root:

```bash
bash docs/generate-rustdoc.sh
```

This runs `cargo doc --workspace --no-deps` and copies the output into:

- `docs/docs/api/rustdoc/`

MkDocs will treat those files as static assets and include them in the built site.

## View the docs

- Directly from `cargo doc` output:
  - `cargo doc --workspace --no-deps --open`

- Local preview:
  - `cd docs && mkdocs serve`
  - open `http://127.0.0.1:8000/api/rustdoc/nextmini/index.html`

## Notes

- The `docs/docs/api/rustdoc/` directory is generated output and should not be committed.
- If you add new public APIs, document them with Rust doc comments so they appear in rustdoc automatically.
