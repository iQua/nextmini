#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

echo "Building rustdoc (workspace, no deps)..."
cargo doc --workspace --no-deps

out_dir="docs/docs/api/rustdoc"
echo "Copying rustdoc into ${out_dir}..."
rm -rf "$out_dir"
mkdir -p "$out_dir"
cp -R target/doc/* "$out_dir"/

cat <<EOF
Rust API docs are available under:
  docs/docs/api/rustdoc/index.html

To preview in MkDocs:
  cd docs && mkdocs serve
EOF

