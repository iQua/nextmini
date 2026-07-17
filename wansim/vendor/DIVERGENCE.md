# Vendored days / nexosim provenance and divergence

This directory is a source vendor, not a generated Cargo cache.

## Upstream identities

- `days/` upstream: <https://github.com/iQua/days>
- Imported days commit: `d6a473b555d4f129c1eb62c36cb4525cdd5240ad`
- Imported days tree: `33800f49fff44ee22a4e586cfef973aaad43bb06`
- Import method: `git archive` of that commit, so only files tracked by upstream are present.
- days license: `AGPL-3.0-only` (see `days/LICENSE`).

The days commit bundles its nexosim fork under `days/crates/nexosim` rather than using the
crates.io source selected by the dependency declaration. Its exact imported tree identity is:

- upstream project: <https://github.com/asynchronics/nexosim>
- declared upstream release: `v1.0.0`
- upstream release commit: `d2207ab5c641e21c4faf3906bc74eb02e77c7e9d`
- days-bundled nexosim tree: `9cad1c1ee25dac8b31c1bb215ce3801eacfc7e29`
- nexosim license: `MIT OR Apache-2.0` (see `days/crates/nexosim/LICENSE-*`).

The bundled tree already differs from the pristine nexosim `v1.0.0` source. Those inherited
differences belong to the pinned days revision; wansim does not attempt to reconstruct or rewrite
their history. The days commit plus the subtree identity above pins them byte-for-byte.

## Wansim-local patches

The import commit has no wansim-local source changes. Later commits must append one entry here for
every local patch to either days or its bundled nexosim, including the reason and affected files.
