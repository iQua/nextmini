#!/usr/bin/env bash
set -euo pipefail

# Convenience wrapper so users can run `./start-database.sh` from the repo root.
# The underlying implementation lives in `utils/start-database.sh`.

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$REPO_ROOT"

exec bash utils/start-database.sh
