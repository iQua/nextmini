#!/usr/bin/env bash
set -euo pipefail

role="${1:-}"
config_path="${2:-}"
shift 2 || true

if [[ -z "${role}" || -z "${config_path}" ]]; then
  echo "Usage: run_multicast_node.sh <source|receiver> <config-path> [extra args...]" >&2
  exit 2
fi

# Sleep briefly to ensure controller is fully ready
# (docker-compose depends_on handles the basic startup order)
sleep 2

export PYTHONUNBUFFERED=1

if [[ ! -d .venv ]]; then
  python -m pip install --upgrade pip >/dev/null
  if ! command -v uv >/dev/null; then
    pip install uv >/dev/null
  fi
  uv venv .venv
fi

source .venv/bin/activate

uv pip install "psycopg[binary]" >/dev/null

if [[ "${SKIP_BUILD:-0}" != "1" ]]; then
  uv pip install maturin >/dev/null
  maturin develop --release -m python-api/Cargo.toml >/dev/null
else
  wheel_path="${NEXTMINI_PY_WHEEL:-}"
  if [[ -z "${wheel_path}" ]]; then
    wheel_path=$(ls -1t /workspace/target/wheels/nextmini_py-*.whl 2>/dev/null | head -n1 || true)
  fi
  if [[ -z "${wheel_path}" || ! -f "${wheel_path}" ]]; then
    echo "SKIP_BUILD=1 but nextmini_py wheel not found. Set NEXTMINI_PY_WHEEL to a valid path." >&2
    exit 1
  fi
  uv pip install "${wheel_path}" >/dev/null
fi

exec python examples/multicast-docker/scripts/multicast_node.py \
  --role "${role}" \
  --config "${config_path}" \
  "$@"
