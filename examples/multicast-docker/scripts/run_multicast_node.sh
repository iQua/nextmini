#!/usr/bin/env bash
set -euo pipefail

role="${1:-}"
config_path="${2:-}"
shift 2 || true

if [[ -z "${role}" || -z "${config_path}" ]]; then
  echo "Usage: run_multicast_node.sh <source|receiver> <config-path> [extra args...]" >&2
  exit 2
fi

if [[ -n "${WAIT_FOR:-}" ]]; then
  IFS=":" read -r wait_host wait_port <<< "${WAIT_FOR}"
  wait_attempts="${WAIT_ATTEMPTS:-60}"
  echo "Waiting for ${wait_host}:${wait_port} (${wait_attempts} attempts)..." >&2
  for attempt in $(seq 1 "${wait_attempts}"); do
    if bash -c "exec 3<>/dev/tcp/${wait_host}/${wait_port}" 2>/dev/null; then
      exec 3>&-
      echo "Controller reachable (attempt ${attempt})." >&2
      break
    fi
    sleep 1
    if [[ "${attempt}" == "${wait_attempts}" ]]; then
      echo "Timed out waiting for ${wait_host}:${wait_port}" >&2
      exit 1
    fi
  done
fi

export PYTHONUNBUFFERED=1

if [[ ! -d .venv ]]; then
  python -m pip install --upgrade pip >/dev/null
  if ! command -v uv >/dev/null; then
    pip install uv >/dev/null
  fi
  uv venv .venv
fi

source .venv/bin/activate

if [[ "${SKIP_BUILD:-0}" != "1" ]]; then
  uv pip install maturin "psycopg[binary]" >/dev/null
  maturin develop --release -m python-api/Cargo.toml >/dev/null
fi

exec python examples/multicast-docker/scripts/multicast_node.py \
  --role "${role}" \
  --config "${config_path}" \
  "$@"
