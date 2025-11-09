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
    # Use timeout with nc (netcat) to avoid WebSocket handshake errors
    if timeout 1 bash -c "echo > /dev/tcp/${wait_host}/${wait_port}" 2>/dev/null; then
      echo "Controller reachable (attempt ${attempt})." >&2
      if [[ "${WAIT_STABILIZE:-2}" != "0" ]]; then
        sleep "${WAIT_STABILIZE:-2}" >&2
      fi
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
