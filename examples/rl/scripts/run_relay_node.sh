#!/usr/bin/env bash
set -euo pipefail

config_path="${1:-}"
if [[ -z "${config_path}" ]]; then
  echo "Usage: run_relay_node.sh <config-path>" >&2
  exit 2
fi

sleep 2

export PYTHONUNBUFFERED=1
export UV_NO_PROGRESS=1
export UV_CACHE_DIR="${UV_CACHE_DIR:-/workspace/.multidc_cache/uv}"

CONTAINER_VENV="${CONTAINER_VENV:-/workspace/.multidc_cache/venv}"
VENV_LOCK="${VENV_LOCK:-/workspace/.multidc_cache/venv.lock}"

mkdir -p "$(dirname "${CONTAINER_VENV}")" "${UV_CACHE_DIR}" "$(dirname "${VENV_LOCK}")"

lock_fd=""
lock_dir=""
if command -v flock >/dev/null 2>&1; then
  exec 9>"${VENV_LOCK}"
  flock -x 9
  lock_fd="9"
else
  lock_dir="${VENV_LOCK}.d"
  while ! mkdir "${lock_dir}" 2>/dev/null; do
    sleep 0.1
  done
fi

release_lock() {
  if [[ -n "${lock_fd}" ]]; then
    flock -u "${lock_fd}" || true
    exec 9>&- || true
    lock_fd=""
  fi
  if [[ -n "${lock_dir}" ]]; then
    rmdir "${lock_dir}" || true
    lock_dir=""
  fi
}
trap 'release_lock' EXIT

if [[ ! -d "${CONTAINER_VENV}" ]]; then
  python -m pip install --upgrade pip >/dev/null
  if ! command -v uv >/dev/null; then
    pip install uv >/dev/null
  fi
  uv venv "${CONTAINER_VENV}"
fi

source "${CONTAINER_VENV}/bin/activate"

python -c 'import numpy' >/dev/null 2>&1 || uv pip install numpy >/dev/null

if [[ "${SKIP_BUILD:-0}" != "1" ]]; then
  python -c 'import maturin' >/dev/null 2>&1 || uv pip install maturin >/dev/null
  maturin develop --release -m python-api/Cargo.toml -F python-extension >/dev/null
else
  wheel_path="${NEXTMINI_PY_WHEEL:-}"
  if [[ -z "${wheel_path}" ]]; then
    wheel_path=$(ls -1t /workspace/target/wheels/nextmini_py-*.whl 2>/dev/null | head -n1 || true)
  fi
  if [[ -z "${wheel_path}" || ! -f "${wheel_path}" ]]; then
    echo "SKIP_BUILD=1 but nextmini_py wheel not found. Set NEXTMINI_PY_WHEEL to a valid path." >&2
    exit 1
  fi
  python -c 'import nextmini_py' >/dev/null 2>&1 || uv pip install "${wheel_path}" >/dev/null
fi

export PYTHONPATH=/workspace:${PYTHONPATH:-}

release_lock

exec python -m examples.rl.src.relay --config "${config_path}"
