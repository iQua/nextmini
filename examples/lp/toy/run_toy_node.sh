#!/usr/bin/env bash
set -euo pipefail

role="${1:-}"
config_path="${2:-}"
shift 2 || true

if [[ -z "${role}" || -z "${config_path}" ]]; then
  echo "Usage: run_toy_node.sh <source|receiver> <config-path>" >&2
  exit 2
fi

export PYTHONUNBUFFERED=1
export UV_NO_PROGRESS=1

# Use a container-specific venv path to avoid conflicts with host mounts.
CONTAINER_VENV="/tmp/.venv-nextmini-toy"
SHARED_DIR="${TOY_SHARED_DIR:-/shared}"
WHEEL_DIR="${SHARED_DIR}/wheels"
LOCK_DIR="${WHEEL_DIR}/.nextmini_py_build_lock"

if [[ ! -d "${CONTAINER_VENV}" ]]; then
  python -m pip install --upgrade pip >/dev/null
  if ! command -v uv >/dev/null; then
    pip install uv >/dev/null
  fi
  uv venv "${CONTAINER_VENV}"
fi

source "${CONTAINER_VENV}/bin/activate"

mkdir -p "${WHEEL_DIR}"

echo "Installing build deps for nextmini_py..."
uv pip install maturin >/dev/null

echo "Installing mFlow dependencies (numpy, cvxopt)..."
uv pip install numpy >/dev/null
uv pip install cvxopt >/dev/null

WHEEL_GLOB="${WHEEL_DIR}/nextmini_py-*.whl"
if ! ls ${WHEEL_GLOB} >/dev/null 2>&1; then
  echo "nextmini_py wheel missing; building once into ${WHEEL_DIR}..."
  if mkdir "${LOCK_DIR}" 2>/dev/null; then
    # We won the lock.
    trap 'rmdir "${LOCK_DIR}" 2>/dev/null || true' EXIT
    maturin build --release -m python-api/Cargo.toml -F python-extension -o "${WHEEL_DIR}" >/dev/null
    rmdir "${LOCK_DIR}" 2>/dev/null || true
    trap - EXIT
  else
    # Another container is building the wheel; wait for it.
    echo "Waiting for nextmini_py wheel to appear..."
    deadline=$(( $(date +%s) + 900 ))
    until ls ${WHEEL_GLOB} >/dev/null 2>&1; do
      if [[ "$(date +%s)" -ge "${deadline}" ]]; then
        echo "Timed out waiting for nextmini_py wheel in ${WHEEL_DIR}" >&2
        exit 1
      fi
      sleep 2
    done
  fi
fi

echo "Installing nextmini_py from wheel..."
uv pip install ${WHEEL_GLOB} >/dev/null

export PYTHONPATH=/workspace:${PYTHONPATH:-}

exec python /workspace/examples/lp/toy/toy_demo.py --role "${role}" --config "${config_path}"


