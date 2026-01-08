#!/usr/bin/env bash
set -euo pipefail

role="${1:-}"
config_path="${2:-}"
shift 2 || true

if [[ -z "${role}" || -z "${config_path}" ]]; then
  echo "Usage: run_broadcast_bench.sh <trainer|worker> <config-path> [extra args...]" >&2
  exit 2
fi

sleep 2

export PYTHONUNBUFFERED=1
export UV_NO_PROGRESS=1

CONTAINER_VENV="/tmp/.venv-nextmini-bench"

if [[ ! -d "${CONTAINER_VENV}" ]]; then
  python -m pip install --upgrade pip >/dev/null
  if ! command -v uv >/dev/null; then
    pip install uv >/dev/null
  fi
  uv venv "${CONTAINER_VENV}"
fi

source "${CONTAINER_VENV}/bin/activate"

uv pip install numpy >/dev/null
uv pip install "psycopg[binary]" >/dev/null || true

if [[ "${role}" == "trainer" ]]; then
  algo="${BROADCAST_ALGO:-${MULTICAST_TREE_ALGO:-cf_tree}}"
  prev=""
  for arg in "$@"; do
    if [[ "${prev}" == "--algorithm" ]]; then
      algo="${arg}"
      break
    fi
    case "${arg}" in
      --algorithm=*)
        algo="${arg#--algorithm=}"
        break
        ;;
    esac
    prev="${arg}"
  done

  if [[ "${algo}" == *"_mwu" ]]; then
    echo "Skipping cvxopt install (algorithm=${algo})."
  else
    uv pip install cvxopt >/dev/null || {
      echo "Failed to install cvxopt (required for LP-backed planners)." >&2
      echo "Tip: use --algorithm cf_bottleneck_mwu to run without an LP solver." >&2
      exit 1
    }
  fi
fi

if [[ "${SKIP_BUILD:-0}" != "1" ]]; then
  uv pip install maturin >/dev/null
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
  uv pip install "${wheel_path}" >/dev/null
fi

export PYTHONPATH=/workspace:${PYTHONPATH:-}

exec python -m examples.rl.src.broadcast_bench \
  --role "${role}" \
  --config "${config_path}" \
  "$@"
