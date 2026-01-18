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
export UV_CACHE_DIR="${UV_CACHE_DIR:-/workspace/.multidc_cache/uv}"

# Persist per-host Python venv (bind-mounted /workspace lives on the VM).
# Rust toolchains are already baked into the Docker image under /root/.rustup, so
# DO NOT override RUSTUP_HOME by default (doing so breaks rustup's toolchain selection).
CONTAINER_VENV="${CONTAINER_VENV:-/workspace/.multidc_cache/venv}"

# Optionally persist Cargo registry/git caches across runs.
export CARGO_HOME="${CARGO_HOME:-/workspace/.multidc_cache/cargo}"

VENV_LOCK="${VENV_LOCK:-/workspace/.multidc_cache/venv.lock}"

mkdir -p "$(dirname "${CONTAINER_VENV}")" "${CARGO_HOME}" "${UV_CACHE_DIR}" "$(dirname "${VENV_LOCK}")"

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
python -c 'import psycopg' >/dev/null 2>&1 || uv pip install "psycopg[binary]" >/dev/null || true

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

  needs_cvxopt="0"
  case "${algo}" in
    *_mwu)
      needs_cvxopt="0"
      ;;
    cf_tree|cf_bottleneck|mflow)
      needs_cvxopt="1"
      ;;
    *)
      needs_cvxopt="0"
      ;;
  esac

  if [[ "${needs_cvxopt}" == "1" ]]; then
    python -c 'import cvxopt' >/dev/null 2>&1 || uv pip install cvxopt >/dev/null || {
      echo "Failed to install cvxopt (required for LP-backed planners: ${algo})." >&2
      echo "Tip: use --algorithm cf_bottleneck_mwu (or cf_tree_mwu) to run without an LP solver." >&2
      exit 1
    }
  else
    echo "Skipping cvxopt install (algorithm=${algo})."
  fi
fi

# Build nextmini_py only if it's not already importable in the (persistent) venv.
force_rebuild="${NEXTMINI_PY_FORCE_REBUILD:-0}"
if [[ "${force_rebuild}" == "1" ]]; then
  python -m pip uninstall -y nextmini_py >/dev/null 2>&1 || true
fi

if [[ "${force_rebuild}" == "1" ]] || ! python -c 'import nextmini_py' >/dev/null 2>&1; then
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
    uv pip install "${wheel_path}" >/dev/null
  fi
fi

export PYTHONPATH=/workspace:${PYTHONPATH:-}

release_lock

exec python -m examples.rl.src.broadcast_bench \
  --role "${role}" \
  --config "${config_path}" \
  "$@"
