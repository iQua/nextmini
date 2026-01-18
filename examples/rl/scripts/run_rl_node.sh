#!/usr/bin/env bash
set -euo pipefail

role="${1:-}"
config_path="${2:-}"
shift 2 || true

if [[ -z "${role}" || -z "${config_path}" ]]; then
  echo "Usage: run_rl_node.sh <trainer|worker> <config-path> [extra args...]" >&2
  exit 2
fi

# Sleep briefly to ensure controller is fully ready
sleep 2

export PYTHONUNBUFFERED=1
export UV_NO_PROGRESS=1

# Default to a per-container venv to avoid cross-container races under `docker compose up`.
# For persistent caching across runs, set NEXTMINI_CACHE_DIR (e.g., /workspace/.multidc_cache).
NEXTMINI_CACHE_DIR="${NEXTMINI_CACHE_DIR:-}"
if [[ -n "${NEXTMINI_CACHE_DIR}" ]]; then
  mkdir -p "${NEXTMINI_CACHE_DIR}"
  CONTAINER_VENV="${CONTAINER_VENV:-${NEXTMINI_CACHE_DIR}/venv_rl_${HOSTNAME}}"
  export UV_CACHE_DIR="${UV_CACHE_DIR:-${NEXTMINI_CACHE_DIR}/uv}"
  export CARGO_HOME="${CARGO_HOME:-${NEXTMINI_CACHE_DIR}/cargo}"
else
  CONTAINER_VENV="${CONTAINER_VENV:-/tmp/.venv-nextmini-${HOSTNAME}}"
  export UV_CACHE_DIR="${UV_CACHE_DIR:-/tmp/uv-cache-${HOSTNAME}}"
  export CARGO_HOME="${CARGO_HOME:-/tmp/cargo-${HOSTNAME}}"
fi

mkdir -p "$(dirname "${CONTAINER_VENV}")" "${UV_CACHE_DIR}" "${CARGO_HOME}"

if [[ ! -d "${CONTAINER_VENV}" ]]; then
  python -m pip install --upgrade pip >/dev/null
  if ! command -v uv >/dev/null; then
    pip install uv >/dev/null
  fi
  uv venv "${CONTAINER_VENV}"
fi

source "${CONTAINER_VENV}/bin/activate"

# Install Python dependencies
echo "Installing Python dependencies..."
uv pip install "psycopg[binary]" >/dev/null
uv pip install numpy >/dev/null
if [[ "${role}" == "trainer" ]]; then
  algo="${MULTICAST_TREE_ALGO:-cf_tree}"
  if [[ "${algo}" == *"_mwu" ]]; then
    echo "Skipping cvxopt install (MULTICAST_TREE_ALGO=${algo})."
  else
    uv pip install cvxopt >/dev/null || {
      echo "Failed to install cvxopt (required for LP-backed planners)." >&2
      echo "Tip: set MULTICAST_TREE_ALGO=cf_bottleneck_mwu to run without an LP solver." >&2
      exit 1
    }
  fi
fi

# Torch: default to CPU wheels (WAN testbeds are often CPU-only).
TORCH_INDEX_URL="${TORCH_INDEX_URL:-https://download.pytorch.org/whl/cpu}"
if [[ "${TORCH_INDEX_URL}" == "pypi" ]]; then
  uv pip install "torch>=2.4.0" >/dev/null
else
  uv pip install --index-url "${TORCH_INDEX_URL}" "torch>=2.4.0" >/dev/null || {
    echo "Failed to install torch from ${TORCH_INDEX_URL}; falling back to PyPI." >&2
    uv pip install "torch>=2.4.0" >/dev/null
  }
fi

if [[ -n "${CUDA_VISIBLE_DEVICES:-}" && "${CUDA_VISIBLE_DEVICES}" != "-1" ]]; then
  python - <<'PY'
import sys
import torch
if not torch.cuda.is_available():
    print("ERROR: CUDA_VISIBLE_DEVICES is set but torch.cuda.is_available() is false.", file=sys.stderr)
    print("  - Ensure the container has GPU access (nvidia-container-toolkit / docker --gpus).", file=sys.stderr)
    print("  - Ensure you installed a CUDA-enabled torch wheel (set TORCH_INDEX_URL).", file=sys.stderr)
    sys.exit(1)
print(f"torch={torch.__version__} cuda={torch.version.cuda} cuda_available={torch.cuda.is_available()}")
PY
fi

uv pip install transformers>=4.30.0 >/dev/null
uv pip install accelerate >/dev/null
uv pip install tqdm>=4.65.0 >/dev/null
if [[ "${role}" == "trainer" ]]; then
  uv pip install datasets>=2.0.0 >/dev/null
fi

# A lock to avoid concurrent Rust builds across containers sharing the same `/workspace`.
NEXTMINI_WHEELS_DIR="${NEXTMINI_WHEELS_DIR:-}"
if [[ -z "${NEXTMINI_WHEELS_DIR}" ]]; then
  if [[ -d "/workspace/target" ]]; then
    NEXTMINI_WHEELS_DIR="/workspace/target/wheels"
  else
    NEXTMINI_WHEELS_DIR="$(pwd)/target/wheels"
  fi
fi

NEXTMINI_BUILD_LOCK="${NEXTMINI_BUILD_LOCK:-}"
if [[ -z "${NEXTMINI_BUILD_LOCK}" ]]; then
  if [[ -d "/workspace/target" ]]; then
    NEXTMINI_BUILD_LOCK="/workspace/target/.nextmini_py_build.lock"
  else
    NEXTMINI_BUILD_LOCK="/tmp/.nextmini_py_build.lock"
  fi
fi

NEXTMINI_REPO_ROOT="${NEXTMINI_REPO_ROOT:-/workspace}"
NEXTMINI_BUILD_LOCK_TIMEOUT_S="${NEXTMINI_BUILD_LOCK_TIMEOUT_S:-1800}"

mtime_epoch() {
  local path="${1}"
  if stat -c %Y "${path}" >/dev/null 2>&1; then
    stat -c %Y "${path}"
  else
    stat -f %m "${path}"
  fi
}

dir_age_seconds() {
  local path="${1}"
  local now
  now="$(date +%s)"
  echo $((now - $(mtime_epoch "${path}")))
}

git_info() {
  local repo_root="${1}"
  if ! command -v git >/dev/null 2>&1; then
    return 1
  fi
  if ! git -C "${repo_root}" rev-parse --is-inside-work-tree >/dev/null 2>&1; then
    return 1
  fi
  local sha
  sha="$(git -C "${repo_root}" rev-parse HEAD 2>/dev/null)" || return 1
  local dirty=0
  if ! git -C "${repo_root}" diff --quiet 2>/dev/null; then
    dirty=1
  fi
  if ! git -C "${repo_root}" diff --cached --quiet 2>/dev/null; then
    dirty=1
  fi
  echo "${sha} ${dirty}"
}

with_build_lock() {
  echo "Waiting for nextmini_py build lock: ${NEXTMINI_BUILD_LOCK}" >&2
  if command -v flock >/dev/null 2>&1; then
    exec 9>"${NEXTMINI_BUILD_LOCK}"
    if ! flock -x -w "${NEXTMINI_BUILD_LOCK_TIMEOUT_S}" 9; then
      echo "Timed out waiting for build lock: ${NEXTMINI_BUILD_LOCK}" >&2
      exit 1
    fi
    "$@"
    flock -u 9 || true
    exec 9>&- || true
  else
    local lock_dir="${NEXTMINI_BUILD_LOCK}.d"
    local start_ts
    start_ts="$(date +%s)"
    while ! mkdir "${lock_dir}" 2>/dev/null; do
      local elapsed=$(( $(date +%s) - start_ts ))
      if (( elapsed > NEXTMINI_BUILD_LOCK_TIMEOUT_S )); then
        echo "Timed out waiting for build lock dir: ${lock_dir}" >&2
        echo "If a container died mid-build, remove it and retry." >&2
        exit 1
      fi
      if [[ -d "${lock_dir}" ]]; then
        local age
        age="$(dir_age_seconds "${lock_dir}" || true)"
        if [[ -n "${age}" && "${age}" -gt "${NEXTMINI_BUILD_LOCK_TIMEOUT_S}" ]]; then
          echo "Removing stale build lock dir ${lock_dir} (age=${age}s)" >&2
          rm -rf "${lock_dir}" || true
          continue
        fi
      fi
      sleep 0.1
    done
    "$@"
    rmdir "${lock_dir}" || true
  fi
}

# Build nextmini_py only if it's not already importable in the (persistent) venv.
force_rebuild="${NEXTMINI_PY_FORCE_REBUILD:-0}"

nextmini_sha=""
nextmini_dirty=0
if read -r nextmini_sha nextmini_dirty < <(git_info "${NEXTMINI_REPO_ROOT}" 2>/dev/null); then
  :
fi

build_id=""
if [[ -n "${nextmini_sha}" ]]; then
  build_id="${nextmini_sha}"
fi

if [[ "${nextmini_dirty}" == "1" ]]; then
  build_id="${build_id}-dirty"
fi

if [[ -n "${build_id}" ]]; then
  echo "nextmini_py build-id: ${build_id}" >&2
fi

installed_build_id_file="${CONTAINER_VENV}/.nextmini_py_build_id"
installed_build_id=""
if [[ -f "${installed_build_id_file}" ]]; then
  installed_build_id="$(cat "${installed_build_id_file}" 2>/dev/null || true)"
fi

need_reinstall=0
if [[ "${force_rebuild}" == "1" ]]; then
  need_reinstall=1
fi
if [[ "${nextmini_dirty}" == "1" ]]; then
  need_reinstall=1
fi
if [[ -n "${build_id}" && "${installed_build_id}" != "${build_id}" ]]; then
  need_reinstall=1
fi

if [[ "${need_reinstall}" == "1" ]]; then
  python -m pip uninstall -y nextmini_py >/dev/null 2>&1 || true
fi

wheel_stamp=""
if [[ -n "${nextmini_sha}" && "${nextmini_dirty}" == "0" ]]; then
  wheel_stamp="${NEXTMINI_WHEELS_DIR}/nextmini_py.${nextmini_sha}.stamp"
fi

resolve_wheel_path() {
  local path=""
  if [[ -n "${wheel_stamp}" && -f "${wheel_stamp}" ]]; then
    path="$(cat "${wheel_stamp}" 2>/dev/null || true)"
    if [[ -n "${path}" && -f "${path}" ]]; then
      echo "${path}"
      return 0
    fi
  fi

  path="$(ls -1t "${NEXTMINI_WHEELS_DIR}"/nextmini_py-*.whl 2>/dev/null | head -n1 || true)"
  if [[ -n "${path}" && -f "${path}" ]]; then
    echo "${path}"
    return 0
  fi

  return 1
}

build_nextmini_py_wheel() {
  set -euo pipefail
  mkdir -p "${NEXTMINI_WHEELS_DIR}" || true

  if [[ "${force_rebuild}" == "1" ]]; then
    rm -f "${NEXTMINI_WHEELS_DIR}"/nextmini_py-*.whl 2>/dev/null || true
    if [[ -n "${wheel_stamp}" ]]; then
      rm -f "${wheel_stamp}" 2>/dev/null || true
    fi
  fi

  if [[ "${nextmini_dirty}" == "0" && -n "${wheel_stamp}" && -f "${wheel_stamp}" ]]; then
    local stamped
    stamped="$(cat "${wheel_stamp}" 2>/dev/null || true)"
    if [[ -n "${stamped}" && -f "${stamped}" ]]; then
      return 0
    fi
  fi

  echo "Building nextmini_py wheel (this may take a while)..." >&2
  if [[ "${NEXTMINI_BUILD_VERBOSE:-0}" == "1" ]]; then
    maturin build --release -m python-api/Cargo.toml -F python-extension
  else
    maturin build --release -m python-api/Cargo.toml -F python-extension >/dev/null
  fi

  local built
  built="$(ls -1t "${NEXTMINI_WHEELS_DIR}"/nextmini_py-*.whl 2>/dev/null | head -n1 || true)"
  if [[ -z "${built}" || ! -f "${built}" ]]; then
    echo "Failed to build nextmini_py wheel under ${NEXTMINI_WHEELS_DIR}." >&2
    exit 1
  fi

  if [[ "${nextmini_dirty}" == "0" && -n "${wheel_stamp}" ]]; then
    printf "%s\n" "${built}" > "${wheel_stamp}.tmp"
    mv "${wheel_stamp}.tmp" "${wheel_stamp}"
  fi
}

if [[ "${need_reinstall}" == "1" ]] || ! python -c 'import nextmini_py' >/dev/null 2>&1; then
  if [[ "${SKIP_BUILD:-0}" != "1" ]]; then
    uv pip install maturin >/dev/null
    with_build_lock build_nextmini_py_wheel
    wheel_path="$(resolve_wheel_path || true)"
    echo "Installing wheel: ${wheel_path}"
    uv pip install "${wheel_path}" >/dev/null
  else
    wheel_path="${NEXTMINI_PY_WHEEL:-}"
    if [[ -z "${wheel_path}" ]]; then
      wheel_path="$(resolve_wheel_path || true)"
    fi
    if [[ -z "${wheel_path}" || ! -f "${wheel_path}" ]]; then
      echo "SKIP_BUILD=1 but nextmini_py wheel not found. Set NEXTMINI_PY_WHEEL to a valid path." >&2
      exit 1
    fi
    echo "Installing pre-built wheel: ${wheel_path}"
    uv pip install "${wheel_path}" >/dev/null
  fi

  if [[ -n "${build_id}" ]]; then
    printf "%s\n" "${build_id}" > "${installed_build_id_file}"
  else
    rm -f "${installed_build_id_file}" 2>/dev/null || true
  fi
fi

# Set PYTHONPATH so relative imports work
export PYTHONPATH=/workspace:${PYTHONPATH:-}

# Run the appropriate role using -m to support relative imports
if [[ "${role}" == "trainer" ]]; then
  echo "Starting Trainer..."
  exec python -m examples.rl.src.trainer --config "${config_path}"
elif [[ "${role}" == "worker" ]]; then
  echo "Starting Worker..."
  exec python -m examples.rl.src.worker "$@" --config "${config_path}"
else
  echo "Unknown role: ${role}" >&2
  exit 1
fi
