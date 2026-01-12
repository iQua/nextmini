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

# Persist per-host Python venv (bind-mounted /workspace lives on the VM).
# Rust toolchains are already baked into the Docker image under /root/.rustup, so
# DO NOT override RUSTUP_HOME by default (doing so breaks rustup's toolchain selection).
CONTAINER_VENV="${CONTAINER_VENV:-/workspace/.multidc_cache/venv_rl}"

# Optionally persist Cargo registry/git caches across runs.
export CARGO_HOME="${CARGO_HOME:-/workspace/.multidc_cache/cargo}"

mkdir -p "$(dirname "${CONTAINER_VENV}")" "${CARGO_HOME}"

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

# Build nextmini_py only if it's not already importable in the (persistent) venv.
force_rebuild="${NEXTMINI_PY_FORCE_REBUILD:-0}"
if [[ "${force_rebuild}" == "1" ]]; then
  python -m pip uninstall -y nextmini_py >/dev/null 2>&1 || true
fi

if [[ "${force_rebuild}" == "1" ]] || ! python -c 'import nextmini_py' >/dev/null 2>&1; then
  if [[ "${SKIP_BUILD:-0}" != "1" ]]; then
    echo "Building nextmini_py..."
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
    echo "Installing pre-built wheel: ${wheel_path}"
    uv pip install "${wheel_path}" >/dev/null
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
