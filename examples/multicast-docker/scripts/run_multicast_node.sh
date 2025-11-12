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

cleanup_dir() {
  local target="$1"
  if [[ -z "${target}" ]]; then
    return
  fi
  mkdir -p "${target}"
  find "${target}" -mindepth 1 ! -name '.gitkeep' -exec rm -rf {} + 2>/dev/null || true
}

tensor_path_provided=0
for arg in "$@"; do
  case "${arg}" in
    --tensor-path|--tensor-path=*)
      tensor_path_provided=1
      break
      ;;
  esac
done

if [[ "${role}" == "source" && "${CLEAN_SHARED_DIRS:-1}" == "1" ]]; then
  cleanup_dir "${ARTIFACT_DIR:-/artifacts}"
  if [[ "${tensor_path_provided}" == "0" ]]; then
    cleanup_dir "${TENSOR_STAGE_DIR:-/workspace/tensors}"
  fi
fi

if [[ ! -d .venv ]]; then
  python -m pip install --upgrade pip >/dev/null
  if ! command -v uv >/dev/null; then
    pip install uv >/dev/null
  fi
  uv venv .venv
fi

source .venv/bin/activate

uv pip install "psycopg[binary]" >/dev/null
uv pip install numpy >/dev/null
uv pip install torch --index-url https://download.pytorch.org/whl/cpu >/dev/null

if [[ "${SKIP_BUILD:-0}" != "1" ]]; then
  uv pip install maturin >/dev/null
  # Enable the dataplane's reliable engine in the Python bindings for this demo.
  maturin develop --release -m python-api/Cargo.toml -F reliable >/dev/null
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
