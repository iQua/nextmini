#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
root_dir="$(cd "${script_dir}/../.." && pwd)"

config_path="${script_dir}/config.toml"
compose_file="${script_dir}/docker-compose.yml"
session_name="nextmini-ns-flow"
log_level="${RUST_LOG:-info}"
binary_path="${NEXTMINI_BIN:-${root_dir}/target/release/nextmini}"
cargo_bin="${CARGO_BIN:-}"
no_build="false"
sysctl_only="false"
apply_sysctl_tuning="true"
skip_generate="false"
startup_timeout_seconds="900"

n_nodes=""
n_flows=""
src=""
dst=""
flow_bytes=""
flow_rate=""
flow_weight=""
cargo_available="false"
cargo_build_cmd=""

usage() {
  cat <<'EOF'
Usage: run.sh [options]

Options:
  --n-nodes N       Total namespace nodes (default: 136).
  --n-flows N       Number of flows (default: 180).
  --src N           Flow source node id (default: 1).
  --dst N           Flow destination node id (default: 136).
  --flow-bytes N    Per-flow bytes (default: 1000000).
  --flow-rate N     Per-flow rate bytes/s (default: 500000).
  --flow-weight N   Per-flow weight (default: 1).
  --log-level LVL   RUST_LOG for dataplane (default: info).
  --bin PATH        Path to nextmini binary (default: <repo>/target/release/nextmini).
  --no-build        Skip cargo build and run the existing binary.
  --session NAME    tmux session name (default: nextmini-ns-flow).
  --no-generate     Do not rewrite config files (skip generate.py).
  --sysctl-only     Apply sysctl tuning and exit.
  --no-sysctl       Skip sysctl tuning.
  -h, --help        Show this help.
EOF
}

resolve_user_home() {
  local user="$1"
  local entry=""

  if [[ -z "$user" ]]; then
    return 1
  fi

  if command -v getent >/dev/null 2>&1; then
    entry="$(getent passwd "$user" 2>/dev/null || true)"
  fi

  if [[ -z "$entry" ]]; then
    return 1
  fi

  printf '%s\n' "$entry" | cut -d: -f6
}

shell_quote() {
  printf '%q' "$1"
}

configure_cargo_build() {
  local resolved_cargo_bin="$cargo_bin"

  cargo_available="false"
  cargo_build_cmd=""

  if [[ "${EUID:-$(id -u)}" -eq 0 && -n "${SUDO_USER:-}" && "${SUDO_USER}" != "root" ]]; then
    local invoking_home=""
    local invoking_cargo_home=""
    local invoking_rustup_home=""
    local path_prefix=""

    invoking_home="$(resolve_user_home "$SUDO_USER" || true)"
    if [[ -z "$invoking_home" ]]; then
      return 0
    fi

    invoking_cargo_home="${CARGO_HOME:-${invoking_home}/.cargo}"
    invoking_rustup_home="${RUSTUP_HOME:-${invoking_home}/.rustup}"
    resolved_cargo_bin="${resolved_cargo_bin:-${invoking_cargo_home}/bin/cargo}"
    if [[ ! -x "$resolved_cargo_bin" ]]; then
      return 0
    fi

    path_prefix="${invoking_cargo_home}/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
    cargo_build_cmd="$(
      printf 'sudo -u %s env HOME=%s CARGO_HOME=%s RUSTUP_HOME=%s PATH=%s %s build -p nextmini --release' \
        "$(shell_quote "$SUDO_USER")" \
        "$(shell_quote "$invoking_home")" \
        "$(shell_quote "$invoking_cargo_home")" \
        "$(shell_quote "$invoking_rustup_home")" \
        "$(shell_quote "$path_prefix")" \
        "$(shell_quote "$resolved_cargo_bin")"
    )"
    cargo_available="true"
    return 0
  fi

  if [[ -z "$resolved_cargo_bin" ]]; then
    resolved_cargo_bin="$(command -v cargo 2>/dev/null || true)"
  fi

  if [[ -z "$resolved_cargo_bin" || ! -x "$resolved_cargo_bin" ]]; then
    return 0
  fi

  cargo_build_cmd="$(printf '%s build -p nextmini --release' "$(shell_quote "$resolved_cargo_bin")")"
  cargo_available="true"
}

apply_sysctl() {
  if [[ "$(uname -s)" != "Linux" ]]; then
    echo "sysctl tuning is only supported on Linux; skipping." >&2
    return 0
  fi

  local sudo_cmd=()
  if [[ "${EUID:-$(id -u)}" -ne 0 ]]; then
    if ! command -v sudo >/dev/null 2>&1; then
      echo "sudo is required to apply sysctl tuning, but sudo was not found." >&2
      return 1
    fi
    sudo_cmd=(sudo)
    sudo -v
  fi

  local args=(
    net.ipv4.neigh.default.gc_thresh1=32768
    net.ipv4.neigh.default.gc_thresh2=65536
    net.ipv4.neigh.default.gc_thresh3=131072
    net.core.somaxconn=131070
    net.core.netdev_max_backlog=262144
    net.core.netdev_budget=2400
    net.core.rmem_max=536870912
    net.core.wmem_max=536870912
    net.core.rmem_default=2097152
    net.core.wmem_default=2097152
  )

  "${sudo_cmd[@]}" sysctl -w "${args[@]}"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --n-nodes) n_nodes="${2:-}"; shift 2 ;;
    --n-flows) n_flows="${2:-}"; shift 2 ;;
    --src) src="${2:-}"; shift 2 ;;
    --dst) dst="${2:-}"; shift 2 ;;
    --flow-bytes) flow_bytes="${2:-}"; shift 2 ;;
    --flow-rate) flow_rate="${2:-}"; shift 2 ;;
    --flow-weight) flow_weight="${2:-}"; shift 2 ;;
    --log-level) log_level="${2:-}"; shift 2 ;;
    --bin) binary_path="${2:-}"; shift 2 ;;
    --no-build) no_build="true"; shift ;;
    --session) session_name="${2:-}"; shift 2 ;;
    --no-generate) skip_generate="true"; shift ;;
    --sysctl-only) sysctl_only="true"; shift ;;
    --no-sysctl) apply_sysctl_tuning="false"; shift ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown option: $1" >&2; usage; exit 1 ;;
  esac
done

if [[ "$sysctl_only" == "true" ]]; then
  apply_sysctl
  exit 0
fi

if [[ "$apply_sysctl_tuning" == "true" ]]; then
  apply_sysctl
fi

configure_cargo_build

if [[ "$skip_generate" != "true" ]]; then
  gen_cmd=(python3 "${script_dir}/generate.py")
  [[ -n "$n_nodes" ]] && gen_cmd+=(--n-nodes "$n_nodes")
  [[ -n "$n_flows" ]] && gen_cmd+=(--n-flows "$n_flows")
  [[ -n "$src" ]] && gen_cmd+=(--src "$src")
  [[ -n "$dst" ]] && gen_cmd+=(--dst "$dst")
  [[ -n "$flow_bytes" ]] && gen_cmd+=(--flow-bytes "$flow_bytes")
  [[ -n "$flow_rate" ]] && gen_cmd+=(--flow-rate "$flow_rate")
  [[ -n "$flow_weight" ]] && gen_cmd+=(--flow-weight "$flow_weight")
  "${gen_cmd[@]}"
fi

if [[ "$(uname -s)" != "Linux" ]]; then
  echo "ns-flow requires Linux network namespaces (iproute2). Use generate.py on this host and run run.sh on Linux." >&2
  exit 1
fi

if [[ "$binary_path" != /* ]]; then
  binary_path="${root_dir}/${binary_path}"
fi

if [[ "$no_build" == "true" ]] || [[ "$cargo_available" != "true" ]]; then
  if [[ ! -x "$binary_path" ]]; then
    echo "nextmini binary not found/executable at: $binary_path" >&2
    if [[ "$no_build" != "true" ]]; then
      echo "cargo is not available, so run.sh cannot build it automatically." >&2
    fi
    echo "Fix options:" >&2
    echo "  - Install Rust (cargo) and rerun, OR" >&2
    echo "  - Build nextmini elsewhere and copy it to: $binary_path, then rerun with --no-build" >&2
    exit 1
  fi
fi

if ! command -v tmux >/dev/null 2>&1; then
  echo "tmux is required for run.sh. Run manually instead:" >&2
  echo "  (A) cd examples/ns-flow && docker compose up --build" >&2
  echo "  (B) cd \"$root_dir\"" >&2
  echo "      cargo build -p nextmini --release   # if cargo is installed" >&2
  echo "      sudo -E env RUST_LOG=\"$log_level\" \"$binary_path\" --config-path \"$config_path\"" >&2
  exit 1
fi

if tmux has-session -t "$session_name" 2>/dev/null; then
  echo "Session exists: $session_name (attach with: tmux attach -t $session_name)" >&2
  exit 1
fi

controller_host="127.0.0.1"
controller_port="3000"
controller_addr=""
if [[ -f "$config_path" ]]; then
  controller_addr="$(awk -F'"' '/^[[:space:]]*controller_addr[[:space:]]*=/{print $2; exit}' "$config_path" 2>/dev/null || true)"
fi
if [[ -n "$controller_addr" ]]; then
  controller_addr="${controller_addr#ws://}"
  controller_addr="${controller_addr#wss://}"
  controller_addr="${controller_addr%%/*}"
fi
if [[ -n "$controller_addr" ]]; then
  if [[ "$controller_addr" =~ ^\\[([^\\]]+)\\]:(.+)$ ]]; then
    controller_host="${BASH_REMATCH[1]}"
    controller_port="${BASH_REMATCH[2]}"
  elif [[ "$controller_addr" == *:* ]]; then
    controller_host="${controller_addr%:*}"
    controller_port="${controller_addr##*:}"
  fi
fi

expected_flows=""
expected_routes=""
controller_config_path="${script_dir}/controller-config.toml"
if [[ -f "$controller_config_path" ]]; then
  expected_flows="$(grep -c '^\[\[flows\]\]' "$controller_config_path" 2>/dev/null || true)"
  expected_routes="$(grep -c '^\[\[routes\]\]' "$controller_config_path" 2>/dev/null || true)"
fi

compose_bin="docker compose"
if ! docker compose version >/dev/null 2>&1; then
  if command -v docker-compose >/dev/null 2>&1; then
    compose_bin="docker-compose"
  else
    echo "docker compose is required (either 'docker compose' or 'docker-compose')." >&2
    exit 1
  fi
fi

compose_cmd="cd \"$script_dir\" && $compose_bin -f \"$compose_file\" up --build"
dataplane_cmd="cd \"$root_dir\" && (ulimit -u 20000 2>/dev/null || true) && (ulimit -n 200000 2>/dev/null || true)"
if [[ "$no_build" != "true" ]]; then
  if [[ "$cargo_available" == "true" ]]; then
    dataplane_cmd+=" && ${cargo_build_cmd}"
  fi
fi

dataplane_cmd+=" && echo 'Waiting for Postgres to accept connections...'"
dataplane_cmd+=" && for ((i=1; i<=${startup_timeout_seconds}; i++)); do docker exec postgres pg_isready -U pgusr -d nextmini >/dev/null 2>&1 && break; sleep 1; done"
dataplane_cmd+=" && docker exec postgres pg_isready -U pgusr -d nextmini >/dev/null 2>&1 || (echo 'Timed out waiting for postgres container (pg_isready).' >&2; exit 1)"

if [[ -n "$expected_routes" && "$expected_routes" =~ ^[0-9]+$ && "$expected_routes" -gt 0 ]]; then
  dataplane_cmd+=" && echo 'Waiting for controller to seed ${expected_routes} routes in Postgres...'"
  dataplane_cmd+=" && for ((i=1; i<=${startup_timeout_seconds}; i++)); do docker exec postgres psql -U pgusr -d nextmini -tAc 'SELECT COUNT(*) FROM routes;' 2>/dev/null | tr -d '[:space:]' | grep -qx '${expected_routes}' && break; sleep 1; done"
  dataplane_cmd+=" && docker exec postgres psql -U pgusr -d nextmini -tAc 'SELECT COUNT(*) FROM routes;' 2>/dev/null | tr -d '[:space:]' | grep -qx '${expected_routes}' || (echo 'Timed out waiting for controller to seed routes.' >&2; exit 1)"
fi

if [[ -n "$expected_flows" && "$expected_flows" =~ ^[0-9]+$ && "$expected_flows" -gt 0 ]]; then
  dataplane_cmd+=" && echo 'Waiting for controller to seed ${expected_flows} flows in Postgres...'"
  dataplane_cmd+=" && for ((i=1; i<=${startup_timeout_seconds}; i++)); do docker exec postgres psql -U pgusr -d nextmini -tAc 'SELECT COUNT(*) FROM flows;' 2>/dev/null | tr -d '[:space:]' | grep -qx '${expected_flows}' && break; sleep 1; done"
  dataplane_cmd+=" && docker exec postgres psql -U pgusr -d nextmini -tAc 'SELECT COUNT(*) FROM flows;' 2>/dev/null | tr -d '[:space:]' | grep -qx '${expected_flows}' || (echo 'Timed out waiting for controller to seed flows.' >&2; exit 1)"
fi

dataplane_cmd+=" && echo \"Waiting for controller at ${controller_host}:${controller_port}...\""
dataplane_cmd+=" && for ((i=1; i<=${startup_timeout_seconds}; i++)); do (echo >/dev/tcp/${controller_host}/${controller_port}) >/dev/null 2>&1 && break; sleep 1; done"
dataplane_cmd+=" && (echo >/dev/tcp/${controller_host}/${controller_port}) >/dev/null 2>&1 || (echo 'Timed out waiting for controller port (check: docker logs controller).' >&2; exit 1)"
dataplane_cmd+=" && sudo -E env RUST_LOG=\"$log_level\" \"$binary_path\" --config-path \"$config_path\""

tmux new-session -d -s "$session_name" -n nsflow bash
tmux send-keys -t "${session_name}:0.0" "$compose_cmd" C-m
tmux split-window -h -t "${session_name}:0.0" bash
tmux send-keys -t "${session_name}:0.1" "$dataplane_cmd" C-m
tmux select-layout -t "${session_name}:0" tiled
tmux attach -t "$session_name"
