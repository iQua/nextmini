#!/usr/bin/env bash
set -euo pipefail

# Resolve paths
script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
root_dir="$(cd "${script_dir}/../.." && pwd)"

# Defaults
config_path="${root_dir}/examples/namespace/config.toml"
compose_file="${root_dir}/examples/namespace/docker-compose.yml"
controller_config="${root_dir}/examples/namespace/controller-config.toml"
session_name="nextmini-namespace"
n_nodes=""
log_level="${RUST_LOG:-info}"
sysctl_only="false"
monitor_memory="true"
memory_report="${script_dir}/memory-report.txt"
nodes_for_report=""

usage() {
  cat <<'EOF'
Usage: run.sh [options]

Options:
  --n-nodes N       Set node count (updates controller config and dataplane).
  --config PATH     Override dataplane config path.
  --compose PATH    Override controller compose file.
  --log-level LVL   Set RUST_LOG for the dataplane (default: info).
  --session NAME    tmux session name (default: nextmini-namespace).
  --sysctl-only     Apply sysctl tuning and exit.
  --no-monitor      Disable automatic memory monitoring.
  -h, --help        Show this help.
EOF
}

resolve_path() {
  local path="$1"
  if [[ "$path" = /* ]]; then
    echo "$path"
  else
    echo "${root_dir}/${path}"
  fi
}

apply_sysctl() {
  sudo -v
  sudo sysctl -w net.ipv4.neigh.default.gc_thresh1=32768
  sudo sysctl -w net.ipv4.neigh.default.gc_thresh2=65536
  sudo sysctl -w net.ipv4.neigh.default.gc_thresh3=131072
  sudo sysctl -w net.core.rmem_max=536870912
  sudo sysctl -w net.core.wmem_max=536870912
  sudo sysctl -w net.core.rmem_default=2097152
  sudo sysctl -w net.core.wmem_default=2097152
}

update_controller_config() {
  local nodes="$1"
  local cfg="$2"
  if [[ ! -f "$cfg" ]]; then
    echo "Controller config not found: $cfg" >&2
    return 1
  fi
  sed -i.bak -E "s/(ring_config[[:space:]]*=[[:space:]]*\{[[:space:]]*n_nodes[[:space:]]*=[[:space:]]*)[0-9]+/\1${nodes}/" "$cfg"
  rm -f "${cfg}.bak"
  echo "Updated controller config: n_nodes = ${nodes}"
}

# Parse arguments
while [[ $# -gt 0 ]]; do
  case "$1" in
    --n-nodes)
      n_nodes="${2:-}"
      shift 2
      ;;
    --config)
      config_path="$(resolve_path "${2:-}")"
      shift 2
      ;;
    --compose)
      compose_file="$(resolve_path "${2:-}")"
      shift 2
      ;;
    --log-level)
      log_level="${2:-}"
      shift 2
      ;;
    --session)
      session_name="${2:-}"
      shift 2
      ;;
    --sysctl-only)
      sysctl_only="true"
      shift
      ;;
    --no-monitor)
      monitor_memory="false"
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown option: $1" >&2
      usage
      exit 1
      ;;
  esac
done

# Validate files
if [[ ! -f "$config_path" ]]; then
  echo "Config file not found: $config_path" >&2
  exit 1
fi

if [[ ! -f "$compose_file" ]]; then
  echo "Compose file not found: $compose_file" >&2
  exit 1
fi

# Apply sysctl tuning
apply_sysctl

if [[ "$sysctl_only" == "true" ]]; then
  exit 0
fi

# Update controller config if n_nodes specified
if [[ -n "$n_nodes" ]]; then
  update_controller_config "$n_nodes" "$controller_config"
fi

# Determine node count for memory report
if [[ -n "$n_nodes" ]]; then
  nodes_for_report="$n_nodes"
else
  nodes_for_report="$(awk -F= '/^n_nodes[[:space:]]*=/ {sub(/#.*/, "", $2); gsub(/[[:space:]]/, "", $2); print $2; exit}' "$config_path")"
fi

if [[ ! "$nodes_for_report" =~ ^[0-9]+$ ]]; then
  nodes_for_report="0"
fi

# Check tmux
if ! command -v tmux >/dev/null 2>&1; then
  echo "tmux is required. Install it or use --sysctl-only." >&2
  exit 1
fi

if tmux has-session -t "$session_name" 2>/dev/null; then
  echo "Session exists: $session_name (attach with: tmux attach -t $session_name)" >&2
  exit 1
fi

# Build commands
compose_dir="$(dirname "$compose_file")"
compose_cmd="cd \"$compose_dir\" && docker compose -f \"$compose_file\" up --build"

dataplane_cmd="cd \"$root_dir\" && ulimit -u 20000 && ulimit -n 200000 && cargo build -p nextmini --release"
dataplane_cmd+=" && sudo -E RUST_LOG=\"$log_level\" ./target/release/nextmini --config-path \"$config_path\""
if [[ -n "$n_nodes" ]]; then
  dataplane_cmd+=" --n-nodes \"$n_nodes\""
fi

# Memory monitoring command (appends to report file)
node_count="${nodes_for_report}"
if [[ "$node_count" == "0" ]]; then
  node_count="unknown"
fi
monitor_cmd="echo '' >> \"$memory_report\""
monitor_cmd+=" && echo '========================================' | tee -a \"$memory_report\""
monitor_cmd+=" && echo \"Run: n_nodes=${node_count} at \$(date -Iseconds)\" | tee -a \"$memory_report\""
monitor_cmd+=" && echo '========================================' | tee -a \"$memory_report\""
monitor_cmd+=" && echo 'Waiting for controller...' | tee -a \"$memory_report\""
monitor_cmd+=" && while ! nc -z localhost 3000 2>/dev/null; do sleep 1; done"
monitor_cmd+=" && sleep 2"
monitor_cmd+=" && echo '' | tee -a \"$memory_report\""
monitor_cmd+=" && echo '--- Memory BEFORE nodes connect ---' | tee -a \"$memory_report\""
monitor_cmd+=" && free -h | tee -a \"$memory_report\""
monitor_cmd+=" && mem_before=\$(free -b | awk '/Mem:/ {print \\$3}')"
monitor_cmd+=" && echo '' | tee -a \"$memory_report\""
monitor_cmd+=" && echo 'Waiting for all nodes to connect...' | tee -a \"$memory_report\""
monitor_cmd+=" && wiring_time=''"
monitor_cmd+=" && while read -r line; do"
monitor_cmd+=" if echo \"\$line\" | grep -q 'All dataplane nodes have connected'; then"
monitor_cmd+=" echo \"\$line\" | tee -a \"$memory_report\";"
monitor_cmd+=" wiring_time=\$(echo \"\$line\" | sed -n 's/.*It takes \\([0-9.]*\\) seconds.*/\\1/p');"
monitor_cmd+=" break; fi; done < <(docker logs -f controller 2>&1)"
monitor_cmd+=" && sleep 2"
monitor_cmd+=" && echo '' | tee -a \"$memory_report\""
monitor_cmd+=" && echo '--- Memory AFTER nodes connect ---' | tee -a \"$memory_report\""
monitor_cmd+=" && free -h | tee -a \"$memory_report\""
monitor_cmd+=" && mem_after=\$(free -b | awk '/Mem:/ {print \\$3}')"
monitor_cmd+=" && mem_diff=\$((mem_after - mem_before))"
monitor_cmd+=" && mem_before_mb=\$((mem_before / 1024 / 1024))"
monitor_cmd+=" && mem_after_mb=\$((mem_after / 1024 / 1024))"
monitor_cmd+=" && mem_diff_mb=\$((mem_diff / 1024 / 1024))"
monitor_cmd+=" && echo '' | tee -a \"$memory_report\""
monitor_cmd+=" && echo '--- Summary ---' | tee -a \"$memory_report\""
monitor_cmd+=" && echo \"Memory before: \${mem_before_mb} MB\" | tee -a \"$memory_report\""
monitor_cmd+=" && echo \"Memory after:  \${mem_after_mb} MB\" | tee -a \"$memory_report\""
monitor_cmd+=" && echo \"Difference:    \${mem_diff_mb} MB\" | tee -a \"$memory_report\""
monitor_cmd+=" && echo \"Nodes:         ${node_count}\" | tee -a \"$memory_report\""
monitor_cmd+=" && if [ -n \"\$wiring_time\" ]; then echo \"Wiring time:   \${wiring_time}s\" | tee -a \"$memory_report\";"
monitor_cmd+=" else echo 'Wiring time:   n/a' | tee -a \"$memory_report\"; fi"
monitor_cmd+=" && if [ ${nodes_for_report} -gt 0 ]; then per_node_mb=\$((mem_diff_mb / ${nodes_for_report}));"
monitor_cmd+=" echo \"Per-node:      \${per_node_mb} MB\" | tee -a \"$memory_report\";"
monitor_cmd+=" else echo 'Per-node:      n/a (node count unavailable)' | tee -a \"$memory_report\"; fi"
monitor_cmd+=" && echo '' | tee -a \"$memory_report\""
monitor_cmd+=" && echo 'Done. Report appended to: $memory_report'"
monitor_cmd+=" && echo 'Press Enter to close this pane...'"
monitor_cmd+=" && read -r"

# Launch tmux session
tmux new-session -d -s "$session_name" -n namespace

# Pane 0: Controller (docker compose)
tmux send-keys -t "${session_name}:0.0" "$compose_cmd" C-m

# Pane 1: Dataplane
tmux split-window -h -t "${session_name}:0.0"
tmux send-keys -t "${session_name}:0.1" "$dataplane_cmd" C-m

# Pane 2: Memory monitor (bottom)
if [[ "$monitor_memory" == "true" ]]; then
  tmux split-window -v -t "${session_name}:0.1"
  tmux send-keys -t "${session_name}:0.2" "$monitor_cmd" C-m
fi

tmux select-layout -t "${session_name}:0" tiled
tmux attach -t "$session_name"
