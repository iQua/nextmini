#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
root_dir="$(cd "${script_dir}/../.." && pwd)"
artifacts_root="${script_dir}/artifacts"
controller_bin="${CONTROLLER_BIN:-${root_dir}/target/release/controller}"
dataplane_bin="${NEXTMINI_BIN:-${root_dir}/target/release/nextmini}"
cargo_bin="${CARGO_BIN:-}"
database_container_name="${DATABASE_CONTAINER_NAME:-nextmini-database}"
mettle_paper_scale_symbols_per_block="2400"

# RaptorQ legacy baseline used for namespace sanity checks:
#   --mode fec --trees 2 --receivers 10 --payload-size $((256*1024*1024)) \
#     --block-size $((128*1024)) --symbols-per-block 16
#
# METTLE small-K runs are useful for engineering sweeps, but they are below the
# paper-scale regime. For paper-scale RaptorQ-vs-METTLE comparisons, keep the
# same topology and payload, and use K >= 2400 for both backends.
case_name=""
no_build="false"
mode=""
fec_scheme=""
receivers=""
trees=""
solution_json=""
block_size=""
symbols_per_block=""
mettle_coded_rate_num=""
mettle_coded_rate_den=""
payload_size=""
synthetic_payload="false"
receive_timeout_ms=""
peer_report_timeout_ms=""
packet_processors=""
channel_capacity=""
queue_capacity=""
tc_profile="none"
tc_target="relay-b"
tc_slow_rate_mbit="100"
tc_slow_delay_ms="20"
tc_min_rate_mbit="0"
tree_sweep_max=""
tree_sweep_receivers="20"
receiver_sweep_max=""
receiver_sweep_trees="3"
status_timeout_seconds="300"
original_args=("$@")
current_case_dir=""
current_cpu_sampler_pid=""
current_cpu_case_dir=""
cpu_ceiling_pct="75"
calibration_mode="false"

usage() {
  cat <<'EOF'
Usage: run.sh [options]

Options:
  --case NAME                Run one named case: plain-1r | fec-1r | fec-2r-block | fec-2r-symbols | raptorq-2t-2r-k2400 | mettle-2t-2r-k2400.
  --mode MODE                Custom run/sweep mode: plain | fec (default for custom runs: fec).
  --fec-scheme SCHEME        FEC backend for custom/sweep runs: raptorq | mettle (default: raptorq).
  --receivers N              Custom run receiver count.
  --trees N                  Custom run tree count.
  --solution-json PATH       Use solver solution.json trees for a custom FEC run.
  --block-size N             Custom run block size (default: 8192).
  --symbols-per-block N      Custom run symbols_per_block (default: 32).
  --mettle-coded-rate-num N  METTLE finite-stream coded-rate numerator (default: 1).
  --mettle-coded-rate-den N  METTLE finite-stream coded-rate denominator (default: 1).
  --payload-size N           Custom run payload size bytes (default: 262144).
  --synthetic-payload        Use deterministic in-dataplane payload generation and skip large artifact files.
  --receive-timeout-ms N     Session completion timeout in ms (default: 120000).
  --peer-report-timeout-ms N Sender feedback timeout after SourceDone in ms (default: 15000).
  --packet-processors N      Dataplane packet processor lanes (default: 1).
  --channel-capacity N       Dataplane channel capacity (default: 2048).
  --queue-capacity N         Dataplane queue capacity (default: 2048).
  --tc-profile PROFILE       Namespace traffic-control profile: none | tree-skew | solution-edge-rates | solution-two-edge-rates (default: none).
  --tc-target TARGET         Tree node to shape for tree-skew: relay-a | relay-b (default: relay-b).
  --tc-slow-rate-mbit N      Rate limit for shaped tree veths in Mbit/s (default: 100).
  --tc-slow-delay-ms N       Added one-way delay for shaped tree veths in ms (default: 20).
  --tc-min-rate-mbit N       Minimum per-edge rate for solution-* tc profiles in Mbit/s (default: 0).
  --tree-sweep-max N         Run a sweep from 1..N trees with fixed receivers.
  --tree-sweep-receivers N   Receiver count for tree sweep (default: 20).
  --receiver-sweep-max N     Run a sweep from 1..N receivers with fixed trees.
  --receiver-sweep-trees N   Tree count for receiver sweep (default: 3).
  --status-timeout-seconds N Seconds to wait for status files per case (default: 300).
  --cpu-ceiling-pct N       Mark a run invalid above this host CPU utilization (default: 75).
  --calibration             Run the requested scenario unshaped to record max-throughput capability.
  --no-build                 Skip cargo build and use the existing binaries.
  -h, --help                 Show this help.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --case)
      case_name="${2:-}"
      shift 2
      ;;
    --mode)
      mode="${2:-}"
      shift 2
      ;;
    --fec-scheme)
      fec_scheme="${2:-}"
      shift 2
      ;;
    --receivers)
      receivers="${2:-}"
      shift 2
      ;;
    --trees)
      trees="${2:-}"
      shift 2
      ;;
    --solution-json)
      solution_json="${2:-}"
      shift 2
      ;;
    --block-size)
      block_size="${2:-}"
      shift 2
      ;;
    --symbols-per-block)
      symbols_per_block="${2:-}"
      shift 2
      ;;
    --mettle-coded-rate-num)
      mettle_coded_rate_num="${2:-}"
      shift 2
      ;;
    --mettle-coded-rate-den)
      mettle_coded_rate_den="${2:-}"
      shift 2
      ;;
    --payload-size)
      payload_size="${2:-}"
      shift 2
      ;;
    --synthetic-payload)
      synthetic_payload="true"
      shift
      ;;
    --receive-timeout-ms)
      receive_timeout_ms="${2:-}"
      shift 2
      ;;
    --peer-report-timeout-ms)
      peer_report_timeout_ms="${2:-}"
      shift 2
      ;;
    --packet-processors)
      packet_processors="${2:-}"
      shift 2
      ;;
    --channel-capacity)
      channel_capacity="${2:-}"
      shift 2
      ;;
    --queue-capacity)
      queue_capacity="${2:-}"
      shift 2
      ;;
    --tc-profile)
      tc_profile="${2:-}"
      shift 2
      ;;
    --tc-target)
      tc_target="${2:-}"
      shift 2
      ;;
    --tc-slow-rate-mbit)
      tc_slow_rate_mbit="${2:-}"
      shift 2
      ;;
    --tc-slow-delay-ms)
      tc_slow_delay_ms="${2:-}"
      shift 2
      ;;
    --tc-min-rate-mbit)
      tc_min_rate_mbit="${2:-}"
      shift 2
      ;;
    --tree-sweep-max)
      tree_sweep_max="${2:-}"
      shift 2
      ;;
    --tree-sweep-receivers)
      tree_sweep_receivers="${2:-}"
      shift 2
      ;;
    --receiver-sweep-max)
      receiver_sweep_max="${2:-}"
      shift 2
      ;;
    --receiver-sweep-trees)
      receiver_sweep_trees="${2:-}"
      shift 2
      ;;
    --status-timeout-seconds)
      status_timeout_seconds="${2:-}"
      shift 2
      ;;
    --cpu-ceiling-pct)
      cpu_ceiling_pct="${2:-}"
      shift 2
      ;;
    --calibration)
      calibration_mode="true"
      shift
      ;;
    --no-build)
      no_build="true"
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

if [[ "$(uname -s)" != "Linux" ]]; then
  echo "run.sh requires Linux network namespaces." >&2
  exit 1
fi

if [[ "${EUID:-$(id -u)}" -ne 0 ]]; then
  exec sudo -E "$0" "${original_args[@]}"
fi

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

require_positive_int() {
  local label="$1"
  local value="$2"

  if [[ ! "$value" =~ ^[0-9]+$ ]] || (( value <= 0 )); then
    echo "${label} must be a positive integer." >&2
    exit 1
  fi
}

require_nonnegative_int() {
  local label="$1"
  local value="$2"

  if [[ ! "$value" =~ ^[0-9]+$ ]]; then
    echo "${label} must be a non-negative integer." >&2
    exit 1
  fi
}

validate_mode() {
  local selected_mode="$1"

  case "$selected_mode" in
    plain|fec) ;;
    *)
      echo "--mode must be either plain or fec." >&2
      exit 1
      ;;
  esac
}

validate_tc_options() {
  case "${tc_profile:-none}" in
    none|tree-skew|solution-edge-rates|solution-two-edge-rates) ;;
    *)
      echo "--tc-profile must be none, tree-skew, solution-edge-rates, or solution-two-edge-rates." >&2
      exit 1
      ;;
  esac

  case "${tc_target:-relay-b}" in
    relay-a|relay-b) ;;
    *)
      echo "--tc-target must be either relay-a or relay-b." >&2
      exit 1
      ;;
  esac

  require_positive_int "--tc-slow-rate-mbit" "${tc_slow_rate_mbit:-100}"
  require_nonnegative_int "--tc-slow-delay-ms" "${tc_slow_delay_ms:-20}"
  require_nonnegative_int "--tc-min-rate-mbit" "${tc_min_rate_mbit:-0}"

  if [[ "${tc_profile:-none}" == solution-* && -z "${solution_json:-}" ]]; then
    echo "--tc-profile ${tc_profile} requires --solution-json." >&2
    exit 1
  fi
}

validate_fec_scheme() {
  local selected_fec_scheme="$1"

  case "$selected_fec_scheme" in
    raptorq|mettle) ;;
    *)
      echo "--fec-scheme must be either raptorq or mettle." >&2
      exit 1
      ;;
  esac
}

validate_run_request() {
  local selected_mode="$1"
  local selected_fec_scheme="$2"
  local selected_receivers="$3"
  local selected_trees="$4"
  local selected_block_size="$5"
  local selected_symbols_per_block="$6"
  local selected_payload_size="$7"
  local selected_receive_timeout_ms="$8"
  local selected_packet_processors="$9"
  local selected_channel_capacity="${10}"
  local selected_queue_capacity="${11}"

  validate_mode "$selected_mode"
  validate_fec_scheme "$selected_fec_scheme"
  require_positive_int "--receivers" "$selected_receivers"
  require_positive_int "--trees" "$selected_trees"
  require_positive_int "--block-size" "$selected_block_size"
  require_positive_int "--symbols-per-block" "$selected_symbols_per_block"
  require_positive_int "--mettle-coded-rate-num" "${mettle_coded_rate_num:-1}"
  require_positive_int "--mettle-coded-rate-den" "${mettle_coded_rate_den:-1}"
  if (( ${mettle_coded_rate_num:-1} < ${mettle_coded_rate_den:-1} )); then
    echo "--mettle-coded-rate-num must be >= --mettle-coded-rate-den." >&2
    exit 1
  fi
  require_positive_int "--payload-size" "$selected_payload_size"
  require_positive_int "--receive-timeout-ms" "$selected_receive_timeout_ms"
  require_positive_int "--packet-processors" "$selected_packet_processors"
  require_positive_int "--channel-capacity" "$selected_channel_capacity"
  require_positive_int "--queue-capacity" "$selected_queue_capacity"

  if [[ "$selected_mode" == "plain" ]] && (( selected_trees != 1 )); then
    echo "plain mode only supports exactly one tree." >&2
    exit 1
  fi
}

make_case_name() {
  local prefix="$1"
  local selected_mode="$2"
  local selected_fec_scheme="$3"
  local selected_receivers="$4"
  local selected_trees="$5"
  local selected_block_size="$6"
  local selected_symbols_per_block="$7"
  local selected_packet_processors="$8"
  local selected_channel_capacity="$9"
  local selected_queue_capacity="${10}"
  local mode_label="$selected_mode"

  if [[ "$selected_mode" == "fec" ]]; then
    mode_label="${selected_mode}-${selected_fec_scheme}"
  fi

  printf '%s-%s-%st-%sr-b%s-s%s-p%s-c%s-q%s' \
    "$prefix" \
    "$mode_label" \
    "$selected_trees" \
    "$selected_receivers" \
    "$selected_block_size" \
    "$selected_symbols_per_block" \
    "$selected_packet_processors" \
    "$selected_channel_capacity" \
    "$selected_queue_capacity"

  if [[ "${tc_profile:-none}" != "none" ]]; then
    printf -- '-tc%s-%s-%sm-%sms' \
      "$tc_profile" \
      "$tc_target" \
      "$tc_slow_rate_mbit" \
      "$tc_slow_delay_ms"
    if [[ "${tc_min_rate_mbit:-0}" != "0" ]]; then
      printf -- '-floor%sm' "$tc_min_rate_mbit"
    fi
  fi
  if [[ "$calibration_mode" == "true" ]]; then
    printf -- '-calibration'
  fi
}

validate_solution_json() {
  if [[ -z "${solution_json:-}" ]]; then
    return 0
  fi

  if [[ ! -f "$solution_json" ]]; then
    echo "--solution-json not found: ${solution_json}" >&2
    exit 1
  fi
}

build_binaries() {
  if [[ "$no_build" == "true" ]]; then
    return
  fi

  local cargo_prefix=()
  local resolved_cargo_bin="$cargo_bin"

  if [[ "${EUID:-$(id -u)}" -eq 0 && -n "${SUDO_USER:-}" && "${SUDO_USER}" != "root" ]]; then
    local invoking_home=""
    local invoking_cargo_home=""
    local invoking_rustup_home=""

    invoking_home="$(resolve_user_home "$SUDO_USER" || true)"
    if [[ -z "$invoking_home" ]]; then
      echo "Could not determine the home directory for sudo user ${SUDO_USER}." >&2
      echo "Build the binaries manually, then rerun with --no-build." >&2
      exit 1
    fi

    invoking_cargo_home="${CARGO_HOME:-${invoking_home}/.cargo}"
    invoking_rustup_home="${RUSTUP_HOME:-${invoking_home}/.rustup}"
    resolved_cargo_bin="${resolved_cargo_bin:-${invoking_cargo_home}/bin/cargo}"
    cargo_prefix=(
      sudo -u "$SUDO_USER"
      env
      "HOME=${invoking_home}"
      "CARGO_HOME=${invoking_cargo_home}"
      "RUSTUP_HOME=${invoking_rustup_home}"
      "PATH=${invoking_cargo_home}/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
    )
  else
    resolved_cargo_bin="${resolved_cargo_bin:-$(command -v cargo 2>/dev/null || true)}"
  fi

  if [[ -z "$resolved_cargo_bin" || ! -x "$resolved_cargo_bin" ]]; then
    echo "cargo was not found for the current build context." >&2
    echo "Run this script without sudo so it can re-exec itself after building, or build manually and rerun with --no-build." >&2
    exit 1
  fi

  (
    cd "$root_dir"
    "${cargo_prefix[@]}" "$resolved_cargo_bin" build -p controller --release
    "${cargo_prefix[@]}" "$resolved_cargo_bin" build -p nextmini --release --features python-extension
  )
}

wait_for_port() {
  local host="$1"
  local port="$2"
  local pid="${3:-}"
  local label="${4:-service}"
  local deadline=$((SECONDS + 30))
  while (( SECONDS < deadline )); do
    if port_is_open "$host" "$port"; then
      return 0
    fi
    if [[ -n "$pid" ]] && ! kill -0 "$pid" >/dev/null 2>&1; then
      echo "${label} exited before ${host}:${port} became reachable." >&2
      return 1
    fi
    sleep 1
  done
  return 1
}

port_is_open() {
  local host="$1"
  local port="$2"
  (echo >"/dev/tcp/${host}/${port}") >/dev/null 2>&1
}

assert_port_available() {
  local host="$1"
  local port="$2"

  if port_is_open "$host" "$port"; then
    echo "Required port ${host}:${port} is already in use." >&2
    echo "Stop the conflicting service or container before running ns-lossless." >&2
    return 1
  fi
}

report_case_logs() {
  local case_dir="$1"

  if [[ -f "${case_dir}/controller.log" ]]; then
    echo "Controller log tail:" >&2
    tail -n 40 "${case_dir}/controller.log" >&2 || true
  fi

  if [[ -f "${case_dir}/dataplane.log" ]]; then
    echo "Dataplane log tail:" >&2
    tail -n 40 "${case_dir}/dataplane.log" >&2 || true
  fi
}

ensure_database() {
  if port_is_open 127.0.0.1 5432; then
    return 0
  fi

  (
    cd "$root_dir"
    bash utils/start-database.sh
  )

  if wait_for_port 127.0.0.1 5432 "" "Postgres"; then
    return 0
  fi

  if command -v docker >/dev/null 2>&1; then
    if docker ps -a --format '{{.Names}}' | grep -qx "$database_container_name"; then
      if ! docker port "$database_container_name" 5432 >/dev/null 2>&1; then
        echo "Database container '$database_container_name' is not publishing host port 5432." >&2
        echo "Recreate that container so the controller can reach 127.0.0.1:5432." >&2
        exit 1
      fi
    fi
  fi

  echo "Postgres did not become reachable on 127.0.0.1:5432." >&2
  exit 1
}

generate_case() {
  local case_dir="$1"
  shift
  python3 "${script_dir}/generate.py" --out-dir "$case_dir" "$@"
}

start_controller() {
  local case_dir="$1"
  local controller_pid=""

  assert_port_available 127.0.0.1 3000
  mkdir -p "${case_dir}/controller-run"
  cp "${case_dir}/controller-config.toml" "${case_dir}/controller-run/config.toml"
  echo "Starting controller for case $(basename "$case_dir")."
  (
    cd "${case_dir}/controller-run"
    RUST_LOG="${RUST_LOG:-info}" "$controller_bin" >"${case_dir}/controller.log" 2>&1 &
    echo $! >"${case_dir}/controller.pid"
  )
  controller_pid="$(cat "${case_dir}/controller.pid")"

  if ! wait_for_port 127.0.0.1 3000 "$controller_pid" "controller"; then
    echo "Controller failed to bind 127.0.0.1:3000 for case ${case_dir}." >&2
    report_case_logs "$case_dir"
    exit 1
  fi
}

start_dataplane() {
  local case_dir="$1"
  echo "Starting dataplane for case $(basename "$case_dir")."
  RUST_LOG="${RUST_LOG:-info}" "$dataplane_bin" --config-path "${case_dir}/dataplane-config.toml" \
    >"${case_dir}/dataplane.log" 2>&1 &
  echo $! >"${case_dir}/dataplane.pid"
}

stop_case() {
  local case_dir="$1"
  local config_path="${case_dir}/dataplane-config.toml"
  stop_cpu_monitor "$case_dir"
  if [[ -f "${case_dir}/dataplane.pid" ]]; then
    kill "$(cat "${case_dir}/dataplane.pid")" >/dev/null 2>&1 || true
    wait "$(cat "${case_dir}/dataplane.pid")" 2>/dev/null || true
    rm -f "${case_dir}/dataplane.pid"
  fi
  if [[ -f "${case_dir}/controller.pid" ]]; then
    kill "$(cat "${case_dir}/controller.pid")" >/dev/null 2>&1 || true
    wait "$(cat "${case_dir}/controller.pid")" 2>/dev/null || true
    rm -f "${case_dir}/controller.pid"
  fi
  while IFS= read -r pid; do
    [[ -z "$pid" ]] && continue
    kill "$pid" >/dev/null 2>&1 || true
    wait "$pid" 2>/dev/null || true
  done < <(
    ps -eo pid=,args= | awk -v bin="$dataplane_bin" -v cfg="$config_path" '
      index($0, bin " --config-path " cfg) { print $1 }
    '
  )
  bash "${script_dir}/cleanup.sh" --config "${case_dir}/dataplane-config.toml" >/dev/null 2>&1 || true
}

start_cpu_monitor() {
  local case_dir="$1"
  local samples_path="${case_dir}/cpu-utilization.csv"

  python3 "${script_dir}/tools/cpu_monitor.py" sample --output "$samples_path" &
  current_cpu_sampler_pid=$!
  current_cpu_case_dir="$case_dir"
  printf '%s\n' "$current_cpu_sampler_pid" >"${case_dir}/cpu-monitor.pid"
}

stop_cpu_monitor() {
  local case_dir="$1"
  if [[ -z "$current_cpu_sampler_pid" || "$current_cpu_case_dir" != "$case_dir" ]]; then
    return 0
  fi

  kill "$current_cpu_sampler_pid" >/dev/null 2>&1 || true
  wait "$current_cpu_sampler_pid" 2>/dev/null || true
  rm -f "${case_dir}/cpu-monitor.pid"
  current_cpu_sampler_pid=""
  current_cpu_case_dir=""

  local summarize_args=(
    summarize "${case_dir}/cpu-utilization.csv"
    --ceiling-pct "$cpu_ceiling_pct"
    --ledger "${case_dir}/run-ledger.json"
    --metrics-dir "${case_dir}/artifacts"
  )
  if [[ "$calibration_mode" == "true" ]]; then
    summarize_args+=(--calibration)
  fi
  python3 "${script_dir}/tools/cpu_monitor.py" "${summarize_args[@]}" \
    >"${case_dir}/cpu-validity.json"
}

wait_for_veth_devices() {
  local n_nodes="$1"
  local deadline=$((SECONDS + 30))

  while (( SECONDS < deadline )); do
    local missing="false"
    for ((idx = 0; idx < n_nodes; idx++)); do
      if [[ ! -e "/sys/class/net/veth${idx}a" ]]; then
        missing="true"
        break
      fi
    done

    if [[ "$missing" == "false" ]]; then
      return 0
    fi

    sleep 0.1
  done

  echo "Timed out waiting for namespace veth devices." >&2
  return 1
}

find_node_namespace_pid() {
  local dataplane_parent_pid="$1"
  local node_id="$2"
  local target_ip="172.16.8.$((node_id + 1))"
  local deadline=$((SECONDS + 30))

  while (( SECONDS < deadline )); do
    local child_pid
    while IFS= read -r child_pid; do
      [[ -z "$child_pid" ]] && continue
      if nsenter -t "$child_pid" -n ip -o -4 addr show 2>/dev/null |
        grep -q " ${target_ip}/"; then
        printf '%s\n' "$child_pid"
        return 0
      fi
    done < <(pgrep -P "$dataplane_parent_pid" 2>/dev/null || true)
    sleep 0.1
  done

  echo "Could not find network namespace for node ${node_id}." >&2
  return 1
}

apply_tc_profile() {
  local case_dir="$1"
  local receivers="$2"
  local trees="$3"

  if [[ "${tc_profile:-none}" == "none" ]]; then
    return 0
  fi

  if ! command -v tc >/dev/null 2>&1; then
    echo "--tc-profile requires iproute2 'tc'." >&2
    return 1
  fi

  local n_nodes
  n_nodes="$(awk -F'= *' '/^n_nodes =/{print $2; exit}' "${case_dir}/dataplane-config.toml" | tr -d '[:space:]')"
  if [[ -z "$n_nodes" ]]; then
    n_nodes=$((1 + (trees * 2) + receivers))
  fi
  wait_for_veth_devices "$n_nodes"

  local tc_log="${case_dir}/tc-profile.txt"
  {
    echo "profile=${tc_profile}"
    echo "target=${tc_target}"
    echo "slow_rate_mbit=${tc_slow_rate_mbit}"
    echo "slow_delay_ms=${tc_slow_delay_ms}"
    echo "min_rate_mbit=${tc_min_rate_mbit}"
  } >"$tc_log"

  case "$tc_profile" in
    tree-skew)
      for ((tree_idx = 0; tree_idx < trees; tree_idx++)); do
        if (( tree_idx % 2 == 0 )); then
          continue
        fi

        local node_id
        case "$tc_target" in
          relay-a) node_id=$((2 + (tree_idx * 2))) ;;
          relay-b) node_id=$((3 + (tree_idx * 2))) ;;
        esac

        local dev="veth$((node_id - 1))a"
        echo "tree=${tree_idx} node_id=${node_id} dev=${dev}" >>"$tc_log"
        tc qdisc del dev "$dev" root >/dev/null 2>&1 || true
        if (( tc_slow_delay_ms > 0 )); then
          tc qdisc replace dev "$dev" root netem delay "${tc_slow_delay_ms}ms" rate "${tc_slow_rate_mbit}mbit"
        else
          tc qdisc replace dev "$dev" root netem rate "${tc_slow_rate_mbit}mbit"
        fi
        tc qdisc show dev "$dev" >>"$tc_log"
      done
      ;;
    solution-edge-rates|solution-two-edge-rates)
      local edge_file="${case_dir}/tc-solution-edges.tsv"
      local node_budget_file="${case_dir}/tc-node-budgets.tsv"
      local node_budget_edge_file="${case_dir}/tc-node-budget-edges.tsv"
      python3 "${script_dir}/tools/solution_edges.py" \
        "$solution_json" \
        --profile "$tc_profile" \
        --min-rate-mbit "${tc_min_rate_mbit:-0}" \
        --node-budgets-output "$node_budget_file" \
        --node-budget-edges-output "$node_budget_edge_file" \
        >"$edge_file"

      {
        echo "solution_json=${solution_json}"
        echo "edge_file=${edge_file}"
        echo "node_budget_file=${node_budget_file}"
        echo "node_budget_edge_file=${node_budget_edge_file}"
      } >>"$tc_log"

      if [[ -s "$node_budget_file" ]]; then
        local dataplane_parent_pid
        dataplane_parent_pid="$(cat "${case_dir}/dataplane.pid")"
        local budget_id direction node_id dev budget_kbit budget_name
        while IFS=$'\t' read -r budget_id direction node_id dev budget_kbit budget_name; do
          [[ -z "$budget_id" ]] && continue
          local tc_prefix=()
          local namespace_pid=""
          if [[ "$direction" == "egress" ]]; then
            namespace_pid="$(find_node_namespace_pid "$dataplane_parent_pid" "$node_id")"
            tc_prefix=(nsenter -t "$namespace_pid" -n tc)
          else
            tc_prefix=(tc)
          fi

          "${tc_prefix[@]}" qdisc del dev "$dev" root >/dev/null 2>&1 || true
          "${tc_prefix[@]}" qdisc replace dev "$dev" root handle 1: htb default ffff
          "${tc_prefix[@]}" class replace dev "$dev" parent 1: classid 1:1 \
            htb rate "${budget_kbit}kbit" ceil "${budget_kbit}kbit"
          "${tc_prefix[@]}" class replace dev "$dev" parent 1: classid 1:ffff \
            htb rate 100000mbit ceil 100000mbit

          local class_id=10
          local leaf_budget_id src_ip dst_ip guaranteed_kbit ceil_kbit src_node dst_node delay_ms jitter_ms loss_pct
          while IFS=$'\t' read -r leaf_budget_id src_ip dst_ip guaranteed_kbit ceil_kbit src_node dst_node delay_ms jitter_ms loss_pct; do
            [[ "$leaf_budget_id" != "$budget_id" ]] && continue
            local class_hex
            class_hex="$(printf '%x' "$class_id")"
            "${tc_prefix[@]}" class replace dev "$dev" parent 1:1 \
              classid "1:${class_hex}" htb rate "${guaranteed_kbit}kbit" ceil "${ceil_kbit}kbit"
            "${tc_prefix[@]}" filter add dev "$dev" parent 1: protocol ip prio "$class_id" u32 \
              match ip src "${src_ip}/32" \
              match ip dst "${dst_ip}/32" \
              flowid "1:${class_hex}"
            if [[ "$delay_ms" != "0" || "$jitter_ms" != "0" || "$loss_pct" != "0" ]]; then
              local netem_args=()
              if [[ "$delay_ms" != "0" || "$jitter_ms" != "0" ]]; then
                netem_args+=(delay "${delay_ms}ms")
                if [[ "$jitter_ms" != "0" ]]; then
                  netem_args+=("${jitter_ms}ms")
                fi
              fi
              if [[ "$loss_pct" != "0" ]]; then
                netem_args+=(loss "${loss_pct}%")
              fi
              "${tc_prefix[@]}" qdisc replace dev "$dev" parent "1:${class_hex}" \
                handle "${class_hex}:" netem "${netem_args[@]}"
            fi
            printf 'budget_edge=%s edge=%s->%s direction=%s dev=%s src_ip=%s dst_ip=%s guaranteed_kbit=%s ceil_kbit=%s class=1:%s delay_ms=%s jitter_ms=%s loss_pct=%s\n' \
              "$budget_id" "$src_node" "$dst_node" "$direction" "$dev" "$src_ip" \
              "$dst_ip" "$guaranteed_kbit" "$ceil_kbit" "$class_hex" "$delay_ms" "$jitter_ms" "$loss_pct" \
              >>"$tc_log"
            class_id=$((class_id + 1))
          done <"$node_budget_edge_file"

          {
            echo "budget=${budget_id} name=${budget_name} direction=${direction} node=${node_id} dev=${dev} rate_kbit=${budget_kbit} namespace_pid=${namespace_pid:-host}"
            "${tc_prefix[@]}" qdisc show dev "$dev"
            "${tc_prefix[@]}" class show dev "$dev"
            "${tc_prefix[@]}" filter show dev "$dev" parent 1:
          } >>"$tc_log"
        done <"$node_budget_file"
      else
        local shaped_devs
        shaped_devs="$(cut -f1 "$edge_file" | sort -u)"
        while IFS= read -r dev; do
          [[ -z "$dev" ]] && continue
          tc qdisc del dev "$dev" root >/dev/null 2>&1 || true
          tc qdisc replace dev "$dev" root handle 1: htb default ffff
          tc class replace dev "$dev" parent 1: classid 1:ffff htb rate 10000mbit ceil 10000mbit
        done <<<"$shaped_devs"

        local class_id=10
        local dev src_ip dst_ip bw_kbit src_node dst_node delay_ms jitter_ms loss_pct
        while IFS=$'\t' read -r dev src_ip dst_ip bw_kbit src_node dst_node delay_ms jitter_ms loss_pct; do
          [[ -z "$dev" ]] && continue
          local class_hex
          class_hex="$(printf '%x' "$class_id")"
          tc class replace dev "$dev" parent 1: classid "1:${class_hex}" htb rate "${bw_kbit}kbit" ceil "${bw_kbit}kbit"
          tc filter add dev "$dev" parent 1: protocol ip prio "$class_id" u32 \
            match ip src "${src_ip}/32" \
            match ip dst "${dst_ip}/32" \
            flowid "1:${class_hex}"
          if [[ "$delay_ms" != "0" || "$jitter_ms" != "0" || "$loss_pct" != "0" ]]; then
            local netem_args=()
            if [[ "$delay_ms" != "0" || "$jitter_ms" != "0" ]]; then
              netem_args+=(delay "${delay_ms}ms")
              if [[ "$jitter_ms" != "0" ]]; then
                netem_args+=("${jitter_ms}ms")
              fi
            fi
            if [[ "$loss_pct" != "0" ]]; then
              netem_args+=(loss "${loss_pct}%")
            fi
            tc qdisc replace dev "$dev" parent "1:${class_hex}" handle "${class_hex}:" netem "${netem_args[@]}"
          fi
          printf 'edge=%s->%s dev=%s src_ip=%s dst_ip=%s rate_kbit=%s class=1:%s delay_ms=%s jitter_ms=%s loss_pct=%s\n' \
            "$src_node" "$dst_node" "$dev" "$src_ip" "$dst_ip" "$bw_kbit" "$class_hex" \
            "$delay_ms" "$jitter_ms" "$loss_pct" >>"$tc_log"
          class_id=$((class_id + 1))
        done <"$edge_file"
        while IFS= read -r dev; do
          [[ -z "$dev" ]] && continue
          {
            tc qdisc show dev "$dev"
            tc class show dev "$dev"
            tc filter show dev "$dev" parent 1:
          } >>"$tc_log"
        done <<<"$shaped_devs"
      fi
      ;;
  esac
}

cleanup_on_exit() {
  if [[ -n "$current_case_dir" ]]; then
    stop_case "$current_case_dir"
  fi
}

wait_for_statuses() {
  local case_dir="$1"
  local expected_receivers="$2"
  local artifact_dir="${case_dir}/artifacts"
  local controller_pid=""
  local dataplane_pid=""
  local deadline=$((SECONDS + status_timeout_seconds))

  if [[ -f "${case_dir}/controller.pid" ]]; then
    controller_pid="$(cat "${case_dir}/controller.pid")"
  fi
  if [[ -f "${case_dir}/dataplane.pid" ]]; then
    dataplane_pid="$(cat "${case_dir}/dataplane.pid")"
  fi

  echo "Waiting for source and ${expected_receivers} receiver status file(s)."
  while (( SECONDS < deadline )); do
    if [[ -n "$controller_pid" ]] && ! kill -0 "$controller_pid" >/dev/null 2>&1; then
      echo "Controller exited before the case completed." >&2
      report_case_logs "$case_dir"
      return 1
    fi
    if [[ -n "$dataplane_pid" ]] && ! kill -0 "$dataplane_pid" >/dev/null 2>&1; then
      echo "Dataplane exited before the case completed." >&2
      report_case_logs "$case_dir"
      return 1
    fi

    if [[ -f "${artifact_dir}/source-1.status" ]]; then
      local ready_count
      ready_count="$(find "$artifact_dir" -maxdepth 1 -name 'receiver-*.status' | wc -l | tr -d '[:space:]')"
      if [[ "$ready_count" == "$expected_receivers" ]]; then
        return 0
      fi
    fi
    sleep 1
  done

  echo "Timed out waiting for case status files in ${artifact_dir}." >&2
  report_case_logs "$case_dir"
  return 1
}

assert_status_ok() {
  local path="$1"
  if [[ ! -f "$path" ]]; then
    echo "Missing status file: $path" >&2
    return 1
  fi
  if ! grep -qx 'ok' "$path"; then
    echo "Non-success status in $path:" >&2
    cat "$path" >&2
    return 1
  fi
}

run_case() {
  local name="$1"
  local mode="$2"
  local selected_fec_scheme="$3"
  local receivers="$4"
  local trees="$5"
  local block_size="$6"
  local symbols_per_block="$7"
  local payload_size="$8"
  local receive_timeout_ms="$9"
  local selected_packet_processors="${10}"
  local selected_channel_capacity="${11}"
  local selected_queue_capacity="${12}"
  local selected_peer_report_timeout_ms="${13:-15000}"
  local case_dir="${artifacts_root}/${name}"

  current_case_dir="$case_dir"
  rm -rf "$case_dir"
  mkdir -p "$case_dir"
  echo "Preparing case ${name}."

  local generate_args=(
    "$case_dir" \
    --case-name "$name" \
    --mode "$mode" \
    --fec-scheme "$selected_fec_scheme" \
    --receivers "$receivers" \
    --trees "$trees" \
    --block-size "$block_size" \
    --symbols-per-block "$symbols_per_block" \
    --mettle-coded-rate-num "${mettle_coded_rate_num:-1}" \
    --mettle-coded-rate-den "${mettle_coded_rate_den:-1}" \
    --payload-size "$payload_size" \
    --receive-timeout-ms "$receive_timeout_ms" \
    --peer-report-timeout-ms "$selected_peer_report_timeout_ms" \
    --packet-processors "$selected_packet_processors" \
    --channel-capacity "$selected_channel_capacity" \
    --queue-capacity "$selected_queue_capacity"
  )
  if [[ -n "${solution_json:-}" ]]; then
    generate_args+=(--solution-json "$solution_json")
  fi
  if [[ "$synthetic_payload" == "true" ]]; then
    generate_args+=(--synthetic-payload)
  fi
  generate_case "${generate_args[@]}"
  if [[ -n "${solution_json:-}" ]]; then
    python3 "${script_dir}/tools/check_laminarity.py" \
      "$solution_json" \
      --output "${case_dir}/laminarity.json" \
      --ledger "${case_dir}/run-ledger.json" \
      >/dev/null
  fi

  start_controller "$case_dir"
  start_cpu_monitor "$case_dir"
  start_dataplane "$case_dir"
  apply_tc_profile "$case_dir" "$receivers" "$trees"
  wait_for_statuses "$case_dir" "$receivers"
  stop_cpu_monitor "$case_dir"

  assert_status_ok "${case_dir}/artifacts/source-1.status"
  while IFS= read -r status_file; do
    [[ -z "$status_file" ]] && continue
    assert_status_ok "$status_file"
  done < <(find "${case_dir}/artifacts" -maxdepth 1 -name 'receiver-*.status' | sort)

  echo "Verifying hashes for case ${name}."
  local verify_args=("${case_dir}/artifacts")
  if [[ "$synthetic_payload" == "true" ]]; then
    verify_args+=(--synthetic)
  fi
  python3 "${script_dir}/verify_hashes.py" "${verify_args[@]}"
  stop_case "$case_dir"
  current_case_dir=""
  echo "Case ${name} completed successfully."
}

run_tree_sweep() {
  local max_trees="$1"
  local selected_receivers="$2"
  local selected_mode="$3"
  local selected_fec_scheme="$4"
  local selected_block_size="$5"
  local selected_symbols_per_block="$6"
  local selected_payload_size="$7"
  local selected_receive_timeout_ms="$8"
  local selected_packet_processors="$9"
  local selected_channel_capacity="${10}"
  local selected_queue_capacity="${11}"

  require_positive_int "--tree-sweep-max" "$max_trees"
  require_positive_int "--tree-sweep-receivers" "$selected_receivers"

  for ((tree_count = 1; tree_count <= max_trees; tree_count++)); do
    validate_run_request \
      "$selected_mode" \
      "$selected_fec_scheme" \
      "$selected_receivers" \
      "$tree_count" \
      "$selected_block_size" \
      "$selected_symbols_per_block" \
      "$selected_payload_size" \
      "$selected_receive_timeout_ms" \
      "$selected_packet_processors" \
      "$selected_channel_capacity" \
      "$selected_queue_capacity"

    run_case \
      "$(make_case_name tree-sweep "$selected_mode" "$selected_fec_scheme" "$selected_receivers" "$tree_count" "$selected_block_size" "$selected_symbols_per_block" "$selected_packet_processors" "$selected_channel_capacity" "$selected_queue_capacity")" \
      "$selected_mode" \
      "$selected_fec_scheme" \
      "$selected_receivers" \
      "$tree_count" \
      "$selected_block_size" \
      "$selected_symbols_per_block" \
      "$selected_payload_size" \
      "$selected_receive_timeout_ms" \
      "$selected_packet_processors" \
      "$selected_channel_capacity" \
      "$selected_queue_capacity"
  done
}

run_receiver_sweep() {
  local max_receivers="$1"
  local selected_trees="$2"
  local selected_mode="$3"
  local selected_fec_scheme="$4"
  local selected_block_size="$5"
  local selected_symbols_per_block="$6"
  local selected_payload_size="$7"
  local selected_receive_timeout_ms="$8"
  local selected_packet_processors="$9"
  local selected_channel_capacity="${10}"
  local selected_queue_capacity="${11}"

  require_positive_int "--receiver-sweep-max" "$max_receivers"
  require_positive_int "--receiver-sweep-trees" "$selected_trees"

  for ((receiver_count = 1; receiver_count <= max_receivers; receiver_count++)); do
    validate_run_request \
      "$selected_mode" \
      "$selected_fec_scheme" \
      "$receiver_count" \
      "$selected_trees" \
      "$selected_block_size" \
      "$selected_symbols_per_block" \
      "$selected_payload_size" \
      "$selected_receive_timeout_ms" \
      "$selected_packet_processors" \
      "$selected_channel_capacity" \
      "$selected_queue_capacity"

    run_case \
      "$(make_case_name receiver-sweep "$selected_mode" "$selected_fec_scheme" "$receiver_count" "$selected_trees" "$selected_block_size" "$selected_symbols_per_block" "$selected_packet_processors" "$selected_channel_capacity" "$selected_queue_capacity")" \
      "$selected_mode" \
      "$selected_fec_scheme" \
      "$receiver_count" \
      "$selected_trees" \
      "$selected_block_size" \
      "$selected_symbols_per_block" \
      "$selected_payload_size" \
      "$selected_receive_timeout_ms" \
      "$selected_packet_processors" \
      "$selected_channel_capacity" \
      "$selected_queue_capacity"
  done
}

trap cleanup_on_exit EXIT

require_positive_int "--status-timeout-seconds" "$status_timeout_seconds"
if [[ ! "$cpu_ceiling_pct" =~ ^([0-9]+([.][0-9]*)?|[.][0-9]+)$ ]]; then
  echo "--cpu-ceiling-pct must be a number in (0, 100]." >&2
  exit 1
fi
if ! awk -v ceiling="$cpu_ceiling_pct" 'BEGIN { exit !(ceiling > 0 && ceiling <= 100) }'; then
  echo "--cpu-ceiling-pct must be in (0, 100]." >&2
  exit 1
fi
if [[ "$calibration_mode" == "true" ]]; then
  if [[ "${tc_profile:-none}" != "none" ]]; then
    echo "Calibration mode disabled the requested tc profile to remain unshaped."
  fi
  tc_profile="none"
fi
validate_tc_options
validate_solution_json

if [[ -n "$case_name" ]]; then
  if [[ -n "$mode" || -n "$fec_scheme" || -n "$receivers" || -n "$trees" || -n "$solution_json" || -n "$block_size" || -n "$symbols_per_block" || -n "$payload_size" || "$synthetic_payload" == "true" || -n "$receive_timeout_ms" || -n "$peer_report_timeout_ms" || -n "$packet_processors" || -n "$channel_capacity" || -n "$queue_capacity" || "${tc_profile:-none}" != "none" || -n "$tree_sweep_max" || -n "$receiver_sweep_max" || "$calibration_mode" == "true" ]]; then
    echo "--case cannot be combined with custom run or sweep options." >&2
    exit 1
  fi
fi

if [[ ( -n "$tree_sweep_max" || -n "$receiver_sweep_max" ) && ( -n "$receivers" || -n "$trees" ) ]]; then
  echo "--receivers and --trees are only for one-off custom runs." >&2
  echo "Use --tree-sweep-receivers or --receiver-sweep-trees with sweep options." >&2
  exit 1
fi

build_binaries
ensure_database
mkdir -p "$artifacts_root"

if [[ -n "$case_name" ]]; then
  case "$case_name" in
    plain-1r) run_case plain-1r plain raptorq 1 1 8192 32 262144 120000 1 2048 2048 ;;
    fec-1r) run_case fec-1r fec raptorq 1 1 8192 32 262144 120000 1 2048 2048 ;;
    fec-2r-block) run_case fec-2r-block fec raptorq 2 2 4096 32 393216 120000 1 2048 2048 ;;
    fec-2r-symbols) run_case fec-2r-symbols fec raptorq 2 2 8192 16 393216 120000 1 2048 2048 ;;
    raptorq-2t-2r-k2400) run_case raptorq-2t-2r-k2400 fec raptorq 2 2 2457600 "$mettle_paper_scale_symbols_per_block" 67108864 120000 1 2048 2048 ;;
    mettle-2t-2r-k2400) run_case mettle-2t-2r-k2400 fec mettle 2 2 2457600 "$mettle_paper_scale_symbols_per_block" 67108864 120000 1 2048 2048 ;;
    *)
      echo "Unknown case: ${case_name}" >&2
      exit 1
      ;;
  esac
  exit 0
fi

selected_mode="${mode:-fec}"
selected_fec_scheme="${fec_scheme:-raptorq}"
selected_block_size="${block_size:-8192}"
selected_symbols_per_block="${symbols_per_block:-32}"
mettle_coded_rate_num="${mettle_coded_rate_num:-1}"
mettle_coded_rate_den="${mettle_coded_rate_den:-1}"
selected_payload_size="${payload_size:-262144}"
selected_receive_timeout_ms="${receive_timeout_ms:-120000}"
selected_peer_report_timeout_ms="${peer_report_timeout_ms:-15000}"
require_positive_int "--peer-report-timeout-ms" "$selected_peer_report_timeout_ms"
selected_packet_processors="${packet_processors:-1}"
selected_channel_capacity="${channel_capacity:-2048}"
selected_queue_capacity="${queue_capacity:-2048}"
ran_any="false"

if [[ -n "$tree_sweep_max" ]]; then
  run_tree_sweep \
    "$tree_sweep_max" \
    "$tree_sweep_receivers" \
    "$selected_mode" \
    "$selected_fec_scheme" \
    "$selected_block_size" \
    "$selected_symbols_per_block" \
    "$selected_payload_size" \
    "$selected_receive_timeout_ms" \
    "$selected_packet_processors" \
    "$selected_channel_capacity" \
    "$selected_queue_capacity"
  ran_any="true"
fi

if [[ -n "$receiver_sweep_max" ]]; then
  run_receiver_sweep \
    "$receiver_sweep_max" \
    "$receiver_sweep_trees" \
    "$selected_mode" \
    "$selected_fec_scheme" \
    "$selected_block_size" \
    "$selected_symbols_per_block" \
    "$selected_payload_size" \
    "$selected_receive_timeout_ms" \
    "$selected_packet_processors" \
    "$selected_channel_capacity" \
    "$selected_queue_capacity"
  ran_any="true"
fi

if [[ "$ran_any" == "false" && ( -n "$mode" || -n "$fec_scheme" || -n "$receivers" || -n "$trees" || -n "$solution_json" || -n "$block_size" || -n "$symbols_per_block" || -n "$payload_size" || "$synthetic_payload" == "true" || -n "$receive_timeout_ms" || -n "$peer_report_timeout_ms" || -n "$packet_processors" || -n "$channel_capacity" || -n "$queue_capacity" || "${tc_profile:-none}" != "none" || "$calibration_mode" == "true" ) ]]; then
  if [[ -z "$receivers" || -z "$trees" ]]; then
    echo "Custom runs require both --receivers and --trees." >&2
    exit 1
  fi

  validate_run_request \
    "$selected_mode" \
    "$selected_fec_scheme" \
    "$receivers" \
    "$trees" \
    "$selected_block_size" \
    "$selected_symbols_per_block" \
    "$selected_payload_size" \
    "$selected_receive_timeout_ms" \
    "$selected_packet_processors" \
    "$selected_channel_capacity" \
    "$selected_queue_capacity"

  run_case \
    "$(make_case_name custom "$selected_mode" "$selected_fec_scheme" "$receivers" "$trees" "$selected_block_size" "$selected_symbols_per_block" "$selected_packet_processors" "$selected_channel_capacity" "$selected_queue_capacity")" \
    "$selected_mode" \
    "$selected_fec_scheme" \
    "$receivers" \
    "$trees" \
    "$selected_block_size" \
    "$selected_symbols_per_block" \
    "$selected_payload_size" \
    "$selected_receive_timeout_ms" \
    "$selected_packet_processors" \
    "$selected_channel_capacity" \
    "$selected_queue_capacity" \
    "$selected_peer_report_timeout_ms"
  ran_any="true"
fi

if [[ "$ran_any" == "false" ]]; then
  run_case plain-1r plain raptorq 1 1 8192 32 262144 120000 1 2048 2048
fi
