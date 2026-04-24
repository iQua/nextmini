#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
root_dir="$(cd "${script_dir}/../.." && pwd)"
artifacts_root="${script_dir}/artifacts"
controller_bin="${CONTROLLER_BIN:-${root_dir}/target/release/controller}"
dataplane_bin="${NEXTMINI_BIN:-${root_dir}/target/release/nextmini}"
cargo_bin="${CARGO_BIN:-}"
database_container_name="${DATABASE_CONTAINER_NAME:-nextmini-database}"
mettle_min_symbols_per_block="2400"

# RaptorQ legacy baseline used for namespace sanity checks:
#   --mode fec --trees 2 --receivers 10 --payload-size $((256*1024*1024)) \
#     --block-size $((128*1024)) --symbols-per-block 16
#
# METTLE cannot use K=16. For RaptorQ-vs-METTLE backend comparisons, keep the
# same topology and payload, but use K >= 2400 for both backends.
case_name=""
no_build="false"
mode=""
fec_scheme=""
receivers=""
trees=""
block_size=""
symbols_per_block=""
payload_size=""
receive_timeout_ms=""
packet_processors=""
channel_capacity=""
queue_capacity=""
tree_sweep_max=""
tree_sweep_receivers="20"
receiver_sweep_max=""
receiver_sweep_trees="3"
status_timeout_seconds="300"
original_args=("$@")
current_case_dir=""

usage() {
  cat <<'EOF'
Usage: run.sh [options]

Options:
  --case NAME                Run one named case: plain-1r | fec-1r | fec-2r-block | fec-2r-symbols | raptorq-2t-2r-k2400 | mettle-2t-2r-k2400.
  --mode MODE                Custom run/sweep mode: plain | fec (default for custom runs: fec).
  --fec-scheme SCHEME        FEC backend for custom/sweep runs: raptorq | mettle (default: raptorq).
  --receivers N              Custom run receiver count.
  --trees N                  Custom run tree count.
  --block-size N             Custom run block size (default: 8192).
  --symbols-per-block N      Custom run symbols_per_block (default: 32).
  --payload-size N           Custom run payload size bytes (default: 262144).
  --receive-timeout-ms N     Session completion timeout in ms (default: 120000).
  --packet-processors N      Dataplane packet processor lanes (default: 1).
  --channel-capacity N       Dataplane channel capacity (default: 2048).
  --queue-capacity N         Dataplane queue capacity (default: 2048).
  --tree-sweep-max N         Run a sweep from 1..N trees with fixed receivers.
  --tree-sweep-receivers N   Receiver count for tree sweep (default: 20).
  --receiver-sweep-max N     Run a sweep from 1..N receivers with fixed trees.
  --receiver-sweep-trees N   Tree count for receiver sweep (default: 3).
  --status-timeout-seconds N Seconds to wait for status files per case (default: 300).
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
    --block-size)
      block_size="${2:-}"
      shift 2
      ;;
    --symbols-per-block)
      symbols_per_block="${2:-}"
      shift 2
      ;;
    --payload-size)
      payload_size="${2:-}"
      shift 2
      ;;
    --receive-timeout-ms)
      receive_timeout_ms="${2:-}"
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
  require_positive_int "--payload-size" "$selected_payload_size"
  require_positive_int "--receive-timeout-ms" "$selected_receive_timeout_ms"
  require_positive_int "--packet-processors" "$selected_packet_processors"
  require_positive_int "--channel-capacity" "$selected_channel_capacity"
  require_positive_int "--queue-capacity" "$selected_queue_capacity"

  if [[ "$selected_mode" == "plain" ]] && (( selected_trees != 1 )); then
    echo "plain mode only supports exactly one tree." >&2
    exit 1
  fi
  if [[ "$selected_mode" == "fec" && "$selected_fec_scheme" == "mettle" ]] && (( selected_symbols_per_block < mettle_min_symbols_per_block )); then
    echo "METTLE requires --symbols-per-block >= ${mettle_min_symbols_per_block}." >&2
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
    RUST_LOG=info "$controller_bin" >"${case_dir}/controller.log" 2>&1 &
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
  RUST_LOG=info "$dataplane_bin" --config-path "${case_dir}/dataplane-config.toml" \
    >"${case_dir}/dataplane.log" 2>&1 &
  echo $! >"${case_dir}/dataplane.pid"
}

stop_case() {
  local case_dir="$1"
  local config_path="${case_dir}/dataplane-config.toml"
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
  local case_dir="${artifacts_root}/${name}"

  current_case_dir="$case_dir"
  rm -rf "$case_dir"
  mkdir -p "$case_dir"
  echo "Preparing case ${name}."

  generate_case \
    "$case_dir" \
    --case-name "$name" \
    --mode "$mode" \
    --fec-scheme "$selected_fec_scheme" \
    --receivers "$receivers" \
    --trees "$trees" \
    --block-size "$block_size" \
    --symbols-per-block "$symbols_per_block" \
    --payload-size "$payload_size" \
    --receive-timeout-ms "$receive_timeout_ms" \
    --packet-processors "$selected_packet_processors" \
    --channel-capacity "$selected_channel_capacity" \
    --queue-capacity "$selected_queue_capacity"

  start_controller "$case_dir"
  start_dataplane "$case_dir"
  wait_for_statuses "$case_dir" "$receivers"

  assert_status_ok "${case_dir}/artifacts/source-1.status"
  while IFS= read -r status_file; do
    [[ -z "$status_file" ]] && continue
    assert_status_ok "$status_file"
  done < <(find "${case_dir}/artifacts" -maxdepth 1 -name 'receiver-*.status' | sort)

  echo "Verifying hashes for case ${name}."
  python3 "${script_dir}/verify_hashes.py" "${case_dir}/artifacts"
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

if [[ -n "$case_name" ]]; then
  if [[ -n "$mode" || -n "$fec_scheme" || -n "$receivers" || -n "$trees" || -n "$block_size" || -n "$symbols_per_block" || -n "$payload_size" || -n "$receive_timeout_ms" || -n "$packet_processors" || -n "$channel_capacity" || -n "$queue_capacity" || -n "$tree_sweep_max" || -n "$receiver_sweep_max" ]]; then
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
    raptorq-2t-2r-k2400) run_case raptorq-2t-2r-k2400 fec raptorq 2 2 2457600 "$mettle_min_symbols_per_block" 67108864 120000 1 2048 2048 ;;
    mettle-2t-2r-k2400) run_case mettle-2t-2r-k2400 fec mettle 2 2 2457600 "$mettle_min_symbols_per_block" 67108864 120000 1 2048 2048 ;;
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
selected_payload_size="${payload_size:-262144}"
selected_receive_timeout_ms="${receive_timeout_ms:-120000}"
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

if [[ "$ran_any" == "false" && ( -n "$mode" || -n "$fec_scheme" || -n "$receivers" || -n "$trees" || -n "$block_size" || -n "$symbols_per_block" || -n "$payload_size" || -n "$receive_timeout_ms" || -n "$packet_processors" || -n "$channel_capacity" || -n "$queue_capacity" ) ]]; then
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
    "$selected_queue_capacity"
  ran_any="true"
fi

if [[ "$ran_any" == "false" ]]; then
  run_case plain-1r plain raptorq 1 1 8192 32 262144 120000 1 2048 2048
fi
