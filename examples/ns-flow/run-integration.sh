#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
root_dir="$(cd "${script_dir}/../.." && pwd)"
artifacts_root="${script_dir}/integration-artifacts"
controller_bin="${CONTROLLER_BIN:-${root_dir}/target/release/controller}"
dataplane_bin="${NEXTMINI_BIN:-${root_dir}/target/release/nextmini}"
case_name=""
no_build="false"
original_args=("$@")

usage() {
  cat <<'EOF'
Usage: run-integration.sh [options]

Options:
  --case NAME    Run one named case: plain-1r | fec-1r | fec-2r-block | fec-2r-symbols.
  --no-build     Skip cargo build and use the existing binaries.
  -h, --help     Show this help.
EOF
}

if [[ "$(uname -s)" != "Linux" ]]; then
  echo "run-integration.sh requires Linux network namespaces." >&2
  exit 1
fi

while [[ $# -gt 0 ]]; do
  case "$1" in
    --case)
      case_name="${2:-}"
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

if [[ "${EUID:-$(id -u)}" -ne 0 ]]; then
  exec sudo -E "$0" "${original_args[@]}"
fi

build_binaries() {
  if [[ "$no_build" == "true" ]]; then
    return
  fi

  (
    cd "$root_dir"
    cargo build -p controller --release
    cargo build -p nextmini --release --features python-extension
  )
}

wait_for_port() {
  local host="$1"
  local port="$2"
  local deadline=$((SECONDS + 30))
  while (( SECONDS < deadline )); do
    if (echo >"/dev/tcp/${host}/${port}") >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  return 1
}

ensure_database() {
  if command -v pg_isready >/dev/null 2>&1; then
    if pg_isready -h 127.0.0.1 -p 5432 -U pgusr -d nextmini >/dev/null 2>&1; then
      return 0
    fi
  fi

  (
    cd "$root_dir"
    bash utils/start-database.sh
  )

  if command -v pg_isready >/dev/null 2>&1; then
    local deadline=$((SECONDS + 30))
    while (( SECONDS < deadline )); do
      if pg_isready -h 127.0.0.1 -p 5432 -U pgusr -d nextmini >/dev/null 2>&1; then
        return 0
      fi
      sleep 1
    done
    echo "Postgres did not become ready on 127.0.0.1:5432." >&2
    exit 1
  fi
}

generate_case() {
  local case_dir="$1"
  shift
  python3 "${script_dir}/generate_integration.py" --out-dir "$case_dir" "$@"
}

start_controller() {
  local case_dir="$1"
  mkdir -p "${case_dir}/controller-run"
  cp "${case_dir}/controller-config.toml" "${case_dir}/controller-run/config.toml"
  (
    cd "${case_dir}/controller-run"
    RUST_LOG=info "$controller_bin" >"${case_dir}/controller.log" 2>&1 &
    echo $! >"${case_dir}/controller.pid"
  )

  if ! wait_for_port 127.0.0.1 3000; then
    echo "Controller failed to bind 127.0.0.1:3000 for case ${case_dir}." >&2
    exit 1
  fi
}

start_dataplane() {
  local case_dir="$1"
  RUST_LOG=info "$dataplane_bin" --config-path "${case_dir}/dataplane-config.toml" \
    >"${case_dir}/dataplane.log" 2>&1 &
  echo $! >"${case_dir}/dataplane.pid"
}

stop_case() {
  local case_dir="$1"
  if [[ -f "${case_dir}/dataplane.pid" ]]; then
    kill "$(cat "${case_dir}/dataplane.pid")" >/dev/null 2>&1 || true
    wait "$(cat "${case_dir}/dataplane.pid")" 2>/dev/null || true
  fi
  if [[ -f "${case_dir}/controller.pid" ]]; then
    kill "$(cat "${case_dir}/controller.pid")" >/dev/null 2>&1 || true
    wait "$(cat "${case_dir}/controller.pid")" 2>/dev/null || true
  fi
  bash "${script_dir}/cleanup.sh" --skip-docker --config "${case_dir}/dataplane-config.toml" >/dev/null 2>&1 || true
}

wait_for_statuses() {
  local case_dir="$1"
  local expected_receivers="$2"
  local artifact_dir="${case_dir}/artifacts"
  local deadline=$((SECONDS + 120))

  while (( SECONDS < deadline )); do
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
  local receivers="$3"
  local trees="$4"
  local block_size="$5"
  local symbols_per_block="$6"
  local payload_size="$7"
  local case_dir="${artifacts_root}/${name}"
  local ok="false"

  trap 'stop_case "$case_dir"' RETURN

  rm -rf "$case_dir"
  mkdir -p "$case_dir"

  generate_case \
    "$case_dir" \
    --case-name "$name" \
    --mode "$mode" \
    --receivers "$receivers" \
    --trees "$trees" \
    --block-size "$block_size" \
    --symbols-per-block "$symbols_per_block" \
    --payload-size "$payload_size"

  start_controller "$case_dir"
  start_dataplane "$case_dir"

  if wait_for_statuses "$case_dir" "$receivers"; then
    assert_status_ok "${case_dir}/artifacts/source-1.status"
    while IFS= read -r status_file; do
      [[ -z "$status_file" ]] && continue
      assert_status_ok "$status_file"
    done < <(find "${case_dir}/artifacts" -maxdepth 1 -name 'receiver-*.status' | sort)
    python3 "${script_dir}/verify_hashes.py" "${case_dir}/artifacts"
    ok="true"
  fi

  if [[ "$ok" != "true" ]]; then
    echo "Case ${name} failed. See ${case_dir}/controller.log and ${case_dir}/dataplane.log." >&2
    exit 1
  fi

  trap - RETURN
  stop_case "$case_dir"
}

build_binaries
ensure_database
mkdir -p "$artifacts_root"

if [[ -n "$case_name" ]]; then
  case "$case_name" in
    plain-1r) run_case plain-1r plain 1 1 8192 32 262144 ;;
    fec-1r) run_case fec-1r fec 1 1 8192 32 262144 ;;
    fec-2r-block) run_case fec-2r-block fec 2 2 4096 32 393216 ;;
    fec-2r-symbols) run_case fec-2r-symbols fec 2 2 8192 16 393216 ;;
    *)
      echo "Unknown case: ${case_name}" >&2
      exit 1
      ;;
  esac
  exit 0
fi

run_case plain-1r plain 1 1 8192 32 262144
run_case fec-1r fec 1 1 8192 32 262144
run_case fec-2r-block fec 2 2 4096 32 393216
run_case fec-2r-symbols fec 2 2 8192 16 393216
