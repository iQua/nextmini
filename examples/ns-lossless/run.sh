#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
root_dir="$(cd "${script_dir}/../.." && pwd)"
artifacts_root="${script_dir}/artifacts"
controller_bin="${CONTROLLER_BIN:-${root_dir}/target/release/controller}"
dataplane_bin="${NEXTMINI_BIN:-${root_dir}/target/release/nextmini}"
cargo_bin="${CARGO_BIN:-}"
case_name=""
no_build="false"
original_args=("$@")
current_case_dir=""

usage() {
  cat <<'EOF'
Usage: run.sh [options]

Options:
  --case NAME    Run one named case: plain-1r | fec-1r | fec-2r-block | fec-2r-symbols.
  --no-build     Skip cargo build and use the existing binaries.
  -h, --help     Show this help.
EOF
}

if [[ "$(uname -s)" != "Linux" ]]; then
  echo "run.sh requires Linux network namespaces." >&2
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
  python3 "${script_dir}/generate.py" --out-dir "$case_dir" "$@"
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
    rm -f "${case_dir}/dataplane.pid"
  fi
  if [[ -f "${case_dir}/controller.pid" ]]; then
    kill "$(cat "${case_dir}/controller.pid")" >/dev/null 2>&1 || true
    wait "$(cat "${case_dir}/controller.pid")" 2>/dev/null || true
    rm -f "${case_dir}/controller.pid"
  fi
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

  current_case_dir="$case_dir"
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
  wait_for_statuses "$case_dir" "$receivers"

  assert_status_ok "${case_dir}/artifacts/source-1.status"
  while IFS= read -r status_file; do
    [[ -z "$status_file" ]] && continue
    assert_status_ok "$status_file"
  done < <(find "${case_dir}/artifacts" -maxdepth 1 -name 'receiver-*.status' | sort)

  python3 "${script_dir}/verify_hashes.py" "${case_dir}/artifacts"
  stop_case "$case_dir"
  current_case_dir=""
}

trap cleanup_on_exit EXIT

build_binaries
ensure_database
mkdir -p "$artifacts_root"

case "${case_name:-plain-1r}" in
  plain-1r) run_case plain-1r plain 1 1 8192 32 262144 ;;
  fec-1r) run_case fec-1r fec 1 1 8192 32 262144 ;;
  fec-2r-block) run_case fec-2r-block fec 2 2 4096 32 393216 ;;
  fec-2r-symbols) run_case fec-2r-symbols fec 2 2 8192 16 393216 ;;
  *)
    echo "Unknown case: ${case_name}" >&2
    exit 1
    ;;
esac
