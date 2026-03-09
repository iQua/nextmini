#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
root_dir="$(cd "${script_dir}/../.." && pwd)"
work_dir="${WORK_DIR:-${script_dir}/.work}"

source "$script_dir/config.sh"
source "$script_dir/services.sh"
source "$script_dir/namespace.sh"

mode=""
source_node=""
receivers_csv=""
input_file=""
declare -a tree_specs=()

python_bin="${PYTHON_BIN:-python3.13}"
gen_python="${GEN_PYTHON_BIN:-python3}"
controller_port="${CONTROLLER_PORT:-}"
postgres_port="${POSTGRES_PORT:-}"
bridge_ip="${BRIDGE_IP:-10.240.0.1}"
bridge_cidr="${BRIDGE_CIDR:-${bridge_ip}/24}"
bridge_prefix="${BRIDGE_IP_PREFIX:-10.240.0}"
controller_target_dir="${NEXTMINI_CONTROLLER_TARGET_DIR:-$root_dir/target-mtns}"
controller_bin="${NEXTMINI_CONTROLLER_BIN:-$controller_target_dir/release/controller}"
run_id="${RUN_ID:-$$}"
link_tag="${LINK_TAG:-$((10#$run_id % 10000))}"

bridge_name="mtns-br-${link_tag}"
postgres_name="mtns-pg-${run_id}"

declare -a node_pids=()
declare -a receiver_pids=()
source_pid=""
controller_pid=""
n_nodes=""

cleanup() {
  local status=$?
  set +e

  cleanup_namespaces
  cleanup_services

  if [[ $status -eq 0 ]]; then
    sudo rm -rf "$work_dir"
  else
    echo "Logs kept in $work_dir" >&2
  fi

  exit $status
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --mode) mode="${2:-}"; shift 2 ;;
    --source) source_node="${2:-}"; shift 2 ;;
    --receivers) receivers_csv="${2:-}"; shift 2 ;;
    --tree) tree_specs+=("${2:-}"); shift 2 ;;
    --file) input_file="${2:-}"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown option: $1" >&2; usage; exit 1 ;;
  esac
done

[[ "$mode" == "plain" || "$mode" == "fec" ]] || { echo "--mode must be plain or fec" >&2; exit 1; }
[[ -n "$source_node" ]] || { echo "--source is required" >&2; exit 1; }
[[ -n "$receivers_csv" ]] || { echo "--receivers is required" >&2; exit 1; }
[[ -n "$input_file" ]] || { echo "--file is required" >&2; exit 1; }
[[ -f "$input_file" ]] || { echo "Input file not found: $input_file" >&2; exit 1; }
[[ ${#tree_specs[@]} -gt 0 ]] || { echo "At least one --tree is required" >&2; exit 1; }

require_cmd "$gen_python"
require_cmd "$python_bin"
require_cmd docker
require_cmd cmp
require_cmd sudo
[[ "$(uname -s)" == "Linux" ]] || { echo "This example requires Linux namespaces" >&2; exit 1; }

[[ -n "$controller_port" ]] || controller_port="$(pick_free_port)"
[[ -n "$postgres_port" ]] || postgres_port="$(pick_free_port)"

trap cleanup EXIT HUP INT TERM

build_controller_if_needed
prepare_work_dir
generate_runtime_files
load_n_nodes
start_postgres
start_controller
create_namespaces
launch_nodes
wait_for_transfer
verify_outputs

echo "plain/fec transfer verification passed for receivers: ${receivers_csv}"
