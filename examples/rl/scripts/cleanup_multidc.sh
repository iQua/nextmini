#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  bash examples/rl/scripts/cleanup_multidc.sh --inventory <path> [--batch-ssh] [--skip-hosts host1,host2] [--protect-hosts host1,host2] [--full-wipe] [--extra-cmd host=CMD ...] [--dry-run]

What it does:
  - Default behavior (non-protected hosts, recommended for experiments):
    - Removes benchmark containers named skyrocket-*
    - Removes controller containers: skyrocket-controller, skyrocket-postgres
    - Removes ONLY the postgres data volume: skyrocket-postgres-data
    - Keeps other containers/images/volumes intact (faster + less disruptive)
  - Protected hosts (see --protect-hosts):
    - Only removes benchmark containers named skyrocket-*
    - Does NOT touch other containers/images/volumes
  - Full wipe mode (--full-wipe, destructive):
    - Removes ALL containers
    - Removes ALL images/build cache/volumes (docker system prune -af --volumes)

Safety:
  - Use --protect-hosts for machines where you must not delete unrelated containers.
  - You can skip hosts and/or run a one-off extra command on a host.
EOF
}

inventory=""
batch_ssh="0"
skip_hosts=""
protect_hosts=""
full_wipe="0"
dry_run="0"
extra_cmds=()

while [[ $# -gt 0 ]]; do
  case "$1" in
    --inventory)
      inventory="${2:-}"; shift 2 ;;
    --batch-ssh)
      batch_ssh="1"; shift ;;
    --skip-hosts)
      skip_hosts="${2:-}"; shift 2 ;;
    --protect-hosts)
      protect_hosts="${2:-}"; shift 2 ;;
    --full-wipe)
      full_wipe="1"; shift ;;
    --extra-cmd)
      extra_cmds+=("${2:-}"); shift 2 ;;
    --dry-run)
      dry_run="1"; shift ;;
    -h|--help)
      usage; exit 0 ;;
    *)
      echo "Unknown arg: $1" >&2
      usage; exit 2 ;;
  esac
done

if [[ -z "${inventory}" ]]; then
  echo "--inventory is required" >&2
  usage
  exit 2
fi

ssh_common=(-o StrictHostKeyChecking=accept-new -o ConnectTimeout=10)
if [[ "${batch_ssh}" == "1" ]]; then
  ssh_common+=(-o BatchMode=yes)
fi

cleanup_cmd_safe=$'docker rm -f $(docker ps -aq --filter "name=^/skyrocket-") >/dev/null 2>&1 || true\n'
cleanup_cmd_safe+=$'docker rm -f skyrocket-controller skyrocket-postgres >/dev/null 2>&1 || true\n'

cleanup_cmd_light=$'docker rm -f $(docker ps -aq --filter "name=^/skyrocket-") >/dev/null 2>&1 || true\n'
cleanup_cmd_light+=$'docker rm -f skyrocket-controller skyrocket-postgres >/dev/null 2>&1 || true\n'
# Reset probe DB state by dropping only the postgres data volume.
cleanup_cmd_light+=$'docker volume rm -f skyrocket-postgres-data >/dev/null 2>&1 || true\n'

cleanup_cmd_full=$'docker rm -f $(docker ps -aq) >/dev/null 2>&1 || true\n'
cleanup_cmd_full+=$'docker system prune -af --volumes >/dev/null 2>&1 || true\n'

# Emit unique targets as tab-separated: host \t user \t port \t identity_file(optional)
targets=$(
  python - <<'PY' "${inventory}"
from __future__ import annotations
import os, sys
from pathlib import Path
try:
    import tomllib
except ImportError:
    import tomli as tomllib  # type: ignore

inv = Path(sys.argv[1]).read_bytes().decode("utf-8")
raw = tomllib.loads(inv)
ssh_cfg = raw.get("ssh", {}) or {}
default_user = str(ssh_cfg.get("user", "ubuntu"))
default_port = int(ssh_cfg.get("port", 22))
identity = ssh_cfg.get("identity_file")
identity = str(identity) if identity else ""

def add(targets, host, user, port, ident):
    key = (host, user, int(port), ident or "")
    targets.add(key)

targets=set()
ctrl = raw.get("controller", {}) or {}
if ctrl.get("host"):
    add(targets, str(ctrl.get("host")).strip(),
        str(ctrl.get("user", default_user)),
        int(ctrl.get("port", default_port)),
        str(ctrl.get("identity_file", identity)) if ctrl.get("identity_file") else identity)

for n in (raw.get("nodes", []) or []):
    if not isinstance(n, dict): continue
    host=str(n.get("host","")).strip()
    if not host: continue
    add(targets, host,
        str(n.get("user", default_user)),
        int(n.get("port", default_port)),
        str(n.get("identity_file", identity)) if n.get("identity_file") else identity)

for host, user, port, ident in sorted(targets):
    print(host, user, port, ident, sep="\t")
PY
)

skip_arr=()
if [[ -n "${skip_hosts}" ]]; then
  IFS=',' read -r -a skip_arr <<< "${skip_hosts}"
fi
is_skipped() {
  local host="$1"
  for s in "${skip_arr[@]:-}"; do
    if [[ -n "${s}" && "${host}" == "${s}" ]]; then
      return 0
    fi
  done
  return 1
}

protect_arr=()
if [[ -n "${protect_hosts}" ]]; then
  IFS=',' read -r -a protect_arr <<< "${protect_hosts}"
fi
is_protected() {
  local host="$1"
  for s in "${protect_arr[@]:-}"; do
    if [[ -n "${s}" && "${host}" == "${s}" ]]; then
      return 0
    fi
  done
  return 1
}

run_ssh() {
  local host="$1" user="$2" port="$3" ident="$4" cmd="$5"
  local args=(ssh -p "${port}" "${ssh_common[@]}")
  if [[ -n "${ident}" ]]; then
    args+=(-i "${ident}")
  fi
  args+=("${user}@${host}" /usr/bin/env bash -s)
  if [[ "${dry_run}" == "1" ]]; then
    echo "  [dry-run] ${args[*]}  <<< (script body omitted)"
  else
    # Feed the script body via stdin to avoid quoting issues and to avoid
    # running a login shell (which can print noisy environment output).
    printf '%s\n' "set -euo pipefail" "${cmd}" | "${args[@]}"
  fi
}

while IFS=$'\t' read -r host user port ident; do
  [[ -z "${host}" ]] && continue
  echo "==> ${user}@${host}:${port}"

  # Extra cmds: run even if host is skipped
  for spec in "${extra_cmds[@]:-}"; do
    [[ "${spec}" != *"="* ]] && continue
    eh="${spec%%=*}"
    ec="${spec#*=}"
    if [[ "${eh}" == "${host}" ]]; then
      echo "  [extra] ${ec}"
      run_ssh "${host}" "${user}" "${port}" "${ident}" "${ec}"
    fi
  done

  if is_skipped "${host}"; then
    echo "  [skip] cleanup skipped"
    continue
  fi
  if is_protected "${host}"; then
    echo "  [cleanup] protected host: removing only skyrocket containers"
    run_ssh "${host}" "${user}" "${port}" "${ident}" "${cleanup_cmd_safe}"
  else
    if [[ "${full_wipe}" == "1" ]]; then
      echo "  [cleanup] full wipe: removing ALL containers + pruning images/volumes"
      run_ssh "${host}" "${user}" "${port}" "${ident}" "${cleanup_cmd_full}"
    else
      echo "  [cleanup] light: removing skyrocket containers + resetting skyrocket-postgres-data volume"
      run_ssh "${host}" "${user}" "${port}" "${ident}" "${cleanup_cmd_light}"
    fi
  fi
done <<< "${targets}"

