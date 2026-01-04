#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT_DIR"

ORIG_ARGS=("$@")
SESSION_NAME="${SESSION_NAME:-routes-iperf}"
RESULTS_FILE="${RESULTS_FILE:-$ROOT_DIR/iperf-results.md}"
CONTROLLER_CONFIG="$ROOT_DIR/controller-config.toml"
PROJECT_NAME="${COMPOSE_PROJECT_NAME:-$(basename "$ROOT_DIR")}"
NETWORK_NAME="${PROJECT_NAME}_network"
LOCK_DIR="$ROOT_DIR/.run_routes_tmux.${SESSION_NAME}.lock"
LOCK_HELD="false"
SRC_NODE="1"
DST_NODE="3"
DST_SET="false"
PATH_LABEL=""
HOPS=""
HOPS_RANGE=""
HOPS_LIST=()
TTY_FLAG="-it"
ATTACH_TMUX="true"
PRE_CLEAN="true"
FORCE_CLEAN="true"
NO_WAIT="false"
CONTROLLER_LOGS="true"
INTERNAL_RUN="false"
KEEP_TMUX="true"
KEEP_CONFIG="false"
CONFIG_BACKUP=""
CASE_LIST=()
COMPOSE_PANE=""
CONTROLLER_PANE=""
DST_PANE=""
SRC_PANE=""

usage() {
  cat <<'EOF'
Usage:
  run.sh --src N --dst N [--path 1-2-3] [--hops N] [--results FILE]
  run.sh --case "src:dst[:path[:hops]]" [--case ...] [--results FILE]
  run.sh --src N --hops-range A-B [--results FILE]
  run.sh --src N --hops-list A,B,C [--results FILE]

Examples:
  ./run.sh --src 1 --dst 3 --path 1-2-3
  ./run.sh --src 1 --hops 2
  ./run.sh --src 1 --hops-range 2-21
  ./run.sh --src 1 --hops-list 2,4,8
  ./run.sh --case "1:3:1-2-3" --case "1:4:1-2-3-4:3"

Notes:
  - The script waits for the controller log line:
    "Broadcasting topology-ready signal."
  - Results are appended to the markdown file as fenced code blocks.
  - The script removes the [routing] block in controller-config.toml and
    writes [[routes]] for src->dst and dst->src (one hop back route).
  - When --hops is provided without --path, the path is auto-generated as a
    linear chain starting at src: src-(src+1)-...-(src+hops).
  - Use --detach if you do not want to attach to the tmux session.
  - Use --no-wait to return immediately while tmux keeps running.
  - By default the tmux session is kept; use --kill-tmux to remove it after the run.
  - Controller logs are shown by default; use --no-controller-logs to show `docker compose ps` instead.
  - Use --no-preclean to skip docker compose down before each run.
  - Use --no-force-clean to skip removing existing node*/controller/postgres containers.
  - The script expands docker-compose.yml nodes and full_mesh_config.n_nodes
    to match the required hop count for each run.
EOF
}

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "Missing required command: $1" >&2
    exit 1
  fi
}

get_max_node() {
  local max
  if command -v rg >/dev/null 2>&1; then
    max=$(rg -n "^  node[0-9]+:" "$ROOT_DIR/docker-compose.yml" \
      | sed -E 's/^.*node([0-9]+).*/\1/' \
      | sort -n | tail -1)
  else
    max=$(grep -E "^  node[0-9]+:" "$ROOT_DIR/docker-compose.yml" \
      | sed -E 's/^.*node([0-9]+).*/\1/' \
      | sort -n | tail -1)
  fi
  echo "${max:-0}"
}

ensure_node_available() {
  local node_id="$1"
  local max_node

  validate_number "node id" "$node_id"
  max_node="$(get_max_node)"
  if [[ "$max_node" -le 0 ]]; then
    echo "No node services found in docker-compose.yml." >&2
    exit 1
  fi
  if (( node_id > max_node )); then
    echo "node${node_id} exceeds docker-compose.yml max node${max_node}." >&2
    echo "Update docker-compose.yml/topology before using hops that large." >&2
    exit 1
  fi
}

ensure_compose_nodes() {
  local required="$1"
  local current
  local file

  validate_number "required max node" "$required"
  file="$ROOT_DIR/docker-compose.yml"
  if [[ ! -f "$file" ]]; then
    echo "Missing docker-compose.yml: $file" >&2
    exit 1
  fi

  current="$(get_max_node)"
  if (( required <= current )); then
    return 0
  fi

  for ((n=current+1; n<=required; n++)); do
    local ip
    ip=$((3 + n))
    if (( ip > 254 )); then
      echo "Node${n} would exceed subnet 172.16.8.0/24 (ip ${ip})." >&2
      exit 1
    fi
    {
      echo
      echo "  node${n}:"
      echo "    container_name: node${n}"
      echo "    hostname: node${n}"
      echo "    image: nextmini_datapath"
      echo "    build:"
      echo "      context: ../../"
      echo "      dockerfile: ./dataplane/Dockerfile"
      echo "    networks:"
      echo "      network:"
      echo "        ipv4_address: 172.16.8.${ip}"
      echo "    stdin_open: true"
      echo "    privileged: true"
      echo "    volumes:"
      echo "      - ./config.toml:/var/nextmini/config.toml"
      echo "      - ../../tools/:/var/nextmini/tools"
      echo "    depends_on:"
      echo "      - controller"
      echo "      - node$((n - 1))"
      echo "    cap_add:"
      echo "      - NET_ADMIN"
      echo "    command: /bin/bash -c \"sleep 7 && /var/nextmini/nextmini ws://controller:3000\""
    } >> "$file"
  done
}

validate_number() {
  local label="$1"
  local value="$2"
  if [[ -z "$value" || ! "$value" =~ ^[0-9]+$ ]]; then
    echo "Invalid ${label}: ${value}" >&2
    exit 1
  fi
}

build_compose_services() {
  local max_node="$1"
  local -a services

  validate_number "max node" "$max_node"
  services=(postgres controller)
  for ((i=1; i<=max_node; i++)); do
    services+=("node${i}")
  done

  (IFS=' '; echo "${services[*]}")
}

wait_for_topology_ready() {
  local since_ts="${1:-}"
  local ready_pat="Broadcasting topology-ready signal"
  local since_args=()
  if [[ -n "$since_ts" ]]; then
    since_args=(--since "$since_ts")
  fi
  while true; do
    if command -v rg >/dev/null 2>&1; then
      if docker compose logs --no-color "${since_args[@]}" controller 2>/dev/null | rg -q "$ready_pat"; then
        return 0
      fi
    else
      if docker compose logs --no-color "${since_args[@]}" controller 2>/dev/null | grep -q "$ready_pat"; then
        return 0
      fi
    fi
    sleep 1
  done
}

build_ready_cmd() {
  local since_ts="$1"
  local ready_pat="Broadcasting topology-ready signal"
  local since_args=""
  if [[ -n "$since_ts" ]]; then
    since_args="--since \"$since_ts\""
  fi
  if command -v rg >/dev/null 2>&1; then
    echo "until docker compose logs --no-color ${since_args} controller 2>/dev/null | rg -q '${ready_pat}'; do sleep 1; done"
  else
    echo "until docker compose logs --no-color ${since_args} controller 2>/dev/null | grep -q '${ready_pat}'; do sleep 1; done"
  fi
}

create_config_backup() {
  if [[ -z "$CONFIG_BACKUP" ]]; then
    CONFIG_BACKUP="$(mktemp "${ROOT_DIR}/.controller-config.toml.bak.XXXXXX")"
    cp "$CONTROLLER_CONFIG" "$CONFIG_BACKUP"
  fi
}

restore_config_backup() {
  if [[ -n "$CONFIG_BACKUP" && -f "$CONFIG_BACKUP" ]]; then
    cp "$CONFIG_BACKUP" "$CONTROLLER_CONFIG"
    rm -f "$CONFIG_BACKUP"
  fi
}

cleanup() {
  if [[ "$KEEP_CONFIG" != "true" ]]; then
    restore_config_backup
  fi
}

cleanup_all() {
  cleanup
  if [[ "$LOCK_HELD" == "true" ]]; then
    rm -rf "$LOCK_DIR" >/dev/null 2>&1 || true
  fi
}

acquire_lock() {
  local existing_pid=""
  if mkdir "$LOCK_DIR" 2>/dev/null; then
    echo "$$" >"$LOCK_DIR/pid"
    LOCK_HELD="true"
    return 0
  fi

  if [[ -f "$LOCK_DIR/pid" ]]; then
    existing_pid="$(cat "$LOCK_DIR/pid" 2>/dev/null || true)"
  fi
  if [[ -n "$existing_pid" && "$existing_pid" =~ ^[0-9]+$ ]]; then
    if ! kill -0 "$existing_pid" 2>/dev/null; then
      rm -rf "$LOCK_DIR" >/dev/null 2>&1 || true
      mkdir "$LOCK_DIR" 2>/dev/null || true
      echo "$$" >"$LOCK_DIR/pid"
      LOCK_HELD="true"
      return 0
    fi
  fi

  echo "Another run is already in progress (lock: $LOCK_DIR pid: ${existing_pid:-unknown})." >&2
  echo "Attach with: tmux attach -t ${SESSION_NAME}" >&2
  exit 1
}

generate_path_from_hops() {
  local src="$1"
  local hops="$2"
  local dst_locked="$3"
  local dst_value="$4"
  local end
  local -a nodes
  local path

  validate_number "src" "$src"
  validate_number "hops" "$hops"
  end=$((src + hops))

  if [[ "$dst_locked" == "true" && "$dst_value" -ne "$end" ]]; then
    echo "dst ${dst_value} does not match src ${src} + hops ${hops}." >&2
    exit 1
  fi

  nodes=()
  for ((i=src; i<=end; i++)); do
    nodes+=("$i")
  done

  path=$(IFS=-; echo "${nodes[*]}")
  echo "${end}|${path}"
}

resolve_route() {
  local src="$1"
  local dst="$2"
  local dst_locked="$3"
  local path="$4"
  local hops="$5"
  local -a nodes
  local computed_src computed_dst

  validate_number "src" "$src"

  if [[ -n "$path" && -z "$hops" && "$path" =~ ^[0-9]+$ ]]; then
    hops="$path"
    path=""
  fi

  if [[ -n "$path" ]]; then
    IFS='-' read -r -a nodes <<< "$path"
    if [[ ${#nodes[@]} -lt 2 ]]; then
      echo "Invalid --path '${path}'. Expected format like 1-2-3." >&2
      exit 1
    fi
    computed_src="${nodes[0]}"
    computed_dst="${nodes[${#nodes[@]}-1]}"
    if [[ "$computed_src" != "$src" ]]; then
      echo "Path '${path}' does not start with src ${src}." >&2
      exit 1
    fi
    if [[ "$dst_locked" == "true" && "$dst" != "$computed_dst" ]]; then
      echo "Path '${path}' does not end at dst ${dst}." >&2
      exit 1
    fi
    dst="$computed_dst"
    if [[ -z "$hops" ]]; then
      hops=$(( ${#nodes[@]} - 1 ))
    fi
  elif [[ -n "$hops" ]]; then
    validate_number "hops" "$hops"
    local generated
    generated="$(generate_path_from_hops "$src" "$hops" "$dst_locked" "$dst")"
    dst="${generated%%|*}"
    path="${generated#*|}"
  else
    if [[ -z "$dst" ]]; then
      echo "Missing dst or hops for route generation." >&2
      exit 1
    fi
    validate_number "dst" "$dst"
    path="${src}-${dst}"
    hops=1
  fi

  echo "${dst}|${path}|${hops}"
}

update_controller_config() {
  local src="$1"
  local dst="$2"
  local path_label="$3"
  local n_nodes="$4"
  local tmp_file
  local route_line
  local first last
  local -a route_nodes

  if [[ ! -f "$CONTROLLER_CONFIG" ]]; then
    echo "Missing controller config: $CONTROLLER_CONFIG" >&2
    exit 1
  fi

  validate_number "n_nodes" "$n_nodes"
  if (( n_nodes < src || n_nodes < dst )); then
    echo "n_nodes ${n_nodes} must be >= src ${src} and dst ${dst}." >&2
    exit 1
  fi

  create_config_backup

  if [[ -n "$path_label" ]]; then
    IFS='-' read -r -a route_nodes <<< "$path_label"
    if [[ ${#route_nodes[@]} -lt 2 ]]; then
      echo "Invalid --path '${path_label}'. Expected format like 1-2-3." >&2
      exit 1
    fi
    first="${route_nodes[0]}"
    last="${route_nodes[${#route_nodes[@]}-1]}"
    if [[ "$first" != "$src" || "$last" != "$dst" ]]; then
      echo "Path '${path_label}' does not match src ${src} and dst ${dst}." >&2
      exit 1
    fi
  else
    route_nodes=("$src" "$dst")
  fi

  route_line=$(printf "%s, " "${route_nodes[@]}")
  route_line="${route_line%, }"

  tmp_file="$(mktemp "${ROOT_DIR}/.controller-config.toml.tmp.XXXXXX")"
  if ! awk -v max_nodes="$n_nodes" '
    function is_header(line) {
      return match(line, /^[[:space:]]*\[.*\][[:space:]]*$/)
    }
    function is_skip_header(line) {
      return match(line, /^[[:space:]]*\[routing\][[:space:]]*$/) || \
             match(line, /^[[:space:]]*\[\[routes\]\][[:space:]]*$/)
    }
    function update_full_mesh(line,    prefix, inner, start_pos, end_pos, rest) {
      if (match(line, /full_mesh_config[ \t]*=[ \t]*\{/)) {
        start_pos = RSTART + RLENGTH
        prefix = substr(line, 1, start_pos - 1)
        rest = substr(line, start_pos)
        if (match(rest, /\}/)) {
          end_pos = RSTART - 1
          inner = substr(rest, 1, end_pos)
          if (inner ~ /n_nodes[ \t]*=/) {
            gsub(/n_nodes[ \t]*=[ \t]*[0-9]+/, "n_nodes = " max_nodes, inner)
          } else if (inner ~ /[^ \t]/) {
            inner = inner ", n_nodes = " max_nodes
          } else {
            inner = "n_nodes = " max_nodes
          }
          return prefix inner "}"
        }
      }
      return line
    }
    {
      if (is_header($0)) {
        if (is_skip_header($0)) {
          skip = 1
          next
        }
        if (skip) {
          skip = 0
        }
        print
        next
      }
      if (skip) {
        next
      }
      print update_full_mesh($0)
    }
  ' "$CONTROLLER_CONFIG" > "$tmp_file"; then
    rm -f "$tmp_file" >/dev/null 2>&1 || true
    exit 1
  fi

  {
    echo
    echo "[[routes]]"
    echo "route = [${route_line}]"
    echo
    echo "[[routes]]"
    echo "route = [${dst}, ${src}]"
    echo
  } >> "$tmp_file"

  mv "$tmp_file" "$CONTROLLER_CONFIG"
}

ensure_tmux_session() {
  local session="$1"

  if ! tmux has-session -t "$session" 2>/dev/null; then
    tmux new-session -d -s "$session" -n routes -c "$ROOT_DIR"
    tmux split-window -h -t "$session:0.0"
    tmux split-window -v -t "$session:0.0"
    tmux split-window -v -t "$session:0.1"
    tmux select-layout -t "${session}:0" tiled
  fi

  COMPOSE_PANE="${session}:0.0"
  CONTROLLER_PANE="${session}:0.1"
  DST_PANE="${session}:0.2"
  SRC_PANE="${session}:0.3"

  tmux select-pane -t "$COMPOSE_PANE" -T "compose"
  tmux select-pane -t "$CONTROLLER_PANE" -T "controller"
  tmux select-pane -t "$DST_PANE" -T "dst"
  tmux select-pane -t "$SRC_PANE" -T "src"
  tmux select-layout -t "${session}:0" tiled
}

reset_tmux_panes() {
  tmux send-keys -t "$COMPOSE_PANE" C-c
  tmux send-keys -t "$CONTROLLER_PANE" C-c
  tmux send-keys -t "$DST_PANE" C-c
  tmux send-keys -t "$SRC_PANE" C-c
  tmux send-keys -t "$COMPOSE_PANE" "clear" C-m
  tmux send-keys -t "$CONTROLLER_PANE" "clear" C-m
  tmux send-keys -t "$DST_PANE" "clear" C-m
  tmux send-keys -t "$SRC_PANE" "clear" C-m
}

append_results_header() {
  local src="$1"
  local dst="$2"
  local path_label="$3"
  local hops="$4"
  local ts
  ts="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"

  mkdir -p "$(dirname "$RESULTS_FILE")"
  {
    echo "## ${ts}"
    echo "- src: node${src}"
    echo "- dst: node${dst}"
    if [[ -n "$path_label" ]]; then
      echo "- path: ${path_label}"
    fi
    if [[ -n "$hops" ]]; then
      echo "- hops: ${hops}"
    fi
    echo
    echo '```'
  } >> "$RESULTS_FILE"
}

parse_case() {
  local case_str="$1"
  local src dst path hops

  IFS=':' read -r src dst path hops <<< "$case_str"
  if [[ -z "$src" || -z "$dst" ]]; then
    echo "Invalid --case '${case_str}'. Use: src:dst[:path[:hops]]" >&2
    exit 1
  fi

  if [[ -z "$hops" && -n "$path" ]]; then
    local node_count
    node_count=$(awk -F- '{print NF}' <<< "$path")
    if [[ "$node_count" -ge 2 ]]; then
      hops=$((node_count - 1))
    fi
  fi

  SRC_NODE="$src"
  DST_NODE="$dst"
  PATH_LABEL="$path"
  HOPS="$hops"
}

run_case() {
  local src="$1"
  local dst="$2"
  local path_label="$3"
  local hops="$4"
  local session event down_event start_ts
  local compose_cmd
  local ready_cmd
  local required_max
  local event_suffix
  local services

  start_ts="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
  session="$SESSION_NAME"
  event_suffix="${dst}_${hops}_${RANDOM}"
  event="iperf_done_${session}_${event_suffix}"
  down_event="compose_down_${session}_${event_suffix}"

  required_max="$src"
  if (( dst > required_max )); then
    required_max="$dst"
  fi
  if [[ -n "$path_label" ]]; then
    local -a path_nodes
    local node_id
    IFS='-' read -r -a path_nodes <<< "$path_label"
    for node_id in "${path_nodes[@]}"; do
      validate_number "path node id" "$node_id"
      if (( node_id > required_max )); then
        required_max="$node_id"
      fi
    done
  fi
  ensure_compose_nodes "$required_max"
  ensure_node_available "$src"
  ensure_node_available "$dst"
  update_controller_config "$src" "$dst" "$path_label" "$required_max"
  services="$(build_compose_services "$required_max")"

  ensure_tmux_session "$session"
  reset_tmux_panes

  compose_cmd=""
  compose_cmd+="echo \"=== $(date -u +\"%Y-%m-%dT%H:%M:%SZ\") src=node${src} dst=node${dst} hops=${hops} n_nodes=${required_max} ===\"; "
  if [[ "$FORCE_CLEAN" == "true" ]]; then
    compose_cmd+='for name in controller postgres $(docker ps -a --format "{{.Names}}" | grep -E "^node[0-9]+$" || true); do docker rm -f "$name" >/dev/null 2>&1 || true; done; '
    compose_cmd+="docker network rm \"$NETWORK_NAME\" >/dev/null 2>&1 || true; "
  fi
  if [[ "$PRE_CLEAN" == "true" ]]; then
    compose_cmd+="docker compose down --remove-orphans; "
  fi
  compose_cmd+="docker compose up -d --build ${services}; docker compose ps"

  tmux send-keys -t "$COMPOSE_PANE" "$compose_cmd" C-m
  if [[ "$CONTROLLER_LOGS" == "true" ]]; then
    tmux send-keys -t "$CONTROLLER_PANE" \
      "until docker compose ps -q controller | grep -q .; do sleep 1; done; docker compose logs -f controller" C-m
  else
    tmux send-keys -t "$CONTROLLER_PANE" \
      "until docker compose ps -q controller | grep -q .; do sleep 1; done; echo 'Watching docker compose ps (Ctrl-C to stop)...'; if command -v watch >/dev/null 2>&1; then watch -n 1 docker compose ps; else while true; do printf '\\033[H\\033[2J'; docker compose ps; sleep 1; done; fi" C-m
  fi

  ready_cmd="$(build_ready_cmd "$start_ts")"

  append_results_header "$src" "$dst" "$path_label" "$hops"
  tmux send-keys -t "$DST_PANE" \
    "$ready_cmd; docker exec ${TTY_FLAG} node${dst} bash -lc \"iperf3 -s\"" C-m

  local client_cmd
  client_cmd="$ready_cmd; docker exec ${TTY_FLAG} node${src} bash -lc \"for i in {1..30}; do (echo >/dev/tcp/10.0.0.${dst}/5201) >/dev/null 2>&1 && break; sleep 1; done; iperf3 -c 10.0.0.${dst}\" | tee -a \"$RESULTS_FILE\"; printf '\`\`\`\\n\\n' >> \"$RESULTS_FILE\"; tmux wait-for -S \"$event\""
  tmux send-keys -t "$SRC_PANE" "$client_cmd" C-m

  tmux wait-for "$event"
  tmux send-keys -t "$DST_PANE" C-c
  tmux send-keys -t "$COMPOSE_PANE" "docker compose down --remove-orphans; tmux wait-for -S \"$down_event\"" C-m
  tmux wait-for "$down_event"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --src)
      SRC_NODE="$2"
      shift 2
      ;;
    --dst)
      DST_NODE="$2"
      DST_SET="true"
      shift 2
      ;;
    --path)
      PATH_LABEL="$2"
      shift 2
      ;;
    --hops)
      HOPS="$2"
      shift 2
      ;;
    --hops-range)
      HOPS_RANGE="$2"
      shift 2
      ;;
    --hops-list)
      IFS=',' read -r -a HOPS_LIST <<< "$2"
      shift 2
      ;;
    --results)
      RESULTS_FILE="$2"
      shift 2
      ;;
    --case)
      CASE_LIST+=("$2")
      shift 2
      ;;
    --no-tty)
      TTY_FLAG="-i"
      shift
      ;;
    --keep-tmux)
      KEEP_TMUX="true"
      shift
      ;;
    --kill-tmux)
      KEEP_TMUX="false"
      shift
      ;;
    --detach)
      ATTACH_TMUX="false"
      shift
      ;;
    --no-wait)
      NO_WAIT="true"
      shift
      ;;
    --controller-logs)
      CONTROLLER_LOGS="true"
      shift
      ;;
    --no-controller-logs)
      CONTROLLER_LOGS="false"
      shift
      ;;
    --internal-run)
      INTERNAL_RUN="true"
      shift
      ;;
    --no-preclean)
      PRE_CLEAN="false"
      shift
      ;;
    --no-force-clean)
      FORCE_CLEAN="false"
      shift
      ;;
    --keep-config)
      KEEP_CONFIG="true"
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

trap cleanup_all EXIT

require_cmd tmux
require_cmd docker

if ! docker compose version >/dev/null 2>&1; then
  echo "docker compose is required (docker compose version failed)." >&2
  exit 1
fi

if [[ "$NO_WAIT" == "true" ]]; then
  ATTACH_TMUX="false"
fi

if [[ "$INTERNAL_RUN" != "true" ]]; then
  filtered_args=()
  for arg in "${ORIG_ARGS[@]}"; do
    case "$arg" in
      --no-wait|--detach|--internal-run)
        ;;
      *)
        filtered_args+=("$arg")
        ;;
    esac
  done
  filtered_args+=("--detach" "--internal-run")

  ensure_tmux_session "$SESSION_NAME"
  log_file="$ROOT_DIR/.run_routes_tmux.${SESSION_NAME}.log"
  : >"$log_file"
  "$0" "${filtered_args[@]}" >"$log_file" 2>&1 &
  pid="$!"

  if [[ "$NO_WAIT" == "true" || "$ATTACH_TMUX" != "true" ]]; then
    echo "Started run in tmux session ${SESSION_NAME} (pid ${pid})."
    echo "Attach with: tmux attach -t ${SESSION_NAME}"
    echo "Log file: ${log_file}"
    exit 0
  fi

  echo "Running in tmux session ${SESSION_NAME} (pid ${pid}). Detach with Ctrl-b d."
  echo "Log file: ${log_file}"
  tmux attach -t "$SESSION_NAME"
  exit 0
fi

acquire_lock

if [[ ${#CASE_LIST[@]} -gt 0 ]]; then
  if [[ -n "$HOPS_RANGE" || ${#HOPS_LIST[@]} -gt 0 ]]; then
    echo "--case cannot be combined with --hops-range or --hops-list." >&2
    exit 1
  fi
  for case_str in "${CASE_LIST[@]}"; do
    parse_case "$case_str"
    resolved="$(resolve_route "$SRC_NODE" "$DST_NODE" "true" "$PATH_LABEL" "$HOPS")"
    DST_NODE="${resolved%%|*}"
    rest="${resolved#*|}"
    PATH_LABEL="${rest%%|*}"
    HOPS="${rest#*|}"
    run_case "$SRC_NODE" "$DST_NODE" "$PATH_LABEL" "$HOPS"
  done
else
  if [[ -n "$HOPS_RANGE" || ${#HOPS_LIST[@]} -gt 0 ]]; then
    if [[ -n "$PATH_LABEL" ]]; then
      echo "--path cannot be combined with --hops-range or --hops-list." >&2
      exit 1
    fi
    if [[ "$DST_SET" == "true" ]]; then
      echo "--dst cannot be combined with --hops-range or --hops-list." >&2
      exit 1
    fi
    if [[ -n "$HOPS_RANGE" ]]; then
      if [[ ! "$HOPS_RANGE" =~ ^[0-9]+-[0-9]+$ ]]; then
        echo "Invalid --hops-range '${HOPS_RANGE}'. Use A-B." >&2
        exit 1
      fi
      start="${HOPS_RANGE%-*}"
      end="${HOPS_RANGE#*-}"
      validate_number "hops-range start" "$start"
      validate_number "hops-range end" "$end"
      if (( start > end )); then
        echo "Invalid --hops-range '${HOPS_RANGE}': start > end." >&2
        exit 1
      fi
      HOPS_LIST=()
      for ((h=start; h<=end; h++)); do
        HOPS_LIST+=("$h")
      done
    fi
    for h in "${HOPS_LIST[@]}"; do
      HOPS="$h"
      resolved="$(resolve_route "$SRC_NODE" "$DST_NODE" "false" "" "$HOPS")"
      DST_NODE="${resolved%%|*}"
      rest="${resolved#*|}"
      PATH_LABEL="${rest%%|*}"
      HOPS="${rest#*|}"
      run_case "$SRC_NODE" "$DST_NODE" "$PATH_LABEL" "$HOPS"
    done
  else
    resolved="$(resolve_route "$SRC_NODE" "$DST_NODE" "$DST_SET" "$PATH_LABEL" "$HOPS")"
    DST_NODE="${resolved%%|*}"
    rest="${resolved#*|}"
    PATH_LABEL="${rest%%|*}"
    HOPS="${rest#*|}"
    run_case "$SRC_NODE" "$DST_NODE" "$PATH_LABEL" "$HOPS"
  fi
fi

if [[ "$KEEP_TMUX" != "true" ]]; then
  if tmux has-session -t "$SESSION_NAME" 2>/dev/null; then
    tmux kill-session -t "$SESSION_NAME"
  fi
fi
