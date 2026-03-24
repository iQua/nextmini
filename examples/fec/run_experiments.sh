#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
INVENTORY="$ROOT/inventory.example.toml"
GROUP_TIMEOUT=120
RECEIVE_TIMEOUT_MS=300000
CONTROLLER_PORT=3000
CONTROLLER_NETWORK="fec-control"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --inventory) INVENTORY="$2"; shift 2 ;;
    *) echo "usage: run_experiments.sh [--inventory path]"; exit 1 ;;
  esac
done

node_value() {
  local key="${1}_${2}"
  printf '%s' "${!key}"
}

remote_cmd() {
  local user="$1" host="$2" key="$3" port="$4" script="$5" remote_script
  local cmd=(ssh -n -o BatchMode=yes)
  [[ -n "$key" ]] && cmd+=(-i "$key")
  [[ "$port" != "22" ]] && cmd+=(-p "$port")
  printf -v remote_script 'bash -lc %q' "$script"
  cmd+=("${user}@${host}" "$remote_script")
  "${cmd[@]}"
}
copy_to() {
  local user="$1" host="$2" key="$3" port="$4" src="$5" dst="$6" cmd=(scp -o BatchMode=yes)
  [[ -n "$key" ]] && cmd+=(-i "$key")
  [[ "$port" != "22" ]] && cmd+=(-P "$port")
  cmd+=("$src" "${user}@${host}:$dst")
  "${cmd[@]}"
}
copy_from() {
  local user="$1" host="$2" key="$3" port="$4" src="$5" dst="$6" cmd=(scp -o BatchMode=yes)
  [[ -n "$key" ]] && cmd+=(-i "$key")
  [[ "$port" != "22" ]] && cmd+=(-P "$port")
  cmd+=("${user}@${host}:$src" "$dst")
  "${cmd[@]}"
}

remote_node() { remote_cmd "$(node_value NODE_USER "$1")" "$(node_value NODE_HOST "$1")" "$(node_value NODE_KEY "$1")" "$(node_value NODE_PORT "$1")" "$2"; }
copy_to_node() { copy_to "$(node_value NODE_USER "$1")" "$(node_value NODE_HOST "$1")" "$(node_value NODE_KEY "$1")" "$(node_value NODE_PORT "$1")" "$2" "$3"; }
copy_from_node() { copy_from "$(node_value NODE_USER "$1")" "$(node_value NODE_HOST "$1")" "$(node_value NODE_KEY "$1")" "$(node_value NODE_PORT "$1")" "$2" "$3"; }

wait_remote_file() {
  local node_id="$1" path="$2" deadline=$((SECONDS + $3))
  until remote_node "$node_id" "test -f $path" >/dev/null 2>&1; do
    (( SECONDS < deadline )) || { echo "timed out waiting for $path on node $node_id" >&2; return 1; }
    sleep 1
  done
}
cleanup_case() {
  local purge="$1" suffix=""
  [[ "$purge" == "true" ]] && suffix="; rm -rf ~/$REMOTE_RUN_DIR"
  remote_cmd "$CONTROLLER_USER" "$CONTROLLER_HOST" "$CONTROLLER_KEY" "$CONTROLLER_SSH_PORT" "docker rm -f fec-ctrl-$RUN_ID fec-pg-$RUN_ID >/dev/null 2>&1 || true$suffix" || true
  for node_id in $ACTIVE_NODE_IDS; do
    remote_node "$node_id" "docker rm -f fec-node-$node_id-$RUN_ID >/dev/null 2>&1 || true$suffix" || true
  done
}

start_controller() {
  local script
  read -r -d '' script <<EOF || true
mkdir -p ~/$REMOTE_RUN_DIR
docker network inspect $CONTROLLER_NETWORK >/dev/null 2>&1 || docker network create $CONTROLLER_NETWORK >/dev/null
docker run -d --name fec-pg-$RUN_ID --network $CONTROLLER_NETWORK --network-alias postgres \
  -e POSTGRES_USER=pgusr \
  -e POSTGRES_PASSWORD=pgpwrd \
  -e POSTGRES_DB=nextmini \
  docker.io/postgres:16-alpine >/dev/null
for _ in \$(seq 1 60); do
  docker exec fec-pg-$RUN_ID pg_isready -U pgusr -d nextmini >/dev/null 2>&1 && break
  sleep 1
done
docker image inspect $CONTROLLER_IMAGE >/dev/null 2>&1 || docker pull $CONTROLLER_IMAGE >/dev/null
docker run -d --name fec-ctrl-$RUN_ID --network $CONTROLLER_NETWORK \
  -p $CONTROLLER_PORT:3000 \
  -e RUST_LOG=info \
  -v \$HOME/$REMOTE_RUN_DIR/controller-config.toml:/var/nextmini/config.toml:ro \
  $CONTROLLER_IMAGE /var/nextmini/controller >/dev/null
for _ in \$(seq 1 60); do
  (echo >/dev/tcp/127.0.0.1/$CONTROLLER_PORT) >/dev/null 2>&1 && exit 0
  sleep 1
done
exit 1
EOF
  remote_cmd "$CONTROLLER_USER" "$CONTROLLER_HOST" "$CONTROLLER_KEY" "$CONTROLLER_SSH_PORT" "$script"
}

start_node() {
  local node_id="$1" role="$2" extra="--role router" script
  if [[ "$role" == "worker" ]]; then
    extra="--role receiver --node-id $node_id --expected-bytes $PAYLOAD_SIZE_BYTES --receive-timeout-ms $RECEIVE_TIMEOUT_MS"
  elif [[ "$role" == "trainer" ]]; then
    extra="--role source --controller-config /run/controller-config.toml --receiver-ids ${RECEIVER_IDS// /,} --tensor-path /run/payload.bin"
  fi
  read -r -d '' script <<EOF || true
mkdir -p ~/$REMOTE_RUN_DIR/artifacts
docker image inspect $NODE_IMAGE >/dev/null 2>&1 || docker pull $NODE_IMAGE >/dev/null
docker run -d --name fec-node-$node_id-$RUN_ID \
  --network host \
  --cap-add NET_ADMIN \
  --device /dev/net/tun \
  -e PYTHONUNBUFFERED=1 \
  -e RUST_LOG=info \
  -v \$HOME/$REMOTE_RUN_DIR:/run \
  $NODE_IMAGE \
  python /app/examples/multicast-docker/scripts/multicast_node.py \
  $extra \
  --config /run/node.toml \
  --group-label $RUN_ID \
  --source-node-id $SOURCE_NODE_ID \
  --artifact-dir /run/artifacts \
  --chunk-size $BLOCK_SIZE \
  --group-timeout $GROUP_TIMEOUT \
  --fec $FEC_MODE >/dev/null
EOF
  remote_node "$node_id" "$script"
}

collect_logs() {
  local node_id
  for node_id in $ACTIVE_NODE_IDS; do
    remote_node "$node_id" "docker logs fec-node-$node_id-$RUN_ID 2>&1" >"$LOGS_DIR/node-$node_id.log" || true
  done
  remote_cmd "$CONTROLLER_USER" "$CONTROLLER_HOST" "$CONTROLLER_KEY" "$CONTROLLER_SSH_PORT" "docker logs fec-ctrl-$RUN_ID 2>&1" >"$LOGS_DIR/controller.log" || true
}

run_case() {
  local metadata_path="$RUN_DIR/tensor-metadata.json"
  local node_id role verify_failed=0 source_hash receiver_path receiver_hash

  cleanup_case false
  printf '{"path":"/run/payload.bin","bytes":%s}\n' "$PAYLOAD_SIZE_BYTES" >"$metadata_path"
  remote_cmd "$CONTROLLER_USER" "$CONTROLLER_HOST" "$CONTROLLER_KEY" "$CONTROLLER_SSH_PORT" "mkdir -p ~/$REMOTE_RUN_DIR"
  copy_to "$CONTROLLER_USER" "$CONTROLLER_HOST" "$CONTROLLER_KEY" "$CONTROLLER_SSH_PORT" "$CONTROLLER_CONFIG_PATH" "~/$REMOTE_RUN_DIR/controller-config.toml"
  start_controller
  for node_id in $ACTIVE_NODE_IDS; do
    role="$(node_value NODE_ROLE "$node_id")"
    remote_node "$node_id" "mkdir -p ~/$REMOTE_RUN_DIR/artifacts"
    copy_to_node "$node_id" "$(node_value NODE_CONFIG "$node_id")" "~/$REMOTE_RUN_DIR/node.toml"
    [[ "$role" == "worker" ]] && copy_to_node "$node_id" "$metadata_path" "~/$REMOTE_RUN_DIR/artifacts/tensor-metadata.json"
    if [[ "$role" == "trainer" ]]; then
      copy_to_node "$node_id" "$CONTROLLER_CONFIG_PATH" "~/$REMOTE_RUN_DIR/controller-config.toml"
      copy_to_node "$node_id" "$PAYLOAD_PATH" "~/$REMOTE_RUN_DIR/payload.bin"
    fi
    start_node "$node_id" "$role"
  done
  wait_remote_file "$SOURCE_NODE_ID" "~/$REMOTE_RUN_DIR/artifacts/group-info.json" "$GROUP_TIMEOUT"
  copy_from_node "$SOURCE_NODE_ID" "~/$REMOTE_RUN_DIR/artifacts/group-info.json" "$RUN_DIR/group-info.json"
  for node_id in $RECEIVER_IDS; do
    copy_to_node "$node_id" "$RUN_DIR/group-info.json" "~/$REMOTE_RUN_DIR/artifacts/group-info.json.tmp"
    remote_node "$node_id" "mv ~/$REMOTE_RUN_DIR/artifacts/group-info.json.tmp ~/$REMOTE_RUN_DIR/artifacts/group-info.json"
  done
  for node_id in $RECEIVER_IDS; do
    wait_remote_file "$node_id" "~/$REMOTE_RUN_DIR/artifacts/receiver-ready-$node_id.json" "$GROUP_TIMEOUT"
    copy_from_node "$node_id" "~/$REMOTE_RUN_DIR/artifacts/receiver-ready-$node_id.json" "$RUN_DIR/receiver-ready-$node_id.json"
    copy_to_node "$SOURCE_NODE_ID" "$RUN_DIR/receiver-ready-$node_id.json" "~/$REMOTE_RUN_DIR/artifacts/receiver-ready-$node_id.json"
  done
  for node_id in $RECEIVER_IDS; do
    wait_remote_file "$node_id" "~/$REMOTE_RUN_DIR/artifacts/receiver-$node_id.bin" "$((RECEIVE_TIMEOUT_MS / 1000))"
  done
  collect_logs
  "$ROOT/collect_throughput.sh" "$RUN_DIR"
  source_hash="$(sha256sum "$PAYLOAD_PATH" | awk '{print $1}')"
  for node_id in $RECEIVER_IDS; do
    receiver_path="$RUN_DIR/receiver-$node_id.bin"
    if copy_from_node "$node_id" "~/$REMOTE_RUN_DIR/artifacts/receiver-$node_id.bin" "$receiver_path"; then
      receiver_hash="$(sha256sum "$receiver_path" | awk '{print $1}')"
      [[ "$receiver_hash" == "$source_hash" ]] || verify_failed=1
    else
      verify_failed=1
    fi
  done
  cleanup_case true
  (( verify_failed == 0 )) || return 1
  printf '%s -> %s\n' "$CASE_NAME" "$RUN_DIR"
}

while IFS= read -r case_name; do
  [[ -n "$case_name" ]] || continue
  plan_output="$(python3 "$ROOT/plan_case.py" --inventory "$INVENTORY" --case "$case_name")"
  eval "$plan_output"
  run_case
done < <(python3 "$ROOT/plan_case.py" --inventory "$INVENTORY" --list-cases)
