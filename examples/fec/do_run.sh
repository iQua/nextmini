#!/usr/bin/env bash
# do_run.sh — Pure-bash FEC WAN benchmark on DigitalOcean.
# No Ansible required. Each node gets its own dedicated VM.
#
# Prerequisites:
#   - DO_API_TOKEN env var (or in ~/.env)
#   - SSH key registered with DigitalOcean (DO_SSH_KEY_ID)
#   - ssh / scp / curl / python3 available locally
#
# Usage:
#   export DO_API_TOKEN=xxx
#   bash examples/fec/do_run.sh [OPTIONS]
#
# Options:
#   --keep-vms          Don't destroy VMs after the run
#   --skip-provision    Reuse existing VMs
#   --skip-build        Reuse previously built Docker images
#   --mode MODE         plain (default) or fec
#   --payload-size SZ   e.g. 100MiB (default)

set -euo pipefail

###############################################################################
# Config
###############################################################################
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
STATE_FILE="$SCRIPT_DIR/.do-state.json"
IMAGE_TAG_FILE="$SCRIPT_DIR/.do-image-tag"
TIMESTAMP="$(date +%Y%m%d-%H%M%S)"
RUN_ID="do-${TIMESTAMP}"
IMAGE_TAG="fec-${RUN_ID}"
RESULTS_DIR="$SCRIPT_DIR/results/$RUN_ID"

# DO defaults
DO_API="https://api.digitalocean.com/v2"
DO_SSH_KEY_ID="${DO_SSH_KEY_ID:-55047570}"
DO_SSH_KEY="${DO_SSH_KEY_FILE:-$HOME/.ssh/do_wan_benchmark}"
DO_IMAGE="ubuntu-24-04-x64"
DO_TAG="fec-bench"
DO_SIZE="s-2vcpu-4gb"
DO_BUILD_SIZE="s-4vcpu-8gb"

# Experiment defaults
MODE="plain"
PAYLOAD_SIZE="100MiB"
BLOCK_SIZE=65536
GROUP_TIMEOUT=120
RECV_TIMEOUT_MS=300000
CTRL_PORT=3000

# Node topology (5 nodes, 2 relays)
#   Source(1) → Relay(5) → Receiver(2)
#   Source(1) → Relay(6) → Receiver(3)
N_NODES=5
TOPO_EDGES="[1,5],[5,2],[1,6],[6,3]"
RECEIVER_IDS="2,3"
SOURCE_NODE_ID=1

# VM definitions: name region size role node_id
DROPLETS=(
  "fec-src:nyc3:${DO_BUILD_SIZE}:source:1"
  "fec-relay-a:nyc3:${DO_SIZE}:router:5"
  "fec-recv-a:nyc3:${DO_SIZE}:receiver:2"
  "fec-relay-b:sfo3:${DO_SIZE}:router:6"
  "fec-recv-b:sfo3:${DO_SIZE}:receiver:3"
)

###############################################################################
# Parse args
###############################################################################
KEEP_VMS=0; SKIP_PROVISION=0; SKIP_BUILD=0

# Auto-load token from ~/.env
if [ -z "${DO_API_TOKEN:-}" ] && [ -f "$HOME/.env" ]; then
  set -a; source "$HOME/.env"; set +a
fi
: "${DO_API_TOKEN:?Set DO_API_TOKEN}"

for arg in "$@"; do
  case "$arg" in
    --keep-vms)       KEEP_VMS=1 ;;
    --skip-provision) SKIP_PROVISION=1 ;;
    --skip-build)     SKIP_BUILD=1 ;;
    --mode=*)         MODE="${arg#*=}" ;;
    --payload-size=*) PAYLOAD_SIZE="${arg#*=}" ;;
  esac
done

SSH_OPTS="-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ConnectTimeout=30 -o BatchMode=yes -o LogLevel=ERROR -o ServerAliveInterval=10"

log() { echo "[$(date +%H:%M:%S)]  $*"; }

remote() {
  local ip=$1; shift
  ssh $SSH_OPTS -i "$DO_SSH_KEY" "root@${ip}" "$@"
}

upload() {
  scp $SSH_OPTS -i "$DO_SSH_KEY" "$1" "root@${2}:${3}"
}

download() {
  scp $SSH_OPTS -i "$DO_SSH_KEY" "root@${1}:${2}" "$3"
}

###############################################################################
# Cleanup trap
###############################################################################
cleanup_on_exit() {
  local rc=$?
  if [ "$KEEP_VMS" = "1" ]; then
    log "KEEP_VMS=1 — VMs preserved. Destroy later:"
    log "  bash $0 --destroy"
    return
  fi
  if [ -f "$STATE_FILE" ]; then
    log "Destroying VMs..."
    destroy_vms
  fi
}
trap cleanup_on_exit EXIT

# Handle --destroy flag
if [[ "${1:-}" == "--destroy" ]]; then
  destroy_vms() { :; }  # define stub first
  source /dev/stdin <<'DESTROY_FUNC'
destroy_vms() {
  if [ ! -f "$STATE_FILE" ]; then
    echo "No state file found. Nothing to destroy."
    return
  fi
  local ids
  ids=$(python3 -c "import json; [print(d['id']) for d in json.load(open('$STATE_FILE'))['droplets']]")
  for id in $ids; do
    log "  Deleting droplet $id..."
    curl -s -X DELETE "${DO_API}/droplets/${id}" \
      -H "Authorization: Bearer ${DO_API_TOKEN}" > /dev/null
  done
  # Also clean by tag
  curl -s -X DELETE "${DO_API}/droplets?tag_name=${DO_TAG}" \
    -H "Authorization: Bearer ${DO_API_TOKEN}" > /dev/null
  rm -f "$STATE_FILE"
  log "  All VMs destroyed."
}
DESTROY_FUNC
  destroy_vms
  exit 0
fi

###############################################################################
# Helper: destroy VMs
###############################################################################
destroy_vms() {
  if [ ! -f "$STATE_FILE" ]; then return; fi
  local ids
  ids=$(python3 -c "import json; [print(d['id']) for d in json.load(open('$STATE_FILE'))['droplets']]" 2>/dev/null || true)
  for id in $ids; do
    curl -s -X DELETE "${DO_API}/droplets/${id}" \
      -H "Authorization: Bearer ${DO_API_TOKEN}" > /dev/null 2>&1 || true
  done
  curl -s -X DELETE "${DO_API}/droplets?tag_name=${DO_TAG}" \
    -H "Authorization: Bearer ${DO_API_TOKEN}" > /dev/null 2>&1 || true
  rm -f "$STATE_FILE"
}

###############################################################################
# Phase 1: Provision VMs
###############################################################################
provision_vms() {
  log ""
  log "═══════════════════════════════════════════════"
  log "  Phase 1: Provisioning $((${#DROPLETS[@]})) VMs"
  log "═══════════════════════════════════════════════"

  local droplet_json="[]"

  for spec in "${DROPLETS[@]}"; do
    IFS=: read -r name region size role node_id <<< "$spec"
    log "  Creating $name ($region, $size)..."

    local resp
    resp=$(curl -s -X POST "${DO_API}/droplets" \
      -H "Authorization: Bearer ${DO_API_TOKEN}" \
      -H "Content-Type: application/json" \
      -d "{
        \"name\": \"$name\",
        \"region\": \"$region\",
        \"size\": \"$size\",
        \"image\": \"$DO_IMAGE\",
        \"ssh_keys\": [\"$DO_SSH_KEY_ID\"],
        \"tags\": [\"$DO_TAG\"]
      }")

    local did
    did=$(echo "$resp" | python3 -c "import sys,json; print(json.load(sys.stdin)['droplet']['id'])")
    droplet_json=$(echo "$droplet_json" | python3 -c "
import sys,json
arr = json.load(sys.stdin)
arr.append({'name':'$name','id':$did,'region':'$region','role':'$role','node_id':$node_id})
print(json.dumps(arr))
")
  done

  # Wait for all droplets to become active and get IPs
  log "  Waiting for droplets to become active..."
  local state_droplets="[]"
  for spec in "${DROPLETS[@]}"; do
    IFS=: read -r name region size role node_id <<< "$spec"
    local did
    did=$(echo "$droplet_json" | python3 -c "
import sys,json
for d in json.load(sys.stdin):
    if d['name']=='$name': print(d['id']); break
")

    local ip=""
    for _ in $(seq 1 60); do
      local info
      info=$(curl -s "${DO_API}/droplets/${did}" \
        -H "Authorization: Bearer ${DO_API_TOKEN}")
      local status
      status=$(echo "$info" | python3 -c "import sys,json; print(json.load(sys.stdin)['droplet']['status'])")
      if [ "$status" = "active" ]; then
        ip=$(echo "$info" | python3 -c "
import sys,json
nets = json.load(sys.stdin)['droplet']['networks']['v4']
print(next(n['ip_address'] for n in nets if n['type']=='public'))
")
        break
      fi
      sleep 5
    done

    if [ -z "$ip" ]; then
      log "  ERROR: Droplet $name ($did) never became active"
      exit 1
    fi

    log "  ✓ $name → $ip (node $node_id, $role)"
    state_droplets=$(echo "$state_droplets" | python3 -c "
import sys,json
arr = json.load(sys.stdin)
arr.append({'name':'$name','id':$did,'ip':'$ip','region':'$region','role':'$role','node_id':$node_id})
print(json.dumps(arr))
")
  done

  # Save state
  python3 -c "
import json
print(json.dumps({'tag':'$DO_TAG','droplets':$state_droplets}, indent=2))
" > "$STATE_FILE"

  # Wait for SSH
  log "  Waiting for SSH..."
  for spec in "${DROPLETS[@]}"; do
    IFS=: read -r name _ _ _ _ <<< "$spec"
    local ip
    ip=$(get_ip "$name")
    for _ in $(seq 1 30); do
      if remote "$ip" "true" 2>/dev/null; then break; fi
      sleep 3
    done
    log "    ✓ $name ($ip) SSH ready"
  done

  # Install Docker on all VMs
  log "  Installing Docker..."
  for spec in "${DROPLETS[@]}"; do
    IFS=: read -r name _ _ _ _ <<< "$spec"
    local ip
    ip=$(get_ip "$name")
    remote "$ip" bash -s <<'DOCKER_INSTALL' &
set -euo pipefail
# Wait for cloud-init
cloud-init status --wait >/dev/null 2>&1 || true
# Wait for apt lock
for i in $(seq 1 60); do
  if ! fuser /var/lib/dpkg/lock-frontend >/dev/null 2>&1; then break; fi
  sleep 2
done
export DEBIAN_FRONTEND=noninteractive
if ! command -v docker &>/dev/null; then
  curl -fsSL https://get.docker.com | sh >/dev/null 2>&1
fi
DOCKER_INSTALL
  done
  wait
  log "  Docker installed on all VMs."
}

###############################################################################
# State helpers
###############################################################################
get_ip() {
  python3 -c "
import json
for d in json.load(open('$STATE_FILE'))['droplets']:
    if d['name']=='$1': print(d['ip']); break
"
}

get_ip_by_role() {
  python3 -c "
import json
for d in json.load(open('$STATE_FILE'))['droplets']:
    if d['role']=='$1': print(d['ip']); break
"
}

get_all_ips() {
  python3 -c "
import json
for d in json.load(open('$STATE_FILE'))['droplets']:
    print(d['ip'])
"
}

get_vm1_ip() { get_ip "fec-src"; }

###############################################################################
# Phase 2: Build + distribute images
###############################################################################
build_images() {
  log ""
  log "═══════════════════════════════════════════════"
  log "  Phase 2: Building Docker images (on VM1)"
  log "═══════════════════════════════════════════════"

  local vm1_ip
  vm1_ip=$(get_vm1_ip)

  # Check if VM1 already has the image from a previous build
  if [ -f "$IMAGE_TAG_FILE" ]; then
    local prev_tag
    prev_tag=$(cat "$IMAGE_TAG_FILE")
    if remote "$vm1_ip" "docker image inspect nextmini-fec:${prev_tag} >/dev/null 2>&1"; then
      IMAGE_TAG="$prev_tag"
      log "  VM1 already has image nextmini-fec:${IMAGE_TAG}, skipping build."
      # Fall through to distribution below
    else
      # Image tag file exists but image not on VM1, need to rebuild
      prev_tag=""
    fi
  fi

  # Only build if we don't already have an image
  if ! remote "$vm1_ip" "docker image inspect nextmini-fec:${IMAGE_TAG} >/dev/null 2>&1"; then
    # Create source tarball
    log "  Creating source tarball..."
    local tarball="/tmp/nextmini-src-${TIMESTAMP}.tar.gz"
    cd "$REPO_ROOT"
    tar czf "$tarball" \
      --exclude='target' --exclude='.git' \
      --exclude='examples/fec/generated' \
      --exclude='examples/fec/payload.bin' \
      --exclude='examples/fec/results' \
      --exclude='examples/fec/.do-*' \
      --exclude='examples/fec/ansible/payload.bin' \
      --exclude='examples/fec/ansible/results' .
    log "  Source tarball: $(du -h "$tarball" | cut -f1)"

    # Upload and build
    log "  Uploading source to VM1 ($vm1_ip)..."
    upload "$tarball" "$vm1_ip" "/tmp/nextmini-src.tar.gz"
    rm -f "$tarball"

    log "  Building images on VM1..."
    remote "$vm1_ip" bash -s <<BUILDEOF
set -euo pipefail
mkdir -p /tmp/nextmini-build && cd /tmp/nextmini-build
tar xzf /tmp/nextmini-src.tar.gz
echo "=== Building FEC node image ==="
docker build -t "nextmini-fec:${IMAGE_TAG}" -f examples/fec/Dockerfile . 2>&1 | tail -5
echo "=== Building controller image ==="
docker build -t "nextmini-controller:${IMAGE_TAG}" -f examples/fec/Dockerfile.controller.local . 2>&1 | tail -5
echo "BUILD_DONE"
rm -rf /tmp/nextmini-build /tmp/nextmini-src.tar.gz
BUILDEOF
    log "  Build complete."
  fi

  # Distribute to other VMs
  local other_ips
  other_ips=$(python3 -c "
import json
vm1='$(get_vm1_ip)'
for d in json.load(open('$STATE_FILE'))['droplets']:
    if d['ip'] != vm1: print(d['ip'])
")

  if [ -n "$other_ips" ]; then
    # Check which VMs actually need the image
    local need_dist=()
    for ip in $other_ips; do
      if ! remote "$ip" "docker image inspect nextmini-fec:${IMAGE_TAG} >/dev/null 2>&1"; then
        need_dist+=("$ip")
      else
        log "    $ip already has image, skipping."
      fi
    done

    if [ ${#need_dist[@]} -eq 0 ]; then
      log "  All VMs already have the image, skipping distribution."
    else
      log "  Saving & compressing images on VM1..."
      remote "$vm1_ip" "docker save nextmini-fec:${IMAGE_TAG} nextmini-controller:${IMAGE_TAG} | gzip > /tmp/fec-image.tar.gz"
      local img_size
      img_size=$(remote "$vm1_ip" "du -h /tmp/fec-image.tar.gz | cut -f1")
      log "  Compressed image: ${img_size}"

      # Upload SSH key to VM1 so it can SCP directly to other VMs
      upload "$DO_SSH_KEY" "$vm1_ip" "/tmp/dist-key"
      remote "$vm1_ip" "chmod 600 /tmp/dist-key"
      local DIST_SSH="-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ConnectTimeout=30 -o BatchMode=yes -o LogLevel=ERROR"

      log "  Distributing to ${#need_dist[@]} VMs (compressed, direct)..."
      local dist_pids=()
      local dist_ips=()
      for ip in "${need_dist[@]}"; do
        log "    VM1 → $ip"
        remote "$vm1_ip" "scp ${DIST_SSH} -i /tmp/dist-key /tmp/fec-image.tar.gz root@${ip}:/tmp/fec-image.tar.gz" &
        dist_pids+=($!)
        dist_ips+=("$ip")
    done

    # Wait and check each transfer, fallback to local relay if needed
    for i in "${!dist_pids[@]}"; do
      if ! wait "${dist_pids[$i]}" 2>/dev/null; then
        local fip="${dist_ips[$i]}"
        log "    ⚠ Direct transfer to $fip failed, falling back to local relay..."
        remote "$vm1_ip" "cat /tmp/fec-image.tar.gz" | remote "$fip" "cat > /tmp/fec-image.tar.gz"
      fi
    done

    for ip in "${need_dist[@]}"; do
      remote "$ip" "gunzip -c /tmp/fec-image.tar.gz | docker load && rm -f /tmp/fec-image.tar.gz" &
    done
    wait

    remote "$vm1_ip" "rm -f /tmp/fec-image.tar.gz /tmp/dist-key"
    log "  Images distributed."
    fi  # end need_dist check
  fi

  echo "${IMAGE_TAG}" > "$IMAGE_TAG_FILE"
}

###############################################################################
# Phase 3: Generate payload
###############################################################################
generate_payload() {
  log ""
  log "═══════════════════════════════════════════════"
  log "  Phase 3: Generating payload"
  log "═══════════════════════════════════════════════"

  PAYLOAD_BYTES=$(python3 -c "
suffixes = {'b':1,'kb':1000,'mb':1000**2,'gb':1000**3,'kib':1024,'mib':1024**2,'gib':1024**3}
v = '${PAYLOAD_SIZE}'.strip().lower().replace('_','')
for s, m in sorted(suffixes.items(), key=lambda x: -len(x[0])):
    if v.endswith(s):
        print(int(float(v[:-len(s)].strip()) * m)); break
else:
    print(int(v))
")

  PAYLOAD_FILE="$SCRIPT_DIR/payload.bin"
  if [ ! -f "$PAYLOAD_FILE" ] || [ "$(stat -c %s "$PAYLOAD_FILE" 2>/dev/null || echo 0)" != "$PAYLOAD_BYTES" ]; then
    log "  Creating ${PAYLOAD_SIZE} payload..."
    head -c "$PAYLOAD_BYTES" /dev/urandom > "$PAYLOAD_FILE"
  fi
  log "  Payload ready: $PAYLOAD_FILE ($PAYLOAD_BYTES bytes)"
}

###############################################################################
# Phase 4: Run experiment
###############################################################################
run_experiment() {
  log ""
  log "═══════════════════════════════════════════════"
  log "  Phase 4: Running experiment (mode=$MODE)"
  log "═══════════════════════════════════════════════"

  local ctrl_ip
  ctrl_ip=$(get_vm1_ip)
  local run_dir="/tmp/fec-runs/$RUN_ID"

  local ctrl_image="nextmini-controller:${IMAGE_TAG}"
  local node_image="nextmini-fec:${IMAGE_TAG}"

  local fec_enabled="false"
  local fec_flag="off"
  if [ "$MODE" = "fec" ]; then fec_enabled="true"; fec_flag="on"; fi

  # ── Step 1: Start controller on VM1 ──
  log "  Starting controller on VM1 ($ctrl_ip)..."
  remote "$ctrl_ip" bash -s <<CTRL_EOF
set -euo pipefail
mkdir -p ${run_dir}

# Clean up old containers
docker ps -a --format '{{.Names}}' | grep -E '^fec-' | xargs -r docker rm -f >/dev/null 2>&1 || true
docker network rm fec-control >/dev/null 2>&1 || true

# Write controller config
cat > ${run_dir}/controller-config.toml <<'TOML'
[topology]
n_nodes = ${N_NODES}
edges = [${TOPO_EDGES}]

[routing]
protocol = "shortest_path"

[db]
user = "pgusr"
password = "pgpwrd"
host = "postgres"
database = "nextmini"
port = "5432"
TOML

# Start postgres + controller
docker network create fec-control >/dev/null
docker run -d --name fec-postgres --network fec-control --network-alias postgres \
  -e POSTGRES_USER=pgusr -e POSTGRES_PASSWORD=pgpwrd -e POSTGRES_DB=nextmini \
  docker.io/postgres:16-alpine >/dev/null

for i in \$(seq 1 60); do
  docker exec fec-postgres pg_isready -U pgusr -d nextmini >/dev/null 2>&1 && break
  sleep 1
done

docker run -d --name fec-controller --network fec-control -p ${CTRL_PORT}:3000 \
  -e RUST_LOG=info -v ${run_dir}/controller-config.toml:/var/nextmini/config.toml:ro \
  ${ctrl_image} /var/nextmini/controller >/dev/null

# Wait for port
for i in \$(seq 1 60); do
  nc -z 127.0.0.1 ${CTRL_PORT} 2>/dev/null && break
  sleep 1
done
echo "CONTROLLER_READY"
CTRL_EOF
  log "  Controller started."

  # ── Step 2: Deploy node configs + start containers ──
  log "  Starting node containers..."

  # Process each droplet
  local droplet_info
  droplet_info=$(python3 -c "
import json
for d in json.load(open('$STATE_FILE'))['droplets']:
    print(f\"{d['name']}:{d['ip']}:{d['role']}:{d['node_id']}\")
")

  for info in $droplet_info; do
    IFS=: read -r name ip role node_id <<< "$info"

    # Write node.toml
    remote "$ip" bash -s <<NODE_CONF_EOF
mkdir -p ${run_dir}/artifacts
cat > ${run_dir}/node.toml <<'TOML'
controller_addr = "ws://${ctrl_ip}:${CTRL_PORT}"
public_network_addr = "${ip}"
private_network_addr = "${ip}"
node_id = ${node_id}
channel_backpressure = true

[lossless_runtime_config]
default_block_size = ${BLOCK_SIZE}
fec_enabled = ${fec_enabled}
fec_default_tree_ids = [0]
fec_default_symbols_per_block = 1
TOML
NODE_CONF_EOF

    # Build docker run command based on role
    local role_args=""
    case "$role" in
      source)
        # Also write controller-config for source
        remote "$ip" bash -s <<SRC_CONF_EOF
cat > ${run_dir}/controller-config.toml <<'TOML'
[topology]
n_nodes = ${N_NODES}
edges = [${TOPO_EDGES}]

[routing]
protocol = "shortest_path"

[db]
user = "pgusr"
password = "pgpwrd"
host = "postgres"
database = "nextmini"
port = "5432"
TOML
SRC_CONF_EOF
        # Upload payload
        log "    Uploading payload to source ($ip)..."
        upload "$PAYLOAD_FILE" "$ip" "${run_dir}/payload.bin"
        role_args="--controller-config /run/controller-config.toml --receiver-ids ${RECEIVER_IDS} --tensor-path /run/payload.bin"
        ;;
      receiver)
        role_args="--node-id ${node_id} --tensor-path /run/payload.bin --expected-bytes ${PAYLOAD_BYTES} --receive-timeout-ms ${RECV_TIMEOUT_MS}"
        ;;
      router)
        role_args=""
        ;;
    esac

    log "    Starting ${role} (node ${node_id}) on $ip..."
    remote "$ip" bash -s <<RUN_EOF
docker rm -f fec-node-${node_id} >/dev/null 2>&1 || true
docker run -d --name fec-node-${node_id} --network host --cap-add NET_ADMIN --device /dev/net/tun \
  -e PYTHONUNBUFFERED=1 -e RUST_LOG=info -v ${run_dir}:/run \
  ${node_image} \
  python /app/examples/multicast-docker/scripts/multicast_node.py \
    --role ${role} --config /run/node.toml \
    ${role_args} \
    --group-label ${MODE}-bench --source-node-id ${SOURCE_NODE_ID} \
    --artifact-dir /run/artifacts --chunk-size ${BLOCK_SIZE} \
    --group-timeout ${GROUP_TIMEOUT} --fec ${fec_flag} >/dev/null
RUN_EOF
  done

  # ── Step 3: Coordinate file exchange ──
  local src_ip
  src_ip=$(get_vm1_ip)

  # Wait for group-info.json from source
  log "  Waiting for source group info..."
  local deadline=$((SECONDS + GROUP_TIMEOUT))
  while [ $SECONDS -lt $deadline ]; do
    if remote "$src_ip" "test -f ${run_dir}/artifacts/group-info.json" 2>/dev/null; then
      break
    fi
    sleep 2
  done

  if ! remote "$src_ip" "test -f ${run_dir}/artifacts/group-info.json" 2>/dev/null; then
    log "  ERROR: Timed out waiting for group-info.json"
    log "  Source container logs:"
    remote "$src_ip" "docker logs fec-node-1 2>&1 | tail -15" || true
    return 1
  fi
  log "  ✓ group-info.json ready"

  # Download group-info and upload to receivers
  local coord_dir="/tmp/fec-coord-${RUN_ID}"
  mkdir -p "$coord_dir"
  download "$src_ip" "${run_dir}/artifacts/group-info.json" "$coord_dir/group-info.json"

  for info in $droplet_info; do
    IFS=: read -r name ip role node_id <<< "$info"
    if [ "$role" = "receiver" ]; then
      upload "$coord_dir/group-info.json" "$ip" "${run_dir}/artifacts/group-info.json"
      log "    group-info → receiver node $node_id ($ip)"
    fi
  done

  # Wait for receiver-ready files
  log "  Waiting for receivers to be ready..."
  for info in $droplet_info; do
    IFS=: read -r name ip role node_id <<< "$info"
    if [ "$role" != "receiver" ]; then continue; fi

    local rdl=$((SECONDS + GROUP_TIMEOUT))
    while [ $SECONDS -lt $rdl ]; do
      if remote "$ip" "test -f ${run_dir}/artifacts/receiver-ready-${node_id}.json" 2>/dev/null; then
        break
      fi
      sleep 2
    done
    download "$ip" "${run_dir}/artifacts/receiver-ready-${node_id}.json" "$coord_dir/receiver-ready-${node_id}.json"
    log "    ✓ receiver node $node_id ready"
  done

  # Upload receiver-ready files to source
  for rid in ${RECEIVER_IDS//,/ }; do
    upload "$coord_dir/receiver-ready-${rid}.json" "$src_ip" "${run_dir}/artifacts/receiver-ready-${rid}.json"
  done
  log "  All receivers ready, transmission starting..."

  # Wait for receiver artifacts (data transfer complete)
  log "  Waiting for data transfer to complete..."
  local recv_timeout=$((RECV_TIMEOUT_MS / 1000))
  for info in $droplet_info; do
    IFS=: read -r name ip role node_id <<< "$info"
    if [ "$role" != "receiver" ]; then continue; fi

    local rdl=$((SECONDS + recv_timeout))
    while [ $SECONDS -lt $rdl ]; do
      if remote "$ip" "test -f ${run_dir}/artifacts/receiver-${node_id}.bin" 2>/dev/null; then
        break
      fi
      sleep 3
    done
    if remote "$ip" "test -f ${run_dir}/artifacts/receiver-${node_id}.bin" 2>/dev/null; then
      log "    ✓ receiver $node_id data received"
    else
      log "    ✗ receiver $node_id timed out"
    fi
  done

  # Print logs
  log ""
  log "  ─── Node logs ───"
  for info in $droplet_info; do
    IFS=: read -r name ip role node_id <<< "$info"
    log "  [$role node $node_id]"
    remote "$ip" "docker logs fec-node-${node_id} 2>&1 | grep -iE 'throughput|transfer|complete|bytes|Mbps|MiB|ERROR' | tail -5" || true
  done

  rm -rf "$coord_dir"
  log ""
  log "  Experiment complete!"
}

###############################################################################
# Phase 5: Collect results
###############################################################################
collect_results() {
  log ""
  log "═══════════════════════════════════════════════"
  log "  Phase 5: Collecting results"
  log "═══════════════════════════════════════════════"

  mkdir -p "$RESULTS_DIR"
  local run_dir="/tmp/fec-runs/$RUN_ID"

  local droplet_info
  droplet_info=$(python3 -c "
import json
for d in json.load(open('$STATE_FILE'))['droplets']:
    print(f\"{d['name']}:{d['ip']}:{d['role']}:{d['node_id']}\")
")

  for info in $droplet_info; do
    IFS=: read -r name ip role node_id <<< "$info"
    log "  Fetching from $name ($ip)..."
    mkdir -p "$RESULTS_DIR/$name"

    # Get full container logs
    remote "$ip" "docker logs fec-node-${node_id} 2>&1" \
      > "$RESULTS_DIR/$name/node.log" 2>/dev/null || true

    # Get artifacts
    download "$ip" "${run_dir}/artifacts/*" "$RESULTS_DIR/$name/" 2>/dev/null || true
    download "$ip" "${run_dir}/node.toml" "$RESULTS_DIR/$name/node.toml" 2>/dev/null || true
  done

  # Get controller logs
  local ctrl_ip
  ctrl_ip=$(get_vm1_ip)
  remote "$ctrl_ip" "docker logs fec-controller 2>&1" \
    > "$RESULTS_DIR/controller.log" 2>/dev/null || true

  log "  Results saved to: $RESULTS_DIR/"
}

###############################################################################
# MAIN
###############################################################################

# Phase 1
if [ "$SKIP_PROVISION" = "0" ]; then
  provision_vms
else
  log ""
  log "═══════════════════════════════════════════════"
  log "  Phase 1: Skipped (--skip-provision)"
  log "═══════════════════════════════════════════════"
fi

# Phase 2
if [ "$SKIP_BUILD" = "0" ]; then
  build_images
else
  log ""
  log "═══════════════════════════════════════════════"
  log "  Phase 2: Skipped (--skip-build)"
  log "═══════════════════════════════════════════════"
  if [ -f "$IMAGE_TAG_FILE" ]; then
    IMAGE_TAG=$(cat "$IMAGE_TAG_FILE")
    log "  Using image: nextmini-fec:${IMAGE_TAG}"
  else
    log "ERROR: No image tag file. Run without --skip-build first."
    exit 1
  fi
fi

# Phase 3
generate_payload

# Phase 4
run_experiment

# Phase 5
collect_results

# Summary
log ""
log "═══════════════════════════════════════════════"
log "  COMPLETE"
log "═══════════════════════════════════════════════"
log "  Mode:      $MODE"
log "  Payload:   $PAYLOAD_SIZE"
log "  Image:     nextmini-fec:${IMAGE_TAG}"
log "  Results:   $RESULTS_DIR/"
if [ "$KEEP_VMS" = "1" ]; then
  log ""
  log "  VMs preserved. Destroy with:"
  log "    bash $SCRIPT_DIR/do_run.sh --destroy"
fi
