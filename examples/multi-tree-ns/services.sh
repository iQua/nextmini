wait_for_port() {
  local host=$1
  local port=$2
  local label=$3
  for _ in $(seq 1 60); do
    if (echo >"/dev/tcp/${host}/${port}") >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  echo "${label} did not open ${host}:${port}" >&2
  return 1
}

start_postgres() {
  sudo docker run -d --rm \
    --name "$postgres_name" \
    -p "127.0.0.1:${postgres_port}:5432" \
    -e POSTGRES_USER=pgusr \
    -e POSTGRES_PASSWORD=pgpwrd \
    -e POSTGRES_DB=nextmini \
    -v "$root_dir/controller/init.sql:/docker-entrypoint-initdb.d/init.sql:ro" \
    postgres:alpine >/dev/null

  for _ in $(seq 1 60); do
    if sudo docker exec "$postgres_name" pg_isready -U pgusr -d nextmini >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done

  echo "Postgres did not become ready" >&2
  exit 1
}

start_controller() {
  (
    cd "$work_dir"
    exec env RUST_LOG="${RUST_LOG:-info}" "$controller_bin"
  ) >"$work_dir/logs/controller.log" 2>&1 &
  controller_pid=$!

  wait_for_port 127.0.0.1 "$controller_port" "Controller"
}

cleanup_services() {
  [[ -n "$controller_pid" ]] && kill "$controller_pid" 2>/dev/null || true
  [[ -n "$controller_pid" ]] && wait "$controller_pid" 2>/dev/null || true
  sudo docker rm -f "$postgres_name" >/dev/null 2>&1 || true
}
