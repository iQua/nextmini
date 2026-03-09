cleanup_namespaces() {
  for pid in "${node_pids[@]:-}"; do
    kill "$pid" 2>/dev/null || true
  done
  wait "${node_pids[@]:-}" 2>/dev/null || true

  if [[ -n "${n_nodes:-}" && "${n_nodes}" =~ ^[0-9]+$ ]]; then
    for id in $(seq 1 "$n_nodes"); do
      sudo ip netns del "mtns-ns-${link_tag}-${id}" 2>/dev/null || true
    done
  fi

  sudo ip link del "$bridge_name" 2>/dev/null || true
}

create_namespaces() {
  sudo ip link add "$bridge_name" type bridge
  sudo ip addr add "$bridge_cidr" dev "$bridge_name"
  sudo ip link set "$bridge_name" up

  for id in $(seq 1 "$n_nodes"); do
    local ns="mtns-ns-${link_tag}-${id}"
    local host_if="mv${link_tag}a${id}"
    local ns_if="mv${link_tag}b${id}"
    local ns_ip="${bridge_prefix}.$((id + 1))"

    sudo ip netns add "$ns"
    sudo ip link add "$host_if" type veth peer name "$ns_if"
    sudo ip link set "$host_if" master "$bridge_name"
    sudo ip link set "$host_if" up
    sudo ip link set "$ns_if" netns "$ns"
    sudo ip -n "$ns" link set lo up
    sudo ip -n "$ns" link set "$ns_if" name eth0
    sudo ip -n "$ns" addr add "${ns_ip}/24" dev eth0
    sudo ip -n "$ns" link set eth0 up
    sudo ip -n "$ns" route add default via "$bridge_ip"
  done
}

launch_nodes() {
  for id in $(seq 1 "$n_nodes"); do
    local role
    role="$("$gen_python" -c 'import json, sys; print(json.load(open(sys.argv[1]))["roles"][sys.argv[2]])' "$work_dir/scenario.json" "$id")"
    local ns="mtns-ns-${link_tag}-${id}"
    local log_path="$work_dir/logs/node-${id}.log"

    sudo ip netns exec "$ns" env RUST_LOG="${RUST_LOG:-info}" "$python_bin" \
      "$script_dir/node.py" \
      --role "$role" \
      --config "$work_dir/node-${id}.toml" \
      --work-dir "$work_dir" >"$log_path" 2>&1 &

    node_pids+=("$!")
    if [[ "$role" == "source" ]]; then
      source_pid="$!"
    elif [[ "$role" == "receiver" ]]; then
      receiver_pids+=("$!")
    fi
  done
}

wait_for_transfer() {
  [[ -n "$source_pid" ]] || { echo "source process did not start" >&2; exit 1; }
  wait "$source_pid"

  for pid in "${receiver_pids[@]:-}"; do
    wait "$pid"
  done
}

verify_outputs() {
  for rid in ${receivers_csv//,/ }; do
    local output_file="$work_dir/out-${rid}.bin"
    [[ -f "$output_file" ]] || { echo "missing receiver output: $output_file" >&2; exit 1; }
    cmp -s "$input_file" "$output_file" || {
      echo "receiver output mismatch for node ${rid}" >&2
      exit 1
    }
  done
}
