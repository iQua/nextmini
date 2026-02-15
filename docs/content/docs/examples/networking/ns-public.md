---
title: "Namespace Public Multi-Host Example"
description: "Run namespace dataplane nodes on two Linux VMs against one public controller endpoint."
---

This example splits namespace dataplane nodes across two Linux hosts while the controller and Postgres run on a third host. It uses the checked-in config set under `examples/ns-public/`.

## Linux prerequisites

- Three reachable Linux machines:
  - controller host (runs Docker services)
  - VM1 dataplane host
  - VM2 dataplane host
- Controller host: Docker Engine + `docker compose`.
- VM1/VM2 hosts: `sudo`, `iproute2`, and either `cargo` or a prebuilt `nextmini` binary.
- Network reachability from VM1/VM2 to `controller_addr` on TCP port `3000`.

Namespace-specific behavior from this example:

- `VM1-config.toml` and `VM2-config.toml` both enable:
  - `auto_enable_ip_forward = true`
  - `auto_add_forward_rules = true`
  - `auto_add_nat = true`
- `VM2-config.toml` sets `node_id_offset = 5` so VM2 node IDs do not overlap VM1.
- `cleanup.sh` removes host veth interfaces; run it only on the dataplane hosts used for this test.

## 1. Prepare config files on each host

The `ns-public` example does not use a generator script; edit the three TOML files directly.

On the controller host, confirm `examples/ns-public/controller-config.toml` matches your node count (default ring with `n_nodes = 10`).

On VM1 and VM2, update both interface names and controller endpoint in:

- `examples/ns-public/VM1-config.toml`
- `examples/ns-public/VM2-config.toml`

By default these files assume `public_network_interface = "ens3"` and `controller_addr = "206.12.95.232:3000"`.

## 2. Start controller and Postgres (controller host)

From repository root on the controller host:

```bash
docker compose -f examples/ns-public/docker-compose.yml up --build
```

This launches:

- `postgres` (with `nextmini` DB initialized from `controller/init.sql`)
- `controller` (reads `examples/ns-public/controller-config.toml`)

## 3. Start VM1 dataplane

From repository root on VM1:

```bash
cargo build -p nextmini --release
sudo -E env RUST_LOG=info ./target/release/nextmini --config-path examples/ns-public/VM1-config.toml
```

## 4. Start VM2 dataplane

From repository root on VM2:

```bash
cargo build -p nextmini --release
sudo -E env RUST_LOG=info ./target/release/nextmini --config-path examples/ns-public/VM2-config.toml
```

Because `VM2-config.toml` uses `node_id_offset = 5`, VM2 contributes nodes `6..10` while VM1 contributes `1..5`.

## 5. Verify cluster state

On the controller host, verify all namespace nodes registered:

```bash
docker exec postgres psql -U pgusr -d nextmini -c \
  "SELECT COUNT(*) AS nodes_connected, COUNT(DISTINCT node_id) AS unique_nodes FROM nodes;"
```

For the default configs, expect `10` connected and `10` unique nodes.

You can also watch controller logs:

```bash
docker compose -f examples/ns-public/docker-compose.yml logs -f controller
```

## 6. Cleanup

On VM1 and VM2:

1. Stop dataplane processes with `Ctrl+C`.
2. Run veth cleanup:

```bash
./examples/ns-public/cleanup.sh
```

On the controller host, stop controller and Postgres:

```bash
docker compose -f examples/ns-public/docker-compose.yml down
```
