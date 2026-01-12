#!/usr/bin/env python3

from __future__ import annotations

import argparse
import concurrent.futures
import dataclasses
import json
import os
import pathlib
import shlex
import subprocess
import textwrap
import time
import typing as t

try:
    import tomllib  # Python 3.11+
except ImportError:  # pragma: no cover
    import tomli as tomllib  # type: ignore[no-redef]


REPO_ROOT = pathlib.Path(__file__).resolve().parents[3]
GENERATED_DIR = REPO_ROOT / "examples" / "rl" / "multidc" / "generated"
RESULTS_DIR = REPO_ROOT / "examples" / "rl" / "multidc" / "results"


def _expand_remote_home(path: str) -> str:
    """Expand a leading ~ or ~/ to $HOME for remote bash commands."""
    if path == "~":
        return "$HOME"
    if path.startswith("~/"):
        return "$HOME/" + path[2:]
    return path


def _expand_remote_home_for_scp(path: str) -> str:
    """Convert a leading $HOME back to ~ for scp-style remote paths."""
    if path == "$HOME":
        return "~"
    if path.startswith("$HOME/"):
        return "~/" + path[len("$HOME/") :]
    return path


def _bash_dquote(expr: str) -> str:
    """Double-quote for bash while allowing $VAR expansion."""
    expr = expr.replace("\\", "\\\\").replace('"', '\\"')
    return f'"{expr}"'


@dataclasses.dataclass(frozen=True)
class SshTarget:
    host: str
    user: str
    port: int
    identity_file: str | None

    def display(self) -> str:
        return f"{self.user}@{self.host}:{self.port}"


@dataclasses.dataclass(frozen=True)
class Controller:
    ssh: SshTarget
    public_ip: str


@dataclasses.dataclass(frozen=True)
class Node:
    role: str  # trainer|worker|relay
    node_id: int
    ssh: SshTarget
    public_ip: str
    region: str | None
    rank: int | None
    public_network_interface: str
    private_network_interface: str

    def container_name(self) -> str:
        return f"skyrocket-{self.role}-{self.node_id}"


def _expand_user(path: str) -> str:
    return os.path.expanduser(path)


def _ssh_base_args(target: SshTarget, *, batch: bool) -> list[str]:
    args = ["ssh", "-p", str(target.port)]
    if target.identity_file:
        args += ["-i", _expand_user(target.identity_file)]
    # Avoid interactive host-key prompts on freshly provisioned VMs.
    args += ["-o", "StrictHostKeyChecking=accept-new"]
    if batch:
        args += ["-o", "BatchMode=yes"]
    # Keep logs readable and avoid hanging forever.
    args += ["-o", "ConnectTimeout=10"]
    return args + [f"{target.user}@{target.host}"]


def _run(cmd: list[str], *, capture: bool = False) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        cmd,
        check=True,
        text=True,
        stdout=subprocess.PIPE if capture else None,
        stderr=subprocess.STDOUT if capture else None,
    )


def ssh_run(target: SshTarget, command: str, *, batch: bool, capture: bool = False) -> str:
    wrapped = f"set -euo pipefail\n{command}"
    # IMPORTANT: ssh builds a remote command string by joining argv with spaces.
    # If we pass an unquoted multi-word script as a separate argv element, only the
    # first word becomes bash's -c string and the rest become positional parameters.
    # Always shell-quote the wrapped script so it stays a single argument remotely.
    args = _ssh_base_args(target, batch=batch) + ["bash", "-lc", shlex.quote(wrapped)]
    try:
        res = _run(args, capture=capture)
        return res.stdout or ""
    except subprocess.CalledProcessError as exc:
        out = exc.stdout or ""
        raise SystemExit(
            f"Remote command failed on {target.display()}.\n"
            f"Output:\n{out}"
        ) from exc


def rsync_repo(target: SshTarget, *, remote_repo_dir: str, batch: bool, delete: bool) -> None:
    ssh_cmd = _ssh_base_args(target, batch=batch)
    # rsync wants the remote shell as a string; exclude the user@host part (last element).
    ssh_shell = " ".join(shlex.quote(part) for part in ssh_cmd[:-1])
    excludes = [
        ".git/",
        "target/",
        "**/target/",
        # Per-host caches (cargo/venv) should never be synced.
        ".multidc_cache/",
        "**/.multidc_cache/**",
        "**/__pycache__/",
        "**/.venv/",
        "**/.mypy_cache/",
        "**/.pytest_cache/",
        "nextmini-data-plane-wrapper.log",
        # Large artifacts not needed for WAN microbenchmarks.
        "tensors/",
        "**/tensors/**",
        # Local-only artifacts; avoid syncing large/ever-growing outputs to every host.
        "examples/rl/multidc/results/",
        "examples/rl/multidc/archives/",
        "examples/rl/multidc/artifacts/received/",
        "**/multidc/results/**",
        "**/multidc/archives/**",
        "**/multidc/artifacts/received/**",
    ]
    cmd = [
        "rsync",
        "-az",
        "--stats",
    ]
    if delete:
        cmd.append("--delete")
    for item in excludes:
        cmd += ["--exclude", item]
    cmd += ["-e", ssh_shell, str(REPO_ROOT) + "/", f"{target.user}@{target.host}:{remote_repo_dir}/"]
    # Tolerate rsync exit code 23 (partial transfer due to error) which often happens
    # with special files or symlinks that don't affect core code.
    result = subprocess.run(cmd, check=False, text=True)
    if result.returncode not in (0, 23):
        raise subprocess.CalledProcessError(result.returncode, cmd)


def collect_ping_matrix(
    nodes: list[Node],
    *,
    count: int,
    timeout_secs: int,
    batch: bool,
) -> dict[str, object]:
    id_by_ip = {n.public_ip: n.node_id for n in nodes}
    all_ips = [n.public_ip for n in nodes]

    def _parse_int(value: str) -> int | None:
        value = value.strip()
        return int(value) if value else None

    def _parse_float(value: str) -> float | None:
        value = value.strip()
        return float(value) if value else None

    def ping_from(src: Node) -> tuple[int, list[dict[str, object]], list[str]]:
        dest_ips = [ip for ip in all_ips if ip != src.public_ip]
        dests_expr = " ".join(shlex.quote(ip) for ip in dest_ips)
        script = textwrap.dedent(
            f"""
            if ! command -v ping >/dev/null 2>&1; then
              echo "PING_MATRIX_ERROR\tmissing_ping"
              exit 0
            fi

            dests=({dests_expr})
            for dst in "${{dests[@]}}"; do
              out=$(ping -n -q -c {int(count)} -W {int(timeout_secs)} "$dst" 2>&1 || true)
              tx=$(echo "$out" | awk '/packets transmitted/ {{print $1; exit}}')
              rx=$(echo "$out" | awk -F',' '/packets transmitted/ {{gsub(/^ +| +$/, "", $2); split($2,a," "); print a[1]; exit}}')
              loss=$(echo "$out" | awk -F',' '/packets transmitted/ {{gsub(/^ +| +$/, "", $3); split($3,a," "); gsub(/%/, "", a[1]); print a[1]; exit}}')
              rtt=$(echo "$out" | awk -F'=' '/rtt/ {{gsub(/^ +| +$/, "", $2); split($2,a,"/"); print a[2]; exit}}')
              printf '%s\\t%s\\t%s\\t%s\\t%s\\n' "$dst" "${{tx:-}}" "${{rx:-}}" "${{loss:-}}" "${{rtt:-}}"
            done
            """
        ).strip()

        raw = ssh_run(src.ssh, script, batch=batch, capture=True)
        entries: list[dict[str, object]] = []
        errors: list[str] = []
        for line in raw.splitlines():
            if not line.strip():
                continue
            if line.startswith("PING_MATRIX_ERROR\t"):
                errors.append(line.strip())
                continue
            parts = line.split("\t")
            if len(parts) != 5:
                errors.append(f"PING_MATRIX_ERROR\tbad_line\t{line.strip()}")
                continue
            dst_ip, tx_s, rx_s, loss_s, rtt_s = parts
            dst_id = id_by_ip.get(dst_ip.strip())
            if dst_id is None:
                errors.append(f"PING_MATRIX_ERROR\tunknown_dst_ip\t{dst_ip.strip()}")
                continue
            entries.append(
                {
                    "src": int(src.node_id),
                    "dst": int(dst_id),
                    "src_ip": str(src.public_ip),
                    "dst_ip": str(dst_ip.strip()),
                    "transmitted": _parse_int(tx_s),
                    "received": _parse_int(rx_s),
                    "loss_pct": _parse_float(loss_s),
                    "rtt_avg_ms": _parse_float(rtt_s),
                }
            )
        return src.node_id, entries, errors

    entries: list[dict[str, object]] = []
    errors: list[str] = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=min(8, len(nodes))) as ex:
        futs = {ex.submit(ping_from, node): node for node in nodes}
        for fut in concurrent.futures.as_completed(futs):
            node = futs[fut]
            try:
                _src_id, row, row_errors = fut.result()
            except Exception as exc:  # pragma: no cover
                errors.append(f"PING_MATRIX_ERROR\tssh_failed\t{node.ssh.display()}\t{exc}")
                continue
            entries.extend(row)
            errors.extend(row_errors)

    entries.sort(key=lambda e: (int(t.cast(int, e["src"])), int(t.cast(int, e["dst"]))))
    nodes_out = [
        {
            "node_id": int(n.node_id),
            "public_ip": str(n.public_ip),
            "role": str(n.role),
            "region": str(n.region or ""),
        }
        for n in sorted(nodes, key=lambda n: n.node_id)
    ]
    return {
        "timestamp_unix_s": time.time(),
        "ping_count": int(count),
        "ping_timeout_secs": int(timeout_secs),
        "nodes": nodes_out,
        "pings": entries,
        "errors": errors,
    }


def load_inventory(path: pathlib.Path) -> tuple[Controller, list[Node], str, bool]:
    raw = tomllib.loads(path.read_bytes().decode("utf-8"))

    ssh_cfg = raw.get("ssh", {}) or {}
    default_user = str(ssh_cfg.get("user", "ubuntu"))
    default_port = int(ssh_cfg.get("port", 22))
    identity_file = ssh_cfg.get("identity_file")
    identity_file = str(identity_file) if identity_file else None

    paths_cfg = raw.get("paths", {}) or {}
    remote_repo_dir = str(paths_cfg.get("remote_repo_dir", "~/skyrocket/nextmini"))

    controller_cfg = raw.get("controller", {}) or {}
    controller_host = str(controller_cfg.get("host", "")).strip()
    if not controller_host:
        raise SystemExit("inventory: [controller].host is required")
    controller_public_ip = str(controller_cfg.get("public_ip", controller_host)).strip()

    controller_user = str(controller_cfg.get("user", default_user))
    controller_port = int(controller_cfg.get("port", default_port))
    controller_identity = str(controller_cfg.get("identity_file", identity_file)) if controller_cfg.get("identity_file") else identity_file

    controller = Controller(
        ssh=SshTarget(
            host=controller_host,
            user=controller_user,
            port=controller_port,
            identity_file=controller_identity,
        ),
        public_ip=controller_public_ip,
    )

    nodes_raw = raw.get("nodes", [])
    if not isinstance(nodes_raw, list) or not nodes_raw:
        raise SystemExit("inventory: [[nodes]] entries are required")

    nodes: list[Node] = []
    for idx, entry in enumerate(nodes_raw):
        if not isinstance(entry, dict):
            raise SystemExit(f"inventory: nodes[{idx}] must be a table")
        role = str(entry.get("role", "")).strip()
        if role not in ("trainer", "worker", "relay"):
            raise SystemExit(f"inventory: nodes[{idx}].role must be trainer|worker|relay")
        node_id = int(entry.get("node_id", 0))
        if node_id <= 0:
            raise SystemExit(f"inventory: nodes[{idx}].node_id must be positive")
        host = str(entry.get("host", "")).strip()
        if not host:
            raise SystemExit(f"inventory: nodes[{idx}].host is required")
        public_ip = str(entry.get("public_ip", host)).strip()
        region = entry.get("region")
        region = str(region).strip() if region else None
        rank = entry.get("rank")
        if role == "worker":
            if rank is None:
                raise SystemExit(f"inventory: nodes[{idx}] worker requires rank")
            rank = int(rank)
        else:
            rank = None

        user = str(entry.get("user", default_user))
        port = int(entry.get("port", default_port))
        node_identity = str(entry.get("identity_file", identity_file)) if entry.get("identity_file") else identity_file
        pub_itf = str(entry.get("public_network_interface", entry.get("network_interface", "eth0")))
        priv_itf = str(entry.get("private_network_interface", entry.get("network_interface", "eth0")))

        nodes.append(
            Node(
                role=role,
                node_id=node_id,
                ssh=SshTarget(host=host, user=user, port=port, identity_file=node_identity),
                public_ip=public_ip,
                region=region,
                rank=rank,
                public_network_interface=pub_itf,
                private_network_interface=priv_itf,
            )
        )

    # Basic validation.
    trainers = [n for n in nodes if n.role == "trainer"]
    if len(trainers) != 1:
        raise SystemExit("inventory: exactly one trainer node is required")
    worker_nodes = sorted((n for n in nodes if n.role == "worker"), key=lambda n: t.cast(int, n.rank))
    if not worker_nodes:
        raise SystemExit("inventory: at least one worker node is required")

    ranks = [t.cast(int, n.rank) for n in worker_nodes]
    if ranks != list(range(len(ranks))):
        raise SystemExit(f"inventory: worker ranks must be contiguous 0..{len(ranks)-1}, got {ranks}")

    ids = [n.node_id for n in nodes]
    if len(ids) != len(set(ids)):
        raise SystemExit(f"inventory: node_id values must be unique, got {ids}")

    # For host-network mode, assume one node per VM.
    hosts = [n.ssh.host for n in nodes]
    if len(hosts) != len(set(hosts)):
        raise SystemExit(
            "inventory: multiple nodes share the same host; this runner assumes one Nextmini node per VM "
            "(host-network containers bind fixed ports)."
        )

    max_id = max(ids)
    if set(ids) != set(range(1, max_id + 1)):
        raise SystemExit(
            f"inventory: node_id must be contiguous 1..{max_id} for controller full-mesh config, got {sorted(ids)}"
        )

    return controller, nodes, remote_repo_dir, bool(raw.get("dangerously_delete_remote_repo", False))


def write_controller_config(*, n_nodes: int) -> None:
    GENERATED_DIR.mkdir(parents=True, exist_ok=True)
    cfg = textwrap.dedent(
        f"""\
        protocol = "tcp"
        flow_transport = "lossless_unicast"

        [topology]
        type = "full_mesh"
        full_mesh_config = {{ n_nodes = {n_nodes} }}

        [routing]
        protocol = "shortest_path"

        [db]
        host = "postgres"
        port = "5432"
        user = "pgusr"
        password = "pgpwrd"
        database = "nextmini"
        """
    )
    (GENERATED_DIR / "controller-config.toml").write_text(cfg, encoding="utf-8")


def write_node_config(*, node: Node, controller_ip: str) -> None:
    GENERATED_DIR.mkdir(parents=True, exist_ok=True)
    # Default to unique private_network_name so the controller chooses public addresses between nodes.
    private_name = f"node-{node.node_id}"
    cfg = textwrap.dedent(
        f"""\
        controller_addr = "ws://{controller_ip}:3000"
        private_network_interface = "{node.private_network_interface}"
        private_network_name = "{private_name}"
        public_network_interface = "{node.public_network_interface}"
        public_network_addr = "{node.public_ip}"
        private_network_addr = "{node.public_ip}"
        public_network_port = "8080"
        private_network_port = "8080"
        node_id = {node.node_id}
        num_tun_queues = 1
        num_packet_processors = 4
        channel_capacity = 3000
        queue_capacity = 4000
        feature = "concurrent"
        channel_backpressure = true
        """
    )
    (GENERATED_DIR / f"node-{node.node_id}.toml").write_text(cfg, encoding="utf-8")


def generate_configs(controller: Controller, nodes: list[Node]) -> None:
    write_controller_config(n_nodes=max(n.node_id for n in nodes))
    for node in nodes:
        write_node_config(node=node, controller_ip=controller.public_ip)


def docker_rm(target: SshTarget, name: str, *, batch: bool) -> None:
    ssh_run(target, f"docker rm -f {shlex.quote(name)} >/dev/null 2>&1 || true", batch=batch)


def controller_up(controller: Controller, *, remote_repo_dir: str, batch: bool) -> None:
    repo_expr = _expand_remote_home(remote_repo_dir)
    repo_q = _bash_dquote(repo_expr)
    ssh_run(
        controller.ssh,
        f"cd {repo_q} && docker pull postgres:16-alpine >/dev/null",
        batch=batch,
    )
    ssh_run(
        controller.ssh,
        "docker network create skyrocket-ctrl >/dev/null 2>&1 || true",
        batch=batch,
    )
    docker_rm(controller.ssh, "skyrocket-postgres", batch=batch)
    docker_rm(controller.ssh, "skyrocket-controller", batch=batch)

    ssh_run(
        controller.ssh,
        textwrap.dedent(
            f"""
            docker run -d --name skyrocket-postgres --hostname postgres --network skyrocket-ctrl \\
              -e POSTGRES_USER=pgusr -e POSTGRES_PASSWORD=pgpwrd -e POSTGRES_DB=nextmini \\
              -v skyrocket-postgres-data:/var/lib/postgresql/data \\
              -v {_bash_dquote(f"{repo_expr}/controller/init.sql:/docker-entrypoint-initdb.d/init.sql:ro")} \\
              postgres:16-alpine >/dev/null

            for i in $(seq 1 60); do
              if docker exec skyrocket-postgres pg_isready -U pgusr -d nextmini >/dev/null 2>&1; then
                exit 0
              fi
              sleep 1
            done
            echo "postgres not ready in time" >&2
            exit 1
            """
        ).strip(),
        batch=batch,
    )

    ssh_run(
        controller.ssh,
        f"cd {repo_q} && docker build -t nextmini_controller -f controller/Dockerfile . >/dev/null 2>&1",
        batch=batch,
    )
    ssh_run(
        controller.ssh,
        textwrap.dedent(
            f"""
            docker run -d --name skyrocket-controller --hostname controller --network skyrocket-ctrl \\
              -p 3000:3000 \\
              -v {_bash_dquote(f"{repo_expr}/examples/rl/multidc/generated/controller-config.toml:/var/nextmini/config.toml:ro")} \\
              nextmini_controller \\
              /bin/bash -lc "cd /var/nextmini && /var/nextmini/controller" >/dev/null
            """
        ).strip(),
        batch=batch,
    )


def node_image_build(node: Node, *, remote_repo_dir: str, batch: bool) -> None:
    repo_q = _bash_dquote(_expand_remote_home(remote_repo_dir))
    ssh_run(
        node.ssh,
        f"cd {repo_q} && docker build -t nextmini_rl_python -f examples/rl/Dockerfile . >/dev/null 2>&1",
        batch=batch,
    )


def run_relay(node: Node, *, remote_repo_dir: str, batch: bool) -> None:
    docker_rm(node.ssh, node.container_name(), batch=batch)
    repo_expr = _expand_remote_home(remote_repo_dir)
    config_path = f"examples/rl/multidc/generated/node-{node.node_id}.toml"
    inner = f"examples/rl/scripts/run_relay_node.sh {config_path}"
    cmd = textwrap.dedent(
        f"""
        docker run -d --name {shlex.quote(node.container_name())} --network host \\
          -v {_bash_dquote(f"{repo_expr}:/workspace")} \\
          -w /workspace \\
          -e RUST_LOG=${{RUST_LOG:-info}} \\
          nextmini_rl_python \\
          /bin/bash -lc {shlex.quote(inner)} \\
          >/dev/null
        """
    ).strip()
    ssh_run(node.ssh, cmd, batch=batch)


def run_worker(
    node: Node,
    *,
    remote_repo_dir: str,
    trainer_node_id: int,
    rounds: int,
    timeout_ms: int,
    force_rebuild: bool,
    batch: bool,
) -> None:
    assert node.rank is not None
    docker_rm(node.ssh, node.container_name(), batch=batch)
    repo_expr = _expand_remote_home(remote_repo_dir)
    config_path = f"examples/rl/multidc/generated/node-{node.node_id}.toml"
    sink_dir = f"examples/rl/multidc/artifacts/received/node-{node.node_id}"
    inner = (
        f"examples/rl/scripts/run_broadcast_bench.sh worker {config_path} "
        f"--rounds {rounds} --timeout-ms {timeout_ms} "
        f"--trainer-node-id {trainer_node_id} --rank {node.rank} --sink-dir {sink_dir}"
    )
    cmd_lines = [
        f"docker run -d --name {shlex.quote(node.container_name())} --network host \\",
        f"  -v {_bash_dquote(f'{repo_expr}:/workspace')} \\",
        "  -w /workspace \\",
        "  -e RUST_LOG=${RUST_LOG:-info} \\",
    ]
    if force_rebuild:
        cmd_lines.append("  -e NEXTMINI_PY_FORCE_REBUILD=1 \\")
    cmd_lines.extend(
        [
            "  nextmini_rl_python \\",
            f"  /bin/bash -lc {shlex.quote(inner)} \\",
            "  >/dev/null",
        ]
    )
    cmd = "\n".join(cmd_lines)
    ssh_run(node.ssh, cmd, batch=batch)


def run_rl_worker(
    node: Node,
    *,
    remote_repo_dir: str,
    trainer_node_id: int,
    worker_node_ids: list[int],
    controller_config: str,
    model_name: str,
    train_steps: int,
    batch_size: int,
    grpo_group_size: int,
    generation_len: int,
    max_seq_len: int,
    multicast_timeout_ms: int,
    use_sharded_weights: bool,
    shard_size: str,
    multicast_tree_algo: str,
    hop_limit: int,
    eta: float,
    num_paths: int,
    relay_scoring: str,
    max_relays: int | None,
    allow_worker_relays: bool,
    torch_index_url: str,
    cpu_only: bool,
    force_rebuild: bool,
    batch: bool,
) -> None:
    assert node.rank is not None
    docker_rm(node.ssh, node.container_name(), batch=batch)
    repo_expr = _expand_remote_home(remote_repo_dir)
    config_path = f"examples/rl/multidc/generated/node-{node.node_id}.toml"
    workers_csv = ",".join(str(v) for v in worker_node_ids)

    max_relays_env = "" if max_relays is None else str(max_relays)
    allow_worker_relays_env = "true" if allow_worker_relays else "false"
    use_sharded_weights_env = "true" if use_sharded_weights else "false"

    inner = f"examples/rl/scripts/run_rl_node.sh worker {config_path} --rank {node.rank}"

    cmd_lines = [
        f"docker run -d --name {shlex.quote(node.container_name())} --network host \\",
        f"  -v {_bash_dquote(f'{repo_expr}:/workspace')} \\",
        "  -w /workspace \\",
        "  -e RUST_LOG=${RUST_LOG:-info} \\",
        f"  -e TRAINER_NODE_ID={int(trainer_node_id)} \\",
        f"  -e WORKER_NODE_IDS={_bash_dquote(workers_csv)} \\",
        f"  -e CONTROLLER_CONFIG={_bash_dquote(controller_config)} \\",
        f"  -e MODEL_NAME={_bash_dquote(model_name)} \\",
        f"  -e TRAIN_STEPS={int(train_steps)} \\",
        f"  -e BATCH_SIZE={int(batch_size)} \\",
        f"  -e GRPO_GROUP_SIZE={int(grpo_group_size)} \\",
        f"  -e GENERATION_LEN={int(generation_len)} \\",
        f"  -e MAX_SEQ_LEN={int(max_seq_len)} \\",
        f"  -e MULTICAST_TIMEOUT_MS={int(multicast_timeout_ms)} \\",
        f"  -e USE_SHARDED_WEIGHTS={use_sharded_weights_env} \\",
        f"  -e SHARD_SIZE={_bash_dquote(shard_size)} \\",
        f"  -e MULTICAST_TREE_ALGO={_bash_dquote(multicast_tree_algo)} \\",
        f"  -e MULTICAST_HOP_LIMIT={int(hop_limit)} \\",
        f"  -e MULTICAST_ETA={float(eta)} \\",
        f"  -e MULTICAST_NUM_PATHS={int(num_paths)} \\",
        f"  -e MULTICAST_RELAY_SCORING={_bash_dquote(relay_scoring)} \\",
        f"  -e MULTICAST_MAX_RELAYS={_bash_dquote(max_relays_env)} \\",
        f"  -e MULTICAST_ALLOW_WORKER_RELAYS={allow_worker_relays_env} \\",
        f"  -e TORCH_INDEX_URL={_bash_dquote(torch_index_url)} \\",
        "  -e SKIP_SAVE_MODEL=1 \\",
    ]
    if cpu_only:
        cmd_lines.append("  -e CUDA_VISIBLE_DEVICES=-1 \\")
    if force_rebuild:
        cmd_lines.append("  -e NEXTMINI_PY_FORCE_REBUILD=1 \\")
    cmd_lines.extend(
        [
            "  nextmini_rl_python \\",
            f"  /bin/bash -lc {shlex.quote(inner)} \\",
            "  >/dev/null",
        ]
    )
    cmd = "\n".join(cmd_lines)
    ssh_run(node.ssh, cmd, batch=batch)


def run_rl_trainer(
    node: Node,
    *,
    remote_repo_dir: str,
    trainer_node_id: int,
    worker_node_ids: list[int],
    controller_config: str,
    capacity_snapshot: str | None,
    model_name: str,
    train_steps: int,
    batch_size: int,
    grpo_group_size: int,
    generation_len: int,
    max_seq_len: int,
    multicast_timeout_ms: int,
    use_sharded_weights: bool,
    shard_size: str,
    multicast_tree_algo: str,
    hop_limit: int,
    eta: float,
    num_paths: int,
    relay_scoring: str,
    max_relays: int | None,
    allow_worker_relays: bool,
    torch_index_url: str,
    cpu_only: bool,
    force_rebuild: bool,
    batch: bool,
) -> str:
    docker_rm(node.ssh, node.container_name(), batch=batch)
    repo_expr = _expand_remote_home(remote_repo_dir)
    config_path = f"examples/rl/multidc/generated/node-{node.node_id}.toml"
    workers_csv = ",".join(str(v) for v in worker_node_ids)

    max_relays_env = "" if max_relays is None else str(max_relays)
    allow_worker_relays_env = "true" if allow_worker_relays else "false"
    use_sharded_weights_env = "true" if use_sharded_weights else "false"

    inner = f"examples/rl/scripts/run_rl_node.sh trainer {config_path}"

    cmd_lines = [
        f"docker run --name {shlex.quote(node.container_name())} --network host \\",
        f"  -v {_bash_dquote(f'{repo_expr}:/workspace')} \\",
        "  -w /workspace \\",
        "  -e RUST_LOG=${RUST_LOG:-info} \\",
        f"  -e TRAINER_NODE_ID={int(trainer_node_id)} \\",
        f"  -e WORKER_NODE_IDS={_bash_dquote(workers_csv)} \\",
        f"  -e CONTROLLER_CONFIG={_bash_dquote(controller_config)} \\",
        *(
            [f"  -e MULTICAST_CAPACITY_SNAPSHOT={_bash_dquote(capacity_snapshot)} \\"]
            if capacity_snapshot
            else []
        ),
        f"  -e MODEL_NAME={_bash_dquote(model_name)} \\",
        f"  -e TRAIN_STEPS={int(train_steps)} \\",
        f"  -e BATCH_SIZE={int(batch_size)} \\",
        f"  -e GRPO_GROUP_SIZE={int(grpo_group_size)} \\",
        f"  -e GENERATION_LEN={int(generation_len)} \\",
        f"  -e MAX_SEQ_LEN={int(max_seq_len)} \\",
        f"  -e MULTICAST_TIMEOUT_MS={int(multicast_timeout_ms)} \\",
        f"  -e USE_SHARDED_WEIGHTS={use_sharded_weights_env} \\",
        f"  -e SHARD_SIZE={_bash_dquote(shard_size)} \\",
        f"  -e MULTICAST_TREE_ALGO={_bash_dquote(multicast_tree_algo)} \\",
        f"  -e MULTICAST_HOP_LIMIT={int(hop_limit)} \\",
        f"  -e MULTICAST_ETA={float(eta)} \\",
        f"  -e MULTICAST_NUM_PATHS={int(num_paths)} \\",
        f"  -e MULTICAST_RELAY_SCORING={_bash_dquote(relay_scoring)} \\",
        f"  -e MULTICAST_MAX_RELAYS={_bash_dquote(max_relays_env)} \\",
        f"  -e MULTICAST_ALLOW_WORKER_RELAYS={allow_worker_relays_env} \\",
        f"  -e TORCH_INDEX_URL={_bash_dquote(torch_index_url)} \\",
        "  -e SKIP_SAVE_MODEL=1 \\",
    ]
    if cpu_only:
        cmd_lines.append("  -e CUDA_VISIBLE_DEVICES=-1 \\")
    if force_rebuild:
        cmd_lines.append("  -e NEXTMINI_PY_FORCE_REBUILD=1 \\")
    cmd_lines.extend(
        [
            "  nextmini_rl_python \\",
            f"  /bin/bash -lc {shlex.quote(inner)}",
        ]
    )
    cmd = "\n".join(cmd_lines)
    return ssh_run(node.ssh, cmd, batch=batch, capture=True)


def run_trainer(
    node: Node,
    *,
    remote_repo_dir: str,
    worker_node_ids: list[int],
    rounds: int,
    timeout_ms: int,
    bytes_to_generate: int,
    algorithm: str,
    hop_limit: int,
    eta: float,
    num_paths: int,
    relay_scoring: str,
    max_relays: int | None,
    relay_selection: str,
    selection_seed: int,
    probe_links: bool,
    probe_bytes: int,
    probe_warmup_bytes: int,
    probe_batch_size: int,
    probe_timeout_secs: float,
    allow_worker_relays: bool,
    ping_matrix: str | None,
    capacity_snapshot: str | None,
    probe_retest_outliers: bool,
    probe_outlier_mbps: float,
    probe_outlier_median_frac: float,
    probe_outlier_asymmetry_frac: float,
    probe_retest_bytes: int,
    probe_retest_repeats: int,
    probe_retest_max_edges: int,
    force_rebuild: bool,
    batch: bool,
) -> str:
    docker_rm(node.ssh, node.container_name(), batch=batch)
    repo_expr = _expand_remote_home(remote_repo_dir)
    config_path = f"examples/rl/multidc/generated/node-{node.node_id}.toml"
    controller_cfg = "examples/rl/multidc/generated/controller-config.toml"
    results_dir = "examples/rl/multidc/results"
    out_json = f"{results_dir}/results.json"
    artifact = "examples/rl/multidc/artifacts/broadcast.bin"
    workers_csv = ",".join(str(n) for n in worker_node_ids)

    inner = (
        f"mkdir -p {results_dir} && "
        f"examples/rl/scripts/run_broadcast_bench.sh trainer {config_path} "
        f"--controller-config {controller_cfg} --rounds {rounds} --timeout-ms {timeout_ms} "
        f"--worker-node-ids {workers_csv} --file {artifact} --generate-bytes {bytes_to_generate} "
        f"--output-json {out_json} --algorithm {algorithm} --hop-limit {hop_limit} --eta {eta} "
        f"--num-paths {num_paths} --relay-scoring {relay_scoring} "
        f"--relay-selection {relay_selection} --selection-seed {selection_seed}"
    )
    if max_relays is not None:
        inner += f" --max-relays {max_relays}"
    if probe_links:
        inner += (
            f" --probe-links --probe-bytes {probe_bytes} --probe-timeout-secs {probe_timeout_secs} "
            f"--probe-batch-size {probe_batch_size}"
        )
        if int(probe_warmup_bytes) > 0:
            inner += f" --probe-warmup-bytes {int(probe_warmup_bytes)}"
        inner += " --probe-retest-outliers" if probe_retest_outliers else " --no-probe-retest-outliers"
        inner += (
            f" --probe-outlier-mbps {float(probe_outlier_mbps)}"
            f" --probe-outlier-median-frac {float(probe_outlier_median_frac)}"
            f" --probe-outlier-asymmetry-frac {float(probe_outlier_asymmetry_frac)}"
            f" --probe-retest-bytes {int(probe_retest_bytes)}"
            f" --probe-retest-repeats {int(probe_retest_repeats)}"
            f" --probe-retest-max-edges {int(probe_retest_max_edges)}"
        )
    if capacity_snapshot:
        inner += f" --capacity-snapshot {shlex.quote(str(capacity_snapshot))}"
    if ping_matrix:
        inner += f" --ping-matrix {shlex.quote(str(ping_matrix))}"

    db_env = ""
    if probe_links:
        # The trainer container runs in host-network mode for WAN realism, while the controller's
        # postgres runs in a local docker bridge network (skyrocket-ctrl) without a published port.
        # When the trainer VM is colocated with the controller VM (recommended for this runner),
        # the host can still reach the postgres container via its bridge-network IP.
        #
        # Provide that IP to the probe helper via NEXTMINI_DB_HOST so --probe-links can connect.
        db_env = (
            "-e NEXTMINI_DB_HOST=$(docker inspect -f "
            "'{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "
            "skyrocket-postgres) "
            "-e NEXTMINI_DB_PORT=5432 -e NEXTMINI_DB_USER=pgusr "
            "-e NEXTMINI_DB_PASSWORD=pgpwrd -e NEXTMINI_DB_NAME=nextmini "
        )

    worker_relays_env = "true" if allow_worker_relays else "false"
    cmd_lines = [
        f"docker run --name {shlex.quote(node.container_name())} --network host \\",
        f"  -v {_bash_dquote(f'{repo_expr}:/workspace')} \\",
        "  -w /workspace \\",
        "  -e RUST_LOG=${RUST_LOG:-info} \\",
        f"  -e MULTICAST_ALLOW_WORKER_RELAYS={worker_relays_env} \\",
    ]
    if force_rebuild:
        cmd_lines.append("  -e NEXTMINI_PY_FORCE_REBUILD=1 \\")
    if db_env:
        cmd_lines.append(f"  {db_env}\\")
    cmd_lines.extend(
        [
            "  nextmini_rl_python \\",
            f"  /bin/bash -lc {shlex.quote(inner)}",
        ]
    )
    cmd = "\n".join(cmd_lines)
    return ssh_run(node.ssh, cmd, batch=batch, capture=True)


def down_cluster(controller: Controller, nodes: list[Node], *, batch: bool) -> None:
    docker_rm(controller.ssh, "skyrocket-controller", batch=batch)
    docker_rm(controller.ssh, "skyrocket-postgres", batch=batch)
    for node in nodes:
        docker_rm(node.ssh, node.container_name(), batch=batch)


def main() -> int:
    parser = argparse.ArgumentParser(description="Multi-DC WAN multicast bench runner (SSH orchestrated).")
    sub = parser.add_subparsers(dest="cmd", required=True)

    def add_inventory_arg(p: argparse.ArgumentParser) -> None:
        p.add_argument("--inventory", type=pathlib.Path, required=True)
        p.add_argument("--batch-ssh", action="store_true", help="Require key-based auth (no interactive prompts).")

    p_run = sub.add_parser("run-bench", help="Sync, start controller, run WAN broadcast benchmark, collect results.")
    add_inventory_arg(p_run)
    p_run.add_argument("--bytes", type=int, required=True, help="Artifact size in bytes (sparse file).")
    p_run.add_argument("--rounds", type=int, default=20)
    p_run.add_argument("--timeout-ms", type=int, default=180_000)
    p_run.add_argument("--algorithm", default="cf_bottleneck")
    p_run.add_argument("--hop-limit", type=int, default=3)
    p_run.add_argument("--eta", type=float, default=0.1)
    p_run.add_argument("--num-paths", type=int, default=2)
    p_run.add_argument("--relay-scoring", default="coverage")
    p_run.add_argument("--max-relays", type=int, default=-1)
    p_run.add_argument(
        "--relay-selection",
        choices=("lp", "capacity", "random"),
        default="lp",
        help="Relay selection method when --max-relays is set (lp = LP-guided).",
    )
    p_run.add_argument(
        "--selection-seed",
        type=int,
        default=0,
        help="Seed used for random relay selection (when --relay-selection=random).",
    )
    p_run.add_argument(
        "--allow-worker-relays",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Allow destination workers to forward/relay (default: true).",
    )
    p_run.add_argument(
        "--probe-links",
        action="store_true",
        help="Insert DB probe flows and overwrite link capacities before planning.",
    )
    p_run.add_argument(
        "--capacity-snapshot",
        type=pathlib.Path,
        default=None,
        help="Local JSON capacity snapshot (list of {src,dst,capacity_mbps}) to use instead of probing.",
    )
    p_run.add_argument(
        "--save-capacity-snapshot",
        type=pathlib.Path,
        default=None,
        help="Write the capacity snapshot used by this run to this local path.",
    )
    p_run.add_argument("--probe-bytes", type=int, default=64 * 1024 * 1024)
    p_run.add_argument(
        "--probe-warmup-bytes",
        type=int,
        default=0,
        help="Optional warmup bytes per probe flow (per batch). If >0, run warmup probes first and exclude them from capacity estimation.",
    )
    p_run.add_argument("--probe-timeout-secs", type=float, default=60.0)
    p_run.add_argument(
        "--probe-batch-size",
        type=int,
        default=0,
        help="If >0, probe links in batches of this size (0 = all concurrent).",
    )
    p_run.add_argument(
        "--probe-retest-outliers",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="After --probe-links, re-probe suspicious outlier edges sequentially (default: true).",
    )
    p_run.add_argument(
        "--probe-outlier-mbps",
        type=float,
        default=10.0,
        help="Flag probed capacities <= this Mbps as outliers for re-probing.",
    )
    p_run.add_argument(
        "--probe-outlier-median-frac",
        type=float,
        default=0.1,
        help="Flag edges <= median(outgoing[src]) * frac as outliers for re-probing.",
    )
    p_run.add_argument(
        "--probe-outlier-asymmetry-frac",
        type=float,
        default=0.1,
        help="Flag edges <= reverse_capacity * frac as outliers for re-probing.",
    )
    p_run.add_argument(
        "--probe-retest-bytes",
        type=int,
        default=0,
        help="Bytes per re-probe flow (0 = reuse --probe-bytes).",
    )
    p_run.add_argument(
        "--probe-retest-repeats",
        type=int,
        default=3,
        help="Number of re-probe flows per outlier edge (averaged).",
    )
    p_run.add_argument(
        "--probe-retest-max-edges",
        type=int,
        default=8,
        help="Maximum number of outlier edges to re-probe (lowest capacities first).",
    )
    p_run.add_argument(
        "--collect-ping-matrix",
        action="store_true",
        help="Collect ICMP ping RTT/loss matrix between nodes and pass it to the trainer.",
    )
    p_run.add_argument("--ping-count", type=int, default=3)
    p_run.add_argument("--ping-timeout-secs", type=int, default=3)
    p_run.add_argument(
        "--no-sync",
        action="store_true",
        help="Skip syncing the repo to hosts (assumes remote_repo_dir is already up to date).",
    )
    p_run.add_argument(
        "--delete",
        action="store_true",
        help="When syncing, delete remote files not present locally.",
    )
    p_run.add_argument(
        "--no-cleanup",
        action="store_true",
        help="Keep containers running after the benchmark (useful for debugging).",
    )
    p_run.add_argument(
        "--force-rebuild",
        action="store_true",
        help="Force rebuild of nextmini_py on all hosts (sets NEXTMINI_PY_FORCE_REBUILD=1).",
    )

    p_down = sub.add_parser("down", help="Stop containers on all hosts.")
    add_inventory_arg(p_down)

    p_rl = sub.add_parser("run-rl", help="Sync, start controller, run a minimal RL step over WAN, collect logs.")
    add_inventory_arg(p_rl)
    p_rl.add_argument("--model-name", default="sshleifer/tiny-gpt2")
    p_rl.add_argument("--train-steps", type=int, default=1)
    p_rl.add_argument(
        "--batch-size",
        type=int,
        default=0,
        help="Prompts per step (0 = auto: 1 per worker).",
    )
    p_rl.add_argument("--grpo-group-size", type=int, default=1)
    p_rl.add_argument("--generation-len", type=int, default=16)
    p_rl.add_argument("--max-seq-len", type=int, default=256)
    p_rl.add_argument("--multicast-timeout-ms", type=int, default=600_000)
    p_rl.add_argument(
        "--use-sharded-weights",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Send weights via file-backed shards (default: true; avoids huge in-memory buffers).",
    )
    p_rl.add_argument("--shard-size", default="8GB")
    p_rl.add_argument("--multicast-tree-algo", default="cf_bottleneck_mwu")
    p_rl.add_argument("--hop-limit", type=int, default=3)
    p_rl.add_argument("--eta", type=float, default=0.1)
    p_rl.add_argument("--num-paths", type=int, default=2)
    p_rl.add_argument("--relay-scoring", default="coverage")
    p_rl.add_argument("--max-relays", type=int, default=-1)
    p_rl.add_argument(
        "--allow-worker-relays",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Allow destination workers to forward/relay (default: true).",
    )
    p_rl.add_argument(
        "--capacity-snapshot",
        type=pathlib.Path,
        default=None,
        help="Local JSON capacity snapshot (list of {src,dst,capacity_mbps}) to apply before planning.",
    )
    p_rl.add_argument("--torch-index-url", default="https://download.pytorch.org/whl/cpu")
    p_rl.add_argument(
        "--cpu-only",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Force CPU execution (default: true).",
    )
    p_rl.add_argument(
        "--no-sync",
        action="store_true",
        help="Skip syncing the repo to hosts (assumes remote_repo_dir is already up to date).",
    )
    p_rl.add_argument(
        "--delete",
        action="store_true",
        help="When syncing, delete remote files not present locally.",
    )
    p_rl.add_argument(
        "--no-cleanup",
        action="store_true",
        help="Keep containers running after the run (useful for debugging).",
    )
    p_rl.add_argument(
        "--force-rebuild",
        action="store_true",
        help="Force rebuild of nextmini_py on all hosts (sets NEXTMINI_PY_FORCE_REBUILD=1).",
    )

    args = parser.parse_args()

    controller, nodes, remote_repo_dir, inventory_delete = load_inventory(args.inventory)
    batch = bool(getattr(args, "batch_ssh", False))

    if args.cmd == "down":
        down_cluster(controller, nodes, batch=batch)
        return 0

    if args.cmd == "run-rl":
        generate_configs(controller, nodes)

        seen_targets: set[tuple[str, str, int, str | None]] = set()
        all_targets: list[SshTarget] = []
        for target in [controller.ssh] + [n.ssh for n in nodes]:
            key = (target.host, target.user, target.port, target.identity_file)
            if key in seen_targets:
                continue
            seen_targets.add(key)
            all_targets.append(target)

        try:
            if batch:
                with concurrent.futures.ThreadPoolExecutor(max_workers=min(32, len(all_targets))) as ex:
                    futs = {ex.submit(ssh_run, t, "true", batch=True): t for t in all_targets}
                    for fut in concurrent.futures.as_completed(futs):
                        target = futs[fut]
                        try:
                            fut.result()
                        except subprocess.CalledProcessError as exc:
                            raise SystemExit(
                                f"SSH preflight failed for {target.display()}.\n"
                                "If your key is passphrase-protected, run `ssh-add <key>` and retry.\n"
                                f"Output:\n{exc.stdout}"
                            ) from exc

            if not getattr(args, "no_sync", False):
                delete = bool(getattr(args, "delete", False) or inventory_delete)

                def sync_one(target: SshTarget) -> None:
                    ssh_run(
                        target,
                        f"mkdir -p {_bash_dquote(_expand_remote_home(remote_repo_dir))}",
                        batch=batch,
                    )
                    rsync_repo(target, remote_repo_dir=remote_repo_dir, batch=batch, delete=delete)

                with concurrent.futures.ThreadPoolExecutor(max_workers=min(8, len(all_targets))) as ex:
                    list(ex.map(sync_one, all_targets))

            with concurrent.futures.ThreadPoolExecutor(max_workers=min(8, len(nodes))) as ex:
                list(
                    ex.map(
                        lambda n: node_image_build(n, remote_repo_dir=remote_repo_dir, batch=batch),
                        nodes,
                    )
                )

            controller_up(controller, remote_repo_dir=remote_repo_dir, batch=batch)

            trainer = next(n for n in nodes if n.role == "trainer")
            workers = sorted((n for n in nodes if n.role == "worker"), key=lambda n: t.cast(int, n.rank))
            relays = [n for n in nodes if n.role == "relay"]

            if not workers:
                raise SystemExit("inventory must include at least one worker for run-rl")

            trainer_node_id = trainer.node_id
            worker_ids = [n.node_id for n in workers]

            capacity_snapshot_remote: str | None = None
            if getattr(args, "capacity_snapshot", None):
                snap_path = t.cast(pathlib.Path, getattr(args, "capacity_snapshot"))
                if not snap_path.is_file():
                    raise SystemExit(f"--capacity-snapshot not found: {snap_path}")

                # Copy snapshot onto the trainer host under the repo so the container can read it.
                repo_scp = _expand_remote_home_for_scp(_expand_remote_home(remote_repo_dir))
                capacity_snapshot_host = f"{repo_scp}/examples/rl/multidc/generated/capacity_snapshot.json"
                capacity_snapshot_remote = "examples/rl/multidc/generated/capacity_snapshot.json"
                ssh_run(
                    trainer.ssh,
                    f"mkdir -p {_bash_dquote(_expand_remote_home(remote_repo_dir))}/examples/rl/multidc/generated",
                    batch=batch,
                )
                scp = ["scp", "-P", str(trainer.ssh.port)]
                scp += ["-o", "StrictHostKeyChecking=accept-new"]
                if batch:
                    scp += ["-o", "BatchMode=yes"]
                if trainer.ssh.identity_file:
                    scp += ["-i", _expand_user(trainer.ssh.identity_file)]
                scp += [str(snap_path), f"{trainer.ssh.user}@{trainer.ssh.host}:{capacity_snapshot_host}"]
                _run(scp)

            batch_size = int(getattr(args, "batch_size", 0))
            if batch_size <= 0:
                batch_size = max(1, len(workers))

            controller_cfg = "examples/rl/multidc/generated/controller-config.toml"

            with concurrent.futures.ThreadPoolExecutor(max_workers=min(8, len(workers) + len(relays))) as ex:
                tasks: list[concurrent.futures.Future[None]] = []
                for node in relays:
                    tasks.append(
                        ex.submit(run_relay, node, remote_repo_dir=remote_repo_dir, batch=batch)
                    )
                for node in workers:
                    tasks.append(
                        ex.submit(
                            run_rl_worker,
                            node,
                            remote_repo_dir=remote_repo_dir,
                            trainer_node_id=trainer_node_id,
                            worker_node_ids=worker_ids,
                            controller_config=controller_cfg,
                            model_name=str(getattr(args, "model_name")),
                            train_steps=int(getattr(args, "train_steps")),
                            batch_size=batch_size,
                            grpo_group_size=int(getattr(args, "grpo_group_size")),
                            generation_len=int(getattr(args, "generation_len")),
                            max_seq_len=int(getattr(args, "max_seq_len")),
                            multicast_timeout_ms=int(getattr(args, "multicast_timeout_ms")),
                            use_sharded_weights=bool(getattr(args, "use_sharded_weights")),
                            shard_size=str(getattr(args, "shard_size")),
                            multicast_tree_algo=str(getattr(args, "multicast_tree_algo")),
                            hop_limit=int(getattr(args, "hop_limit")),
                            eta=float(getattr(args, "eta")),
                            num_paths=int(getattr(args, "num_paths")),
                            relay_scoring=str(getattr(args, "relay_scoring")),
                            max_relays=None if int(getattr(args, "max_relays")) < 0 else int(getattr(args, "max_relays")),
                            allow_worker_relays=bool(getattr(args, "allow_worker_relays")),
                            torch_index_url=str(getattr(args, "torch_index_url")),
                            cpu_only=bool(getattr(args, "cpu_only")),
                            force_rebuild=bool(getattr(args, "force_rebuild", False)),
                            batch=batch,
                        )
                    )
                for fut in concurrent.futures.as_completed(tasks):
                    fut.result()

            time.sleep(2)

            trainer_output = run_rl_trainer(
                trainer,
                remote_repo_dir=remote_repo_dir,
                trainer_node_id=trainer_node_id,
                worker_node_ids=worker_ids,
                controller_config=controller_cfg,
                capacity_snapshot=capacity_snapshot_remote,
                model_name=str(getattr(args, "model_name")),
                train_steps=int(getattr(args, "train_steps")),
                batch_size=batch_size,
                grpo_group_size=int(getattr(args, "grpo_group_size")),
                generation_len=int(getattr(args, "generation_len")),
                max_seq_len=int(getattr(args, "max_seq_len")),
                multicast_timeout_ms=int(getattr(args, "multicast_timeout_ms")),
                use_sharded_weights=bool(getattr(args, "use_sharded_weights")),
                shard_size=str(getattr(args, "shard_size")),
                multicast_tree_algo=str(getattr(args, "multicast_tree_algo")),
                hop_limit=int(getattr(args, "hop_limit")),
                eta=float(getattr(args, "eta")),
                num_paths=int(getattr(args, "num_paths")),
                relay_scoring=str(getattr(args, "relay_scoring")),
                max_relays=None if int(getattr(args, "max_relays")) < 0 else int(getattr(args, "max_relays")),
                allow_worker_relays=bool(getattr(args, "allow_worker_relays")),
                torch_index_url=str(getattr(args, "torch_index_url")),
                cpu_only=bool(getattr(args, "cpu_only")),
                force_rebuild=bool(getattr(args, "force_rebuild", False)),
                batch=batch,
            )
            print(trainer_output, end="")

            RESULTS_DIR.mkdir(parents=True, exist_ok=True)
            out_path = RESULTS_DIR / "rl_stdout.txt"
            out_path.write_text(trainer_output, encoding="utf-8")
            print(f"Wrote {out_path}")

            manifest = {
                "inventory_path": str(args.inventory),
                "timestamp_unix_s": time.time(),
                "model_name": str(getattr(args, "model_name")),
                "train_steps": int(getattr(args, "train_steps")),
                "batch_size": int(batch_size),
                "grpo_group_size": int(getattr(args, "grpo_group_size")),
                "generation_len": int(getattr(args, "generation_len")),
                "max_seq_len": int(getattr(args, "max_seq_len")),
                "multicast_timeout_ms": int(getattr(args, "multicast_timeout_ms")),
                "use_sharded_weights": bool(getattr(args, "use_sharded_weights")),
                "shard_size": str(getattr(args, "shard_size")),
                "multicast_tree_algo": str(getattr(args, "multicast_tree_algo")),
                "hop_limit": int(getattr(args, "hop_limit")),
                "eta": float(getattr(args, "eta")),
                "num_paths": int(getattr(args, "num_paths")),
                "relay_scoring": str(getattr(args, "relay_scoring")),
                "max_relays": None if int(getattr(args, "max_relays")) < 0 else int(getattr(args, "max_relays")),
                "allow_worker_relays": bool(getattr(args, "allow_worker_relays")),
                "capacity_snapshot": str(getattr(args, "capacity_snapshot", "") or ""),
                "torch_index_url": str(getattr(args, "torch_index_url")),
                "cpu_only": bool(getattr(args, "cpu_only")),
                "nodes": {
                    "trainer_node_id": trainer_node_id,
                    "worker_node_ids": worker_ids,
                    "relay_node_ids": [n.node_id for n in relays],
                },
            }
            (RESULTS_DIR / "rl_manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
            print(f"Wrote {RESULTS_DIR / 'rl_manifest.json'}")
            return 0
        finally:
            if not getattr(args, "no_cleanup", False):
                down_cluster(controller, nodes, batch=batch)

    assert args.cmd == "run-bench"

    if getattr(args, "probe_links", False) and getattr(args, "capacity_snapshot", None):
        raise SystemExit("--probe-links and --capacity-snapshot are mutually exclusive")

    generate_configs(controller, nodes)

    # Controller can be co-located with the trainer VM; avoid duplicate SSH work.
    seen_targets: set[tuple[str, str, int, str | None]] = set()
    all_targets: list[SshTarget] = []
    for target in [controller.ssh] + [n.ssh for n in nodes]:
        key = (target.host, target.user, target.port, target.identity_file)
        if key in seen_targets:
            continue
        seen_targets.add(key)
        all_targets.append(target)

    try:
        # Preflight connectivity (helps avoid parallel passphrase prompts).
        if batch:
            with concurrent.futures.ThreadPoolExecutor(max_workers=min(32, len(all_targets))) as ex:
                futs = {ex.submit(ssh_run, t, "true", batch=True): t for t in all_targets}
                for fut in concurrent.futures.as_completed(futs):
                    target = futs[fut]
                    try:
                        fut.result()
                    except subprocess.CalledProcessError as exc:
                        raise SystemExit(
                            f"SSH preflight failed for {target.display()}.\n"
                            "If your key is passphrase-protected, run `ssh-add <key>` and retry.\n"
                            f"Output:\n{exc.stdout}"
                        ) from exc

        if not getattr(args, "no_sync", False):
            delete = bool(getattr(args, "delete", False) or inventory_delete)

            def sync_one(target: SshTarget) -> None:
                ssh_run(
                    target,
                    f"mkdir -p {_bash_dquote(_expand_remote_home(remote_repo_dir))}",
                    batch=batch,
                )
                rsync_repo(target, remote_repo_dir=remote_repo_dir, batch=batch, delete=delete)

            with concurrent.futures.ThreadPoolExecutor(max_workers=min(8, len(all_targets))) as ex:
                list(ex.map(sync_one, all_targets))

        # Build node images in parallel first; controller build happens during controller_up.
        with concurrent.futures.ThreadPoolExecutor(max_workers=min(8, len(nodes))) as ex:
            list(
                ex.map(
                    lambda n: node_image_build(n, remote_repo_dir=remote_repo_dir, batch=batch),
                    nodes,
                )
            )

        controller_up(controller, remote_repo_dir=remote_repo_dir, batch=batch)

        trainer = next(n for n in nodes if n.role == "trainer")
        workers = sorted(
            (n for n in nodes if n.role == "worker"), key=lambda n: t.cast(int, n.rank)
        )
        relays = [n for n in nodes if n.role == "relay"]

        if bool(getattr(args, "probe_links", False)) and trainer.ssh.host != controller.ssh.host:
            raise SystemExit(
                "--probe-links currently requires the trainer to run on the same VM as the controller, "
                "since postgres is not published and is only reachable locally via its docker-network IP."
            )

        trainer_node_id = trainer.node_id
        worker_ids = [n.node_id for n in workers]

        # Start relays/workers (detached) in parallel.
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=min(8, len(workers) + len(relays))
        ) as ex:
            tasks: list[concurrent.futures.Future[None]] = []
            for node in relays:
                tasks.append(
                    ex.submit(run_relay, node, remote_repo_dir=remote_repo_dir, batch=batch)
                )
            for node in workers:
                tasks.append(
                    ex.submit(
                        run_worker,
                        node,
                        remote_repo_dir=remote_repo_dir,
                        trainer_node_id=trainer_node_id,
                        rounds=int(args.rounds),
                        timeout_ms=int(args.timeout_ms),
                        force_rebuild=bool(getattr(args, "force_rebuild", False)),
                        batch=batch,
                    )
                )
            for fut in concurrent.futures.as_completed(tasks):
                fut.result()

        # Give workers a moment to start and register receivers.
        time.sleep(2)

        ping_matrix_remote: str | None = None
        if getattr(args, "collect_ping_matrix", False):
            ping = collect_ping_matrix(
                nodes,
                count=int(getattr(args, "ping_count", 3)),
                timeout_secs=int(getattr(args, "ping_timeout_secs", 3)),
                batch=batch,
            )
            GENERATED_DIR.mkdir(parents=True, exist_ok=True)
            local_ping = GENERATED_DIR / "ping_matrix.json"
            local_ping.write_text(json.dumps(ping, indent=2), encoding="utf-8")

            # Copy ping matrix onto the trainer host under the repo so the container can read it.
            repo_scp = _expand_remote_home_for_scp(_expand_remote_home(remote_repo_dir))
            ping_host = f"{repo_scp}/examples/rl/multidc/generated/ping_matrix.json"
            ping_matrix_remote = "examples/rl/multidc/generated/ping_matrix.json"
            ssh_run(
                trainer.ssh,
                f"mkdir -p {_bash_dquote(_expand_remote_home(remote_repo_dir))}/examples/rl/multidc/generated",
                batch=batch,
            )
            scp_ping = ["scp", "-P", str(trainer.ssh.port)]
            scp_ping += ["-o", "StrictHostKeyChecking=accept-new"]
            if batch:
                scp_ping += ["-o", "BatchMode=yes"]
            if trainer.ssh.identity_file:
                scp_ping += ["-i", _expand_user(trainer.ssh.identity_file)]
            scp_ping += [str(local_ping), f"{trainer.ssh.user}@{trainer.ssh.host}:{ping_host}"]
            _run(scp_ping)

        capacity_snapshot_remote: str | None = None
        if getattr(args, "capacity_snapshot", None):
            snap_path = t.cast(pathlib.Path, getattr(args, "capacity_snapshot"))
            if not snap_path.is_file():
                raise SystemExit(f"--capacity-snapshot not found: {snap_path}")
            # Copy snapshot onto the trainer host under the repo so the container can read it.
            repo_scp = _expand_remote_home_for_scp(_expand_remote_home(remote_repo_dir))
            capacity_snapshot_host = f"{repo_scp}/examples/rl/multidc/generated/capacity_snapshot.json"
            capacity_snapshot_remote = "examples/rl/multidc/generated/capacity_snapshot.json"
            ssh_run(
                trainer.ssh,
                f"mkdir -p {_bash_dquote(_expand_remote_home(remote_repo_dir))}/examples/rl/multidc/generated",
                batch=batch,
            )
            scp = ["scp", "-P", str(trainer.ssh.port)]
            scp += ["-o", "StrictHostKeyChecking=accept-new"]
            if batch:
                scp += ["-o", "BatchMode=yes"]
            if trainer.ssh.identity_file:
                scp += ["-i", _expand_user(trainer.ssh.identity_file)]
            scp += [str(snap_path), f"{trainer.ssh.user}@{trainer.ssh.host}:{capacity_snapshot_host}"]
            _run(scp)

        trainer_output = run_trainer(
            trainer,
            remote_repo_dir=remote_repo_dir,
            worker_node_ids=worker_ids,
            rounds=int(args.rounds),
            timeout_ms=int(args.timeout_ms),
            bytes_to_generate=int(args.bytes),
            algorithm=str(args.algorithm),
            hop_limit=int(args.hop_limit),
            eta=float(args.eta),
            num_paths=int(args.num_paths),
            relay_scoring=str(args.relay_scoring),
            max_relays=None if int(args.max_relays) < 0 else int(args.max_relays),
            relay_selection=str(args.relay_selection),
            selection_seed=int(args.selection_seed),
            probe_links=bool(args.probe_links),
            probe_bytes=int(args.probe_bytes),
            probe_warmup_bytes=int(getattr(args, "probe_warmup_bytes", 0)),
            probe_batch_size=int(args.probe_batch_size),
            probe_timeout_secs=float(args.probe_timeout_secs),
            allow_worker_relays=bool(args.allow_worker_relays),
            ping_matrix=ping_matrix_remote,
            capacity_snapshot=capacity_snapshot_remote,
            probe_retest_outliers=bool(getattr(args, "probe_retest_outliers", True)),
            probe_outlier_mbps=float(getattr(args, "probe_outlier_mbps", 10.0)),
            probe_outlier_median_frac=float(getattr(args, "probe_outlier_median_frac", 0.1)),
            probe_outlier_asymmetry_frac=float(getattr(args, "probe_outlier_asymmetry_frac", 0.1)),
            probe_retest_bytes=int(getattr(args, "probe_retest_bytes", 0))
            if int(getattr(args, "probe_retest_bytes", 0)) > 0
            else int(args.probe_bytes),
            probe_retest_repeats=int(getattr(args, "probe_retest_repeats", 3)),
            probe_retest_max_edges=int(getattr(args, "probe_retest_max_edges", 8)),
            force_rebuild=bool(getattr(args, "force_rebuild", False)),
            batch=batch,
        )
        print(trainer_output, end="")

        RESULTS_DIR.mkdir(parents=True, exist_ok=True)
        local_out = RESULTS_DIR / "results.json"
        local_meta = RESULTS_DIR / "results.meta.json"

        # Fetch results.json from trainer host.
        scp = ["scp", "-P", str(trainer.ssh.port)]
        scp += ["-o", "StrictHostKeyChecking=accept-new"]
        if batch:
            scp += ["-o", "BatchMode=yes"]
        if trainer.ssh.identity_file:
            scp += ["-i", _expand_user(trainer.ssh.identity_file)]
        remote_path = f"{trainer.ssh.user}@{trainer.ssh.host}:{remote_repo_dir}/examples/rl/multidc/results/results.json"
        scp += [remote_path, str(local_out)]
        _run(scp)

        print(f"Wrote {local_out}")

        # Fetch results.meta.json (planner + probe snapshot).
        scp_meta = ["scp", "-P", str(trainer.ssh.port)]
        scp_meta += ["-o", "StrictHostKeyChecking=accept-new"]
        if batch:
            scp_meta += ["-o", "BatchMode=yes"]
        if trainer.ssh.identity_file:
            scp_meta += ["-i", _expand_user(trainer.ssh.identity_file)]
        remote_meta = f"{trainer.ssh.user}@{trainer.ssh.host}:{remote_repo_dir}/examples/rl/multidc/results/results.meta.json"
        scp_meta += [remote_meta, str(local_meta)]
        _run(scp_meta)

        print(f"Wrote {local_meta}")

        save_snapshot = getattr(args, "save_capacity_snapshot", None)
        if save_snapshot:
            meta = json.loads(local_meta.read_text(encoding="utf-8"))
            snapshot = meta.get("capacity_snapshot")
            if snapshot is None:
                raise SystemExit("results.meta.json missing capacity_snapshot")
            out_path = t.cast(pathlib.Path, save_snapshot)
            out_path.parent.mkdir(parents=True, exist_ok=True)
            out_path.write_text(json.dumps(snapshot, indent=2), encoding="utf-8")
            print(f"Wrote {out_path}")

        manifest = {
            "inventory_path": str(args.inventory),
            "timestamp_unix_s": time.time(),
            "bytes": int(args.bytes),
            "rounds": int(args.rounds),
            "timeout_ms": int(args.timeout_ms),
            "algorithm": str(args.algorithm),
            "hop_limit": int(args.hop_limit),
            "eta": float(args.eta),
            "num_paths": int(args.num_paths),
            "relay_scoring": str(args.relay_scoring),
            "max_relays": None if int(args.max_relays) < 0 else int(args.max_relays),
            "relay_selection": str(args.relay_selection),
            "selection_seed": int(args.selection_seed),
            "allow_worker_relays": bool(args.allow_worker_relays),
            "probe_links": bool(args.probe_links),
            "probe_bytes": int(args.probe_bytes),
            "probe_warmup_bytes": int(getattr(args, "probe_warmup_bytes", 0)),
            "probe_batch_size": int(args.probe_batch_size),
            "probe_timeout_secs": float(args.probe_timeout_secs),
            "probe_retest_outliers": bool(getattr(args, "probe_retest_outliers", True)),
            "probe_outlier_mbps": float(getattr(args, "probe_outlier_mbps", 10.0)),
            "probe_outlier_median_frac": float(getattr(args, "probe_outlier_median_frac", 0.1)),
            "probe_outlier_asymmetry_frac": float(getattr(args, "probe_outlier_asymmetry_frac", 0.1)),
            "probe_retest_bytes": int(getattr(args, "probe_retest_bytes", 0)),
            "probe_retest_repeats": int(getattr(args, "probe_retest_repeats", 3)),
            "probe_retest_max_edges": int(getattr(args, "probe_retest_max_edges", 8)),
            "capacity_snapshot": str(getattr(args, "capacity_snapshot", "") or ""),
            "save_capacity_snapshot": str(getattr(args, "save_capacity_snapshot", "") or ""),
            "collect_ping_matrix": bool(getattr(args, "collect_ping_matrix", False)),
            "ping_count": int(getattr(args, "ping_count", 3)),
            "ping_timeout_secs": int(getattr(args, "ping_timeout_secs", 3)),
            "nodes": {
                "trainer_node_id": trainer_node_id,
                "worker_node_ids": worker_ids,
                "relay_node_ids": [n.node_id for n in relays],
            },
        }
        (RESULTS_DIR / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
        print(f"Wrote {RESULTS_DIR / 'manifest.json'}")
        return 0
    finally:
        if not getattr(args, "no_cleanup", False):
            down_cluster(controller, nodes, batch=batch)


if __name__ == "__main__":
    raise SystemExit(main())
