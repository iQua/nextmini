#!/usr/bin/env python3

from __future__ import annotations

import argparse
import concurrent.futures
import dataclasses
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
    args = _ssh_base_args(target, batch=batch) + ["bash", "-lc", wrapped]
    res = _run(args, capture=capture)
    return res.stdout or ""


def rsync_repo(target: SshTarget, *, remote_repo_dir: str, batch: bool, delete: bool) -> None:
    ssh_cmd = _ssh_base_args(target, batch=batch)
    # rsync wants the remote shell as a string.
    ssh_shell = " ".join(shlex.quote(part) for part in ssh_cmd)
    excludes = [
        ".git/",
        "target/",
        "**/target/",
        "**/__pycache__/",
        "**/.venv/",
        "**/.mypy_cache/",
        "**/.pytest_cache/",
        "nextmini-data-plane-wrapper.log",
    ]
    cmd = [
        "rsync",
        "-az",
        "--progress",
    ]
    if delete:
        cmd.append("--delete")
    for item in excludes:
        cmd += ["--exclude", item]
    cmd += ["-e", ssh_shell, str(REPO_ROOT) + "/", f"{target.user}@{target.host}:{remote_repo_dir}/"]
    _run(cmd)


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
        f"cd {repo_q} && docker build -t nextmini_controller -f controller/Dockerfile . >/dev/null",
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
        f"cd {repo_q} && docker build -t nextmini_rl_python -f examples/rl/Dockerfile . >/dev/null",
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


def run_worker(node: Node, *, remote_repo_dir: str, trainer_node_id: int, rounds: int, timeout_ms: int, batch: bool) -> None:
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
    max_relays: int,
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
        f"--num-paths {num_paths} --relay-scoring {relay_scoring} --max-relays {max_relays}"
    )

    cmd = textwrap.dedent(
        f"""
        docker run --name {shlex.quote(node.container_name())} --network host \\
          -v {_bash_dquote(f"{repo_expr}:/workspace")} \\
          -w /workspace \\
          -e RUST_LOG=${{RUST_LOG:-info}} \\
          nextmini_rl_python \\
          /bin/bash -lc {shlex.quote(inner)}
        """
    ).strip()
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

    p_down = sub.add_parser("down", help="Stop containers on all hosts.")
    add_inventory_arg(p_down)

    args = parser.parse_args()

    controller, nodes, remote_repo_dir, inventory_delete = load_inventory(args.inventory)
    batch = bool(getattr(args, "batch_ssh", False))

    if args.cmd == "down":
        down_cluster(controller, nodes, batch=batch)
        return 0

    assert args.cmd == "run-bench"

    generate_configs(controller, nodes)

    all_targets: list[SshTarget] = [controller.ssh] + [n.ssh for n in nodes]

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
                        batch=batch,
                    )
                )
            for fut in concurrent.futures.as_completed(tasks):
                fut.result()

        # Give workers a moment to start and register receivers.
        time.sleep(2)

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
            max_relays=int(args.max_relays),
            batch=batch,
        )
        print(trainer_output, end="")

        RESULTS_DIR.mkdir(parents=True, exist_ok=True)
        local_out = RESULTS_DIR / "results.json"

        # Fetch results.json from trainer host.
        scp = ["scp", "-P", str(trainer.ssh.port)]
        if trainer.ssh.identity_file:
            scp += ["-i", _expand_user(trainer.ssh.identity_file)]
        remote_path = f"{trainer.ssh.user}@{trainer.ssh.host}:{remote_repo_dir}/examples/rl/multidc/results/results.json"
        scp += [remote_path, str(local_out)]
        _run(scp)

        print(f"Wrote {local_out}")
        return 0
    finally:
        if not getattr(args, "no_cleanup", False):
            down_cluster(controller, nodes, batch=batch)


if __name__ == "__main__":
    raise SystemExit(main())
