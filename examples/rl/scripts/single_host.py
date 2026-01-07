#!/usr/bin/env python3

from __future__ import annotations

import argparse
import os
import pathlib
import shlex
import subprocess
import sys
import textwrap

REPO_ROOT = pathlib.Path(__file__).resolve().parents[3]
BASE_DIR = REPO_ROOT / "examples" / "rl" / "single_host"
GENERATED_DIR = BASE_DIR / "generated"
ARTIFACTS_DIR = BASE_DIR / "artifacts"
RESULTS_DIR = BASE_DIR / "results"
COMPOSE_PATH = BASE_DIR / "docker-compose.generated.yml"


def _run(cmd: list[str]) -> None:
    subprocess.run(cmd, check=True)


def _as_bool(value: str) -> bool:
    return value.strip().lower() in ("1", "true", "yes", "y", "on")


def _comma_ids(ids: list[int]) -> str:
    return ",".join(str(v) for v in ids)


def write_controller_config(*, n_nodes: int) -> pathlib.Path:
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
    path = GENERATED_DIR / "controller-config.toml"
    path.write_text(cfg, encoding="utf-8")
    return path


def write_node_config(*, node_id: int) -> pathlib.Path:
    GENERATED_DIR.mkdir(parents=True, exist_ok=True)
    cfg = textwrap.dedent(
        f"""\
        controller_addr = "ws://controller:3000"
        private_network_interface = "eth0"
        private_network_name = "rl_training"
        node_id = {node_id}
        num_tun_queues = 1
        num_packet_processors = 4
        channel_capacity = 3000
        queue_capacity = 4000
        feature = "concurrent"
        channel_backpressure = true
        """
    )
    path = GENERATED_DIR / f"node-{node_id}.toml"
    path.write_text(cfg, encoding="utf-8")
    return path


def _gpu_block() -> str:
    return textwrap.indent(
        textwrap.dedent(
            """\
            gpus: all
            deploy:
              resources:
                reservations:
                  devices:
                    - driver: nvidia
                      count: all
                      capabilities: [gpu]
            """
        ),
        "  ",
    )


def _service_block(block: str) -> str:
    return textwrap.indent(textwrap.dedent(block).lstrip("\n"), "  ")


def generate_compose(
    *,
    n_workers: int,
    n_relays: int,
    mode: str,
    gpu: bool,
    trainer_gpu: str | None,
    worker_gpus: list[str] | None,
    rounds: int,
    timeout_ms: int,
    artifact_bytes: int,
    algorithm: str,
    hop_limit: int,
    eta: float,
    num_paths: int,
    relay_scoring: str,
    max_relays: int | None,
    probe_links: bool,
    probe_bytes: int,
    probe_timeout_secs: float,
) -> None:
    if n_workers <= 0:
        raise SystemExit("--workers must be >= 1")
    if n_relays < 0:
        raise SystemExit("--relays must be >= 0")

    trainer_id = 1
    worker_ids = list(range(2, 2 + n_workers))
    relay_ids = list(range(2 + n_workers, 2 + n_workers + n_relays))
    n_nodes = 1 + n_workers + n_relays
    if 10 + n_nodes >= 255:
        raise SystemExit(f"too many nodes ({n_nodes}) for the fixed /24 compose subnet")

    controller_cfg_rel = "examples/rl/single_host/generated/controller-config.toml"
    controller_cfg_path = write_controller_config(n_nodes=n_nodes)

    for node_id in [trainer_id] + worker_ids + relay_ids:
        write_node_config(node_id=node_id)

    ARTIFACTS_DIR.mkdir(parents=True, exist_ok=True)
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    out_json_rel = "examples/rl/single_host/results/results.json"
    artifact_rel = "examples/rl/single_host/artifacts/broadcast.bin"

    # GPU mapping.
    if gpu:
        if trainer_gpu is None:
            trainer_gpu = "0"
        if worker_gpus is None:
            worker_gpus = [str(i) for i in range(1, 1 + n_workers)]
        if len(worker_gpus) != n_workers:
            raise SystemExit(f"--worker-gpus must have {n_workers} entries, got {len(worker_gpus)}")
    else:
        trainer_gpu = None
        worker_gpus = None

    worker_ids_csv = _comma_ids(worker_ids)

    services: list[str] = []

    services.append(
        _service_block(
            f"""\
            postgres:
              image: postgres:16-alpine
              container_name: skyrocket-sh-postgres
              hostname: postgres
              restart: unless-stopped
              environment:
                POSTGRES_USER: pgusr
                POSTGRES_PASSWORD: pgpwrd
                POSTGRES_DB: nextmini
              ports:
                - "5432:5432"
              healthcheck:
                test: ["CMD", "pg_isready", "-U", "pgusr", "-d", "nextmini"]
                interval: 5s
                timeout: 5s
                retries: 30
                start_period: 10s
              volumes:
                - {REPO_ROOT / 'controller' / 'init.sql'}:/docker-entrypoint-initdb.d/init.sql:ro
              networks:
                sh_net:
                  ipv4_address: 172.31.10.2
            """
        )
    )

    services.append(
        _service_block(
            f"""\
            controller:
              build:
                context: {REPO_ROOT}
                dockerfile: controller/Dockerfile
              image: nextmini_controller
              container_name: skyrocket-sh-controller
              hostname: controller
              depends_on:
                postgres:
                  condition: service_healthy
              environment:
                RUST_LOG: {shlex.quote(os.environ.get("RUST_LOG", "info"))}
              volumes:
                - {REPO_ROOT}:/workspace:cached
                - {controller_cfg_path}:/var/nextmini/config.toml:ro
              working_dir: /var/nextmini
              command: /bin/bash -c "sleep 5 && /var/nextmini/controller"
              ports:
                - "3000:3000"
              networks:
                sh_net:
                  ipv4_address: 172.31.10.3
            """
        )
    )

    def add_node_service(*, name: str, node_id: int, role: str, cmd: str, gpu_id: str | None, extra_env: dict[str, str] | None = None) -> None:
        env: dict[str, str] = {
            "RUST_LOG": os.environ.get("RUST_LOG", "info"),
            "SKIP_BUILD": os.environ.get("SKIP_BUILD", "0"),
        }
        if extra_env:
            env.update(extra_env)
        if gpu_id is not None:
            env["CUDA_VISIBLE_DEVICES"] = gpu_id
        body = textwrap.dedent(
            f"""\
            {name}:
              build:
                context: {REPO_ROOT}
                dockerfile: examples/rl/Dockerfile
              image: nextmini_rl_python
              container_name: skyrocket-sh-{name}
              hostname: {name}
            """
        )
        if gpu and role in ("trainer", "worker"):
            body += _gpu_block()
        body += textwrap.indent(
            textwrap.dedent(
                f"""\
                depends_on:
                  controller:
                    condition: service_started
                volumes:
                  - {REPO_ROOT}:/workspace:cached
                working_dir: /workspace
                """
            ),
            "  ",
        )
        if env:
            body += "  environment:\n"
            for key, value in env.items():
                body += f"    {key}: {shlex.quote(value)}\n"
        body += textwrap.indent(
            textwrap.dedent(
                f"""\
                command:
                  - bash
                  - -lc
                  - >
                    {cmd}
                networks:
                  sh_net:
                    ipv4_address: 172.31.10.{10 + node_id}
                """
            ),
            "  ",
        )
        services.append(_service_block(body))

    if mode == "broadcast":
        trainer_cmd = (
            f"examples/rl/scripts/run_broadcast_bench.sh trainer examples/rl/single_host/generated/node-1.toml "
            f"--controller-config {controller_cfg_rel} --rounds {rounds} --timeout-ms {timeout_ms} "
            f"--worker-node-ids {worker_ids_csv} --file {artifact_rel} --generate-bytes {artifact_bytes} "
            f"--output-json {out_json_rel} --algorithm {algorithm} --hop-limit {hop_limit} --eta {eta} "
            f"--num-paths {num_paths} --relay-scoring {relay_scoring}"
        )
        if max_relays is not None:
            trainer_cmd += f" --max-relays {max_relays}"
        if probe_links:
            trainer_cmd += f" --probe-links --probe-bytes {probe_bytes} --probe-timeout-secs {probe_timeout_secs}"
    else:
        trainer_cmd = "examples/rl/scripts/run_rl_node.sh trainer examples/rl/single_host/generated/node-1.toml"

    add_node_service(
        name="trainer",
        node_id=trainer_id,
        role="trainer",
        cmd=trainer_cmd,
        gpu_id=trainer_gpu,
        extra_env={
            "WORKER_NODE_IDS": worker_ids_csv,
            "TRAINER_NODE_ID": str(trainer_id),
            "CONTROLLER_CONFIG": controller_cfg_rel,
            "MULTICAST_TREE_ALGO": algorithm,
            "MULTICAST_HOP_LIMIT": str(hop_limit),
            "MULTICAST_ETA": str(eta),
            "MULTICAST_NUM_PATHS": str(num_paths),
            "MULTICAST_RELAY_SCORING": relay_scoring,
            **(
                {"MULTICAST_MAX_RELAYS": str(max_relays)}
                if max_relays is not None
                else {}
            ),
            "MULTICAST_PROBE_LINKS": "true" if probe_links else "false",
            "MULTICAST_PROBE_BYTES": str(probe_bytes),
            "MULTICAST_PROBE_TIMEOUT_SECS": str(probe_timeout_secs),
        }
        if mode == "rl"
        else {"WORKER_NODE_IDS": worker_ids_csv, "TRAINER_NODE_ID": str(trainer_id)},
    )

    for rank, worker_id in enumerate(worker_ids):
        if mode == "broadcast":
            cmd = (
                f"examples/rl/scripts/run_broadcast_bench.sh worker examples/rl/single_host/generated/node-{worker_id}.toml "
                f"--rounds {rounds} --timeout-ms {timeout_ms} --trainer-node-id {trainer_id} --rank {rank} "
                f"--sink-dir examples/rl/single_host/artifacts/received/node-{worker_id}"
            )
        else:
            cmd = (
                f"examples/rl/scripts/run_rl_node.sh worker examples/rl/single_host/generated/node-{worker_id}.toml "
                f"--rank {rank} --gpu 0"
            )
        add_node_service(
            name=f"worker-{rank}",
            node_id=worker_id,
            role="worker",
            cmd=cmd,
            gpu_id=worker_gpus[rank] if worker_gpus else None,
            extra_env={"WORKER_NODE_IDS": worker_ids_csv, "TRAINER_NODE_ID": str(trainer_id)} if mode == "rl" else None,
        )

    for relay_idx, relay_id in enumerate(relay_ids):
        cmd = f"examples/rl/scripts/run_relay_node.sh examples/rl/single_host/generated/node-{relay_id}.toml"
        add_node_service(
            name=f"relay-{relay_idx}",
            node_id=relay_id,
            role="relay",
            cmd=cmd,
            gpu_id=None,
        )

    networks = textwrap.dedent(
        """\
        networks:
          sh_net:
            driver: bridge
            ipam:
              config:
                - subnet: 172.31.10.0/24
        """
    )

    COMPOSE_PATH.parent.mkdir(parents=True, exist_ok=True)
    COMPOSE_PATH.write_text("services:\n" + "".join(services) + networks, encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description="Single-host Skyrocket runner (docker compose).")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_run = sub.add_parser("run", help="Generate configs + compose and run it.")
    p_run.add_argument("--mode", choices=("broadcast", "rl"), default="broadcast")
    p_run.add_argument("--workers", type=int, default=2)
    p_run.add_argument("--relays", type=int, default=0)
    p_run.add_argument("--gpu", action=argparse.BooleanOptionalAction, default=True)
    p_run.add_argument("--trainer-gpu", default=os.environ.get("TRAINER_GPU", "0"))
    p_run.add_argument("--worker-gpus", default=os.environ.get("WORKER_GPUS", ""))
    p_run.add_argument("--detach", action="store_true")
    p_run.add_argument("--cleanup", action=argparse.BooleanOptionalAction, default=True)

    # broadcast mode knobs
    p_run.add_argument("--bytes", type=int, default=0, help="Artifact size in bytes (0 = skip generation).")
    p_run.add_argument("--rounds", type=int, default=20)
    p_run.add_argument("--timeout-ms", type=int, default=180_000)
    p_run.add_argument("--algorithm", default="cf_bottleneck")
    p_run.add_argument("--hop-limit", type=int, default=3)
    p_run.add_argument("--eta", type=float, default=0.1)
    p_run.add_argument("--num-paths", type=int, default=2)
    p_run.add_argument("--relay-scoring", default="coverage")
    p_run.add_argument("--max-relays", type=int, default=-1)
    p_run.add_argument("--probe-links", action=argparse.BooleanOptionalAction, default=True)
    p_run.add_argument("--probe-bytes", type=int, default=64 * 1024 * 1024)
    p_run.add_argument("--probe-timeout-secs", type=float, default=60.0)

    sub.add_parser("down", help="Stop single-host compose services.")

    args = parser.parse_args()

    if args.cmd == "down":
        if COMPOSE_PATH.exists():
            _run(["docker", "compose", "-f", str(COMPOSE_PATH), "down", "-v"])
        return 0

    assert args.cmd == "run"

    worker_gpus: list[str] | None = None
    worker_gpus_spec = str(args.worker_gpus or "").strip()
    if worker_gpus_spec:
        worker_gpus = [part.strip() for part in worker_gpus_spec.split(",") if part.strip()]

    generate_compose(
        n_workers=args.workers,
        n_relays=args.relays,
        mode=args.mode,
        gpu=bool(args.gpu),
        trainer_gpu=str(args.trainer_gpu) if args.gpu else None,
        worker_gpus=worker_gpus,
        rounds=int(args.rounds),
        timeout_ms=int(args.timeout_ms),
        artifact_bytes=int(args.bytes or 0),
        algorithm=str(args.algorithm),
        hop_limit=int(args.hop_limit),
        eta=float(args.eta),
        num_paths=int(args.num_paths),
        relay_scoring=str(args.relay_scoring),
        max_relays=None if int(args.max_relays) < 0 else int(args.max_relays),
        probe_links=bool(args.probe_links),
        probe_bytes=int(args.probe_bytes),
        probe_timeout_secs=float(args.probe_timeout_secs),
    )

    up_cmd = ["docker", "compose", "-f", str(COMPOSE_PATH), "up", "--build"]
    if args.detach:
        up_cmd.append("-d")
        _run(up_cmd)
        return 0

    if args.mode == "broadcast":
        up_cmd += ["--abort-on-container-exit", "--exit-code-from", "trainer"]
    elif args.mode == "rl":
        up_cmd += ["--abort-on-container-exit", "--exit-code-from", "trainer"]
    try:
        _run(up_cmd)
    finally:
        if args.cleanup and not args.detach:
            try:
                _run(["docker", "compose", "-f", str(COMPOSE_PATH), "down", "-v"])
            except subprocess.CalledProcessError as exc:
                print(f"warning: docker compose down failed: {exc}", file=sys.stderr)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
