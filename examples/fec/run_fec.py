#!/usr/bin/env python3
from __future__ import annotations

import argparse
import dataclasses
import hashlib
import json
import os
import shlex
import shutil
import subprocess
import sys
import textwrap
import time
from pathlib import Path

try:
    import tomllib
except ImportError:  # pragma: no cover
    import tomli as tomllib  # type: ignore[no-redef]


REPO_ROOT = Path(__file__).resolve().parents[2]
GENERATED_ROOT = REPO_ROOT / "examples" / "fec" / "generated"
LAST_IMAGE_TAG_PATH = REPO_ROOT / "examples" / "fec" / ".last-image-tag"
REMOTE_STATE_FILE = "remote-state.json"

DEFAULT_INTERFACE = "ens3"
DEFAULT_NETWORK_NAME = "fec_wan"
LOSSLESS_PACKET_OVERHEAD = 20 + 20 + 16
MAX_LOSSLESS_BLOCK_SIZE = 65_535 - LOSSLESS_PACKET_OVERHEAD
DEFAULT_BLOCK_SIZE = 8_192
DEFAULT_SYMBOLS_PER_BLOCK = 1
DEFAULT_GROUP_TIMEOUT = 120
DEFAULT_RECEIVE_TIMEOUT_MS = 300_000
DEFAULT_RUN_TIMEOUT = 900
DEFAULT_REMOTE_RUN_ROOT = "~/fec-runs"
DEFAULT_BOSTON_NETWORK = "fec-control"
RELAYS_PER_TREE = 2

LOCAL_REGISTRY = "127.0.0.1:5000"
PUBLIC_REGISTRY = "boston.csl.toronto.edu:5000"

GROUP_INFO_FILE = "group-info.json"
METADATA_FILE = "tensor-metadata.json"
READY_FILE_TEMPLATE = "receiver-ready-{}.json"

REMOTE_HOME_CACHE: dict[tuple[str, str, int], str] = {}


@dataclasses.dataclass(frozen=True)
class SshTarget:
    host: str
    user: str
    identity_file: str | None
    port: int = 22


@dataclasses.dataclass(frozen=True)
class Controller:
    ssh: SshTarget
    no_sudo: bool


@dataclasses.dataclass(frozen=True)
class Node:
    role: str
    node_id: int
    ssh: SshTarget
    rank: int | None = None
    public_network_interface: str = DEFAULT_INTERFACE
    private_network_interface: str = DEFAULT_INTERFACE


@dataclasses.dataclass(frozen=True)
class Inventory:
    controller: Controller
    remote_repo_dir: str
    trainer: Node
    workers: list[Node]
    relays: list[Node]
    nodes: list[Node]


@dataclasses.dataclass(frozen=True)
class RunPlan:
    mode: str
    source_node_id: int
    receiver_ids: list[int]
    relay_ids: list[int]
    tree_ids: list[int]
    trees: list[tuple[int, list[tuple[int, int]]]]
    topology_edges: list[tuple[int, int]]
    group_label: str


def parse_toml_text(text: str) -> dict:
    return tomllib.loads(text)


def _expand_user(path: str | None) -> str | None:
    return os.path.expanduser(path) if path else None


def parse_size(value: str) -> int:
    raw = value.strip().lower().replace("_", "")
    suffixes = {
        "b": 1,
        "kb": 1000,
        "mb": 1000**2,
        "gb": 1000**3,
        "kib": 1024,
        "mib": 1024**2,
        "gib": 1024**3,
    }
    for suffix, multiplier in sorted(suffixes.items(), key=lambda item: -len(item[0])):
        if raw.endswith(suffix):
            number = raw[: -len(suffix)].strip()
            if not number:
                raise SystemExit(f"Invalid size {value!r}.")
            return int(float(number) * multiplier)
    return int(raw)


def validate_block_size(
    block_size: int,
    *,
    fec_enabled: bool = False,
    symbols_per_block: int = 1,
) -> int:
    if block_size <= 0:
        raise ValueError("block_size must be positive.")
    if fec_enabled:
        if symbols_per_block <= 0:
            raise ValueError("symbols_per_block must be positive for fec mode.")
        symbol_size = (block_size + symbols_per_block - 1) // symbols_per_block
        if symbol_size > MAX_LOSSLESS_BLOCK_SIZE:
            raise ValueError(
                "block_size "
                f"{block_size} with symbols_per_block={symbols_per_block} yields "
                f"symbol_size={symbol_size}, which exceeds the maximum lossless payload "
                f"{MAX_LOSSLESS_BLOCK_SIZE}."
            )
        return block_size
    if block_size > MAX_LOSSLESS_BLOCK_SIZE:
        raise ValueError(
            "block_size "
            f"{block_size} exceeds the maximum lossless payload {MAX_LOSSLESS_BLOCK_SIZE}; "
            "larger blocks overflow the framed IPv4/TCP packet envelope."
        )
    return block_size


def parse_int_list(value: str | None) -> list[int]:
    if not value:
        return []
    return [int(part.strip()) for part in value.split(",") if part.strip()]


def _load_toml(path: Path) -> dict:
    with path.open("rb") as fh:
        return tomllib.load(fh)


def load_inventory(path: Path) -> Inventory:
    raw = _load_toml(path)
    controller_raw = raw.get("controller") or {}
    paths_raw = raw.get("paths") or {}
    nodes_raw = raw.get("nodes") or []

    controller = Controller(
        ssh=SshTarget(
            host=str(controller_raw["host"]),
            user=str(controller_raw["user"]),
            identity_file=_expand_user(controller_raw.get("identity_file")),
            port=int(controller_raw.get("port", 22)),
        ),
        no_sudo=bool(controller_raw.get("no_sudo", False)),
    )

    nodes: list[Node] = []
    trainer: Node | None = None
    workers: list[Node] = []
    relays: list[Node] = []
    seen_ids: set[int] = set()

    for item in nodes_raw:
        role = str(item["role"])
        if role not in {"trainer", "worker", "relay"}:
            raise SystemExit(f"Unsupported node role {role!r}.")
        node_id = int(item["node_id"])
        if node_id in seen_ids:
            raise SystemExit(f"Duplicate node_id={node_id} in inventory.")
        seen_ids.add(node_id)

        rank = item.get("rank")
        if role == "worker" and rank is None:
            raise SystemExit(f"Worker node {node_id} is missing rank.")
        if role != "worker" and rank is not None:
            raise SystemExit(f"Only worker nodes may declare rank (node {node_id}).")

        node = Node(
            role=role,
            node_id=node_id,
            ssh=SshTarget(
                host=str(item["host"]),
                user=str(item["user"]),
                identity_file=_expand_user(item.get("identity_file")),
                port=int(item.get("port", 22)),
            ),
            rank=int(rank) if rank is not None else None,
            public_network_interface=str(
                item.get("public_network_interface", item.get("network_interface", DEFAULT_INTERFACE))
            ),
            private_network_interface=str(
                item.get("private_network_interface", item.get("network_interface", DEFAULT_INTERFACE))
            ),
        )
        nodes.append(node)
        if role == "trainer":
            if trainer is not None:
                raise SystemExit("Inventory must contain exactly one trainer.")
            trainer = node
        elif role == "worker":
            workers.append(node)
        else:
            relays.append(node)

    if trainer is None:
        raise SystemExit("Inventory must contain a trainer node.")
    if not workers:
        raise SystemExit("Inventory must contain at least one worker node.")

    worker_ranks = [node.rank for node in workers]
    expected_ranks = list(range(len(workers)))
    if sorted(worker_ranks) != expected_ranks:
        raise SystemExit(
            f"Worker ranks must be contiguous {expected_ranks}, got {sorted(worker_ranks)}."
        )

    workers.sort(key=lambda node: node.rank if node.rank is not None else -1)
    relays.sort(key=lambda node: node.node_id)
    nodes.sort(key=lambda node: node.node_id)

    return Inventory(
        controller=controller,
        remote_repo_dir=str(paths_raw.get("remote_repo_dir", "~/skyrocket/nextmini")),
        trainer=trainer,
        workers=workers,
        relays=relays,
        nodes=nodes,
    )


def compute_relay_trees(
    *,
    source_node_id: int,
    receiver_ids: list[int],
    relay_ids: list[int],
    tree_ids: list[int],
) -> list[tuple[int, list[tuple[int, int]]]]:
    if not receiver_ids:
        raise SystemExit("At least one receiver is required.")
    if not tree_ids:
        raise SystemExit("At least one tree ID is required.")
    required_relays = len(tree_ids) * RELAYS_PER_TREE
    if len(relay_ids) < required_relays:
        raise SystemExit(
            f"Need at least {required_relays} relays for tree_ids={tree_ids} "
            f"with {RELAYS_PER_TREE} relays per tree, got {relay_ids}."
        )

    trees: list[tuple[int, list[tuple[int, int]]]] = []
    for tree_index, tree_id in enumerate(tree_ids):
        offset = tree_index * RELAYS_PER_TREE
        tree_relays = relay_ids[offset : offset + RELAYS_PER_TREE]
        edges = [(source_node_id, tree_relays[0])]
        edges.extend((src, dst) for src, dst in zip(tree_relays, tree_relays[1:]))
        edges.extend((tree_relays[-1], receiver_id) for receiver_id in receiver_ids)
        trees.append((tree_id, edges))
    return trees


def build_run_plan(
    inventory: Inventory,
    *,
    mode: str,
    tree_ids: list[int] | None,
    receiver_ids: list[int] | None,
    relay_ids: list[int] | None,
    group_label: str | None,
) -> RunPlan:
    if mode not in {"plain", "fec"}:
        raise SystemExit(f"Unsupported mode {mode!r}.")

    source_node_id = inventory.trainer.node_id
    receivers = receiver_ids or [node.node_id for node in inventory.workers]
    relays = relay_ids or [node.node_id for node in inventory.relays]
    trees = tree_ids or ([0] if mode == "plain" else [0, 1])

    if mode == "plain" and len(trees) != 1:
        raise SystemExit("Plain mode currently supports exactly one tree.")
    if mode == "fec" and len(trees) < 2:
        raise SystemExit("FEC mode requires at least two trees for comparison runs.")

    computed_trees = compute_relay_trees(
        source_node_id=source_node_id,
        receiver_ids=receivers,
        relay_ids=relays,
        tree_ids=trees,
    )
    topology_edges = sorted(
        {
            (src, dst)
            for _tree_id, tree_edges in computed_trees
            for src, dst in tree_edges
        }
    )
    label = group_label or f"fec-{mode}"
    return RunPlan(
        mode=mode,
        source_node_id=source_node_id,
        receiver_ids=receivers,
        relay_ids=relays[: len(trees) * RELAYS_PER_TREE],
        tree_ids=trees,
        trees=computed_trees,
        topology_edges=topology_edges,
        group_label=label,
    )


def render_controller_config(plan: RunPlan, *, n_nodes: int) -> str:
    edge_lines = ", ".join(f"[{src}, {dst}]" for src, dst in plan.topology_edges)
    return textwrap.dedent(
        f"""\
        protocol = "tcp"
        multicast_pool_base = "239.255.0.0"
        multicast_pool_mask = "255.255.0.0"

        [topology]
        n_nodes = {n_nodes}
        edges = [{edge_lines}]

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


def render_node_config(
    *,
    node_id: int,
    controller_addr: str,
    tree_ids: list[int],
    block_size: int,
    symbols_per_block: int,
    fec_enabled: bool,
    public_network_addr: str = "",
    public_network_interface: str = DEFAULT_INTERFACE,
    private_network_interface: str = DEFAULT_INTERFACE,
    private_network_name: str = DEFAULT_NETWORK_NAME,
    n_nodes: int | None = None,
) -> str:
    extra_n_nodes = f"n_nodes = {n_nodes}\n" if n_nodes is not None else ""
    public_network_addr = public_network_addr or "127.0.0.1"
    tree_values = ", ".join(str(tree_id) for tree_id in tree_ids)
    fec_value = "true" if fec_enabled else "false"
    return textwrap.dedent(
        f"""\
        controller_addr = "{controller_addr}"
        private_network_interface = "{private_network_interface}"
        private_network_name = "{private_network_name}"
        private_network_addr = "{public_network_addr}"
        private_network_port = "8080"
        public_network_interface = "{public_network_interface}"
        public_network_addr = "{public_network_addr}"
        public_network_port = "8080"
        node_id = {node_id}
        {extra_n_nodes}num_tun_queues = 1
        num_packet_processors = 4
        channel_capacity = 3000
        queue_capacity = 4000
        feature = "sequential"
        channel_backpressure = true
        enable_local_interface = false
        restart_on_disconnect = true

        [lossless_runtime_config]
        default_block_size = {block_size}
        fec_enabled = {fec_value}
        fec_default_tree_ids = [{tree_values}]
        fec_default_symbols_per_block = {symbols_per_block}
        """
    )


def write_deterministic_payload(path: Path, size: int) -> None:
    if size <= 0:
        raise SystemExit("Payload size must be positive.")
    path.parent.mkdir(parents=True, exist_ok=True)
    pattern = bytes((index * 31 + 7) % 256 for index in range(65_536))
    remaining = size
    with path.open("wb") as fh:
        while remaining > 0:
            chunk = pattern[: min(remaining, len(pattern))]
            fh.write(chunk)
            remaining -= len(chunk)


def _source_command(
    plan: RunPlan,
    *,
    block_size: int,
    group_timeout: int,
) -> list[str]:
    return [
        "python",
        "/app/examples/multicast-docker/scripts/multicast_node.py",
        "--role",
        "source",
        "--config",
        "/run/node-1.toml",
        "--controller-config",
        "/run/controller-config.toml",
        "--group-label",
        plan.group_label,
        "--source-node-id",
        str(plan.source_node_id),
        "--receiver-ids",
        ",".join(str(node_id) for node_id in plan.receiver_ids),
        "--tensor-path",
        "/run/payload.bin",
        "--artifact-dir",
        "/run/artifacts",
        "--chunk-size",
        str(block_size),
        "--group-timeout",
        str(group_timeout),
        "--fec",
        "on" if plan.mode == "fec" else "off",
    ]


def _receiver_command(
    plan: RunPlan,
    node_id: int,
    *,
    block_size: int,
    expected_bytes: int,
    group_timeout: int,
    receive_timeout_ms: int,
) -> list[str]:
    return [
        "python",
        "/app/examples/multicast-docker/scripts/multicast_node.py",
        "--role",
        "receiver",
        "--config",
        f"/run/node-{node_id}.toml",
        "--group-label",
        plan.group_label,
        "--node-id",
        str(node_id),
        "--source-node-id",
        str(plan.source_node_id),
        "--artifact-dir",
        "/run/artifacts",
        "--chunk-size",
        str(block_size),
        "--expected-bytes",
        str(expected_bytes),
        "--group-timeout",
        str(group_timeout),
        "--receive-timeout-ms",
        str(receive_timeout_ms),
        "--fec",
        "on" if plan.mode == "fec" else "off",
    ]


def _router_command(
    plan: RunPlan,
    node_id: int,
    *,
    block_size: int,
    group_timeout: int,
) -> list[str]:
    return [
        "python",
        "/app/examples/multicast-docker/scripts/multicast_node.py",
        "--role",
        "router",
        "--config",
        f"/run/node-{node_id}.toml",
        "--group-label",
        plan.group_label,
        "--source-node-id",
        str(plan.source_node_id),
        "--artifact-dir",
        "/run/artifacts",
        "--chunk-size",
        str(block_size),
        "--group-timeout",
        str(group_timeout),
        "--fec",
        "on" if plan.mode == "fec" else "off",
    ]


def generate_case(
    inventory: Inventory,
    *,
    mode: str,
    payload_size: int,
    tree_ids: list[int] | None,
    receiver_ids: list[int] | None,
    relay_ids: list[int] | None,
    group_label: str | None,
    out_dir: Path | None,
    block_size: int,
    symbols_per_block: int,
    group_timeout: int = DEFAULT_GROUP_TIMEOUT,
    receive_timeout_ms: int = DEFAULT_RECEIVE_TIMEOUT_MS,
) -> tuple[Path, RunPlan]:
    validate_block_size(
        block_size,
        fec_enabled=mode == "fec",
        symbols_per_block=symbols_per_block,
    )
    plan = build_run_plan(
        inventory,
        mode=mode,
        tree_ids=tree_ids,
        receiver_ids=receiver_ids,
        relay_ids=relay_ids,
        group_label=group_label,
    )

    run_id = time.strftime(f"%Y%m%d-%H%M%S-{mode}")
    if group_label is None:
        plan = dataclasses.replace(plan, group_label=run_id)
    run_dir = (out_dir or (GENERATED_ROOT / run_id)).resolve()
    run_dir.mkdir(parents=True, exist_ok=True)
    artifact_dir = run_dir / "artifacts"
    artifact_dir.mkdir(parents=True, exist_ok=True)
    active_node_ids = sorted(
        {plan.source_node_id, *plan.receiver_ids, *plan.relay_ids}
    )
    active_n_nodes = len(active_node_ids)

    payload_path = run_dir / "payload.bin"
    controller_config_path = run_dir / "controller-config.toml"
    write_deterministic_payload(payload_path, payload_size)
    controller_config_path.write_text(
        render_controller_config(plan, n_nodes=active_n_nodes),
        encoding="utf-8",
    )

    controller_addr = f"ws://{inventory.controller.ssh.host}:3000"
    for node in inventory.nodes:
        node_config_path = run_dir / f"node-{node.node_id}.toml"
        node_config_path.write_text(
            render_node_config(
                node_id=node.node_id,
                controller_addr=controller_addr,
                tree_ids=plan.tree_ids,
                block_size=block_size,
                symbols_per_block=symbols_per_block,
                fec_enabled=plan.mode == "fec",
                public_network_addr=node.ssh.host,
                public_network_interface=node.public_network_interface,
                private_network_interface=node.private_network_interface,
                n_nodes=active_n_nodes,
            ),
            encoding="utf-8",
        )

    launch_plan = {
        "source": _source_command(
            plan,
            block_size=block_size,
            group_timeout=group_timeout,
        ),
        "receivers": {
            str(node_id): _receiver_command(
                plan,
                node_id,
                block_size=block_size,
                expected_bytes=payload_size,
                group_timeout=group_timeout,
                receive_timeout_ms=receive_timeout_ms,
            )
            for node_id in plan.receiver_ids
        },
        "relays": {
            str(node_id): _router_command(
                plan,
                node_id,
                block_size=block_size,
                group_timeout=group_timeout,
            )
            for node_id in plan.relay_ids
        },
    }
    manifest = {
        "mode": plan.mode,
        "group_label": plan.group_label,
        "source_node_id": plan.source_node_id,
        "receiver_ids": plan.receiver_ids,
        "relay_ids": plan.relay_ids,
        "tree_ids": plan.tree_ids,
        "trees": [
            {"tree_id": tree_id, "edges": edges} for tree_id, edges in plan.trees
        ],
        "paths": {
            "run_dir": str(run_dir),
            "payload": str(payload_path),
            "controller_config": str(controller_config_path),
            "artifact_dir": str(artifact_dir),
        },
        "launch_plan": launch_plan,
    }
    (run_dir / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return run_dir, plan


def build_image_refs(tag: str) -> dict[str, str]:
    return {
        "controller_local": f"{LOCAL_REGISTRY}/nextmini-controller:{tag}",
        "controller_public": f"{PUBLIC_REGISTRY}/nextmini-controller:{tag}",
        "fec_local": f"{LOCAL_REGISTRY}/nextmini-fec:{tag}",
        "fec_public": f"{PUBLIC_REGISTRY}/nextmini-fec:{tag}",
    }


def load_last_image_tag() -> str:
    if not LAST_IMAGE_TAG_PATH.is_file():
        raise SystemExit(
            f"No saved image tag at {LAST_IMAGE_TAG_PATH}. Run prepare-images first or pass --image-tag."
        )
    return LAST_IMAGE_TAG_PATH.read_text(encoding="utf-8").strip()


def save_last_image_tag(tag: str) -> None:
    LAST_IMAGE_TAG_PATH.write_text(f"{tag}\n", encoding="utf-8")


def resolve_image_tag(value: str | None) -> str:
    return value or load_last_image_tag()


def _summary(inventory: Inventory) -> dict[str, object]:
    return {
        "controller": inventory.controller.ssh.host,
        "trainer": inventory.trainer.node_id,
        "workers": [node.node_id for node in inventory.workers],
        "relays": [node.node_id for node in inventory.relays],
        "remote_repo_dir": inventory.remote_repo_dir,
    }


def _ssh_destination(target: SshTarget) -> str:
    return f"{target.user}@{target.host}"


def _ssh_prefix(target: SshTarget) -> list[str]:
    cmd = [
        "ssh",
        "-o",
        "BatchMode=yes",
        "-o",
        "StrictHostKeyChecking=accept-new",
        "-o",
        "ConnectTimeout=10",
    ]
    if target.identity_file:
        cmd.extend(["-i", target.identity_file])
    if target.port != 22:
        cmd.extend(["-p", str(target.port)])
    return cmd


def _scp_prefix(target: SshTarget) -> list[str]:
    cmd = [
        "scp",
        "-o",
        "BatchMode=yes",
        "-o",
        "StrictHostKeyChecking=accept-new",
        "-o",
        "ConnectTimeout=10",
    ]
    if target.identity_file:
        cmd.extend(["-i", target.identity_file])
    if target.port != 22:
        cmd.extend(["-P", str(target.port)])
    return cmd


def run_local(
    cmd: list[str],
    *,
    capture: bool = False,
    check: bool = True,
    input_text: str | None = None,
    retries: int = 0,
) -> subprocess.CompletedProcess[str]:
    last_exc: subprocess.CalledProcessError | None = None
    attempts = retries + 1
    for attempt in range(1, attempts + 1):
        try:
            return subprocess.run(
                cmd,
                text=True,
                input=input_text,
                capture_output=capture,
                check=check,
            )
        except subprocess.CalledProcessError as exc:
            last_exc = exc
            if attempt >= attempts or exc.returncode != 255:
                raise
            time.sleep(min(5 * attempt, 15))
    assert last_exc is not None
    raise last_exc


def remote_bash(
    target: SshTarget,
    script: str,
    *,
    capture: bool = False,
    check: bool = True,
    input_text: str | None = None,
    retries: int = 2,
) -> subprocess.CompletedProcess[str]:
    cmd = _ssh_prefix(target) + [
        _ssh_destination(target),
        f"bash -lc {shlex.quote(script)}",
    ]
    return run_local(
        cmd,
        capture=capture,
        check=check,
        input_text=input_text,
        retries=retries,
    )


def remote_home(target: SshTarget) -> str:
    key = (target.user, target.host, target.port)
    cached = REMOTE_HOME_CACHE.get(key)
    if cached:
        return cached
    home = remote_bash(target, 'printf %s "$HOME"', capture=True).stdout.strip()
    REMOTE_HOME_CACHE[key] = home
    return home


def expand_remote_path(target: SshTarget, path: str) -> str:
    if path == "~":
        return remote_home(target)
    if path.startswith("~/"):
        return f"{remote_home(target)}/{path[2:]}"
    return path


def ensure_remote_dir(target: SshTarget, path: str, *, clean: bool = False) -> None:
    if clean:
        script = f"rm -rf {shlex.quote(path)} && mkdir -p {shlex.quote(path)}"
    else:
        script = f"mkdir -p {shlex.quote(path)}"
    remote_bash(target, script)


def remote_file_exists(target: SshTarget, path: str) -> bool:
    result = remote_bash(target, f"test -f {shlex.quote(path)}", check=False)
    return result.returncode == 0


def remote_file_size(target: SshTarget, path: str) -> int | None:
    result = remote_bash(
        target,
        f"if test -f {shlex.quote(path)}; then stat -c %s {shlex.quote(path)}; fi",
        capture=True,
    )
    raw = result.stdout.strip()
    return int(raw) if raw else None


def read_remote_text(target: SshTarget, path: str) -> str:
    result = remote_bash(target, f"cat {shlex.quote(path)}", capture=True)
    return result.stdout


def write_remote_text(target: SshTarget, path: str, content: str) -> None:
    parent = str(Path(path).parent)
    tmp_pattern = f"{parent}/.tmp-write.XXXXXX"
    script = textwrap.dedent(
        f"""\
        set -e
        mkdir -p {shlex.quote(parent)}
        tmp_file=$(mktemp {shlex.quote(tmp_pattern)})
        cat > "$tmp_file"
        mv "$tmp_file" {shlex.quote(path)}
        """
    ).strip()
    remote_bash(target, script, input_text=content)


def copy_to_remote(target: SshTarget, local_path: Path, remote_path: str) -> None:
    cmd = _scp_prefix(target) + [str(local_path), f"{_ssh_destination(target)}:{remote_path}"]
    run_local(cmd, retries=2)


def copy_from_remote(target: SshTarget, remote_path: str, local_path: Path) -> None:
    local_path.parent.mkdir(parents=True, exist_ok=True)
    cmd = _scp_prefix(target) + [f"{_ssh_destination(target)}:{remote_path}", str(local_path)]
    run_local(cmd, retries=2)


def copy_optional_remote_file(target: SshTarget, remote_path: str, local_path: Path) -> bool:
    if not remote_file_exists(target, remote_path):
        return False
    copy_from_remote(target, remote_path, local_path)
    return True


def remote_container_state(target: SshTarget, container_name: str) -> tuple[str, int] | None:
    result = remote_bash(
        target,
        (
            f"docker inspect -f {shlex.quote('{{.State.Status}} {{.State.ExitCode}}')} "
            f"{shlex.quote(container_name)} 2>/dev/null || true"
        ),
        capture=True,
    )
    raw = result.stdout.strip()
    if not raw:
        return None
    status, exit_code = raw.split(maxsplit=1)
    return status, int(exit_code)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def unique_run_name(run_dir: Path) -> str:
    return run_dir.name.replace("_", "-")


def node_container_name(run_dir: Path, node_id: int, role: str) -> str:
    return f"fec-{role}-{node_id}-{unique_run_name(run_dir)}"


def controller_container_name(run_dir: Path) -> str:
    return f"fec-controller-{unique_run_name(run_dir)}"


def postgres_container_name(run_dir: Path) -> str:
    return f"fec-postgres-{unique_run_name(run_dir)}"


def controller_network_name(run_dir: Path) -> str:
    return f"{DEFAULT_BOSTON_NETWORK}-{unique_run_name(run_dir)}"


def save_state(run_dir: Path, state: dict) -> None:
    (run_dir / REMOTE_STATE_FILE).write_text(json.dumps(state, indent=2), encoding="utf-8")


def load_state(run_dir: Path) -> dict:
    path = run_dir / REMOTE_STATE_FILE
    if not path.is_file():
        raise SystemExit(f"Missing state file: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def ssh_target_from_state(raw: dict) -> SshTarget:
    return SshTarget(
        host=raw["host"],
        user=raw["user"],
        identity_file=raw.get("identity_file"),
        port=int(raw.get("port", 22)),
    )


def _state_remote_run_dir(entry: dict) -> str:
    return expand_remote_path(
        ssh_target_from_state(entry["ssh"]),
        entry["remote_run_dir"],
    )


def _state_artifact_dir(entry: dict) -> str:
    return f"{_state_remote_run_dir(entry)}/artifacts"


def sync_repo_to_boston(inventory: Inventory) -> str:
    controller_target = inventory.controller.ssh
    remote_repo_dir = expand_remote_path(controller_target, inventory.remote_repo_dir)
    ensure_remote_dir(controller_target, remote_repo_dir)
    ssh_transport = [
        "ssh",
        "-o",
        "BatchMode=yes",
        "-o",
        "StrictHostKeyChecking=accept-new",
        "-o",
        "ConnectTimeout=10",
    ]
    if controller_target.identity_file:
        ssh_transport.extend(["-i", controller_target.identity_file])
    if controller_target.port != 22:
        ssh_transport.extend(["-p", str(controller_target.port)])
    cmd = [
        "rsync",
        "-az",
        "-e",
        " ".join(shlex.quote(part) for part in ssh_transport),
        "--exclude=.git/",
        "--exclude=target/",
        "--exclude=.venv/",
        "--exclude=__pycache__/",
        "--exclude=.pytest_cache/",
        "--exclude=.mypy_cache/",
        "--exclude=.ruff_cache/",
        "--exclude=.multidc_cache/",
        "--exclude=examples/fec/generated/",
        "--exclude=examples/fec/artifacts/",
        "--exclude=examples/multicast-docker/artifacts/",
        f"{REPO_ROOT}/",
        f"{_ssh_destination(controller_target)}:{remote_repo_dir}/",
    ]
    run_local(cmd)
    return remote_repo_dir


def ensure_boston_registry(inventory: Inventory) -> None:
    controller_target = inventory.controller.ssh
    remote_bash(
        controller_target,
        textwrap.dedent(
            """\
            set -euo pipefail
            docker volume create nextmini-registry-data >/dev/null
            if ! docker ps --format '{{.Names}}' | grep -qx nextmini-registry; then
              docker rm -f nextmini-registry >/dev/null 2>&1 || true
              docker run -d --restart unless-stopped --name nextmini-registry \
                -p 5000:5000 \
                -v nextmini-registry-data:/var/lib/registry \
                registry:2 >/dev/null
            fi
            curl -fsS http://127.0.0.1:5000/v2/ >/dev/null
            """
        ).strip(),
    )


def prepare_images(inventory: Inventory, *, image_tag: str) -> dict[str, str]:
    remote_repo_dir = sync_repo_to_boston(inventory)
    ensure_boston_registry(inventory)
    refs = build_image_refs(image_tag)
    controller_target = inventory.controller.ssh
    remote_bash(
        controller_target,
        textwrap.dedent(
            f"""\
            set -euo pipefail
            cd {shlex.quote(remote_repo_dir)}
            docker build -t {shlex.quote(refs["controller_local"])} -f controller/Dockerfile .
            docker push {shlex.quote(refs["controller_local"])}
            docker build -t {shlex.quote(refs["fec_local"])} -f examples/fec/Dockerfile .
            docker push {shlex.quote(refs["fec_local"])}
            """
        ).strip(),
    )

    smoke_worker = inventory.workers[0]
    remote_bash(
        smoke_worker.ssh,
        textwrap.dedent(
            f"""\
            set -euo pipefail
            docker pull {shlex.quote(refs["fec_public"])} >/dev/null
            docker run --rm {shlex.quote(refs["fec_public"])} \
              python -c {shlex.quote('import nextmini_py; print("OK")')} >/dev/null
            """
        ).strip(),
    )
    save_last_image_tag(image_tag)
    return refs


def build_state(
    inventory: Inventory,
    *,
    run_dir: Path,
    plan: RunPlan,
    image_tag: str,
) -> dict:
    refs = build_image_refs(image_tag)
    payload_path = run_dir / "payload.bin"
    controller_target = inventory.controller.ssh
    controller_run_dir = f"{DEFAULT_REMOTE_RUN_ROOT}/{run_dir.name}"
    active_node_ids = {
        plan.source_node_id,
        *plan.receiver_ids,
        *plan.relay_ids,
    }
    state = {
        "run_dir": str(run_dir),
        "run_name": run_dir.name,
        "mode": plan.mode,
        "group_label": plan.group_label,
        "source_node_id": plan.source_node_id,
        "receiver_ids": plan.receiver_ids,
        "relay_ids": plan.relay_ids,
        "tree_ids": plan.tree_ids,
        "image_tag": image_tag,
        "image_refs": refs,
        "payload_size": payload_path.stat().st_size,
        "payload_sha256": sha256_file(payload_path),
        "controller": {
            "ssh": dataclasses.asdict(controller_target),
            "remote_run_dir": controller_run_dir,
            "artifact_dir": f"{controller_run_dir}/artifacts",
            "controller_container": controller_container_name(run_dir),
            "postgres_container": postgres_container_name(run_dir),
            "docker_network": controller_network_name(run_dir),
        },
        "nodes": {},
    }

    for node in inventory.nodes:
        if node.node_id not in active_node_ids:
            continue
        target = node.ssh
        remote_run_dir = f"{DEFAULT_REMOTE_RUN_ROOT}/{run_dir.name}"
        if node.node_id == plan.source_node_id:
            role_name = "source"
        elif node.node_id in plan.relay_ids:
            role_name = "relay"
        elif node.node_id in plan.receiver_ids:
            role_name = "receiver"
        else:
            role_name = node.role
        state["nodes"][str(node.node_id)] = {
            "role": node.role,
            "node_id": node.node_id,
            "ssh": dataclasses.asdict(target),
            "remote_run_dir": remote_run_dir,
            "artifact_dir": f"{remote_run_dir}/artifacts",
            "container_name": node_container_name(run_dir, node.node_id, role_name),
        }
    return state


def stage_run_files(inventory: Inventory, run_dir: Path, state: dict) -> None:
    manifest_path = run_dir / "manifest.json"
    controller_cfg = run_dir / "controller-config.toml"
    payload_path = run_dir / "payload.bin"

    controller_entry = state["controller"]
    controller_target = ssh_target_from_state(controller_entry["ssh"])
    controller_run_dir = _state_remote_run_dir(controller_entry)
    controller_artifact_dir = _state_artifact_dir(controller_entry)
    ensure_remote_dir(controller_target, controller_run_dir, clean=True)
    ensure_remote_dir(controller_target, controller_artifact_dir)
    copy_to_remote(
        controller_target,
        controller_cfg,
        f"{controller_run_dir}/controller-config.toml",
    )
    copy_to_remote(
        controller_target,
        manifest_path,
        f"{controller_run_dir}/manifest.json",
    )

    for node in inventory.nodes:
        node_state = state["nodes"].get(str(node.node_id))
        if node_state is None:
            continue
        target = ssh_target_from_state(node_state["ssh"])
        node_run_dir = _state_remote_run_dir(node_state)
        node_artifact_dir = _state_artifact_dir(node_state)
        ensure_remote_dir(target, node_run_dir, clean=True)
        ensure_remote_dir(target, node_artifact_dir)
        copy_to_remote(
            target,
            run_dir / f"node-{node.node_id}.toml",
            f"{node_run_dir}/node-{node.node_id}.toml",
        )
        copy_to_remote(
            target,
            manifest_path,
            f"{node_run_dir}/manifest.json",
        )
        if node.role == "trainer":
            copy_to_remote(
                target,
                controller_cfg,
                f"{node_run_dir}/controller-config.toml",
            )
            copy_to_remote(
                target,
                payload_path,
                f"{node_run_dir}/payload.bin",
            )


def start_controller_stack(state: dict) -> None:
    controller_entry = state["controller"]
    target = ssh_target_from_state(controller_entry["ssh"])
    refs = state["image_refs"]
    config_path = f"{_state_remote_run_dir(controller_entry)}/controller-config.toml"
    bind = f"{config_path}:/var/nextmini/config.toml:ro"
    remote_bash(
        target,
        textwrap.dedent(
            f"""\
            set -euo pipefail
            docker network inspect {shlex.quote(controller_entry["docker_network"])} >/dev/null 2>&1 || \
              docker network create {shlex.quote(controller_entry["docker_network"])} >/dev/null
            docker rm -f {shlex.quote(controller_entry["controller_container"])} \
              {shlex.quote(controller_entry["postgres_container"])} >/dev/null 2>&1 || true
            docker run -d --name {shlex.quote(controller_entry["postgres_container"])} \
              --network {shlex.quote(controller_entry["docker_network"])} \
              --network-alias postgres \
              -e POSTGRES_USER=pgusr \
              -e POSTGRES_PASSWORD=pgpwrd \
              -e POSTGRES_DB=nextmini \
              docker.io/postgres:16-alpine >/dev/null
            for _i in $(seq 1 60); do
              docker exec {shlex.quote(controller_entry["postgres_container"])} \
                pg_isready -U pgusr -d nextmini >/dev/null 2>&1 && break
              sleep 1
            done
            docker image inspect {shlex.quote(refs["controller_local"])} >/dev/null 2>&1 || \
              docker pull {shlex.quote(refs["controller_local"])} >/dev/null
            docker run -d --name {shlex.quote(controller_entry["controller_container"])} \
              --network {shlex.quote(controller_entry["docker_network"])} \
              -p 3000:3000 \
              -e RUST_LOG=info \
              -v {shlex.quote(bind)} \
              {shlex.quote(refs["controller_local"])} \
              /var/nextmini/controller >/dev/null
            for _i in $(seq 1 60); do
              docker inspect -f '{{{{.State.Status}}}}' {shlex.quote(controller_entry["controller_container"])} \
                2>/dev/null | grep -qx running && break
              sleep 1
            done
            docker inspect -f '{{{{.State.Status}}}}' {shlex.quote(controller_entry["controller_container"])} \
              2>/dev/null | grep -qx running || {{
              echo "Controller container is not running." >&2
              exit 1
            }}
            sleep 2
            """
        ).strip(),
    )


def launch_node_container(
    state: dict,
    *,
    node_id: int,
    command: list[str],
) -> None:
    node_state = state["nodes"][str(node_id)]
    target = ssh_target_from_state(node_state["ssh"])
    command_text = " ".join(shlex.quote(part) for part in command)
    bind = f"{_state_remote_run_dir(node_state)}:/run"
    remote_bash(
        target,
        textwrap.dedent(
            f"""\
            set -euo pipefail
            docker rm -f {shlex.quote(node_state["container_name"])} >/dev/null 2>&1 || true
            docker pull {shlex.quote(state["image_refs"]["fec_public"])} >/dev/null
            docker run -d --name {shlex.quote(node_state["container_name"])} \
              --network host \
              --cap-add NET_ADMIN \
              --device /dev/net/tun \
              -e PYTHONUNBUFFERED=1 \
              -e RUST_LOG=info \
              -v {shlex.quote(bind)} \
              {shlex.quote(state["image_refs"]["fec_public"])} \
              {command_text} >/dev/null
            """
        ).strip(),
    )


def relay_group_info(state: dict, *, timeout_seconds: int) -> None:
    trainer_entry = state["nodes"][str(state["source_node_id"])]
    trainer_target = ssh_target_from_state(trainer_entry["ssh"])
    trainer_group_info = f"{_state_artifact_dir(trainer_entry)}/{GROUP_INFO_FILE}"
    pending_receivers = [state["nodes"][str(node_id)] for node_id in state["receiver_ids"]]
    deadline = time.monotonic() + timeout_seconds

    while time.monotonic() < deadline:
        if remote_file_exists(trainer_target, trainer_group_info):
            group_info = read_remote_text(trainer_target, trainer_group_info)
            for entry in pending_receivers:
                target = ssh_target_from_state(entry["ssh"])
                write_remote_text(
                    target,
                    f"{_state_artifact_dir(entry)}/{GROUP_INFO_FILE}",
                    group_info,
                )
            return
        time.sleep(1)

    raise TimeoutError("Timed out waiting for group-info.json on the trainer node.")


def relay_receiver_ready_files(state: dict, *, timeout_seconds: int) -> None:
    trainer_entry = state["nodes"][str(state["source_node_id"])]
    trainer_target = ssh_target_from_state(trainer_entry["ssh"])
    pending = set(state["receiver_ids"])
    deadline = time.monotonic() + timeout_seconds

    while pending and time.monotonic() < deadline:
        for node_id in list(pending):
            receiver_entry = state["nodes"][str(node_id)]
            receiver_target = ssh_target_from_state(receiver_entry["ssh"])
            ready_name = READY_FILE_TEMPLATE.format(node_id)
            ready_path = f"{_state_artifact_dir(receiver_entry)}/{ready_name}"
            if not remote_file_exists(receiver_target, ready_path):
                receiver_state = remote_container_state(
                    receiver_target, receiver_entry["container_name"]
                )
                if receiver_state and receiver_state[0] == "exited" and receiver_state[1] != 0:
                    raise RuntimeError(
                        f"Receiver node {node_id} exited early with code {receiver_state[1]}."
                    )
                continue
            ready_payload = read_remote_text(receiver_target, ready_path)
            write_remote_text(
                trainer_target,
                f"{_state_artifact_dir(trainer_entry)}/{ready_name}",
                ready_payload,
            )
            pending.remove(node_id)
        if pending:
            time.sleep(1)

    if pending:
        raise TimeoutError(
            f"Timed out waiting for receiver ready files from nodes {sorted(pending)}."
        )


def wait_for_receiver_outputs(state: dict, *, timeout_seconds: int) -> None:
    pending = set(state["receiver_ids"])
    expected_size = int(state["payload_size"])
    deadline = time.monotonic() + timeout_seconds

    while pending and time.monotonic() < deadline:
        for node_id in list(pending):
            receiver_entry = state["nodes"][str(node_id)]
            receiver_target = ssh_target_from_state(receiver_entry["ssh"])
            output_path = f"{_state_artifact_dir(receiver_entry)}/receiver-{node_id}.bin"
            size = remote_file_size(receiver_target, output_path)
            if size == expected_size:
                pending.remove(node_id)
                continue
            receiver_state = remote_container_state(
                receiver_target, receiver_entry["container_name"]
            )
            if receiver_state and receiver_state[0] == "exited" and receiver_state[1] != 0:
                raise RuntimeError(
                    f"Receiver node {node_id} exited early with code {receiver_state[1]}."
                )

        if pending:
            source_entry = state["nodes"][str(state["source_node_id"])]
            source_target = ssh_target_from_state(source_entry["ssh"])
            source_state = remote_container_state(source_target, source_entry["container_name"])
            if source_state and source_state[0] == "exited" and source_state[1] != 0:
                raise RuntimeError(
                    f"Source node exited early with code {source_state[1]}."
                )
            time.sleep(2)

    if pending:
        raise TimeoutError(
            f"Timed out waiting for receiver payloads from nodes {sorted(pending)}."
        )


def fetch_logs(state: dict) -> None:
    run_dir = Path(state["run_dir"])
    logs_dir = run_dir / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)

    controller_entry = state["controller"]
    controller_target = ssh_target_from_state(controller_entry["ssh"])
    controller_logs = remote_bash(
        controller_target,
        f"docker logs {shlex.quote(controller_entry['controller_container'])} 2>&1 || true",
        capture=True,
    ).stdout
    (logs_dir / "controller.log").write_text(controller_logs, encoding="utf-8")

    postgres_logs = remote_bash(
        controller_target,
        f"docker logs {shlex.quote(controller_entry['postgres_container'])} 2>&1 || true",
        capture=True,
    ).stdout
    (logs_dir / "postgres.log").write_text(postgres_logs, encoding="utf-8")

    for node_id, node_entry in state["nodes"].items():
        target = ssh_target_from_state(node_entry["ssh"])
        log_text = remote_bash(
            target,
            f"docker logs {shlex.quote(node_entry['container_name'])} 2>&1 || true",
            capture=True,
        ).stdout
        (logs_dir / f"node-{node_id}.log").write_text(log_text, encoding="utf-8")


def fetch_artifacts(state: dict) -> dict:
    run_dir = Path(state["run_dir"])
    fetched_root = run_dir / "fetched"
    fetched_root.mkdir(parents=True, exist_ok=True)

    trainer_entry = state["nodes"][str(state["source_node_id"])]
    trainer_target = ssh_target_from_state(trainer_entry["ssh"])
    trainer_dir = fetched_root / f"node-{trainer_entry['node_id']}"
    trainer_dir.mkdir(parents=True, exist_ok=True)
    copy_optional_remote_file(
        trainer_target,
        f"{_state_artifact_dir(trainer_entry)}/{GROUP_INFO_FILE}",
        trainer_dir / GROUP_INFO_FILE,
    )
    copy_optional_remote_file(
        trainer_target,
        f"{_state_artifact_dir(trainer_entry)}/{METADATA_FILE}",
        trainer_dir / METADATA_FILE,
    )
    for node_id in state["receiver_ids"]:
        ready_name = READY_FILE_TEMPLATE.format(node_id)
        copy_optional_remote_file(
            trainer_target,
            f"{_state_artifact_dir(trainer_entry)}/{ready_name}",
            trainer_dir / ready_name,
        )

    for node_id in state["receiver_ids"]:
        receiver_entry = state["nodes"][str(node_id)]
        receiver_target = ssh_target_from_state(receiver_entry["ssh"])
        node_dir = fetched_root / f"node-{node_id}"
        node_dir.mkdir(parents=True, exist_ok=True)
        copy_optional_remote_file(
            receiver_target,
            f"{_state_artifact_dir(receiver_entry)}/receiver-{node_id}.bin",
            node_dir / f"receiver-{node_id}.bin",
        )
        ready_name = READY_FILE_TEMPLATE.format(node_id)
        copy_optional_remote_file(
            receiver_target,
            f"{_state_artifact_dir(receiver_entry)}/{ready_name}",
            node_dir / ready_name,
        )

    fetch_logs(state)
    return verify_artifacts(state)


def verify_artifacts(state: dict) -> dict:
    run_dir = Path(state["run_dir"])
    payload_path = run_dir / "payload.bin"
    expected_sha = sha256_file(payload_path)
    expected_size = payload_path.stat().st_size

    results = []
    ok = True
    for node_id in state["receiver_ids"]:
        receiver_path = run_dir / "fetched" / f"node-{node_id}" / f"receiver-{node_id}.bin"
        if not receiver_path.is_file():
            ok = False
            results.append(
                {
                    "node_id": node_id,
                    "ok": False,
                    "reason": "missing artifact",
                }
            )
            continue
        size = receiver_path.stat().st_size
        sha = sha256_file(receiver_path)
        node_ok = size == expected_size and sha == expected_sha
        ok = ok and node_ok
        results.append(
            {
                "node_id": node_id,
                "ok": node_ok,
                "size": size,
                "sha256": sha,
            }
        )

    verification = {
        "ok": ok,
        "expected_size": expected_size,
        "expected_sha256": expected_sha,
        "receivers": results,
    }
    (run_dir / "verification.json").write_text(
        json.dumps(verification, indent=2),
        encoding="utf-8",
    )
    return verification


def stop_run(state: dict) -> None:
    for node_entry in state["nodes"].values():
        target = ssh_target_from_state(node_entry["ssh"])
        remote_bash(
            target,
            f"docker rm -f {shlex.quote(node_entry['container_name'])} >/dev/null 2>&1 || true",
            check=False,
        )

    controller_entry = state["controller"]
    controller_target = ssh_target_from_state(controller_entry["ssh"])
    remote_bash(
        controller_target,
        textwrap.dedent(
            f"""\
            docker rm -f {shlex.quote(controller_entry["controller_container"])} \
              {shlex.quote(controller_entry["postgres_container"])} >/dev/null 2>&1 || true
            docker network rm {shlex.quote(controller_entry["docker_network"])} >/dev/null 2>&1 || true
            """
        ).strip(),
        check=False,
    )


def cleanup_remote_run_dirs(state: dict) -> None:
    for node_entry in state["nodes"].values():
        target = ssh_target_from_state(node_entry["ssh"])
        remote_bash(
            target,
            f"rm -rf {shlex.quote(_state_remote_run_dir(node_entry))}",
            check=False,
        )

    controller_entry = state["controller"]
    controller_target = ssh_target_from_state(controller_entry["ssh"])
    remote_bash(
        controller_target,
        f"rm -rf {shlex.quote(_state_remote_run_dir(controller_entry))}",
        check=False,
    )


def remove_local_run_dir(run_dir: Path) -> Path:
    resolved = run_dir.resolve()
    generated_root = GENERATED_ROOT.resolve()
    try:
        resolved.relative_to(generated_root)
    except ValueError as exc:
        raise SystemExit(
            f"Refusing to remove {resolved}; expected a path under {generated_root}."
        ) from exc
    if resolved == generated_root:
        raise SystemExit("Refusing to remove the generated root itself; pass --all instead.")
    shutil.rmtree(resolved, ignore_errors=True)
    return resolved


def run_experiment(
    inventory: Inventory,
    *,
    mode: str,
    payload_size: int,
    tree_ids: list[int] | None,
    receiver_ids: list[int] | None,
    relay_ids: list[int] | None,
    group_label: str | None,
    out_dir: Path | None,
    block_size: int,
    symbols_per_block: int,
    group_timeout: int,
    receive_timeout_ms: int,
    run_timeout: int,
    image_tag: str,
    keep_containers: bool,
) -> tuple[Path, dict]:
    run_dir, plan = generate_case(
        inventory,
        mode=mode,
        payload_size=payload_size,
        tree_ids=tree_ids,
        receiver_ids=receiver_ids,
        relay_ids=relay_ids,
        group_label=group_label,
        out_dir=out_dir,
        block_size=block_size,
        symbols_per_block=symbols_per_block,
        group_timeout=group_timeout,
        receive_timeout_ms=receive_timeout_ms,
    )
    state = build_state(inventory, run_dir=run_dir, plan=plan, image_tag=image_tag)
    save_state(run_dir, state)
    stage_run_files(inventory, run_dir, state)

    verification: dict | None = None
    success = False
    try:
        start_controller_stack(state)
        for relay_id in plan.relay_ids:
            launch_node_container(
                state,
                node_id=relay_id,
                command=_router_command(
                    plan,
                    relay_id,
                    block_size=block_size,
                    group_timeout=group_timeout,
                ),
            )
        for receiver_id in plan.receiver_ids:
            launch_node_container(
                state,
                node_id=receiver_id,
                command=_receiver_command(
                    plan,
                    receiver_id,
                    block_size=block_size,
                    expected_bytes=payload_size,
                    group_timeout=group_timeout,
                    receive_timeout_ms=receive_timeout_ms,
                ),
            )
        launch_node_container(
            state,
            node_id=plan.source_node_id,
            command=_source_command(
                plan,
                block_size=block_size,
                group_timeout=group_timeout,
            ),
        )
        relay_group_info(state, timeout_seconds=run_timeout)
        relay_receiver_ready_files(state, timeout_seconds=run_timeout)
        wait_for_receiver_outputs(state, timeout_seconds=run_timeout)
        verification = fetch_artifacts(state)
        success = bool(verification["ok"])
        return run_dir, verification
    except Exception:
        try:
            fetch_artifacts(state)
        except Exception:
            pass
        raise
    finally:
        if success and not keep_containers:
            stop_run(state)
            cleanup_remote_run_dirs(state)


def cmd_validate(args: argparse.Namespace) -> int:
    inventory = load_inventory(args.inventory)
    print(json.dumps(_summary(inventory), indent=2))
    return 0


def cmd_plan(args: argparse.Namespace) -> int:
    inventory = load_inventory(args.inventory)
    plan = build_run_plan(
        inventory,
        mode=args.mode,
        tree_ids=parse_int_list(args.tree_ids),
        receiver_ids=parse_int_list(args.receiver_ids),
        relay_ids=parse_int_list(args.relay_ids),
        group_label=args.group_label,
    )
    print(
        json.dumps(
            {
                "mode": plan.mode,
                "group_label": plan.group_label,
                "source_node_id": plan.source_node_id,
                "receiver_ids": plan.receiver_ids,
                "relay_ids": plan.relay_ids,
                "tree_ids": plan.tree_ids,
                "trees": [
                    {"tree_id": tree_id, "edges": edges}
                    for tree_id, edges in plan.trees
                ],
                "topology_edges": plan.topology_edges,
            },
            indent=2,
        )
    )
    return 0


def cmd_generate(args: argparse.Namespace) -> int:
    inventory = load_inventory(args.inventory)
    payload_size = parse_size(args.payload_size)
    out_dir, plan = generate_case(
        inventory,
        mode=args.mode,
        payload_size=payload_size,
        tree_ids=parse_int_list(args.tree_ids),
        receiver_ids=parse_int_list(args.receiver_ids),
        relay_ids=parse_int_list(args.relay_ids),
        group_label=args.group_label,
        out_dir=args.out_dir,
        block_size=args.block_size,
        symbols_per_block=args.symbols_per_block,
        group_timeout=args.group_timeout,
        receive_timeout_ms=args.receive_timeout_ms,
    )
    print(f"Generated run in {out_dir}")
    print(f"Mode: {plan.mode}")
    print(f"Group label: {plan.group_label}")
    print(f"Receivers: {plan.receiver_ids}")
    print(f"Relays: {plan.relay_ids}")
    print(f"Trees: {plan.trees}")
    print(f"Payload: {out_dir / 'payload.bin'} ({payload_size} bytes)")
    print(f"Manifest: {out_dir / 'manifest.json'}")
    return 0


def cmd_prepare_images(args: argparse.Namespace) -> int:
    inventory = load_inventory(args.inventory)
    image_tag = args.image_tag or time.strftime("fec-dev-%Y%m%d-%H%M%S", time.gmtime())
    refs = prepare_images(inventory, image_tag=image_tag)
    print(json.dumps({"image_tag": image_tag, "image_refs": refs}, indent=2))
    return 0


def cmd_run(args: argparse.Namespace) -> int:
    inventory = load_inventory(args.inventory)
    image_tag = resolve_image_tag(args.image_tag)
    payload_size = parse_size(args.payload_size)
    run_dir, verification = run_experiment(
        inventory,
        mode=args.mode,
        payload_size=payload_size,
        tree_ids=parse_int_list(args.tree_ids),
        receiver_ids=parse_int_list(args.receiver_ids),
        relay_ids=parse_int_list(args.relay_ids),
        group_label=args.group_label,
        out_dir=args.out_dir,
        block_size=args.block_size,
        symbols_per_block=args.symbols_per_block,
        group_timeout=args.group_timeout,
        receive_timeout_ms=args.receive_timeout_ms,
        run_timeout=args.run_timeout,
        image_tag=image_tag,
        keep_containers=args.keep_containers,
    )
    print(
        json.dumps(
            {
                "run_dir": str(run_dir),
                "image_tag": image_tag,
                "verification": verification,
            },
            indent=2,
        )
    )
    return 0 if verification["ok"] else 1


def cmd_fetch_artifacts(args: argparse.Namespace) -> int:
    state = load_state(args.run_dir.resolve())
    verification = fetch_artifacts(state)
    print(json.dumps({"run_dir": str(args.run_dir.resolve()), "verification": verification}, indent=2))
    return 0 if verification["ok"] else 1


def cmd_down(args: argparse.Namespace) -> int:
    state = load_state(args.run_dir.resolve())
    stop_run(state)
    print(f"Stopped run {args.run_dir.resolve()}")
    return 0


def cmd_clean_generated(args: argparse.Namespace) -> int:
    generated_root = GENERATED_ROOT.resolve()
    removed: list[str] = []
    if args.all:
        if not generated_root.exists():
            print(f"No generated runs under {generated_root}")
            return 0
        for child in sorted(generated_root.iterdir()):
            if child.is_dir():
                remove_local_run_dir(child)
                removed.append(str(child.resolve()))
    else:
        removed_path = remove_local_run_dir(args.run_dir)
        removed.append(str(removed_path))

    print(json.dumps({"removed": removed}, indent=2))
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="FEC WAN experiment runner.")
    sub = parser.add_subparsers(dest="cmd", required=True)

    def add_inventory_arg(p: argparse.ArgumentParser) -> None:
        p.add_argument(
            "--inventory",
            type=Path,
            default=REPO_ROOT / "examples" / "fec" / "inventory.toml",
        )

    def add_mode_args(p: argparse.ArgumentParser) -> None:
        p.add_argument("--mode", choices=("plain", "fec"), required=True)
        p.add_argument("--tree-ids", default="")
        p.add_argument("--receiver-ids", default="")
        p.add_argument("--relay-ids", default="")
        p.add_argument("--group-label", default=None)

    def add_generation_args(p: argparse.ArgumentParser) -> None:
        p.add_argument("--payload-size", default="100MB")
        p.add_argument("--block-size", type=int, default=DEFAULT_BLOCK_SIZE)
        p.add_argument(
            "--symbols-per-block", type=int, default=DEFAULT_SYMBOLS_PER_BLOCK
        )
        p.add_argument("--group-timeout", type=int, default=DEFAULT_GROUP_TIMEOUT)
        p.add_argument(
            "--receive-timeout-ms",
            type=int,
            default=DEFAULT_RECEIVE_TIMEOUT_MS,
        )
        p.add_argument("--out-dir", type=Path, default=None)

    p_validate = sub.add_parser("validate", help="Validate inventory only.")
    add_inventory_arg(p_validate)
    p_validate.set_defaults(func=cmd_validate)

    p_plan = sub.add_parser("plan", help="Plan relay-backed trees for a run.")
    add_inventory_arg(p_plan)
    add_mode_args(p_plan)
    p_plan.set_defaults(func=cmd_plan)

    p_generate = sub.add_parser(
        "generate", help="Generate controller config, node configs, payload, and manifest."
    )
    add_inventory_arg(p_generate)
    add_mode_args(p_generate)
    add_generation_args(p_generate)
    p_generate.set_defaults(func=cmd_generate)

    p_prepare = sub.add_parser(
        "prepare-images",
        help="Sync repo to Boston, build controller/FEC images, and push them to the Boston registry.",
    )
    add_inventory_arg(p_prepare)
    p_prepare.add_argument("--image-tag", default=None)
    p_prepare.set_defaults(func=cmd_prepare_images)

    p_run = sub.add_parser(
        "run",
        help="Generate, stage, launch, fetch, verify, and optionally clean up a WAN experiment run.",
    )
    add_inventory_arg(p_run)
    add_mode_args(p_run)
    add_generation_args(p_run)
    p_run.add_argument("--image-tag", default=None)
    p_run.add_argument("--run-timeout", type=int, default=DEFAULT_RUN_TIMEOUT)
    p_run.add_argument("--keep-containers", action="store_true")
    p_run.set_defaults(func=cmd_run)

    p_fetch = sub.add_parser(
        "fetch-artifacts",
        help="Fetch receiver artifacts and logs for an existing run directory.",
    )
    p_fetch.add_argument("--run-dir", type=Path, required=True)
    p_fetch.set_defaults(func=cmd_fetch_artifacts)

    p_down = sub.add_parser(
        "down",
        help="Stop containers for an existing run directory.",
    )
    p_down.add_argument("--run-dir", type=Path, required=True)
    p_down.set_defaults(func=cmd_down)

    p_clean = sub.add_parser(
        "clean-generated",
        help="Remove local generated run directories under examples/fec/generated.",
    )
    clean_group = p_clean.add_mutually_exclusive_group(required=True)
    clean_group.add_argument("--run-dir", type=Path)
    clean_group.add_argument("--all", action="store_true")
    p_clean.set_defaults(func=cmd_clean_generated)

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    try:
        return args.func(args)
    except subprocess.CalledProcessError as exc:
        print(f"Command failed: {' '.join(exc.cmd)}", file=sys.stderr)
        if exc.stdout:
            print(exc.stdout, file=sys.stderr)
        if exc.stderr:
            print(exc.stderr, file=sys.stderr)
        return exc.returncode or 1
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
