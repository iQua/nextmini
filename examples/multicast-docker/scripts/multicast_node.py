#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from collections import deque
from pathlib import Path
from typing import List, Tuple

try:
    import nextmini_py as nm
except ImportError as exc:  # pragma: no cover - surfaced at launch time
    raise SystemExit(
        "nextmini_py is not installed. Build the wheel with `maturin build`."
    ) from exc


METADATA_FILE = "tensor-metadata.json"
GROUP_INFO_FILE = "group-info.json"
READY_FILE_TEMPLATE = "receiver-ready-{}.json"


def atomic_write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f"{path.name}.tmp")
    tmp.write_text(json.dumps(payload))
    tmp.replace(path)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Lossless session demo")
    parser.add_argument("--role", choices=("source", "receiver", "router"), required=True)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument(
        "--controller-config",
        type=Path,
        default=None,
        help="Path to the controller config TOML (needed for multi-tree route computation).",
    )
    parser.add_argument("--group-label", required=True)
    parser.add_argument("--chunk-size", type=int, default=8500)
    parser.add_argument(
        "--fec",
        choices=("off", "on"),
        default=os.environ.get("FEC", "off").strip().lower(),
        help=(
            "Transfer mode hint for logs/artifacts; runtime config controls "
            "lossless FEC behavior."
        ),
    )
    parser.add_argument(
        "--receive-timeout-ms",
        type=int,
        default=int(os.environ.get("RECEIVE_TIMEOUT_MS", "5000")),
    )
    parser.add_argument(
        "--group-timeout", type=int, default=int(os.environ.get("GROUP_TIMEOUT", "90"))
    )
    parser.add_argument(
        "--payload-count",
        type=int,
        default=None,
        help="Compatibility shim; when provided, expected-bytes defaults to payload-count * chunk-size.",
    )
    parser.add_argument("--expected-bytes", type=int, default=None)
    parser.add_argument(
        "--source-node-id",
        type=int,
        default=int(os.environ.get("SOURCE_NODE_ID", "1")),
    )
    parser.add_argument("--node-id", type=int, default=None)
    parser.add_argument("--src-port", type=int, default=None)
    parser.add_argument("--dst-port", type=int, default=None)
    parser.add_argument(
        "--receiver-ids",
        type=str,
        default=os.environ.get("RECEIVER_IDS", ""),
        help="Comma-separated list of receiver node IDs (source role only).",
    )
    parser.add_argument(
        "--tensor-path",
        type=Path,
        default=None,
        help="Optional tensor path to stream; defaults to generated tensor.",
    )
    parser.add_argument("--generate-tensor", action="store_true")
    parser.add_argument(
        "--artifact-dir",
        type=Path,
        default=Path(os.environ.get("ARTIFACT_DIR", "/artifacts")),
    )
    parser.add_argument("--sink-path", type=Path, default=None)
    parser.add_argument("--quiet", action="store_true")
    return parser.parse_args()


def load_toml(path: Path) -> dict:
    """Load a TOML file, using tomllib (3.11+) or tomli fallback."""
    try:
        import tomllib
    except ModuleNotFoundError:
        import tomli as tomllib  # type: ignore[no-redef]
    return tomllib.loads(path.read_text())


def read_controller_topology_edges(controller_config: Path) -> List[Tuple[int, int]]:
    """Read undirected edges from the controller config."""
    cfg = load_toml(controller_config)
    topo = cfg.get("topology", {})
    raw_edges = topo.get("edges", [])
    return [(int(e[0]), int(e[1])) for e in raw_edges]


def read_fec_tree_ids(node_config: Path) -> List[int]:
    """Read fec_default_tree_ids from a node config."""
    cfg = load_toml(node_config)
    lrc = cfg.get("lossless_runtime_config", {})
    return [int(t) for t in lrc.get("fec_default_tree_ids", [0])]


def compute_shortest_path_tree_edges(
    undirected_edges: List[Tuple[int, int]],
    src: int,
    destinations: List[int],
    neighbor_order: str = "asc",
) -> List[Tuple[int, int]]:
    """BFS shortest-path multicast tree. neighbor_order controls tie-breaking."""
    adj: dict[int, list[int]] = {}
    for u, v in undirected_edges:
        adj.setdefault(u, []).append(v)
        adj.setdefault(v, []).append(u)

    reverse = neighbor_order == "desc"
    for node in adj:
        adj[node] = sorted(set(adj[node]), reverse=reverse)

    remaining = set(d for d in destinations if d != src)
    if not remaining:
        return []

    parents: dict[int, int | None] = {src: None}
    q: deque[int] = deque([src])

    while q and remaining:
        u = q.popleft()
        for v in adj.get(u, []):
            if v in parents:
                continue
            parents[v] = u
            remaining.discard(v)
            if not remaining:
                break
            q.append(v)

    if remaining:
        raise RuntimeError(f"Unreachable destinations from src={src}: {sorted(remaining)}")

    edges: set[Tuple[int, int]] = set()
    for d in destinations:
        if d == src:
            continue
        cur = d
        while cur != src:
            parent = parents[cur]
            edges.add((parent, cur))
            cur = parent
    return sorted(edges)


def parse_receiver_ids(value: str) -> List[int]:
    if not value:
        return []
    return [int(part.strip()) for part in value.split(",") if part.strip()]


def build_star_edges(source_node_id: int, receiver_ids: List[int]) -> List[Tuple[int, int]]:
    edges: List[Tuple[int, int]] = []
    for node_id in sorted(set(receiver_ids)):
        if node_id == source_node_id:
            continue
        edges.append((source_node_id, node_id))
    return edges


def log(message: str, quiet: bool = False) -> None:
    if quiet:
        return
    print(f"[{time.strftime('%H:%M:%S')}] {message}", flush=True)


def format_throughput(bytes_transferred: int, elapsed_seconds: float) -> str:
    if elapsed_seconds <= 0:
        return "N/A"

    bytes_per_sec = bytes_transferred / elapsed_seconds
    mbps = (bytes_per_sec * 8) / 1_000_000
    mib_per_sec = bytes_per_sec / (1024 * 1024)

    return f"{mib_per_sec:.2f} MiB/s ({mbps:.2f} Mbps)"


def metadata_path(args: argparse.Namespace) -> Path:
    return args.artifact_dir / METADATA_FILE


def write_tensor_metadata(
    args: argparse.Namespace, tensor_path: Path, size: int
) -> None:
    args.artifact_dir.mkdir(parents=True, exist_ok=True)
    payload = {"path": str(tensor_path), "bytes": size}
    metadata_path(args).write_text(json.dumps(payload))
    log(
        f"Recorded tensor metadata path={tensor_path} bytes={size} at {metadata_path(args)}",
        args.quiet,
    )


def load_tensor_metadata_if_needed(args: argparse.Namespace) -> None:
    if args.tensor_path is not None and args.expected_bytes is not None:
        return
    path = metadata_path(args)
    deadline = time.monotonic() + args.group_timeout
    while time.monotonic() < deadline:
        if path.exists():
            data = json.loads(path.read_text())
            if args.tensor_path is None:
                args.tensor_path = Path(data["path"])
            if args.expected_bytes is None:
                args.expected_bytes = int(data["bytes"])
            log(
                f"Loaded tensor metadata path={args.tensor_path} bytes={args.expected_bytes}",
                args.quiet,
            )
            return
        time.sleep(1)
    raise TimeoutError(f"Timed out waiting for tensor metadata at {path}.")


def group_info_path(args: argparse.Namespace) -> Path:
    return args.artifact_dir / GROUP_INFO_FILE


def receiver_ready_path(args: argparse.Namespace, node_id: int) -> Path:
    return args.artifact_dir / READY_FILE_TEMPLATE.format(node_id)


def write_receiver_ready(args: argparse.Namespace, node_id: int) -> None:
    payload = {"node_id": node_id, "ready_at": time.time()}
    atomic_write_json(receiver_ready_path(args, node_id), payload)


def wait_for_receivers_ready(args: argparse.Namespace, receiver_ids: List[int]) -> None:
    if not receiver_ids:
        return
    pending = set(receiver_ids)
    deadline = time.monotonic() + args.group_timeout
    while time.monotonic() < deadline:
        for node_id in list(pending):
            if receiver_ready_path(args, node_id).exists():
                pending.remove(node_id)
        if not pending:
            return
        time.sleep(1)
    raise TimeoutError(
        f"Timed out waiting for receiver readiness files: {sorted(pending)}"
    )


def write_group_info(
    args: argparse.Namespace,
    *,
    group_id: int,
    group_ip: str,
    receiver_ids: List[int],
) -> None:
    payload = {
        "label": args.group_label,
        "group_id": group_id,
        "group_ip": group_ip,
        "receiver_ids": receiver_ids,
        "source_node_id": args.source_node_id,
    }
    args.artifact_dir.mkdir(parents=True, exist_ok=True)
    atomic_write_json(group_info_path(args), payload)


def wait_for_group_info(args: argparse.Namespace, timeout: int) -> Tuple[int, str]:
    path = group_info_path(args)
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if path.exists():
            data = json.loads(path.read_text())
            return int(data["group_id"]), data["group_ip"]
        time.sleep(1)
    raise TimeoutError(f"Timed out waiting for group info file at {path}.")


def generate_tensor_if_needed(args: argparse.Namespace) -> None:
    if not args.generate_tensor:
        return

    import torch

    if args.tensor_path is None:
        tensor_dir = Path("/workspace/tensors")
        tensor_dir.mkdir(parents=True, exist_ok=True)
        args.tensor_path = tensor_dir / "tensor-auto-1g.pt"

    log(f"Generating ~1GB tensor at {args.tensor_path}...", args.quiet)
    torch.manual_seed(42)

    # Generate ~1GB tensor: 256 * 1024 * 1024 floats * 4 bytes/float ≈ 1GB
    tensor = torch.randn(256, 1024, 1024, dtype=torch.float32).contiguous().cpu()

    args.tensor_path.parent.mkdir(parents=True, exist_ok=True)
    torch.save(tensor, args.tensor_path)
    size = args.tensor_path.stat().st_size
    log(f"Generated tensor ({size} bytes).", args.quiet)
    args.expected_bytes = size
    write_tensor_metadata(args, args.tensor_path, size)


def run_source(args: argparse.Namespace) -> None:
    receiver_ids = parse_receiver_ids(args.receiver_ids)
    if not receiver_ids:
        raise SystemExit("Source role requires --receiver-ids=<id1,id2,...>.")

    dataplane = nm.Dataplane(str(args.config))
    source_node_id = dataplane.node_id

    if not dataplane.wait_for_topology_ready(timeout_ms=args.group_timeout * 1000):
        raise TimeoutError("Timed out waiting for topology readiness.")

    log(f"Creating multicast group '{args.group_label}'...", args.quiet)
    dataplane.create_group(args.group_label)
    group_id, group_ip, _ = dataplane.group_is_ready(
        timeout_ms=max(args.group_timeout, 1) * 1000
    )
    write_group_info(
        args,
        group_id=group_id,
        group_ip=group_ip,
        receiver_ids=receiver_ids,
    )
    log(f"Controller assigned group {group_id} ({group_ip}).", args.quiet)
    log(
        f"Receiver IDs={receiver_ids} block_size={args.chunk_size}",
        args.quiet,
    )
    log(
        f"Transfer mode hint={args.fec}; runtime config controls FEC behavior.",
        args.quiet,
    )

    tree_ids = read_fec_tree_ids(args.config) if args.controller_config else []

    if args.controller_config and len(tree_ids) > 1:
        # Multi-tree: compute per-tree shortest-path DAGs from controller topology.
        topo_edges = read_controller_topology_edges(args.controller_config)
        trees: list[tuple[int, list[tuple[int, int]]]] = []
        for tid in tree_ids:
            order = "asc" if tid % 2 == 0 else "desc"
            tedges = compute_shortest_path_tree_edges(
                topo_edges, src=source_node_id, destinations=receiver_ids,
                neighbor_order=order,
            )
            if not tedges:
                raise SystemExit(f"No shortest-path edges for tree_id={tid}; check topology.")
            trees.append((tid, tedges))
        log(f"Installing multicast trees={trees}", args.quiet)
        dataplane.set_group_routes_multi(group_id, trees)
    else:
        # Single-tree or no controller config: use star topology.
        edges = build_star_edges(source_node_id, receiver_ids)
        if not edges:
            raise SystemExit("No multicast DAG edges computed; check receiver IDs.")
        log(f"Installing multicast DAG edges={edges}", args.quiet)
        dataplane.set_group_routes(group_id, edges)

    if not dataplane.wait_for_group_routes(
        group_id,
        source_node_id,
        timeout_ms=args.group_timeout * 1000,
    ):
        raise TimeoutError("Timed out waiting for multicast routes to install.")

    log("Waiting for receivers to register receive sessions...", args.quiet)
    wait_for_receivers_ready(args, receiver_ids)
    log("All receivers are ready; starting send.", args.quiet)

    if args.tensor_path is None:
        raise SystemExit(
            "Source role requires a tensor file; set --tensor-path or --generate-tensor."
        )
    if not args.tensor_path.exists():
        raise SystemExit(f"Tensor file {args.tensor_path} does not exist")

    total_bytes = args.tensor_path.stat().st_size
    if total_bytes <= 0:
        raise SystemExit("Tensor file is empty; nothing to transmit.")
    args.expected_bytes = total_bytes
    write_tensor_metadata(args, args.tensor_path, total_bytes)

    log(f"Starting transmission of {total_bytes} bytes...", args.quiet)
    send_start_time = time.perf_counter()

    with args.tensor_path.open("rb") as fh:
        tensor_bytes = fh.read()
    builder = nm.PacketBuilder(size=total_bytes)
    builder.write(tensor_bytes)
    view = builder.freeze()

    sid = dataplane.send_data(
        group_id,
        group_ip,
        receiver_ids,
        view,
        block_size=args.chunk_size,
        src_port=args.src_port,
        dst_port=args.dst_port,
    )
    log(f"Started lossless send session (session ID = {sid}).", args.quiet)

    ok = dataplane.lossless_wait(sid, timeout_ms=args.group_timeout * 1000)
    send_end_time = time.perf_counter()
    elapsed = send_end_time - send_start_time

    log(f"Send completion: {ok}.", args.quiet)
    log(
        f"Transfer completed in {elapsed:.3f} seconds. Throughput: {format_throughput(total_bytes, elapsed)}",
        args.quiet,
    )


def run_receiver(args: argparse.Namespace) -> None:
    if args.node_id is None:
        raise SystemExit("Receiver role requires --node-id.")

    dataplane = nm.Dataplane(str(args.config))
    if not dataplane.wait_for_topology_ready(timeout_ms=args.group_timeout * 1000):
        raise TimeoutError("Timed out waiting for topology readiness.")

    load_tensor_metadata_if_needed(args)
    if args.expected_bytes is None and args.payload_count:
        args.expected_bytes = args.payload_count * args.chunk_size
    if args.expected_bytes is None or args.expected_bytes <= 0:
        raise SystemExit("expected-bytes must be known for lossless reception.")

    group_id, group_ip = wait_for_group_info(args, args.group_timeout)
    local_node_id = dataplane.node_id
    log(f"Joining multicast group id={group_id} ({group_ip})...", args.quiet)
    dataplane.join_group(group_id)
    log(
        f"Receiver transfer mode hint={args.fec}; runtime config controls FEC behavior.",
        args.quiet,
    )

    sink_path = args.sink_path
    if sink_path is None and args.artifact_dir:
        suffix = args.node_id if args.node_id is not None else "receiver"
        sink_path = args.artifact_dir / f"receiver-{suffix}.bin"

    log(f"Starting reception of {args.expected_bytes} bytes...", args.quiet)
    recv_start_time = time.perf_counter()

    sid = dataplane.receive_data(
        group_id,
        group_ip,
        args.source_node_id,
        expected_bytes=args.expected_bytes,
        block_size=args.chunk_size,
        src_port=args.src_port,
        dst_port=args.dst_port,
    )

    write_receiver_ready(args, local_node_id)
    log(f"Receiver ready file written for node {local_node_id}.", args.quiet)

    log(f"Started lossless receive session (session ID = {sid}).", args.quiet)

    payload_bytes: bytes | None = None

    ok = dataplane.lossless_wait(sid, timeout_ms=args.receive_timeout_ms)
    recv_end_time = time.perf_counter()
    elapsed = recv_end_time - recv_start_time

    log(f"Receive completion: {ok}.", args.quiet)
    log(
        f"Reception completed in {elapsed:.3f}s. Throughput: {format_throughput(args.expected_bytes, elapsed)}.",
        args.quiet,
    )

    view = dataplane.get_data_buffer(sid)
    payload_bytes = bytes(view.read())
    log(f"Retrieved {len(payload_bytes)} bytes into PacketView.", args.quiet)

    if payload_bytes is not None and sink_path is not None:
        sink_path.parent.mkdir(parents=True, exist_ok=True)
        sink_path.write_bytes(payload_bytes)
        log(f"Wrote payload to {sink_path}.", args.quiet)


def main() -> int:
    args = parse_args()
    if args.chunk_size <= 0:
        raise SystemExit("--chunk-size must be positive.")
    if args.role == "source" and args.tensor_path is None:
        args.generate_tensor = True
    if args.artifact_dir:
        args.artifact_dir.mkdir(parents=True, exist_ok=True)

    try:
        if args.role == "source":
            generate_tensor_if_needed(args)
            run_source(args)
        elif args.role == "receiver":
            run_receiver(args)
        else:
            dataplane = nm.Dataplane(str(args.config))
            local_node_id = dataplane.node_id
            log(f"Router node started (node_id={local_node_id}).", args.quiet)
            if not dataplane.wait_for_topology_ready(timeout_ms=args.group_timeout * 1000):
                raise TimeoutError("Timed out waiting for topology readiness.")
    except TimeoutError as exc:
        log(f"ERROR: {exc}", quiet=False)
        return 1
    except Exception as exc:  # pragma: no cover
        log(f"ERROR: {exc}", quiet=False)
        return 1

    log(
        "Task completed. Keeping container alive (Ctrl+C to exit)...",
        quiet=False,
    )
    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        log("Received interrupt signal, exiting.", quiet=False)
    return 0


if __name__ == "__main__":
    sys.exit(main())
