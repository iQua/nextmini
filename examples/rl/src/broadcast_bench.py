import argparse
import os
import json
import pickle
import random
import time
from dataclasses import dataclass
from pathlib import Path

from . import config

try:
    import nextmini_py as nm
except ImportError as exc:
    raise SystemExit(
        "nextmini_py is not installed. Build the wheel with:\n"
        "  maturin build --release -m python-api/Cargo.toml\n"
        "  pip install target/wheels/nextmini_py-*.whl"
    ) from exc


def parse_node_ids(value: str) -> list[int]:
    value = value.strip()
    if not value:
        return []
    return [int(part.strip()) for part in value.split(",") if part.strip()]


def maybe_create_sparse_file(path: Path, size_bytes: int) -> None:
    if size_bytes <= 0:
        raise ValueError("size_bytes must be positive")
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("wb") as f:
        f.truncate(size_bytes)


def now_ms() -> int:
    return int(time.time() * 1000)


def send_unicast(dataplane: "nm.Dataplane", *, dst_node_id: int, src_port: int, dst_port: int, msg: dict) -> None:
    payload = pickle.dumps(msg, protocol=pickle.HIGHEST_PROTOCOL)
    view = nm.PacketView(payload)
    dataplane.send_to_node(
        dst_node_id=dst_node_id,
        frozen=view,
        src_port=src_port,
        dst_port=dst_port,
    )


def recv_unicast(receiver: "nm.PacketReceiver", *, timeout_ms: int) -> dict | None:
    delivery = receiver.recv(timeout_ms=timeout_ms)
    if delivery is None:
        return None
    return pickle.loads(delivery.payload)


def select_by_capacity(graph: "Graph", relays: list[int], k: int) -> list[int]:
    """Select top-k relays by sum of outgoing capacities (capacity heuristic)."""
    if k <= 0 or not relays:
        return []
    scored: list[tuple[int, float]] = []
    for relay_id in relays:
        score = sum(float(cap) for _, cap in graph.adj.get(relay_id, []))
        scored.append((relay_id, score))
    scored.sort(key=lambda x: x[1], reverse=True)
    return [relay_id for relay_id, _ in scored[: min(k, len(relays))]]


def select_random(relays: list[int], k: int, seed: int) -> list[int]:
    """Randomly select k relays with a fixed seed."""
    if k <= 0 or not relays:
        return []
    rng = random.Random(int(seed))
    return rng.sample(relays, min(k, len(relays)))


@dataclass(frozen=True)
class WorkerConn:
    rank: int
    node_id: int
    port: int
    receiver: "nm.PacketReceiver"


def compute_routes(
    *,
    controller_config_path: Path,
    src_node_id: int,
    receiver_node_ids: list[int],
    algorithm: str,
    hop_limit: int,
    eta: float,
    max_relays: int | None,
    relay_scoring: str,
    num_paths: int,
    probe_links: bool,
    probe_bytes: int,
    probe_timeout_secs: float,
    relay_selection: str,
    selection_seed: int,
) -> tuple[list[tuple[int, int]], float | None, str, list[int] | None]:
    try:
        from examples.lp.solver import build_graph_from_controller_config, compute_tree_edges, load_toml
    except ImportError as exc:
        raise RuntimeError(
            "LP solver unavailable. Ensure examples/lp dependencies are installed."
        ) from exc

    graph = build_graph_from_controller_config(str(controller_config_path))
    relay_pool = [
        int(node_id)
        for node_id in graph.nodes
        if int(node_id) != int(src_node_id) and int(node_id) not in set(receiver_node_ids)
    ]
    if probe_links:
        try:
            from examples.lp.main import (
                _apply_link_rates,
                _connect_db,
                _db_settings,
                _fetch_link_rates_from_probes,
                _request_link_probes,
                _wait_for_probe_finish,
            )
        except ImportError as exc:
            raise RuntimeError("--probe-links requires examples.lp.main DB helpers.") from exc

        controller_cfg = load_toml(str(controller_config_path))
        settings = _db_settings(controller_cfg)
        conn = _connect_db(settings)
        try:
            probe_ids = _request_link_probes(conn, graph.edges, bytes_per_flow=probe_bytes)
            if probe_ids:
                ok = _wait_for_probe_finish(conn, probe_ids, timeout_secs=probe_timeout_secs)
                if not ok:
                    print(
                        f"warning: probe timed out after {probe_timeout_secs}s; using completed probes only",
                        flush=True,
                    )
                rates = _fetch_link_rates_from_probes(conn, probe_ids)
                _apply_link_rates(graph, rates)
                print(
                    f"probed {len(probe_ids)} links, updated {len(rates)} capacities",
                    flush=True,
                )
        finally:
            conn.close()

    selected_relays: list[int] | None = None
    relay_nodes = list(relay_pool)
    effective_max_relays = max_relays
    if relay_selection != "lp":
        if max_relays is None:
            raise RuntimeError(f"--relay-selection={relay_selection} requires --max-relays")
        if relay_selection == "capacity":
            selected_relays = select_by_capacity(graph, relay_pool, max_relays)
        elif relay_selection == "random":
            selected_relays = select_random(relay_pool, max_relays, seed=selection_seed)
        else:
            raise RuntimeError(f"unknown --relay-selection={relay_selection}")
        relay_nodes = list(selected_relays)
        effective_max_relays = None

    start = time.perf_counter()
    result = compute_tree_edges(
        graph,
        src=src_node_id,
        destinations=receiver_node_ids,
        algorithm=str(algorithm),
        hop_limit=hop_limit,
        eta=eta,
        max_relays=effective_max_relays,
        relay_scoring=relay_scoring,
        allow_destinations_as_relays=config.MULTICAST_ALLOW_WORKER_RELAYS,
        max_length=hop_limit,
        num_paths=num_paths,
        relay_nodes=relay_nodes,
    )
    plan_elapsed_ms = (time.perf_counter() - start) * 1000.0
    if not result.edges:
        details = result.error or "unknown planner failure"
        raise RuntimeError(
            f"Tree planner returned no edges for src={src_node_id} dests={receiver_node_ids}: {details}"
        )

    details = f"algo={result.algorithm} edges={len(result.edges)} plan_ms={plan_elapsed_ms:.2f} relay_selection={relay_selection}"
    if selected_relays is not None:
        details += f" selected_relays={selected_relays}"
    if result.throughput is not None:
        details += f" throughput={result.throughput:.3f}"
    if result.lp_f_star is not None:
        details += f" f_star={result.lp_f_star:.3f}"
    return result.edges, result.throughput, details, selected_relays


def run_trainer(args: argparse.Namespace) -> int:
    dp = nm.Dataplane(args.config)
    node_id = int(dp.node_id)

    worker_node_ids = parse_node_ids(args.worker_node_ids) if args.worker_node_ids else list(config.WORKER_NODE_IDS)
    if not worker_node_ids:
        raise SystemExit("Trainer requires --worker-node-ids or WORKER_NODE_IDS env var.")

    workers: list[WorkerConn] = []
    for rank, worker_node_id in enumerate(worker_node_ids):
        worker_port = config.WORKER_BASE_PORT + rank
        receiver = dp.register_receiver_from_node(
            src_node_id=worker_node_id,
            src_port=worker_port,
            dst_port=config.TRAINER_PORT,
        )
        workers.append(WorkerConn(rank=rank, node_id=worker_node_id, port=worker_port, receiver=receiver))

    print(f"trainer node_id={node_id} workers={worker_node_ids}", flush=True)
    if not dp.wait_for_topology_ready(timeout_ms=args.timeout_ms):
        raise TimeoutError("Topology not ready.")

    group_label = args.group_label or f"wan-broadcast-{int(time.time())}"
    dp.create_group(group_label)
    group = dp.group_is_ready(timeout_ms=args.timeout_ms)
    if group is None:
        raise TimeoutError("Timed out waiting for group creation.")
    group_id, group_ip, _ = group

    controller_cfg = Path(args.controller_config)
    if not controller_cfg.is_absolute():
        controller_cfg = (Path(__file__).resolve().parents[3] / controller_cfg).resolve()
    if not controller_cfg.is_file():
        raise RuntimeError(f"Controller config not found: {controller_cfg}")

    edges, plan_tput, plan_details, selected_relays = compute_routes(
        controller_config_path=controller_cfg,
        src_node_id=node_id,
        receiver_node_ids=worker_node_ids,
        algorithm=args.algorithm,
        hop_limit=args.hop_limit,
        eta=args.eta,
        max_relays=args.max_relays,
        relay_scoring=args.relay_scoring,
        num_paths=args.num_paths,
        probe_links=args.probe_links,
        probe_bytes=args.probe_bytes,
        probe_timeout_secs=args.probe_timeout_secs,
        relay_selection=args.relay_selection,
        selection_seed=args.selection_seed,
    )

    dp.set_group_routes(group_id, edges)
    if not dp.wait_for_group_routes(group_id, node_id, timeout_ms=args.timeout_ms):
        raise TimeoutError("Timed out waiting for group routes install.")

    file_path = Path(args.file)
    if args.generate_bytes:
        maybe_create_sparse_file(file_path, args.generate_bytes)
    if not file_path.exists():
        raise SystemExit(f"File not found: {file_path}")
    expected_bytes = file_path.stat().st_size
    if expected_bytes <= 0:
        raise SystemExit(f"File is empty: {file_path}")

    with file_path.open("rb") as fh:
        payload_bytes = fh.read()
    if len(payload_bytes) != expected_bytes:
        raise RuntimeError(
            f"Read {len(payload_bytes)} bytes but expected {expected_bytes} from {file_path}"
        )
    payload_view = nm.PacketView(payload_bytes)
    del payload_bytes

    print(f"group_id={group_id} group_ip={group_ip} {plan_details}", flush=True)
    if plan_tput is not None:
        print(f"planner_tput_est={plan_tput:.3f}", flush=True)

    # Handshake: wait for all workers to report readiness once, then proceed.
    pending_handshake = {w.rank for w in workers}
    deadline = time.monotonic() + (args.timeout_ms / 1000)
    while pending_handshake and time.monotonic() < deadline:
        for w in workers:
            if w.rank not in pending_handshake:
                continue
            msg = recv_unicast(w.receiver, timeout_ms=100)
            if not msg:
                continue
            if msg.get("type") != "HANDSHAKE":
                continue
            if msg.get("rank") != w.rank:
                print(f"warning: expected rank {w.rank}, got {msg}", flush=True)
            send_unicast(
                dp,
                dst_node_id=w.node_id,
                src_port=config.TRAINER_PORT,
                dst_port=w.port,
                msg={"type": "HANDSHAKE_ACK", "rank": w.rank},
            )
            pending_handshake.remove(w.rank)
    if pending_handshake:
        raise TimeoutError(f"Timed out waiting for HANDSHAKE from ranks {sorted(pending_handshake)}")

    print("all workers handshaked", flush=True)

    results: list[dict] = []
    for round_idx in range(args.rounds):
        # Phase 1: ask workers to register receivers + ack readiness.
        round_msg = {
            "type": "BROADCAST_ROUND",
            "round": round_idx,
            "group_id": group_id,
            "group_ip": group_ip,
            "src_node_id": node_id,
            "expected_bytes": expected_bytes,
            "file_name": file_path.name,
        }
        for w in workers:
            send_unicast(
                dp,
                dst_node_id=w.node_id,
                src_port=config.TRAINER_PORT,
                dst_port=w.port,
                msg=round_msg,
            )

        pending_ready = {w.rank for w in workers}
        last_sent = {w.rank: time.monotonic() for w in workers}
        resend_interval_s = 1.0
        deadline = time.monotonic() + (args.timeout_ms / 1000)
        while pending_ready and time.monotonic() < deadline:
            now = time.monotonic()
            for w in workers:
                if w.rank not in pending_ready:
                    continue
                if now - last_sent[w.rank] < resend_interval_s:
                    continue
                send_unicast(
                    dp,
                    dst_node_id=w.node_id,
                    src_port=config.TRAINER_PORT,
                    dst_port=w.port,
                    msg=round_msg,
                )
                last_sent[w.rank] = now
            for w in workers:
                if w.rank not in pending_ready:
                    continue
                msg = recv_unicast(w.receiver, timeout_ms=50)
                if not msg:
                    continue
                if msg.get("type") != "READY":
                    continue
                if msg.get("round") != round_idx:
                    continue
                pending_ready.remove(w.rank)
        if pending_ready:
            raise TimeoutError(f"Timed out waiting for READY from ranks {sorted(pending_ready)} (round={round_idx})")

        # Phase 2: start multicast transfer.
        start = time.perf_counter()
        sid = dp.send_data(
            group_id,
            group_ip,
            worker_node_ids,
            payload_view,
            chunk_size=args.chunk_size,
            src_port=config.TRAINER_PORT,
            dst_port=config.WORKER_BASE_PORT,
        )

        # Phase 3: wait for DONE from workers, which defines broadcast completion.
        sender_ok = False
        done_msgs: dict[int, dict] = {}
        pending_done = {w.rank for w in workers}
        try:
            deadline = time.monotonic() + (args.timeout_ms / 1000)
            while pending_done and time.monotonic() < deadline:
                for w in workers:
                    if w.rank not in pending_done:
                        continue
                    msg = recv_unicast(w.receiver, timeout_ms=50)
                    if not msg:
                        continue
                    if msg.get("type") != "DONE":
                        continue
                    if msg.get("round") != round_idx:
                        continue
                    done_msgs[w.rank] = msg
                    pending_done.remove(w.rank)
            if pending_done:
                raise TimeoutError(
                    f"Timed out waiting for DONE from ranks {sorted(pending_done)} (round={round_idx})"
                )
        finally:
            # Always attempt to stop the sender session to avoid leaking tasks.
            try:
                sender_ok = bool(dp.lossless_wait(sid, timeout_ms=0))
            except Exception as exc:
                print(f"warning: sender lossless_wait failed: {exc}", flush=True)

        elapsed = time.perf_counter() - start
        goodput_mib_s = (expected_bytes / (1024 * 1024)) / elapsed if elapsed > 0 else 0.0
        goodput_gbps = (expected_bytes * 8 / 1e9) / elapsed if elapsed > 0 else 0.0

        worker_ok = all(bool(done_msgs.get(w.rank, {}).get("ok", False)) for w in workers)
        worker_bytes = [int(done_msgs.get(w.rank, {}).get("bytes", -1)) for w in workers]
        bytes_min = min(worker_bytes) if worker_bytes else 0
        bytes_max = max(worker_bytes) if worker_bytes else 0
        bytes_ok = all(b == expected_bytes for b in worker_bytes)
        ok = bool(bytes_ok)

        record = {
            "ts_ms": now_ms(),
            "round": round_idx,
            "algorithm": args.algorithm,
            "relay_selection": args.relay_selection,
            "selection_seed": args.selection_seed,
            "selected_relays": selected_relays,
            "hop_limit": args.hop_limit,
            "eta": args.eta,
            "chunk_size": args.chunk_size,
            "bytes": expected_bytes,
            "ok": ok,
            "sender_ok": sender_ok,
            "worker_ok": worker_ok,
            "bytes_min": bytes_min,
            "bytes_max": bytes_max,
            "elapsed_s": elapsed,
            "goodput_mib_s": goodput_mib_s,
            "goodput_gbps": goodput_gbps,
            "planner_tput_est": plan_tput,
        }
        results.append(record)
        print(
            f"round={round_idx} ok={ok} elapsed_s={elapsed:.3f} goodput={goodput_gbps:.3f}Gbps",
            flush=True,
        )

    if args.output_json:
        out = Path(args.output_json)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(results, indent=2))
        print(f"wrote results to {out}", flush=True)

    return 0


def run_worker(args: argparse.Namespace) -> int:
    dp = nm.Dataplane(args.config)
    local_node_id = int(dp.node_id)
    trainer_node_id = int(args.trainer_node_id)
    rank = int(args.rank)
    local_port = config.WORKER_BASE_PORT + rank

    receiver = dp.register_receiver_from_node(
        src_node_id=trainer_node_id,
        src_port=config.TRAINER_PORT,
        dst_port=local_port,
    )

    if not dp.wait_for_topology_ready(timeout_ms=args.timeout_ms):
        raise TimeoutError("Topology not ready.")

    send_unicast(
        dp,
        dst_node_id=trainer_node_id,
        src_port=local_port,
        dst_port=config.TRAINER_PORT,
        msg={"type": "HANDSHAKE", "rank": rank, "node_id": local_node_id},
    )

    deadline = time.monotonic() + (args.timeout_ms / 1000)
    while time.monotonic() < deadline:
        msg = recv_unicast(receiver, timeout_ms=200)
        if not msg:
            continue
        if msg.get("type") == "HANDSHAKE_ACK" and msg.get("rank") == rank:
            break
    else:
        raise TimeoutError("Timed out waiting for HANDSHAKE_ACK.")

    out_dir = Path(args.sink_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    joined_group_id: int | None = None
    rounds_seen = 0
    while rounds_seen < args.rounds:
        msg = recv_unicast(receiver, timeout_ms=args.timeout_ms)
        if not msg:
            continue
        if msg.get("type") != "BROADCAST_ROUND":
            continue
        round_idx = int(msg.get("round", -1))
        if round_idx != rounds_seen:
            continue
        group_id = int(msg["group_id"])
        group_ip = str(msg["group_ip"])
        src_node_id = int(msg["src_node_id"])
        expected_bytes = int(msg["expected_bytes"])
        name = str(msg.get("file_name", f"artifact-{round_idx}.bin"))

        if joined_group_id != group_id:
            dp.join_group(group_id)
            if not dp.wait_for_group_routes(group_id, src_node_id, min_routes=1, timeout_ms=args.timeout_ms):
                raise TimeoutError(
                    f"Timed out waiting for group routes (gid={group_id}, src={src_node_id}) on node {local_node_id}"
                )
            joined_group_id = group_id

        if args.keep_files:
            sink_path = out_dir / f"recv-node{local_node_id}-rank{rank}-round{round_idx}-{name}"
        else:
            sink_path = out_dir / f"recv-node{local_node_id}-rank{rank}-{name}"
            if sink_path.exists():
                sink_path.unlink()

        sid = dp.receive_data(
            group_id,
            group_ip,
            src_node_id,
            expected_bytes=expected_bytes,
            chunk_size=args.chunk_size,
            src_port=config.TRAINER_PORT,
            dst_port=config.WORKER_BASE_PORT,
        )

        send_unicast(
            dp,
            dst_node_id=trainer_node_id,
            src_port=local_port,
            dst_port=config.TRAINER_PORT,
            msg={"type": "READY", "round": round_idx},
        )

        ok = dp.lossless_wait(sid, timeout_ms=args.timeout_ms)
        view = dp.get_data_buffer(sid)
        sink_path.parent.mkdir(parents=True, exist_ok=True)
        with sink_path.open("wb") as fh:
            fh.write(view)
        received_bytes = sink_path.stat().st_size if sink_path.exists() else 0
        send_unicast(
            dp,
            dst_node_id=trainer_node_id,
            src_port=local_port,
            dst_port=config.TRAINER_PORT,
            msg={"type": "DONE", "round": round_idx, "ok": bool(ok), "bytes": received_bytes},
        )
        if not args.keep_files and sink_path.exists():
            sink_path.unlink()
        rounds_seen += 1

    if getattr(args, "hold_after_rounds", False):
        print("worker completed rounds; holding until container exit", flush=True)
        while True:
            time.sleep(3600)

    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="WAN multicast broadcast microbenchmark")
    parser.add_argument("--role", choices=("trainer", "worker"), required=True)
    parser.add_argument("--config", required=True, help="Nextmini node config TOML path")
    parser.add_argument("--controller-config", default=config.CONTROLLER_CONFIG)
    parser.add_argument("--timeout-ms", type=int, default=180_000)
    parser.add_argument("--chunk-size", type=int, default=config.CHUNK_SIZE)
    parser.add_argument("--rounds", type=int, default=3)

    # Trainer args
    parser.add_argument("--worker-node-ids", default=os.environ.get("WORKER_NODE_IDS", ""))
    parser.add_argument("--group-label", default=os.environ.get("GROUP_LABEL", ""))
    parser.add_argument("--file", default=os.environ.get("BROADCAST_FILE", "artifacts/broadcast.bin"))
    parser.add_argument("--generate-bytes", type=int, default=0)
    parser.add_argument("--output-json", default=os.environ.get("BROADCAST_OUT", ""))

    # Planner knobs
    parser.add_argument("--algorithm", default=os.environ.get("BROADCAST_ALGO", config.MULTICAST_TREE_ALGO))
    parser.add_argument("--hop-limit", type=int, default=config.MULTICAST_HOP_LIMIT)
    parser.add_argument("--eta", type=float, default=config.MULTICAST_ETA)
    parser.add_argument("--num-paths", type=int, default=config.MULTICAST_NUM_PATHS)
    parser.add_argument("--relay-scoring", default=config.MULTICAST_RELAY_SCORING)
    parser.add_argument("--max-relays", type=int, default=-1)
    parser.add_argument(
        "--relay-selection",
        choices=("lp", "capacity", "random"),
        default=os.environ.get("RELAY_SELECTION", "lp"),
        help="Relay selection method when --max-relays is set (lp = LP-guided).",
    )
    parser.add_argument(
        "--selection-seed",
        type=int,
        default=int(os.environ.get("RELAY_SELECTION_SEED", "0")),
        help="Seed used for random relay selection.",
    )
    parser.add_argument(
        "--probe-links",
        action="store_true",
        help="Insert DB probe flows and overwrite link capacities before planning.",
    )
    parser.add_argument(
        "--probe-bytes",
        type=int,
        default=config.MULTICAST_PROBE_BYTES,
        help="Bytes to send per probe flow (larger keeps the link busy longer).",
    )
    parser.add_argument(
        "--probe-timeout-secs",
        type=float,
        default=config.MULTICAST_PROBE_TIMEOUT_SECS,
        help="Timeout in seconds for probe flows to finish.",
    )

    # Worker args
    parser.add_argument("--trainer-node-id", default=os.environ.get("TRAINER_NODE_ID", str(config.TRAINER_NODE_ID)))
    parser.add_argument("--rank", type=int, default=int(os.environ.get("RANK", "0")))
    parser.add_argument("--sink-dir", default=os.environ.get("SINK_DIR", "artifacts/received"))
    parser.add_argument("--keep-files", action=argparse.BooleanOptionalAction, default=False)
    parser.add_argument(
        "--hold-after-rounds",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Keep the worker process alive after completing all rounds (useful for docker compose).",
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()
    args.max_relays = None if args.max_relays < 0 else args.max_relays
    args.generate_bytes = int(args.generate_bytes or 0)
    if args.role == "trainer":
        return run_trainer(args)
    return run_worker(args)


if __name__ == "__main__":
    raise SystemExit(main())
