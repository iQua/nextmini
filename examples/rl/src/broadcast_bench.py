from __future__ import annotations

import argparse
import json
import os
import pickle
import random
import statistics
import time
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from . import config

if TYPE_CHECKING:
    from examples.lp.graph import Graph


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


def _fetch_per_edge_metrics(
    conn,
    start_time_ms: int,
    end_time_ms: int,
    tree_edges: list[tuple[int, int]],
    capacity_snapshot: list[dict],
) -> dict[str, object]:
    """Query metrics table for per-edge throughput during transmission.
    
    Compares actual transfer rates against probed capacities for each tree edge.
    """
    if not tree_edges:
        return {}
    
    # Convert capacity_snapshot to dict for easy lookup
    probed_caps: dict[tuple[int, int], float] = {}
    for item in capacity_snapshot:
        key = (int(item["src"]), int(item["dst"]))
        probed_caps[key] = float(item["capacity_mbps"])
    
    # Query metrics grouped by edge
    query = """
        SELECT local_node_id, remote_node_id, SUM(bytes) as total_bytes,
               MIN(time_read) as first_time, MAX(time_read) as last_time
        FROM metrics
        WHERE time_read >= to_timestamp(%s) AND time_read <= to_timestamp(%s)
        GROUP BY local_node_id, remote_node_id
    """
    
    per_edge_actual: dict[str, dict] = {}
    with conn.cursor() as cursor:
        cursor.execute(query, (start_time_ms / 1000.0, end_time_ms / 1000.0))
        rows = cursor.fetchall()
        
        for row in rows:
            src, dst, total_bytes, first_time, last_time = row
            edge_key = f"{src}->{dst}"
            
            # Compute duration from timestamps
            if first_time and last_time:
                if hasattr(first_time, 'timestamp'):
                    first_ts = first_time.timestamp()
                    last_ts = last_time.timestamp()
                else:
                    first_ts = float(first_time)
                    last_ts = float(last_time)
                duration_s = max(last_ts - first_ts, 0.001)
            else:
                duration_s = 1.0
            
            rate_mbps = (float(total_bytes) * 8.0 / 1_000_000.0) / duration_s
            
            per_edge_actual[edge_key] = {
                "bytes": int(total_bytes),
                "duration_s": round(duration_s, 3),
                "rate_mbps": round(rate_mbps, 2),
            }
    
    # Build comparison for tree edges only
    per_edge_comparison: dict[str, dict] = {}
    for src, dst in tree_edges:
        edge_key = f"{src}->{dst}"
        actual = per_edge_actual.get(edge_key, {})
        probed = probed_caps.get((int(src), int(dst)), 0.0)
        
        actual_mbps = actual.get("rate_mbps", 0.0)
        ratio = actual_mbps / probed if probed > 0 else 0.0
        
        per_edge_comparison[edge_key] = {
            "probed_mbps": round(probed, 2),
            "actual_mbps": round(actual_mbps, 2),
            "ratio": round(ratio, 3),
        }
    
    return {
        "per_edge_actual": per_edge_actual,
        "per_edge_comparison": per_edge_comparison,
        "metrics_rows_found": len(per_edge_actual),
    }

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
    probe_warmup_bytes: int,
    probe_batch_size: int,
    probe_timeout_secs: float,
    capacity_snapshot: str,
    ping_matrix: str,
    probe_retest_outliers: bool,
    probe_outlier_mbps: float,
    probe_outlier_median_frac: float,
    probe_outlier_asymmetry_frac: float,
    probe_retest_bytes: int,
    probe_retest_repeats: int,
    probe_retest_max_edges: int,
    relay_selection: str,
    selection_seed: int,
) -> tuple[
    list[tuple[int, int]],
    float | None,
    str,
    list[int] | None,
    list[int],
    dict[str, object],
]:
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
    ping_meta: dict[str, object] = {}
    ping_map: dict[tuple[int, int], dict[str, object]] = {}
    ping_path = ping_matrix.strip()
    if ping_path:
        ping_file = Path(ping_path)
        if not ping_file.is_absolute():
            ping_file = (Path(__file__).resolve().parents[3] / ping_file).resolve()
        if not ping_file.is_file():
            raise RuntimeError(f"ping matrix not found: {ping_file}")
        raw_ping = json.loads(ping_file.read_text(encoding="utf-8"))
        pings = raw_ping
        if isinstance(raw_ping, dict) and "pings" in raw_ping:
            pings = raw_ping.get("pings")
        if not isinstance(pings, list):
            raise RuntimeError("ping matrix must be a list of {src,dst,...} (or an object with a 'pings' list)")
        parsed = 0
        skipped = 0
        for entry in pings:
            if not isinstance(entry, dict):
                skipped += 1
                continue
            try:
                src = int(entry["src"])
                dst = int(entry["dst"])
            except Exception:
                skipped += 1
                continue
            ping_map[(src, dst)] = {
                "loss_pct": entry.get("loss_pct"),
                "rtt_avg_ms": entry.get("rtt_avg_ms"),
                "transmitted": entry.get("transmitted"),
                "received": entry.get("received"),
            }
            parsed += 1
        ping_meta = {
            "ping_matrix_loaded": str(ping_file),
            "ping_matrix_entries": int(parsed),
            "ping_matrix_skipped": int(skipped),
            "ping_matrix": raw_ping if isinstance(raw_ping, dict) else {"pings": pings},
        }

    probe_stats: dict[str, object] = {}
    snapshot_source: str | None = None
    snapshot_path = capacity_snapshot.strip()
    if snapshot_path:
        if probe_links:
            raise RuntimeError("--capacity-snapshot cannot be combined with --probe-links")
        snapshot_file = Path(snapshot_path)
        if not snapshot_file.is_file():
            raise RuntimeError(f"capacity snapshot not found: {snapshot_file}")
        raw = json.loads(snapshot_file.read_text(encoding="utf-8"))
        if not isinstance(raw, list):
            raise RuntimeError(
                "capacity snapshot must be a JSON list of {src,dst,capacity_mbps}"
            )
        updated = 0
        skipped = 0
        for entry in raw:
            if not isinstance(entry, dict):
                skipped += 1
                continue
            try:
                src = int(entry["src"])
                dst = int(entry["dst"])
                cap = float(entry["capacity_mbps"])
            except Exception:
                skipped += 1
                continue
            key = (src, dst)
            if key not in graph.capacities:
                skipped += 1
                continue
            graph.capacities[key] = cap
            updated += 1
        graph.adj = graph._build_adjacency()
        snapshot_source = f"file:{snapshot_file}"
        probe_stats = {
            "capacity_snapshot_loaded": str(snapshot_file),
            "capacity_snapshot_updates": int(updated),
            "capacity_snapshot_skipped": int(skipped),
        }
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
            batch_size = int(probe_batch_size) if int(probe_batch_size) > 0 else None

            # When batch_size is set, _request_link_probes enforces a node-disjoint
            # constraint within each batch (no shared src/dst nodes). That implies
            # a hard concurrency cap of floor(N/2) where N is the number of nodes
            # participating in the probed edge set.
            probe_nodes: set[int] = set()
            for u, v in graph.edges:
                probe_nodes.add(int(u))
                probe_nodes.add(int(v))
            disjoint_cap = max(1, len(probe_nodes) // 2)
            effective_batch_size = min(batch_size, disjoint_cap) if batch_size else None

            if batch_size:
                batch_info = (
                    f" (batch_size={batch_size}, effective={effective_batch_size}, "
                    f"disjoint_cap={disjoint_cap}, nodes={len(probe_nodes)})"
                )
            else:
                batch_info = " (all concurrent)"
            probe_start = time.perf_counter()
            warmup_bytes = int(probe_warmup_bytes) if int(probe_warmup_bytes) > 0 else 0
            if warmup_bytes > 0:
                if not effective_batch_size:
                    raise RuntimeError("--probe-warmup-bytes requires --probe-batch-size > 0 (node-disjoint batching)")
                batch_info += f" warmup={warmup_bytes}"
            print(
                f"probing {len(graph.edges)} links with {probe_bytes} bytes each{batch_info}...",
                flush=True,
            )
            probe_ids = _request_link_probes(
                conn,
                graph.edges,
                bytes_per_flow=probe_bytes,
                batch_size=effective_batch_size,
                batch_timeout_secs=probe_timeout_secs,
                warmup_bytes_per_flow=warmup_bytes if warmup_bytes > 0 else None,
            )
            rates: dict[tuple[int, int], float] = {}
            if probe_ids:
                ok = _wait_for_probe_finish(conn, probe_ids, timeout_secs=probe_timeout_secs)
                if not ok:
                    print(
                        f"warning: probe timed out after {probe_timeout_secs}s; using completed probes only",
                        flush=True,
                    )
                rates = _fetch_link_rates_from_probes(conn, probe_ids)
                _apply_link_rates(graph, rates)
                probe_stats = {
                    "probe_elapsed_s": (time.perf_counter() - probe_start),
                    "probe_ids": len(probe_ids),
                    "probe_rates": len(rates),
                    "probe_batch_size_configured": int(batch_size) if batch_size else None,
                    "probe_batch_size_effective": int(effective_batch_size) if effective_batch_size else None,
                    "probe_disjoint_cap": int(disjoint_cap),
                    "probe_nodes": int(len(probe_nodes)),
                    "probe_warmup_bytes": int(warmup_bytes),
                }

            if probe_retest_outliers and graph.edges:
                abs_thresh = float(probe_outlier_mbps)
                median_frac = float(probe_outlier_median_frac)
                asym_frac = float(probe_outlier_asymmetry_frac)
                reprobe_bytes = int(probe_retest_bytes) if int(probe_retest_bytes) > 0 else int(probe_bytes)
                repeats = int(probe_retest_repeats)
                max_edges = int(probe_retest_max_edges)

                out_by_src: dict[int, list[float]] = {}
                for (u, v) in graph.edges:
                    out_by_src.setdefault(int(u), []).append(float(graph.capacities.get((u, v), 0.0)))
                med_out: dict[int, float] = {}
                for src, caps in out_by_src.items():
                    med_out[int(src)] = float(statistics.median(caps)) if caps else 0.0

                candidates: list[tuple[float, int, int, list[str]]] = []
                for (u, v) in graph.edges:
                    cap_uv = float(graph.capacities.get((u, v), 0.0))
                    reasons: list[str] = []
                    if cap_uv <= abs_thresh:
                        reasons.append("abs")
                    med_src = med_out.get(int(u), 0.0)
                    if med_src > 0.0 and cap_uv <= (med_src * median_frac):
                        reasons.append("median_frac")
                    cap_vu = graph.capacities.get((v, u))
                    if cap_vu is not None and cap_uv <= (float(cap_vu) * asym_frac):
                        reasons.append("asymmetry")
                    if reasons:
                        candidates.append((cap_uv, int(u), int(v), reasons))

                candidates.sort(key=lambda x: x[0])
                to_reprobe = candidates[: max(0, max_edges)]
                if to_reprobe:
                    reprobe_start = time.perf_counter()
                    before_caps = {(u, v): float(graph.capacities.get((u, v), 0.0)) for _c, u, v, _r in to_reprobe}
                    edges_to_probe = [(u, v) for _c, u, v, _r in to_reprobe]
                    warmup_reprobe = int(warmup_bytes) if int(warmup_bytes) > 0 else None
                    reprobe_ids = _request_link_probes(
                        conn,
                        edges_to_probe,
                        bytes_per_flow=reprobe_bytes,
                        repeats=repeats,
                        batch_size=1,
                        batch_timeout_secs=probe_timeout_secs,
                        warmup_bytes_per_flow=warmup_reprobe,
                    )
                    ok = _wait_for_probe_finish(conn, reprobe_ids, timeout_secs=probe_timeout_secs)
                    if not ok:
                        print(
                            f"warning: outlier re-probe timed out after {probe_timeout_secs}s; using completed probes only",
                            flush=True,
                        )
                    reprobe_rates = _fetch_link_rates_from_probes(conn, reprobe_ids)
                    _apply_link_rates(graph, reprobe_rates)
                    per_edge: list[dict[str, object]] = []
                    for cap_uv, u, v, reasons in to_reprobe:
                        ping = ping_map.get((int(u), int(v)), {})
                        after = float(graph.capacities.get((u, v), 0.0))
                        per_edge.append(
                            {
                                "src": int(u),
                                "dst": int(v),
                                "reasons": reasons,
                                "before_mbps": float(before_caps.get((u, v), cap_uv)),
                                "after_mbps": float(after),
                                "ping_loss_pct": ping.get("loss_pct"),
                                "ping_rtt_avg_ms": ping.get("rtt_avg_ms"),
                            }
                        )
                    probe_stats["outlier_reprobe"] = {
                        "enabled": True,
                        "threshold_abs_mbps": abs_thresh,
                        "threshold_median_frac": median_frac,
                        "threshold_asymmetry_frac": asym_frac,
                        "bytes_per_flow": int(reprobe_bytes),
                        "repeats": int(repeats),
                        "max_edges": int(max_edges),
                        "candidates": int(len(candidates)),
                        "retested": int(len(to_reprobe)),
                        "probe_ids": int(len(reprobe_ids)),
                        "probe_rates": int(len(reprobe_rates)),
                        "elapsed_s": (time.perf_counter() - reprobe_start),
                        "edges": per_edge,
                    }
                else:
                    probe_stats["outlier_reprobe"] = {
                        "enabled": True,
                        "threshold_abs_mbps": abs_thresh,
                        "threshold_median_frac": median_frac,
                        "threshold_asymmetry_frac": asym_frac,
                        "bytes_per_flow": int(reprobe_bytes),
                        "repeats": int(repeats),
                        "max_edges": int(max_edges),
                        "candidates": int(len(candidates)),
                        "retested": 0,
                    }
            print(
                f"probed {len(probe_ids)} links, updated {len(rates)} capacities",
                flush=True,
            )
        finally:
            conn.close()
        snapshot_source = "probed"

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

    capacity_snapshot = [
        {
            "src": int(src),
            "dst": int(dst),
            "capacity_mbps": float(graph.capacities.get((src, dst), 0.0)),
        }
        for (src, dst) in graph.edges
    ]
    meta: dict[str, object] = {
        "planner_algorithm": result.algorithm,
        "planner_time_ms": plan_elapsed_ms,
        "planner_tput_est": result.throughput,
        "planner_tput_est_mbps": result.throughput,
        "planner_lp_f_star": result.lp_f_star,
        "tree_edges": result.edges,
        "capacity_snapshot": capacity_snapshot,
        "probe_stats": probe_stats,
        "ping_stats": ping_meta,
        "capacity_snapshot_source": snapshot_source,
    }
    # Return relay_pool so the caller can attribute which "non-terminal relays"
    # (i.e., nodes that are neither src nor destinations) were actually used by the tree.
    return result.edges, result.throughput, details, selected_relays, relay_pool, meta


def _tree_depths(edges: list[tuple[int, int]], *, src: int) -> dict[int, int]:
    children: dict[int, list[int]] = {}
    for u, v in edges:
        children.setdefault(int(u), []).append(int(v))

    depths: dict[int, int] = {int(src): 0}
    queue: list[int] = [int(src)]
    while queue:
        u = queue.pop(0)
        for v in children.get(u, []):
            if v in depths:
                continue
            depths[v] = depths[u] + 1
            queue.append(v)
    return depths


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

    # Handshake: make sure trainer->worker unicast works before the trainer does
    # any long-running planning/probing work (otherwise workers can time out and exit).
    pending_handshake = {w.rank for w in workers}  # waiting for HANDSHAKE
    pending_acked = {w.rank for w in workers}  # waiting for HANDSHAKE_ACKED
    last_ack_sent_s: dict[int, float] = {}

    deadline = time.monotonic() + (args.timeout_ms / 1000)
    while pending_acked and time.monotonic() < deadline:
        now = time.monotonic()
        for w in workers:
            # Drain control messages from worker -> trainer.
            msg = recv_unicast(w.receiver, timeout_ms=50)
            if msg:
                if msg.get("type") == "HANDSHAKE" and msg.get("rank") == w.rank:
                    pending_handshake.discard(w.rank)
                elif msg.get("type") == "HANDSHAKE_ACKED" and msg.get("rank") == w.rank:
                    pending_acked.discard(w.rank)

            # Once we've seen HANDSHAKE from a worker, keep re-sending ACK until
            # we observe HANDSHAKE_ACKED back.
            if w.rank in pending_acked and w.rank not in pending_handshake:
                last = last_ack_sent_s.get(w.rank, 0.0)
                if (now - last) >= 0.5:
                    send_unicast(
                        dp,
                        dst_node_id=w.node_id,
                        src_port=config.TRAINER_PORT,
                        dst_port=w.port,
                        msg={"type": "HANDSHAKE_ACK", "rank": w.rank},
                    )
                    last_ack_sent_s[w.rank] = now

    if pending_handshake:
        raise TimeoutError(
            f"Timed out waiting for HANDSHAKE from ranks {sorted(pending_handshake)}"
        )
    if pending_acked:
        raise TimeoutError(
            f"Timed out waiting for HANDSHAKE_ACKED from ranks {sorted(pending_acked)}"
        )

    print("all workers handshaked (two-way)", flush=True)

    controller_cfg = Path(args.controller_config)
    if not controller_cfg.is_absolute():
        controller_cfg = (Path(__file__).resolve().parents[3] / controller_cfg).resolve()
    if not controller_cfg.is_file():
        raise RuntimeError(f"Controller config not found: {controller_cfg}")

    edges, plan_tput, plan_details, selected_relays, relay_pool, plan_meta = compute_routes(
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
        probe_warmup_bytes=int(getattr(args, "probe_warmup_bytes", 0)),
        probe_batch_size=args.probe_batch_size,
        probe_timeout_secs=args.probe_timeout_secs,
        capacity_snapshot=str(getattr(args, "capacity_snapshot", "") or ""),
        ping_matrix=str(getattr(args, "ping_matrix", "") or ""),
        probe_retest_outliers=bool(getattr(args, "probe_retest_outliers", True)),
        probe_outlier_mbps=float(getattr(args, "probe_outlier_mbps", 10.0)),
        probe_outlier_median_frac=float(getattr(args, "probe_outlier_median_frac", 0.1)),
        probe_outlier_asymmetry_frac=float(getattr(args, "probe_outlier_asymmetry_frac", 0.1)),
        probe_retest_bytes=int(getattr(args, "probe_retest_bytes", 0)),
        probe_retest_repeats=int(getattr(args, "probe_retest_repeats", 3)),
        probe_retest_max_edges=int(getattr(args, "probe_retest_max_edges", 8)),
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

    print(f"group_id={group_id} group_ip={group_ip} {plan_details}", flush=True)
    if plan_tput is not None:
        print(f"planner_tput_est_mbps={plan_tput:.3f}", flush=True)

    if args.output_json:
        out = Path(args.output_json)
        meta_out = out.with_suffix(".meta.json")
        depths = _tree_depths(edges, src=node_id)

        edge_senders = {int(u) for (u, _v) in edges}
        used_nonterminal_relays = [rid for rid in relay_pool if rid in edge_senders]
        used_worker_forwarders = [wid for wid in worker_node_ids if wid in edge_senders]

        meta = dict(plan_meta or {})
        meta.update(
            {
                "ts_ms": now_ms(),
                "src_node_id": node_id,
                "worker_node_ids": worker_node_ids,
                "group_id": group_id,
                "group_ip": group_ip,
                "relay_pool": relay_pool,
                "selected_relays": selected_relays,
                "used_nonterminal_relays": used_nonterminal_relays,
                "used_worker_forwarders": used_worker_forwarders,
                "tree_depths": {str(k): int(v) for k, v in depths.items()},
                "tree_max_depth": max(depths.values()) if depths else 0,
            }
        )
        meta_out.parent.mkdir(parents=True, exist_ok=True)
        meta_out.write_text(json.dumps(meta, indent=2), encoding="utf-8")
        print(f"wrote plan metadata to {meta_out}", flush=True)

    # Derived tree attribution (useful for debugging + paper figures).
    #
    # - nonterminal relays: nodes that are neither src nor destinations
    #   (usually your dedicated relay pool, e.g. node 9 in the WAN inventory).
    # - worker forwarders: destination nodes that appear as senders in the tree
    #   (only possible when MULTICAST_ALLOW_WORKER_RELAYS=true).
    edge_nodes: set[int] = set()
    edge_senders: set[int] = set()
    for u, v in edges:
        edge_nodes.add(int(u))
        edge_nodes.add(int(v))
        edge_senders.add(int(u))

    relay_pool_set = {int(r) for r in relay_pool}
    worker_set = {int(w) for w in worker_node_ids}
    nonterminal_relays_used = sorted(edge_nodes & relay_pool_set)
    worker_forwarders_used = sorted(edge_senders & worker_set)

    # NOTE:
    # - `selected_relays` is only meaningful when relay_selection != "lp" (capacity/random),
    #   where we explicitly pre-select a relay subset before calling the planner.
    # - For relay_selection="lp", the planner may use any relays within its allowed set,
    #   and the concrete outcome is best represented by the planned tree itself.
    #   Use `tree_edges` / `tree_nonterminal_relays_used` / `tree_worker_forwarders_used`.

    results: list[dict] = []
    all_rounds_start_ms = now_ms()  # Record start for per-edge metrics
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
        sid = dp.send_file(
            group_id,
            group_ip,
            worker_node_ids,
            str(file_path),
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
            # Explicit relay selection only (capacity/random). For lp, prefer the
            # tree_* fields for the realized topology.
            "selected_relays": selected_relays,
            # Always record the planned tree itself so we can inspect which edges/nodes
            # were chosen and reproduce figures.
            "tree_edges": edges,
            "tree_nonterminal_relays_used": nonterminal_relays_used,
            "tree_worker_forwarders_used": worker_forwarders_used,
            "allow_worker_relays": config.MULTICAST_ALLOW_WORKER_RELAYS,
            "allow_worker_relays_env": os.environ.get("MULTICAST_ALLOW_WORKER_RELAYS", ""),
            "hop_limit": args.hop_limit,
            "eta": args.eta,
            "probe_links": bool(args.probe_links),
            "probe_bytes": int(args.probe_bytes),
            "probe_warmup_bytes": int(getattr(args, "probe_warmup_bytes", 0)),
            "probe_batch_size": int(args.probe_batch_size),
            "probe_timeout_secs": float(args.probe_timeout_secs),
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

        # Query per-edge metrics and update metadata file
        all_rounds_end_ms = now_ms()
        if args.probe_links and plan_meta.get("capacity_snapshot"):
            try:
                from examples.lp.main import _connect_db, _db_settings
                from examples.lp.solver import load_toml
                
                controller_cfg_data = load_toml(str(controller_cfg))
                settings = _db_settings(controller_cfg_data)
                conn = _connect_db(settings)
                try:
                    per_edge_stats = _fetch_per_edge_metrics(
                        conn,
                        all_rounds_start_ms,
                        all_rounds_end_ms,
                        edges,
                        plan_meta["capacity_snapshot"],
                    )
                    if per_edge_stats:
                        meta_out = out.with_suffix(".meta.json")
                        if meta_out.exists():
                            meta = json.loads(meta_out.read_text(encoding="utf-8"))
                            meta["per_edge_stats"] = per_edge_stats
                            meta["per_edge_time_range_ms"] = [all_rounds_start_ms, all_rounds_end_ms]
                            meta_out.write_text(json.dumps(meta, indent=2), encoding="utf-8")
                            print(f"updated {meta_out} with per-edge stats", flush=True)
                finally:
                    conn.close()
            except Exception as exc:
                print(f"warning: failed to fetch per-edge metrics: {exc}", flush=True)

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

    # Worker->trainer handshake: keep sending until we see HANDSHAKE_ACK.
    # (WAN control messages can occasionally drop; this prevents brittle startup.)
    deadline = time.monotonic() + (args.timeout_ms / 1000)
    last_handshake_s = 0.0
    while time.monotonic() < deadline:
        now = time.monotonic()
        if (now - last_handshake_s) >= 1.0:
            send_unicast(
                dp,
                dst_node_id=trainer_node_id,
                src_port=local_port,
                dst_port=config.TRAINER_PORT,
                msg={"type": "HANDSHAKE", "rank": rank, "node_id": local_node_id},
            )
            last_handshake_s = now

        msg = recv_unicast(receiver, timeout_ms=200)
        if not msg:
            continue
        if msg.get("type") == "HANDSHAKE_ACK" and msg.get("rank") == rank:
            send_unicast(
                dp,
                dst_node_id=trainer_node_id,
                src_port=local_port,
                dst_port=config.TRAINER_PORT,
                msg={"type": "HANDSHAKE_ACKED", "rank": rank},
            )
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
            sink_path = None

        if sink_path is None:
            sid = dp.receive_discard(
                group_id,
                group_ip,
                src_node_id,
                expected_bytes,
                chunk_size=args.chunk_size,
                src_port=config.TRAINER_PORT,
                dst_port=config.WORKER_BASE_PORT,
            )
        else:
            sid = dp.receive_to_file(
                group_id,
                group_ip,
                src_node_id,
                expected_bytes,
                str(sink_path),
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
        received_bytes = expected_bytes if ok else 0
        if sink_path is not None:
            received_bytes = sink_path.stat().st_size if sink_path.exists() else 0
            ok = bool(ok and received_bytes == expected_bytes)
        send_unicast(
            dp,
            dst_node_id=trainer_node_id,
            src_port=local_port,
            dst_port=config.TRAINER_PORT,
            msg={"type": "DONE", "round": round_idx, "ok": bool(ok), "bytes": received_bytes},
        )
        if sink_path is not None and (not args.keep_files) and sink_path.exists():
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
        "--probe-warmup-bytes",
        type=int,
        default=0,
        help="Optional warmup bytes per probe flow (per batch). If >0, run warmup probes first and exclude them from capacity estimation.",
    )
    parser.add_argument(
        "--probe-timeout-secs",
        type=float,
        default=config.MULTICAST_PROBE_TIMEOUT_SECS,
        help="Timeout in seconds for probe flows to finish.",
    )
    parser.add_argument(
        "--probe-batch-size",
        type=int,
        default=config.MULTICAST_PROBE_BATCH_SIZE,
        help="If >0, probe links in batches of this size to reduce contention (0 = all concurrent).",
    )
    parser.add_argument(
        "--capacity-snapshot",
        default=os.environ.get("CAPACITY_SNAPSHOT", ""),
        help="Path to a JSON capacity snapshot (list of {src,dst,capacity_mbps}) to use instead of probing.",
    )
    parser.add_argument(
        "--ping-matrix",
        default=os.environ.get("PING_MATRIX", ""),
        help="Optional ping RTT/loss matrix (JSON list of {src,dst,...}) for probe sanity checks.",
    )
    parser.add_argument(
        "--probe-retest-outliers",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="After --probe-links, re-probe suspicious outlier edges sequentially (default: true).",
    )
    parser.add_argument("--probe-outlier-mbps", type=float, default=10.0)
    parser.add_argument("--probe-outlier-median-frac", type=float, default=0.1)
    parser.add_argument("--probe-outlier-asymmetry-frac", type=float, default=0.1)
    parser.add_argument("--probe-retest-bytes", type=int, default=0)
    parser.add_argument("--probe-retest-repeats", type=int, default=3)
    parser.add_argument("--probe-retest-max-edges", type=int, default=8)

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
