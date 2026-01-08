#!/usr/bin/env python3
"""Compute multicast DAG edges for Nextmini (LP / heuristics).

This module is designed to be importable and runnable as:

```bash
python -m examples.lp.main --controller-config examples/rl/configs-docker/controller-config.toml --src 1 --dests 2,3
```

Choose a planning algorithm:

```bash
python -m examples.lp.main \
  --controller-config examples/rl/configs-docker/controller-config.toml \
  --src 1 --dests 2,3 \
  --algorithm cf_tree --hop-limit 3 --eta 0.1
```

Optional: apply the computed edges to a live controller *from the source node* using the Python
bindings:

```bash
python -m examples.lp.main \
  --controller-config examples/rl/configs-docker/controller-config.toml \
  --src 1 --dests 2,3 \
  --apply --node-config examples/rl/configs-docker/trainer-config.toml --group-id 7
```
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path
from time import sleep

from .solver import (
    build_graph_from_controller_config,
    compute_tree_edges,
    load_toml,
)


def _parse_node_list(spec: str) -> list[int]:
    items = [s.strip() for s in spec.split(",")]
    out: list[int] = []
    for item in items:
        if not item:
            continue
        out.append(int(item))
    return out


def _db_settings(controller_cfg: dict) -> dict:
    db = dict(controller_cfg.get("db") or {})

    def pick(key: str, env: str, default: str) -> str:
        value = db.get(key)
        if value:
            return str(value)
        return os.environ.get(env, default)

    return {
        "user": pick("user", "NEXTMINI_DB_USER", "pgusr"),
        "password": pick("password", "NEXTMINI_DB_PASSWORD", "pgpwrd"),
        "host": pick("host", "NEXTMINI_DB_HOST", "localhost"),
        "port": pick("port", "NEXTMINI_DB_PORT", "5432"),
        "database": pick("database", "NEXTMINI_DB_NAME", "nextmini"),
    }


def _connect_db(settings: dict):
    try:
        import psycopg

        conn = psycopg.connect(
            dbname=settings["database"],
            user=settings["user"],
            password=settings["password"],
            host=settings["host"],
            port=settings["port"],
        )
        conn.autocommit = True
        return conn
    except ImportError:
        pass

    try:
        import psycopg2
    except ImportError as exc:
        raise SystemExit(
            "DB probe mode requires psycopg. Install with:\n"
            "  uv pip install 'psycopg[binary]'"
        ) from exc

    conn = psycopg2.connect(
        dbname=settings["database"],
        user=settings["user"],
        password=settings["password"],
        host=settings["host"],
        port=settings["port"],
    )
    conn.autocommit = True
    return conn


def _request_link_probes(
    conn,
    edges: list[tuple[int, int]],
    bytes_per_flow: int,
    *,
    batch_size: int | None = None,
    batch_timeout_secs: float = 60.0,
) -> list[int]:
    """Request probe flows for link capacity measurement.
    
    Args:
        conn: Database connection
        edges: List of (src, dst) tuples to probe
        bytes_per_flow: Bytes to send per probe flow
        batch_size: If set, probe links in batches of this size to reduce contention.
                    Each batch waits for completion before starting next batch.
                    If None, all probes run concurrently.
        batch_timeout_secs: Timeout per batch (only used if batch_size is set)
    
    Returns:
        List of all probe flow IDs
    """
    insert_sql = """
        INSERT INTO flows (
            src_node_id,
            dst_node_id,
            flow_len_type,
            flow_len_bytes,
            flow_len_duration,
            flow_rate,
            flow_weight,
            is_finished,
            is_probe
        )
        VALUES (%s, %s, 'bytes', %s, NULL, NULL, NULL, FALSE, TRUE)
        RETURNING id
    """
    rows = [(src, dst, bytes_per_flow) for src, dst in edges if src != dst]
    if not rows:
        return []

    all_ids: list[int] = []
    
    if batch_size is None or batch_size <= 0 or batch_size >= len(rows):
        # All at once (original behavior)
        with conn.cursor() as cursor:
            for src, dst, size in rows:
                cursor.execute(insert_sql, (src, dst, size))
                row = cursor.fetchone()
                if row:
                    all_ids.append(int(row[0]))
    else:
        # Batch probing: probe batch_size links at a time, wait for completion
        num_batches = (len(rows) + batch_size - 1) // batch_size
        for batch_idx in range(num_batches):
            start = batch_idx * batch_size
            end = min(start + batch_size, len(rows))
            batch_rows = rows[start:end]
            
            batch_ids: list[int] = []
            with conn.cursor() as cursor:
                for src, dst, size in batch_rows:
                    cursor.execute(insert_sql, (src, dst, size))
                    row = cursor.fetchone()
                    if row:
                        batch_ids.append(int(row[0]))
            
            all_ids.extend(batch_ids)
            
            # Wait for this batch to complete before starting next
            if batch_idx < num_batches - 1:  # Don't wait after last batch
                _wait_for_probe_finish(conn, batch_ids, timeout_secs=batch_timeout_secs)
    
    return all_ids


def _wait_for_probe_finish(
    conn,
    probe_ids: list[int],
    *,
    timeout_secs: float,
    poll_interval_secs: float = 0.5,
) -> bool:
    if not probe_ids:
        return True

    query = """
        SELECT COUNT(*)
        FROM flows
        WHERE id = ANY(%s) AND is_finished = FALSE
    """

    deadline = None
    if timeout_secs > 0:
        deadline = time.monotonic() + timeout_secs

    while True:
        with conn.cursor() as cursor:
            cursor.execute(query, (probe_ids,))
            remaining = cursor.fetchone()[0]
        if remaining == 0:
            return True
        if deadline is not None and time.monotonic() >= deadline:
            return False
        if poll_interval_secs > 0:
            sleep(poll_interval_secs)


def _fetch_link_rates_from_probes(
    conn, probe_ids: list[int]
) -> dict[tuple[int, int], float]:
    """Fetch link rates using actual probe flow durations from the flows table.
    
    This computes the rate as: flow_len_bytes * 8 / (finish_time - start_time)
    where times are in milliseconds. Returns rates in bps.
    
    This is more accurate than using a fixed time window because it uses the
    actual duration each probe flow ran for.
    """
    if not probe_ids:
        return {}

    query = """
        SELECT src_node_id,
               dst_node_id,
               flow_len_bytes,
               start_time,
               finish_time
        FROM flows
        WHERE id = ANY(%s)
          AND is_finished = TRUE
          AND start_time IS NOT NULL
          AND finish_time IS NOT NULL
          AND finish_time > start_time
    """

    with conn.cursor() as cursor:
        cursor.execute(query, (probe_ids,))
        rows = cursor.fetchall()

    # Compute per-flow rates
    flow_rates: dict[tuple[int, int], list[float]] = {}
    for src, dst, flow_bytes, start_ms, finish_ms in rows:
        duration_secs = (finish_ms - start_ms) / 1000.0
        if duration_secs <= 0:
            continue
        rate_bps = (flow_bytes * 8.0) / duration_secs
        key = (int(src), int(dst))
        if key not in flow_rates:
            flow_rates[key] = []
        flow_rates[key].append(rate_bps)

    # Average rates per link (if multiple probes for same link)
    result: dict[tuple[int, int], float] = {}
    for (src, dst), rates in flow_rates.items():
        result[(src, dst)] = sum(rates) / len(rates)

    return result


def _apply_link_rates(graph, rates_bps: dict[tuple[int, int], float]) -> None:
    for key, rate_bps in rates_bps.items():
        if key in graph.capacities:
            graph.capacities[key] = rate_bps / 1_000_000.0


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Compute multicast DAG edges for Nextmini"
    )
    parser.add_argument(
        "--controller-config",
        type=str,
        required=True,
        help="Path to controller-config.toml (topology source)",
    )
    parser.add_argument(
        "--src", type=int, required=True, help="Source node ID (e.g. trainer)"
    )
    parser.add_argument(
        "--dests",
        type=str,
        required=True,
        help="Comma-separated destination node IDs (e.g. 2,3)",
    )
    parser.add_argument(
        "--algorithm",
        type=str,
        default="mflow",
        choices=("mflow", "cf_tree", "cf_bottleneck", "basic_tree"),
        help="Multicast planning algorithm (default: mflow)",
    )
    parser.add_argument(
        "--hop-limit",
        type=int,
        default=3,
        help="Hop limit H (used by cf_tree/basic_tree; also used as LP max_length when --max-length is -1)",
    )
    parser.add_argument(
        "--eta",
        type=float,
        default=0.1,
        help="LP guidance weight eta for cf_tree (default: 0.1)",
    )
    parser.add_argument(
        "--max-relays",
        type=int,
        default=None,
        help="Optional cap on number of relay nodes (LP-guided selection).",
    )
    parser.add_argument(
        "--relay-scoring",
        type=str,
        default="coverage",
        choices=("coverage", "path_flow", "incident"),
        help="Relay scoring mode for --max-relays (default: coverage)",
    )
    parser.add_argument(
        "--max-length",
        type=int,
        default=-1,
        help="Maximum path length (hops) for LP candidate paths; -1 uses hop-limit for cf_tree/basic_tree.",
    )
    parser.add_argument(
        "--sort-by",
        type=str,
        default="shortest",
        choices=("shortest", "random"),
        help="Path selection strategy for LP candidates (default: shortest)",
    )
    parser.add_argument(
        "--num-paths",
        type=int,
        default=2,
        help="Number of candidate paths per (src,dst) for LP (default: 2)",
    )
    parser.add_argument(
        "--default-capacity",
        type=int,
        default=None,
        help="Optional default link capacity override (Mbps) for the LP graph",
    )
    parser.add_argument(
        "--probe-links",
        action="store_true",
        help="Insert probe flows and overwrite link capacities from recent metrics",
    )
    parser.add_argument(
        "--probe-bytes",
        type=int,
        default=1_000_000_000,
        help="Bytes to send per probe flow (larger keeps the link busy longer)",
    )
    parser.add_argument(
        "--probe-timeout-secs",
        type=float,
        default=30.0,
        help="Timeout in seconds for probe flows to finish",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print edges as JSON (list of [u,v])",
    )

    # Optional apply step (requires nextmini_py and must run as the source node).
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Apply edges via nextmini_py.set_group_routes()",
    )
    parser.add_argument(
        "--node-config",
        type=str,
        help="Path to the SOURCE node's dataplane config.toml",
    )
    parser.add_argument(
        "--group-id", type=int, help="Existing multicast group_id to override"
    )

    args = parser.parse_args()

    destinations = _parse_node_list(args.dests)
    if not destinations:
        print("Error: --dests must contain at least one node id", file=sys.stderr)
        return 2

    graph = build_graph_from_controller_config(
        args.controller_config, default_capacity=args.default_capacity
    )

    if args.probe_links:
        controller_cfg = load_toml(args.controller_config)
        settings = _db_settings(controller_cfg)
        conn = _connect_db(settings)
        try:
            probe_ids = _request_link_probes(
                conn, graph.edges, bytes_per_flow=args.probe_bytes
            )
            if not probe_ids:
                print("No probe flows inserted; no edges found.", file=sys.stderr)
            else:
                # Wait for probes to finish
                _wait_for_probe_finish(conn, probe_ids, timeout_secs=args.probe_timeout_secs)
                # Use actual probe durations from flows table (more accurate)
                rates = _fetch_link_rates_from_probes(conn, probe_ids)
                _apply_link_rates(graph, rates)
        finally:
            conn.close()

    result = compute_tree_edges(
        graph,
        src=args.src,
        destinations=destinations,
        algorithm=args.algorithm,
        hop_limit=args.hop_limit,
        eta=args.eta,
        max_relays=args.max_relays,
        relay_scoring=args.relay_scoring,
        max_length=args.max_length,
        sort_by=args.sort_by,
        num_paths=args.num_paths,
    )
    edges = result.edges
    throughput = result.throughput

    if args.json:
        print(json.dumps([[a, b] for (a, b) in edges]))
    else:
        print(
            f"algorithm={result.algorithm} edges={edges}"
            + (f" throughput={throughput}" if throughput is not None else "")
            + (
                f" lp_f_star={result.lp_f_star}"
                if result.lp_f_star is not None
                else ""
            )
        )

    if args.apply:
        if args.node_config is None or args.group_id is None:
            print(
                "Error: --apply requires --node-config and --group-id", file=sys.stderr
            )
            return 2

        node_cfg_path = Path(args.node_config)
        if not node_cfg_path.exists():
            print(f"Error: node config not found: {node_cfg_path}", file=sys.stderr)
            return 2

        try:
            import nextmini_py as nm  # type: ignore
        except ImportError as exc:
            raise SystemExit(
                "nextmini_py is not installed. Build/install it first, e.g.:\n"
                "  maturin develop --release -m python-api/Cargo.toml -F python-extension\n"
            ) from exc

        dp = nm.Dataplane(str(node_cfg_path))
        dp.set_group_routes(args.group_id, edges)
        print(f"Applied override: group_id={args.group_id} edges={len(edges)}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
