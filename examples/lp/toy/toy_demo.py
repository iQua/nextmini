#!/usr/bin/env python3
"""Toy demo: convert toy LP paths -> multicast DAG -> install into Nextmini -> send data.

This avoids the full RL pipeline and focuses on:
1) `convert_to_multicast_trees()` extraction
2) `nextmini_py.Dataplane.set_group_routes()` end-to-end wiring
"""

from __future__ import annotations

import argparse
import asyncio
import json
import time

import nextmini_py as nm

from examples.lp.main import (
    _apply_link_rates,
    _connect_db,
    _db_settings,
    _fetch_link_rates_from_probes,
    _request_link_probes,
    _wait_for_probe_finish,
)
from examples.lp.tree_conversion import convert_to_multicast_trees, paths_to_edges
from examples.lp.solver import build_graph_from_controller_config, load_toml
from examples.lp import mFlow


CTRL_SRC_PORT = 40100
CTRL_DST_PORT = 40101

# Probe settings for the toy demo (kept simple; tweak here if needed).
PROBE_BYTES = 1_000_000_000
PROBE_TIMEOUT_SECS = 30.0


def _send_ctrl(
    dp: nm.Dataplane, *, dst_node_id: int, msg: dict, src_port: int, dst_port: int
) -> None:
    payload = json.dumps(msg).encode("utf-8")
    view = nm.PacketView(payload)
    dp.send_to_node(dst_node_id, view, src_port=src_port, dst_port=dst_port)


def pick_best_tree(
    tree_candidates: list[tuple[list[list[int]], float]],
    requested_dests: list[int],
) -> tuple[list[list[int]], float]:
    """Pick a tree that covers as many requested destinations as possible, then max throughput."""
    requested = set(requested_dests)

    def score(item: tuple[list[list[int]], float]) -> tuple[int, float]:
        paths, tput = item
        covered = {p[-1] for p in paths if p}
        return (len(covered & requested), tput)

    return max(tree_candidates, key=score)


def run_source(args: argparse.Namespace) -> nm.Dataplane:
    dp = nm.Dataplane(args.config)
    src_node_id = dp.node_id

    receiver_ids = [int(x) for x in args.receiver_ids.split(",") if x.strip()]
    payload = args.payload.encode("utf-8")

    # Control-plane receivers for HELLO/READY from each receiver.
    ctrl_rxs: dict[int, nm.PacketReceiver] = {}
    for rid in receiver_ids:
        ctrl_rxs[rid] = dp.register_receiver_from_node(
            src_node_id=rid,
            src_port=CTRL_DST_PORT,  # receiver -> source
            dst_port=CTRL_SRC_PORT,
        )

    # 1) RL-style handshake barrier: wait for receivers to say HELLO before we send metadata.
    hello: set[int] = set()
    deadline = time.monotonic() + 120
    while time.monotonic() < deadline and len(hello) < len(receiver_ids):
        for rid, rx in ctrl_rxs.items():
            if rid in hello:
                continue
            delivery = rx.recv(timeout_ms=50)
            if delivery is None:
                continue
            try:
                msg = json.loads(delivery.payload)
            except Exception:
                continue
            if msg.get("type") == "HELLO":
                hello.add(rid)
                print(f"[src] got HELLO from {rid}", flush=True)
        if len(hello) < len(receiver_ids):
            time.sleep(0.05)
    if len(hello) < len(receiver_ids):
        raise TimeoutError(
            f"Timed out waiting for HELLO from receivers. got={sorted(hello)} expected={receiver_ids}"
        )

    # 2) Create group (owner is this node).
    dp.create_group(args.label)
    group_id, group_ip, _ = dp.group_is_ready(timeout_ms=30_000)
    print(
        f"[src] group created: id={group_id} ip={group_ip} src={src_node_id}",
        flush=True,
    )

    # 3) Send metadata to receivers.
    for rid in receiver_ids:
        _send_ctrl(
            dp,
            dst_node_id=rid,
            msg={
                "type": "META",
                "group_id": group_id,
                "group_ip": group_ip,
                "src_node_id": src_node_id,
                "expected_bytes": len(payload),
                "block_size": args.chunk_size,
            },
            src_port=CTRL_SRC_PORT,  # source -> receiver
            dst_port=CTRL_DST_PORT,
        )

    # 4) Wait for READY from all receivers (after they join and register receive).
    ready: set[int] = set()
    deadline = time.monotonic() + 120
    while time.monotonic() < deadline and len(ready) < len(receiver_ids):
        for rid, rx in ctrl_rxs.items():
            delivery = rx.recv(timeout_ms=50)
            if delivery is None:
                continue
            try:
                msg = json.loads(delivery.payload)
            except Exception:
                continue
            if msg.get("type") == "READY" and rid not in ready:
                ready.add(rid)
                print(f"[src] got READY from {rid}", flush=True)
        if len(ready) < len(receiver_ids):
            time.sleep(0.05)
    if len(ready) < len(receiver_ids):
        raise TimeoutError(
            f"Timed out waiting for READY from receivers. got={sorted(ready)} expected={receiver_ids}"
        )

    # Wait for topology to be ready before inserting probe flows
    print("[src] waiting for topology ready...", flush=True)
    if not dp.wait_for_topology_ready(timeout_ms=30_000):
        raise TimeoutError("Topology not ready after 30 seconds")
    print("[src] topology is ready!", flush=True)

    # Compute a real mFlow LP solution, then convert → edges.
    graph = build_graph_from_controller_config(args.controller_config)
    print(f"[src] initial graph capacities (Mbps): {dict(graph.capacities.items())}", flush=True)
    
    controller_cfg = load_toml(args.controller_config)
    settings = _db_settings(controller_cfg)
    conn = _connect_db(settings)
    try:
        print(f"[src] inserting {len(graph.edges)} probe flows...", flush=True)
        probe_ids = _request_link_probes(conn, graph.edges, bytes_per_flow=PROBE_BYTES)
        print(f"[src] waiting for {len(probe_ids)} probes to finish (timeout={PROBE_TIMEOUT_SECS}s)...", flush=True)
        finished = _wait_for_probe_finish(
            conn, probe_ids, timeout_secs=PROBE_TIMEOUT_SECS
        )
        if not finished:
            print("[src] probe timeout; using default capacities", flush=True)
        else:
            print("[src] probes finished, fetching measured rates...", flush=True)
            # Use actual probe durations from flows table (more accurate than time window)
            rates = _fetch_link_rates_from_probes(conn, probe_ids)
            if rates:
                print(f"[src] fetched {len(rates)} link rates from probe flows", flush=True)
                print(f"[src] measured rates (Mbps): {dict((k, round(v/1_000_000, 2)) for k, v in rates.items())}", flush=True)
                _apply_link_rates(graph, rates)
                print(f"[src] applied probed capacities to graph", flush=True)
                print(f"[src] updated graph capacities (Mbps): {dict(graph.capacities.items())}", flush=True)
            else:
                print(
                    "[src] probe produced no metrics; using default capacities", flush=True
                )
    finally:
        conn.close()
    
    print(f"[src] running LP solver with capacities: {dict(graph.capacities.items())}", flush=True)
    variables, sol = mFlow.solve(graph, [src_node_id], {src_node_id: receiver_ids})

    sources, session_trees = convert_to_multicast_trees(variables, sol)
    idx = sources.index(src_node_id)
    best_paths, throughput = pick_best_tree(session_trees[idx], receiver_ids)
    edges = paths_to_edges(best_paths)

    print(
        f"[src] conversion solver=mflow throughput={throughput} edges={edges}",
        flush=True,
    )

    # Install override DAG into controller and wait for local installation.
    dp.set_group_routes(group_id, edges)
    if not dp.wait_for_group_routes(group_id, src_node_id, timeout_ms=30_000):
        raise TimeoutError("Timed out waiting for multicast routes to install.")

    # Send payload via lossless multicast.
    builder = nm.PacketBuilder(size=len(payload))
    builder.write(payload)
    view = builder.freeze()

    sid = dp.send_data(group_id, group_ip, receiver_ids, view, block_size=args.chunk_size)
    ok = dp.lossless_wait(sid, timeout_ms=60_000)
    print(f"[src] send done ok={ok} sid={sid}", flush=True)
    return dp


def run_receiver(args: argparse.Namespace) -> nm.Dataplane:
    dp = nm.Dataplane(args.config)
    node_id = dp.node_id
    src_node_id = int(args.source_node_id)

    # Control receiver for metadata from source.
    ctrl_rx = dp.register_receiver_from_node(
        src_node_id=src_node_id,
        src_port=CTRL_SRC_PORT,  # source -> receiver
        dst_port=CTRL_DST_PORT,
    )

    # Send HELLO periodically until META arrives (prevents "source sent META too early" races).
    meta: dict | None = None
    deadline = time.monotonic() + 120
    while time.monotonic() < deadline and meta is None:
        _send_ctrl(
            dp,
            dst_node_id=src_node_id,
            msg={"type": "HELLO", "node_id": node_id},
            src_port=CTRL_DST_PORT,  # receiver -> source
            dst_port=CTRL_SRC_PORT,
        )
        delivery = ctrl_rx.recv(timeout_ms=1000)
        if delivery is None:
            continue
        try:
            msg = json.loads(delivery.payload)
        except Exception:
            continue
        if msg.get("type") == "META":
            meta = msg
            break

    if meta is None:
        raise TimeoutError("Timed out waiting for META from source.")

    group_id = int(meta["group_id"])
    group_ip = str(meta["group_ip"])
    expected_bytes = int(meta["expected_bytes"])

    dp.join_group(group_id)

    async def receive_payload() -> tuple[bool, bytes, int]:
        # Start the async receive before we tell the source we're ready.
        async def receive_wrapper() -> int:
            return await dp.receive_data_async(
                group_id,
                src_node_id,
                expected_bytes=expected_bytes,
                block_size=int(meta.get("block_size", args.chunk_size)),
            )

        receive_task = asyncio.create_task(receive_wrapper())
        last_ready = 0.0
        deadline = time.monotonic() + 120
        sid: int | None = None

        while time.monotonic() < deadline:
            now = time.monotonic()
            if now - last_ready >= 1.0:
                _send_ctrl(
                    dp,
                    dst_node_id=src_node_id,
                    msg={"type": "READY", "node_id": node_id},
                    src_port=CTRL_DST_PORT,
                    dst_port=CTRL_SRC_PORT,
                )
                last_ready = now

            done, _ = await asyncio.wait({receive_task}, timeout=0.25)
            if receive_task in done:
                sid = receive_task.result()
                break

        if sid is None:
            receive_task.cancel()
            raise TimeoutError("Timed out waiting for multicast session to start.")

        ok = await dp.lossless_wait_async(sid, timeout_ms=60_000)
        view = dp.get_data_buffer(sid)
        payload = bytes(view.read())
        return ok, payload, sid

    print(f"[dst {node_id}] joined group {group_id} ({group_ip}) and READY", flush=True)

    ok, payload, sid = asyncio.run(receive_payload())
    print(f"[dst {node_id}] recv ok={ok} sid={sid} payload={payload!r}", flush=True)
    return dp


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--role", choices=["source", "receiver"], required=True)
    parser.add_argument("--config", required=True, help="Path to node config TOML")
    parser.add_argument(
        "--controller-config",
        default="examples/lp/toy/controller-config.toml",
        help="Topology source for mFlow (controller-config.toml)",
    )
    parser.add_argument("--label", default="toy-lp-group")
    parser.add_argument("--receiver-ids", default="2,3")
    parser.add_argument(
        "--source-node-id",
        default="1",
        help="Source node ID (receivers use this for handshake)",
    )
    parser.add_argument("--payload", default="hello-nextmini")
    parser.add_argument("--chunk-size", type=int, default=8500)
    args = parser.parse_args()

    if args.role == "source":
        dp = run_source(args)
    else:
        dp = run_receiver(args)

    # Keep the dataplane alive after demo completes.
    print(f"[node {dp.node_id}] Demo completed. Staying alive...", flush=True)
    while True:
        time.sleep(60)


if __name__ == "__main__":
    raise SystemExit(main())
