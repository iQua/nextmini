from __future__ import annotations

import argparse
import os
import time

from utils.logging import console, panel, show_toml, metrics_table
from rich.syntax import Syntax


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="E2E reliable multicast harness (rich logs)")
    p.add_argument(
        "scenario",
        choices=["happy", "loss", "pacing", "ack-all", "ack-k", "ack-frac"],
        help="Test scenario",
    )
    p.add_argument("tensor_path", help="Path to test payload (file)")
    p.add_argument("group_ip", help="Multicast group IP (string)")
    p.add_argument("receiver_ids", help="Comma-separated receiver node IDs (e.g., 2,3,4)")
    p.add_argument("--expected-bytes", type=int, default=None, help="Expected receive bytes (optional)")
    p.add_argument("--chunk-size", type=int, default=32768)
    p.add_argument("--src-port", type=int, default=None)
    p.add_argument("--dst-port", type=int, default=None)
    return p.parse_args()


def main() -> None:
    args = parse_args()
    try:
        import nextmini_py  # noqa: F401
    except Exception as e:  # pragma: no cover
        panel("Import Error", f"Failed to import nextmini_py: {e}")
        raise SystemExit(2)

    from nextmini_py import Dataplane

    panel(
        "Scenario",
        f"{args.scenario} | tensor={args.tensor_path} | group={args.group_ip} | receivers={args.receiver_ids}",
    )

    cfg_path = os.environ.get("NEXTMINI_CONFIG", "node.toml")
    if os.path.exists(cfg_path):
        show_toml(open(cfg_path).read(), title="Config")
    else:
        panel("Config", f"Config file {cfg_path!r} not found; proceeding with defaults")

    panel("Step 1 — Init Dataplane", f"Config path: {cfg_path}")
    dp = Dataplane(cfg_path)

    if args.scenario == "loss":
        panel(
            "Loss Scenario (dry-run)",
            "In live mode, configure tc netem drop/limit on the dataplane NIC or enable in-process drop hooks; this run only traces calls.",
        )
    if args.scenario == "pacing":
        panel(
            "Pacing Scenario (dry-run)",
            "In live mode, set [reliable.data_bucket] in node.toml to pace data; control flows get higher WRR weight.",
        )

    # This harness uses the thin wrappers; until engines are wired, they return a sid only.
    recvs = [int(x.strip()) for x in args.receiver_ids.split(",") if x.strip()]
    start = time.time()
    # Scenario-specific ack policy selection
    ack_policy = {
        "ack-all": "all",
        "ack-k": "k:2",
        "ack-frac": "frac:0.66",
    }.get(args.scenario, "all")

    call_src = f"""
sid = dp.reliable_send_file_rs(
    group_ip={args.group_ip!r},
    receiver_ids={recvs!r},
    tensor_path={args.tensor_path!r},
    chunk_size={args.chunk_size}, src_port={args.src_port}, dst_port={args.dst_port},
    ack_policy={ack_policy!r},
)
"""
    console.print(Syntax(call_src, "python", theme="monokai", word_wrap=True))
    sid_send = dp.reliable_send_file_rs(
        args.group_ip,
        recvs,
        args.tensor_path,
        chunk_size=args.chunk_size,
        src_port=args.src_port,
        dst_port=args.dst_port,
        ack_policy=ack_policy,
    )

    # Choose a representative receiver for a sid; in real wiring, each receiver gets its sid
    call_src = f"""
sid = dp.reliable_receive_file_rs(
    group_ip={args.group_ip!r}, source_node_id={(recvs[0] if recvs else 0)!r},
    expected_bytes={args.expected_bytes or 0}, chunk_size={args.chunk_size},
    src_port={args.src_port}, dst_port={args.dst_port}, sink_path=None,
)
"""
    console.print(Syntax(call_src, "python", theme="monokai", word_wrap=True))
    sid_recv = dp.reliable_receive_file_rs(
        args.group_ip,
        recvs[0] if recvs else 0,
        args.expected_bytes or 0,
        chunk_size=args.chunk_size,
        src_port=args.src_port,
        dst_port=args.dst_port,
        sink_path=None,
    )

    # Optional: deterministic wait if wrapper exposes it
    waited = {}
    if hasattr(dp, "reliable_wait"):
        panel("Optional Wait", "Wrapper exposes reliable_wait; attempting deterministic waits.")
        try:
            t0 = time.time()
            ok_send = dp.reliable_wait(sid_send, timeout_ms=2000)
            ok_recv = dp.reliable_wait(sid_recv, timeout_ms=2000)
            waited = {
                "wait_send_ok": ok_send,
                "wait_recv_ok": ok_recv,
                "wait_ms": f"{(time.time() - t0) * 1000.0:.2f}",
            }
        except Exception as e:
            panel("Wait Error", f"reliable_wait failed: {e}")

    elapsed = (time.time() - start) * 1000.0
    panel("Step 2 — Dry Run", "Reliable engines not yet wired; wrappers returned session IDs only.")
    m = {
        "sid_send": sid_send,
        "sid_recv": sid_recv,
        "elapsed_ms": f"{elapsed:.2f}",
        "receivers": len(recvs),
    }
    m.update(waited)
    metrics_table(m)


if __name__ == "__main__":
    main()
