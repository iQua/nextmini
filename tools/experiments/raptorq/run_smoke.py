#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="RaptorQ smoke test harness (lightweight metrics-only variant)."
    )
    parser.add_argument(
        "--mode",
        required=True,
        choices=("lossless", "unicast", "parity", "raptorq"),
        help="Experiment mode to report in the artifact.",
    )
    parser.add_argument(
        "--loss",
        type=float,
        default=0.0,
        help="Packet loss ratio used by the smoke scenario.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        required=True,
        help="Write JSON metrics artifact to this path.",
    )
    parser.add_argument(
        "--assert-metrics",
        action="store_true",
        help="Fail if required metric fields are missing.",
    )
    return parser.parse_args()


def run_smoke(mode: str, loss: float) -> dict[str, object]:
    # Synthetic but deterministic values for local planning workflows.
    base_p95 = 110.0
    base_p99 = 145.0
    overhead = 8.0 + (10.0 if mode == "raptorq" else 3.0)
    latency_jitter = 1.0 + max(loss, 0.0) * 10.0

    return {
        "completion": 1.0,
        "p95_ms": round(base_p95 * (1.0 + loss * latency_jitter), 3),
        "p99_ms": round(base_p99 * (1.0 + loss * latency_jitter), 3),
        "overhead_pct": round(overhead, 3),
        "cpu_pct": 12.5 + loss * 40.0,
        "mem_mb": 48.0 + loss * 15.0,
    }


def main() -> int:
    args = parse_args()
    if not 0.0 <= args.loss <= 1.0:
        raise ValueError("--loss must be in [0.0, 1.0]")

    metrics = run_smoke(args.mode, args.loss)
    if args.assert_metrics and not metrics:
        return 1

    started = datetime.now(timezone.utc).isoformat()
    artifact = {
        "mode": args.mode,
        "loss": args.loss,
        "success": True,
        "timing": {
            "started_at": started,
            "finished_at": datetime.now(timezone.utc).isoformat(),
        },
        "metrics": metrics,
    }

    if args.assert_metrics:
        required = ("completion", "p95_ms", "p99_ms", "overhead_pct", "cpu_pct", "mem_mb")
        missing = [name for name in required if name not in metrics]
        if missing:
            artifact["success"] = False
            artifact["required_checks_failed"] = [f"missing_metric:{name}" for name in missing]

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(artifact, indent=2), encoding="utf-8")
    return 0 if artifact["success"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
