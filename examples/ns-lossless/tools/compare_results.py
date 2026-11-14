#!/usr/bin/env python3
from __future__ import annotations

import argparse
import collections
import json
import pathlib
import re
from typing import Any

from ledger import update_ledger


COUNTER_FIELDS = {
    "admitted_packets",
    "deficit_symbols",
    "stall_entries",
}
COUNTER_PATTERN = re.compile(r"\b([A-Za-z_][A-Za-z0-9_]*)=(\d+)\b")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compare ns-lossless receiver results with the lossless solver benchmark."
    )
    parser.add_argument("solution", type=pathlib.Path)
    parser.add_argument("--metrics-dir", type=pathlib.Path, required=True)
    parser.add_argument("--log", type=pathlib.Path, action="append", default=[])
    parser.add_argument("--output", type=pathlib.Path)
    parser.add_argument("--ledger", type=pathlib.Path)
    return parser.parse_args()


def parse_metrics(path: pathlib.Path) -> dict[str, str]:
    values = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        key, separator, value = line.partition("=")
        if separator:
            values[key.strip()] = value.strip()
    return values


def collect_log_counters(paths: list[pathlib.Path]) -> dict[str, Any]:
    maxima: dict[str, int] = collections.defaultdict(int)
    event_counts: dict[str, int] = collections.Counter()

    files = []
    for path in paths:
        files.extend(sorted(path.glob("*.log")) if path.is_dir() else [path])
    for path in files:
        for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
            fields = {key: int(value) for key, value in COUNTER_PATTERN.findall(line)}
            for key in COUNTER_FIELDS & fields.keys():
                maxima[key] = max(maxima[key], fields[key])
            if "Child-scoped fan-out dispatcher statistics" in line:
                event_counts["fanout_statistics"] += 1
    return {
        "files": [str(path) for path in files],
        "maxima": dict(sorted(maxima.items())),
        "event_counts": dict(sorted(event_counts.items())),
    }


def compare(
    solution: dict[str, Any],
    metrics_dir: pathlib.Path,
    logs: list[pathlib.Path],
) -> dict[str, Any]:
    receivers = [
        int(node_id) for node_id in solution.get("scenario", {}).get("receivers", [])
    ]
    receiver_results = []
    for receiver in receivers:
        path = metrics_dir / f"receiver-{receiver}.metrics"
        if not path.exists():
            raise FileNotFoundError(f"missing receiver metrics: {path}")
        values = parse_metrics(path)
        payload_bytes = int(values["payload_bytes"])
        duration_seconds = float(values["duration_seconds"])
        achieved_mbit = (
            0.0
            if duration_seconds <= 0.0
            else payload_bytes * 8.0 / duration_seconds / 1_000_000.0
        )
        receiver_results.append(
            {
                "node_id": receiver,
                "payload_bytes": payload_bytes,
                "duration_seconds": duration_seconds,
                "achieved_mbit": achieved_mbit,
            }
        )

    min_achieved = min(
        (receiver["achieved_mbit"] for receiver in receiver_results), default=0.0
    )
    pi_star = float(
        solution.get("T_opt", solution.get("session_rate", solution.get("T_sched", 0.0)))
    )
    return {
        "receivers": receiver_results,
        "min_receiver_achieved_mbit": min_achieved,
        "pi_star_mbit": pi_star,
        "achieved_to_pi_star": None if pi_star <= 0.0 else min_achieved / pi_star,
        "log_counters": collect_log_counters(logs),
    }


def main() -> None:
    args = parse_args()
    solution = json.loads(args.solution.read_text(encoding="utf-8"))
    report = compare(solution, args.metrics_dir, args.log)
    rendered = json.dumps(report, indent=2, sort_keys=True) + "\n"
    if args.output is not None:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered, encoding="utf-8")
    if args.ledger is not None:
        update_ledger(args.ledger, "benchmark_comparison", report)
    print(rendered, end="")


if __name__ == "__main__":
    main()
