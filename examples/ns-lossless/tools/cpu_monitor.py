#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import datetime
import json
import pathlib
import signal
import time
from typing import Any

from ledger import update_ledger


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Sample and validate host CPU utilization.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    sample = subparsers.add_parser("sample")
    sample.add_argument("--output", type=pathlib.Path, required=True)
    sample.add_argument("--interval", type=float, default=1.0)

    summarize = subparsers.add_parser("summarize")
    summarize.add_argument("samples", type=pathlib.Path)
    summarize.add_argument("--ceiling-pct", type=float, default=75.0)
    summarize.add_argument("--ledger", type=pathlib.Path)
    summarize.add_argument("--calibration", action="store_true")
    summarize.add_argument("--metrics-dir", type=pathlib.Path)
    return parser.parse_args()


def read_proc_stat(path: pathlib.Path = pathlib.Path("/proc/stat")) -> tuple[int, int]:
    fields = path.read_text(encoding="utf-8").splitlines()[0].split()
    if not fields or fields[0] != "cpu" or len(fields) < 5:
        raise RuntimeError(f"unexpected {path} CPU row")
    ticks = [int(value) for value in fields[1:]]
    idle = ticks[3] + (ticks[4] if len(ticks) > 4 else 0)
    return sum(ticks), idle


def sample(output: pathlib.Path, interval: float) -> None:
    if interval <= 0.0:
        raise ValueError("sample interval must be positive")
    output.parent.mkdir(parents=True, exist_ok=True)
    stopping = False

    def stop(_signum: int, _frame: Any) -> None:
        nonlocal stopping
        stopping = True

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    previous_total, previous_idle = read_proc_stat()
    with output.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.writer(stream)
        writer.writerow(("timestamp_utc", "utilization_pct"))
        stream.flush()
        while not stopping:
            time.sleep(interval)
            total, idle = read_proc_stat()
            total_delta = total - previous_total
            idle_delta = idle - previous_idle
            previous_total, previous_idle = total, idle
            if total_delta <= 0:
                continue
            utilization = 100.0 * (total_delta - idle_delta) / total_delta
            writer.writerow(
                (
                    datetime.datetime.now(datetime.UTC).isoformat(),
                    f"{utilization:.6f}",
                )
            )
            stream.flush()


def metric_capability(metrics_dir: pathlib.Path | None) -> dict[str, Any] | None:
    if metrics_dir is None or not metrics_dir.exists():
        return None
    receivers = []
    for path in sorted(metrics_dir.glob("receiver-*.metrics")):
        fields = {}
        for line in path.read_text(encoding="utf-8").splitlines():
            key, separator, value = line.partition("=")
            if separator:
                fields[key] = value
        if "node_id" not in fields or "throughput_gbps" not in fields:
            continue
        receivers.append(
            {
                "node_id": int(fields["node_id"]),
                "throughput_gbps": float(fields["throughput_gbps"]),
                "duration_seconds": float(fields["duration_seconds"]),
            }
        )
    if not receivers:
        return None
    return {
        "receivers": receivers,
        "min_receiver_throughput_gbps": min(
            receiver["throughput_gbps"] for receiver in receivers
        ),
    }


def summarize(
    samples_path: pathlib.Path,
    ceiling_pct: float,
    *,
    calibration: bool,
    metrics_dir: pathlib.Path | None,
) -> dict[str, Any]:
    if not 0.0 < ceiling_pct <= 100.0:
        raise ValueError("CPU ceiling must be in (0, 100]")
    samples = []
    if samples_path.exists():
        with samples_path.open(encoding="utf-8", newline="") as stream:
            samples = [float(row["utilization_pct"]) for row in csv.DictReader(stream)]
    report: dict[str, Any] = {
        "mode": "calibration" if calibration else "measurement",
        "sample_count": len(samples),
        "ceiling_pct": ceiling_pct,
        "peak_utilization_pct": max(samples) if samples else None,
        "mean_utilization_pct": sum(samples) / len(samples) if samples else None,
        "valid": bool(samples) and max(samples) <= ceiling_pct,
        "invalid_reason": (
            "no_cpu_samples"
            if not samples
            else "cpu_ceiling_exceeded"
            if max(samples) > ceiling_pct
            else None
        ),
    }
    if calibration:
        report["capability"] = metric_capability(metrics_dir)
    return report


def main() -> None:
    args = parse_args()
    if args.command == "sample":
        sample(args.output, args.interval)
        return

    report = summarize(
        args.samples,
        args.ceiling_pct,
        calibration=args.calibration,
        metrics_dir=args.metrics_dir,
    )
    if args.ledger is not None:
        update_ledger(args.ledger, "cpu_validity", report)
    print(json.dumps(report, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
