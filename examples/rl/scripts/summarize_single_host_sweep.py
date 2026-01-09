#!/usr/bin/env python3

from __future__ import annotations

import argparse
import csv
import json
import pathlib
import re
import statistics
import sys
from dataclasses import dataclass


RUN_FILE_RE = re.compile(
    r"^profile=(?P<profile>.+)_cap=(?P<cap_seed>\d+)_method=(?P<method>lp|capacity|random)_sel=(?P<sel_seed>\d+)_w=(?P<workers>\d+)_r=(?P<relays>\d+)_b=(?P<budget>\d+)\.json$"
)


def _p50(values: list[float]) -> float:
    return statistics.median(values) if values else 0.0


def _p95(values: list[float]) -> float:
    if not values:
        return 0.0
    values_sorted = sorted(values)
    idx = int(round(0.95 * (len(values_sorted) - 1)))
    return float(values_sorted[idx])


@dataclass(frozen=True)
class RunKey:
    profile: str
    capacity_seed: int
    method: str
    selection_seed: int
    workers: int
    relays: int
    budget: int


@dataclass(frozen=True)
class RunSummary:
    key: RunKey
    n_rounds: int
    ok_rounds: int
    ok_rate: float
    goodput_gbps_ok: tuple[float, ...]
    elapsed_s_ok: tuple[float, ...]
    goodput_gbps_p50_ok: float
    goodput_gbps_p95_ok: float
    elapsed_s_p50_ok: float
    elapsed_s_p95_ok: float
    planner_tput_est_p50_mbps: float | None


def parse_run_key(filename: str) -> RunKey:
    match = RUN_FILE_RE.match(filename)
    if not match:
        raise ValueError(f"unrecognized run filename: {filename}")
    groups = match.groupdict()
    return RunKey(
        profile=str(groups["profile"]),
        capacity_seed=int(groups["cap_seed"]),
        method=str(groups["method"]),
        selection_seed=int(groups["sel_seed"]),
        workers=int(groups["workers"]),
        relays=int(groups["relays"]),
        budget=int(groups["budget"]),
    )


def summarize_run(path: pathlib.Path) -> RunSummary:
    key = parse_run_key(path.name)
    records = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(records, list):
        raise ValueError(f"expected a list of records in {path}")

    ok_flags = [bool(r.get("ok", False)) for r in records]
    ok_rounds = sum(1 for v in ok_flags if v)
    ok_rate = (ok_rounds / len(records)) if records else 0.0

    ok_goodput = [float(r["goodput_gbps"]) for r in records if bool(r.get("ok", False))]
    ok_elapsed = [float(r["elapsed_s"]) for r in records if bool(r.get("ok", False))]

    planner_vals = [
        float(r["planner_tput_est"])
        for r in records
        if r.get("planner_tput_est") is not None
    ]
    planner_p50 = _p50(planner_vals) if planner_vals else None

    return RunSummary(
        key=key,
        n_rounds=len(records),
        ok_rounds=ok_rounds,
        ok_rate=ok_rate,
        goodput_gbps_ok=tuple(ok_goodput),
        elapsed_s_ok=tuple(ok_elapsed),
        goodput_gbps_p50_ok=_p50(ok_goodput),
        goodput_gbps_p95_ok=_p95(ok_goodput),
        elapsed_s_p50_ok=_p50(ok_elapsed),
        elapsed_s_p95_ok=_p95(ok_elapsed),
        planner_tput_est_p50_mbps=planner_p50,
    )


def iter_run_files(input_dir: pathlib.Path) -> list[pathlib.Path]:
    run_files: list[pathlib.Path] = []
    for path in sorted(input_dir.glob("*.json")):
        if path.name == "results.json":
            continue
        if RUN_FILE_RE.match(path.name):
            run_files.append(path)
    return run_files


def write_run_csv(out_csv: pathlib.Path, runs: list[RunSummary]) -> None:
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    with out_csv.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(
            [
                "profile",
                "capacity_seed",
                "method",
                "selection_seed",
                "workers",
                "relays",
                "budget",
                "n_rounds",
                "ok_rounds",
                "ok_rate",
                "goodput_gbps_p50_ok",
                "goodput_gbps_p95_ok",
                "elapsed_s_p50_ok",
                "elapsed_s_p95_ok",
                "planner_tput_est_p50_mbps",
            ]
        )
        for r in runs:
            writer.writerow(
                [
                    r.key.profile,
                    r.key.capacity_seed,
                    r.key.method,
                    r.key.selection_seed,
                    r.key.workers,
                    r.key.relays,
                    r.key.budget,
                    r.n_rounds,
                    r.ok_rounds,
                    f"{r.ok_rate:.6f}",
                    f"{r.goodput_gbps_p50_ok:.6f}",
                    f"{r.goodput_gbps_p95_ok:.6f}",
                    f"{r.elapsed_s_p50_ok:.6f}",
                    f"{r.elapsed_s_p95_ok:.6f}",
                    "" if r.planner_tput_est_p50_mbps is None else f"{r.planner_tput_est_p50_mbps:.6f}",
                ]
            )


def write_group_csv(out_csv: pathlib.Path, runs: list[RunSummary]) -> None:
    grouped: dict[tuple[str, str], list[RunSummary]] = {}
    for r in runs:
        grouped.setdefault((r.key.profile, r.key.method), []).append(r)

    out_csv.parent.mkdir(parents=True, exist_ok=True)
    with out_csv.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(
            [
                "profile",
                "method",
                "n_runs",
                "n_rounds_total",
                "ok_rate_total",
                "goodput_gbps_p50_ok",
                "goodput_gbps_p95_ok",
                "elapsed_s_p50_ok",
                "elapsed_s_p95_ok",
                "planner_tput_est_p50_mbps",
            ]
        )
        for (profile, method), group_runs in sorted(grouped.items()):
            goodputs = [v for r in group_runs for v in r.goodput_gbps_ok]
            elapsed = [v for r in group_runs for v in r.elapsed_s_ok]
            planner = [
                r.planner_tput_est_p50_mbps
                for r in group_runs
                if r.planner_tput_est_p50_mbps is not None
            ]
            n_rounds_total = sum(r.n_rounds for r in group_runs)
            ok_rounds_total = sum(r.ok_rounds for r in group_runs)
            ok_rate_total = (ok_rounds_total / n_rounds_total) if n_rounds_total else 0.0
            writer.writerow(
                [
                    profile,
                    method,
                    len(group_runs),
                    n_rounds_total,
                    f"{ok_rate_total:.6f}",
                    f"{_p50(goodputs):.6f}",
                    f"{_p95(goodputs):.6f}",
                    f"{_p50(elapsed):.6f}",
                    f"{_p95(elapsed):.6f}",
                    "" if not planner else f"{_p50([float(v) for v in planner]):.6f}",
                ]
            )


def write_scatter_csv(out_csv: pathlib.Path, runs: list[RunSummary]) -> None:
    """Write per-run (profile,method,seed) points for predicted-vs-measured plots."""
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    with out_csv.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(
            [
                "profile",
                "method",
                "capacity_seed",
                "selection_seed",
                "planner_tput_est_mbps",
                "measured_goodput_mbps_p50",
                "ok_rate",
            ]
        )
        for r in runs:
            if r.planner_tput_est_p50_mbps is None:
                continue
            writer.writerow(
                [
                    r.key.profile,
                    r.key.method,
                    r.key.capacity_seed,
                    r.key.selection_seed,
                    f"{float(r.planner_tput_est_p50_mbps):.6f}",
                    f"{float(r.goodput_gbps_p50_ok) * 1000.0:.6f}",
                    f"{float(r.ok_rate):.6f}",
                ]
            )


def pearson_r(points: list[tuple[float, float]]) -> float | None:
    if len(points) < 2:
        return None
    xs = [p[0] for p in points]
    ys = [p[1] for p in points]
    mean_x = statistics.mean(xs)
    mean_y = statistics.mean(ys)
    num = sum((x - mean_x) * (y - mean_y) for x, y in points)
    den_x = sum((x - mean_x) ** 2 for x in xs)
    den_y = sum((y - mean_y) ** 2 for y in ys)
    den = (den_x * den_y) ** 0.5
    if den <= 0.0:
        return None
    return float(num / den)


def main() -> int:
    parser = argparse.ArgumentParser(description="Summarize single-host sweep results.")
    parser.add_argument(
        "--input-dir",
        required=True,
        help="Directory containing profile=*_cap=*_method=*_sel=*_w=*_r=*_b=*.json files.",
    )
    parser.add_argument("--out-run-csv", default="")
    parser.add_argument("--out-group-csv", default="")
    parser.add_argument("--out-scatter-csv", default="")
    args = parser.parse_args()

    input_dir = pathlib.Path(args.input_dir).resolve()
    if not input_dir.is_dir():
        raise SystemExit(f"--input-dir is not a directory: {input_dir}")

    run_files = iter_run_files(input_dir)
    if not run_files:
        raise SystemExit(f"no run json files found in {input_dir}")

    runs = [summarize_run(path) for path in run_files]

    out_run_csv = pathlib.Path(args.out_run_csv) if str(args.out_run_csv).strip() else input_dir / "run_summary.csv"
    out_group_csv = (
        pathlib.Path(args.out_group_csv) if str(args.out_group_csv).strip() else input_dir / "group_summary.csv"
    )
    out_scatter_csv = (
        pathlib.Path(args.out_scatter_csv) if str(args.out_scatter_csv).strip() else input_dir / "scatter_runs.csv"
    )
    write_run_csv(out_run_csv, runs)
    write_group_csv(out_group_csv, runs)
    write_scatter_csv(out_scatter_csv, runs)

    print(f"Wrote: {out_run_csv}")
    print(f"Wrote: {out_group_csv}")
    print(f"Wrote: {out_scatter_csv}")

    scatter_points: list[tuple[float, float]] = []
    for r in runs:
        if r.planner_tput_est_p50_mbps is None:
            continue
        if r.ok_rounds <= 0:
            continue
        scatter_points.append((float(r.planner_tput_est_p50_mbps), float(r.goodput_gbps_p50_ok) * 1000.0))

    r_val = pearson_r(scatter_points)
    if r_val is not None:
        print(f"Pearson r (planner vs measured p50 goodput): {r_val:.4f} over {len(scatter_points)} runs")
    return 0


if __name__ == "__main__":
    sys.exit(main())
