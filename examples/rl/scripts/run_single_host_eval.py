#!/usr/bin/env python3

from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import os
import pathlib
import statistics
import subprocess
import sys
from dataclasses import dataclass


REPO_ROOT = pathlib.Path(__file__).resolve().parents[3]
SINGLE_HOST = REPO_ROOT / "examples" / "rl" / "scripts" / "single_host.py"


def _split_csv(value: str) -> list[str]:
    return [part.strip() for part in value.split(",") if part.strip()]


def _mkdir(path: pathlib.Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def _p50(values: list[float]) -> float:
    return statistics.median(values)


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


@dataclass(frozen=True)
class RunSummary:
    key: RunKey
    n_rounds: int
    bytes: int
    goodput_gbps_p50: float
    goodput_gbps_p95: float
    elapsed_s_p50: float
    elapsed_s_p95: float
    planner_tput_est_p50: float | None


def summarize_results(path: pathlib.Path, key: RunKey) -> RunSummary:
    obj = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(obj, list):
        raise ValueError(f"expected a list of records in {path}")
    elapsed = [float(r["elapsed_s"]) for r in obj]
    goodput = [float(r["goodput_gbps"]) for r in obj]
    bytes_ = int(obj[0]["bytes"]) if obj else 0
    planner_vals = [
        float(r["planner_tput_est"])
        for r in obj
        if r.get("planner_tput_est") is not None
    ]
    planner_p50 = _p50(planner_vals) if planner_vals else None
    return RunSummary(
        key=key,
        n_rounds=len(obj),
        bytes=bytes_,
        goodput_gbps_p50=_p50(goodput),
        goodput_gbps_p95=_p95(goodput),
        elapsed_s_p50=_p50(elapsed),
        elapsed_s_p95=_p95(elapsed),
        planner_tput_est_p50=planner_p50,
    )


def run_one(
    *,
    out_json: pathlib.Path,
    workers: int,
    relays: int,
    budget: int,
    hop_limit: int,
    eta: float,
    num_paths: int,
    relay_scoring: str,
    algorithm: str,
    rounds: int,
    timeout_ms: int,
    bytes_: int,
    profile: str,
    capacity_seed: int,
    method: str,
    selection_seed: int,
    bucket_secs: float,
) -> None:
    _mkdir(out_json.parent)
    try:
        out_json_rel = out_json.relative_to(REPO_ROOT).as_posix()
    except ValueError as exc:
        raise ValueError(f"--out-dir must be inside repo root ({REPO_ROOT}), got {out_json}") from exc
    env = os.environ.copy()
    env.setdefault("SKIP_BUILD", "1")

    cmd = [
        sys.executable,
        str(SINGLE_HOST),
        "run",
        "--mode",
        "broadcast",
        "--no-gpu",
        "--no-build-images",
        "--workers",
        str(workers),
        "--relays",
        str(relays),
        "--bytes",
        str(bytes_),
        "--rounds",
        str(rounds),
        "--timeout-ms",
        str(timeout_ms),
        "--algorithm",
        str(algorithm),
        "--hop-limit",
        str(hop_limit),
        "--eta",
        str(eta),
        "--num-paths",
        str(num_paths),
        "--relay-scoring",
        str(relay_scoring),
        "--max-relays",
        str(budget),
        "--relay-selection",
        str(method),
        "--selection-seed",
        str(selection_seed),
        "--capacity-profile",
        str(profile),
        "--capacity-seed",
        str(capacity_seed),
        "--bucket-secs",
        str(bucket_secs),
        "--no-probe-links",
        "--results-json",
        out_json_rel,
    ]
    subprocess.run(cmd, check=True, env=env, cwd=str(REPO_ROOT))


def main() -> int:
    parser = argparse.ArgumentParser(description="Run single-host broadcast eval sweep.")
    parser.add_argument("--profiles", default="lp_trap,multi_tier,random_wide")
    parser.add_argument("--methods", default="lp,capacity,random")
    parser.add_argument("--capacity-seeds", default="0,1,2")
    parser.add_argument("--selection-seeds", default="0,1,2")
    parser.add_argument("--workers", type=int, default=6)
    parser.add_argument("--relays", type=int, default=6)
    parser.add_argument("--budget", type=int, default=2)
    parser.add_argument("--hop-limit", type=int, default=4)
    parser.add_argument("--eta", type=float, default=0.1)
    parser.add_argument("--num-paths", type=int, default=2)
    parser.add_argument("--relay-scoring", default="coverage")
    parser.add_argument("--algorithm", default="cf_bottleneck_mwu")
    parser.add_argument("--rounds", type=int, default=5)
    parser.add_argument("--timeout-ms", type=int, default=300_000)
    parser.add_argument("--bytes", type=int, default=64 * 1024 * 1024)
    parser.add_argument("--bucket-secs", type=float, default=3.0)
    parser.add_argument("--out-dir", default="")
    args = parser.parse_args()

    profiles = _split_csv(args.profiles)
    methods = _split_csv(args.methods)
    capacity_seeds = [int(v) for v in _split_csv(args.capacity_seeds)]
    selection_seeds = [int(v) for v in _split_csv(args.selection_seeds)]

    timestamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    if str(args.out_dir).strip():
        out_dir = pathlib.Path(args.out_dir)
        if not out_dir.is_absolute():
            out_dir = (REPO_ROOT / out_dir).resolve()
    else:
        out_dir = REPO_ROOT / "examples" / "rl" / "single_host" / "results" / f"sweep_{timestamp}"
    _mkdir(out_dir)

    summaries: list[RunSummary] = []
    for profile in profiles:
        for cap_seed in capacity_seeds:
            for method in methods:
                seeds = selection_seeds if method == "random" else [0]
                for sel_seed in seeds:
                    key = RunKey(
                        profile=profile,
                        capacity_seed=int(cap_seed),
                        method=method,
                        selection_seed=int(sel_seed),
                    )
                    name = (
                        f"profile={profile}_cap={cap_seed}_method={method}_sel={sel_seed}"
                        f"_w={args.workers}_r={args.relays}_b={args.budget}.json"
                    )
                    out_json = out_dir / name
                    print(f"[run] {out_json}", flush=True)
                    run_one(
                        out_json=out_json,
                        workers=int(args.workers),
                        relays=int(args.relays),
                        budget=int(args.budget),
                        hop_limit=int(args.hop_limit),
                        eta=float(args.eta),
                        num_paths=int(args.num_paths),
                        relay_scoring=str(args.relay_scoring),
                        algorithm=str(args.algorithm),
                        rounds=int(args.rounds),
                        timeout_ms=int(args.timeout_ms),
                        bytes_=int(args.bytes),
                        profile=str(profile),
                        capacity_seed=int(cap_seed),
                        method=str(method),
                        selection_seed=int(sel_seed),
                        bucket_secs=float(args.bucket_secs),
                    )
                    summaries.append(summarize_results(out_json, key))

    csv_path = out_dir / "summary.csv"
    with csv_path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(
            [
                "profile",
                "capacity_seed",
                "method",
                "selection_seed",
                "n_rounds",
                "bytes",
                "goodput_gbps_p50",
                "goodput_gbps_p95",
                "elapsed_s_p50",
                "elapsed_s_p95",
                "planner_tput_est_p50",
            ]
        )
        for s in summaries:
            writer.writerow(
                [
                    s.key.profile,
                    s.key.capacity_seed,
                    s.key.method,
                    s.key.selection_seed,
                    s.n_rounds,
                    s.bytes,
                    f"{s.goodput_gbps_p50:.6f}",
                    f"{s.goodput_gbps_p95:.6f}",
                    f"{s.elapsed_s_p50:.6f}",
                    f"{s.elapsed_s_p95:.6f}",
                    "" if s.planner_tput_est_p50 is None else f"{s.planner_tput_est_p50:.6f}",
                ]
            )
    print(f"[done] wrote {csv_path}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
