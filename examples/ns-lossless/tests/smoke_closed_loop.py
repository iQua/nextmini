#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import pathlib
import subprocess
import sys
import tempfile


ROOT = pathlib.Path(__file__).parents[3]
NS_DIR = pathlib.Path(__file__).parents[1]
TOOLS_DIR = NS_DIR / "tools"
FIXTURE_TOML = """\
name = "tiny-closed-loop"
source = 1
receivers = [4, 5, 6]
forwarding_nodes = [2, 3]
uplink_bw = 90.0
downlink_bw = 60.0

[heterogeneity]
delay_ms = 2.0
jitter_ms = 0.25
loss_pct = 0.0

[[uplinks]]
dst = 2
bw = 90.0

[[uplinks]]
dst = 3
bw = 60.0
"""


def run(*args: str) -> None:
    subprocess.run(args, check=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Dry-run the closed-loop solver chain.")
    parser.add_argument("--paper-repo", type=pathlib.Path, required=True)
    parser.add_argument(
        "--python",
        type=pathlib.Path,
        help="Python with the paper solver and scipy installed.",
    )
    parser.add_argument(
        "--solver-backend",
        choices=("gurobi", "scipy"),
        default="gurobi",
        help="Use the production edge-based solver by default.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    paper_repo = args.paper_repo.resolve()
    solver = paper_repo / "experiments" / "solve_multitree.py"
    python = args.python or paper_repo / ".venv" / "bin" / "python"
    if not solver.exists():
        raise SystemExit(f"paper solver not found: {solver}")
    if not python.exists():
        raise SystemExit(f"solver Python not found: {python}")

    with tempfile.TemporaryDirectory(prefix="nextmini-closed-loop-") as directory:
        work = pathlib.Path(directory)
        fixture = work / "scenario.toml"
        probe = work / "probe.json"
        inventory = work / "inventory.toml"
        solution = work / "solution.json"
        generated = work / "generated"
        metrics = generated / "artifacts"
        ledger = generated / "run-ledger.json"
        fixture.write_text(FIXTURE_TOML, encoding="utf-8")

        run(
            sys.executable,
            str(TOOLS_DIR / "scenario_adapter.py"),
            str(fixture),
            "--output",
            str(probe),
            "--inventory-output",
            str(inventory),
            "--solver-backend",
            args.solver_backend,
        )
        run(
            str(python),
            str(solver),
            "--inventory",
            str(inventory),
            "--probe-json",
            str(probe),
            "--output",
            str(solution),
        )

        solved = json.loads(solution.read_text(encoding="utf-8"))
        actual_backend = (
            solved.get("pricing_backend")
            if args.solver_backend == "gurobi"
            else solved.get("lp_backend")
        )
        if actual_backend != args.solver_backend:
            raise RuntimeError(
                f"smoke did not use {args.solver_backend}: {actual_backend}"
            )
        scenario = solved["scenario"]
        receiver_ids = {int(node_id) for node_id in scenario["receivers"]}
        forwarding_nodes = {
            int(node_id) for node_id in scenario.get("forwarding_nodes", [])
        }
        if not receiver_ids <= forwarding_nodes:
            raise RuntimeError("smoke scenario did not make every receiver forwarding-eligible")
        receiver_pairs = {
            (int(edge["src"]), int(edge["dst"]))
            for edge in scenario["edges"]
            if int(edge["src"]) in receiver_ids and int(edge["dst"]) in receiver_ids
        }
        expected_receiver_pairs = {
            (src, dst)
            for src in receiver_ids
            for dst in receiver_ids
            if src != dst
        }
        if receiver_pairs != expected_receiver_pairs:
            raise RuntimeError("smoke scenario did not contain the complete directed D->D layer")
        solved_receiver_edges = {
            (int(src), int(dst))
            for tree in solved["trees"]
            for src, dst in tree["edges"]
            if int(src) in receiver_ids and int(dst) in receiver_ids
        }
        if not solved_receiver_edges:
            raise RuntimeError("smoke solution did not exercise receiver-as-relay routing")
        tree_count = len(solved["trees"])
        receiver_count = len(solved["scenario"]["receivers"])
        run(
            sys.executable,
            str(NS_DIR / "generate.py"),
            "--case-name",
            "closed-loop-smoke",
            "--mode",
            "fec",
            "--fec-scheme",
            "raptorq",
            "--receivers",
            str(receiver_count),
            "--trees",
            str(tree_count),
            "--solution-json",
            str(solution),
            "--block-size",
            "8192",
            "--symbols-per-block",
            "32",
            "--payload-size",
            "125000",
            "--synthetic-payload",
            "--packet-processors",
            str(tree_count + 1),
            "--out-dir",
            str(generated),
        )
        run(
            sys.executable,
            str(TOOLS_DIR / "check_laminarity.py"),
            str(solution),
            "--output",
            str(generated / "laminarity.json"),
            "--ledger",
            str(ledger),
        )

        metrics.mkdir(parents=True, exist_ok=True)
        for receiver in solved["scenario"]["receivers"]:
            (metrics / f"receiver-{receiver}.metrics").write_text(
                f"role=receiver\nnode_id={receiver}\npayload_bytes=125000\n"
                "duration_seconds=0.010000000\nthroughput_gbps=0.100000000\n",
                encoding="utf-8",
            )
        run(
            str(python),
            str(TOOLS_DIR / "compare_results.py"),
            str(solution),
            "--metrics-dir",
            str(metrics),
            "--output",
            str(generated / "comparison.json"),
            "--ledger",
            str(ledger),
        )

        final_ledger = json.loads(ledger.read_text(encoding="utf-8"))
        expected_sections = {"worker_feasibility", "laminarity", "benchmark_comparison"}
        if not expected_sections <= final_ledger.keys():
            raise RuntimeError(
                f"smoke ledger missing sections: {expected_sections - final_ledger.keys()}"
            )
        if not final_ledger["worker_feasibility"]["feasible"]:
            raise RuntimeError("smoke worker contract was unexpectedly degraded")
        print(
            json.dumps(
                {
                    "status": "PASS",
                    "solver_backend": args.solver_backend,
                    "tree_count": tree_count,
                    "receiver_count": receiver_count,
                    "laminar": final_ledger["laminarity"]["laminar"],
                    "pi_star_mbit": final_ledger["benchmark_comparison"]["pi_star_mbit"],
                    "node_caps": solved["scenario"].get("node_caps"),
                },
                indent=2,
            )
        )


if __name__ == "__main__":
    main()
