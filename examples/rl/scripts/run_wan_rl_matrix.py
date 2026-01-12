#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime as dt
import json
import pathlib
import shlex
import subprocess
import sys
from typing import Sequence


REPO_ROOT = pathlib.Path(__file__).resolve().parents[3]
RESULTS_DIR = REPO_ROOT / "examples" / "rl" / "multidc" / "results"


def _now_slug() -> str:
    return dt.datetime.now().strftime("%Y%m%d_%H%M%S")


def _run_stream(cmd: list[str], *, cwd: pathlib.Path, log_path: pathlib.Path) -> int:
    """Run a command and stream stdout/stderr to both console and log file."""
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("w", encoding="utf-8") as f:
        p = subprocess.Popen(
            cmd,
            cwd=str(cwd),
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            bufsize=1,
        )
        assert p.stdout is not None
        for line in p.stdout:
            sys.stdout.write(line)
            sys.stdout.flush()
            f.write(line)
            f.flush()
        return int(p.wait())


def _ensure_dir(p: pathlib.Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def _read_json(path: pathlib.Path) -> object:
    return json.loads(path.read_text(encoding="utf-8"))


def archive_last_run_into(run_dir: pathlib.Path, *, label: str) -> None:
    """Archive the most recent run-rl outputs into run_dir."""
    _ensure_dir(run_dir)

    for name in ("rl_stdout.txt", "rl_manifest.json"):
        src = RESULTS_DIR / name
        if src.exists():
            (run_dir / name).write_bytes(src.read_bytes())

    idx: dict[str, object] = {"label": label}
    manifest = run_dir / "rl_manifest.json"
    if manifest.exists():
        try:
            idx["manifest"] = _read_json(manifest)
        except Exception:
            pass
    (run_dir / "index.json").write_text(json.dumps(idx, indent=2), encoding="utf-8")


def run_rl(
    *,
    inventory: pathlib.Path,
    batch_ssh: bool,
    args: Sequence[str],
    label: str,
    out_dir: pathlib.Path,
) -> pathlib.Path:
    cmd = [
        sys.executable,
        str(REPO_ROOT / "examples" / "rl" / "scripts" / "multidc.py"),
        "run-rl",
        "--inventory",
        str(inventory),
    ]
    if batch_ssh:
        cmd.append("--batch-ssh")
    cmd += list(args)

    run_dir = out_dir / f"{_now_slug()}_{label}"
    _ensure_dir(run_dir)

    print(f"\n=== RUN {label} ===", flush=True)
    print("cmd:", " ".join(shlex.quote(x) for x in cmd), flush=True)

    rc = _run_stream(cmd, cwd=REPO_ROOT, log_path=run_dir / "stdout.txt")
    archive_last_run_into(run_dir, label=label)

    if rc != 0:
        raise SystemExit(f"run-rl failed (label={label}). See {run_dir}/stdout.txt")

    print(f"archived => {run_dir}", flush=True)
    return run_dir


def main() -> int:
    p = argparse.ArgumentParser(description="Run WAN RL step and archive each run's outputs.")
    p.add_argument("--inventory", type=pathlib.Path, required=True)
    p.add_argument("--batch-ssh", action="store_true")
    p.add_argument(
        "--out-dir",
        type=pathlib.Path,
        default=REPO_ROOT / "examples" / "rl" / "multidc" / "archives",
    )
    p.add_argument("--label", type=str, default="manual")
    p.add_argument(
        "--args",
        type=str,
        default="",
        help="Extra args to pass to multidc.py run-rl (as a single shell-like string).",
    )

    args = p.parse_args()
    out_root = args.out_dir / _now_slug()
    _ensure_dir(out_root)

    extra = shlex.split(args.args) if args.args else []
    run_rl(
        inventory=args.inventory,
        batch_ssh=bool(args.batch_ssh),
        args=extra,
        label=args.label,
        out_dir=out_root,
    )
    print(f"\nAll outputs under: {out_root}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

