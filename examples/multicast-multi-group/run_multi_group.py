#!/usr/bin/env python3
"""
Helper launcher (Python version) for the multicast multi-group demo.

It mirrors the previous shell script: waits for the controller, sets up a
virtualenv (unless SKIP_BUILD=1), installs/loads nextmini_py, and runs the main
demo script with any extra CLI arguments.
"""

from __future__ import annotations

import argparse
import os
import socket
import subprocess
import sys
import time
from contextlib import contextmanager
from pathlib import Path

import fcntl


LOCK_PATH = Path("target/.nextmini_build.lock")


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Launch the multi-group demo roles.")
    parser.add_argument("role", choices=("source", "receiver"))
    parser.add_argument("config", type=Path)
    parser.add_argument("extra", nargs=argparse.REMAINDER)
    return parser.parse_args(argv)


def wait_for_host(wait_for: str, attempts: int) -> None:
    host, sep, port_str = wait_for.partition(":")
    if not sep or not port_str.isdigit():
        raise ValueError(f"WAIT_FOR must be host:port (got {wait_for!r})")
    port = int(port_str)
    for attempt in range(1, attempts + 1):
        try:
            with socket.create_connection((host, port), timeout=2):
                print(f"Controller reachable (attempt {attempt}).", flush=True)
                return
        except OSError:
            time.sleep(1)
    raise RuntimeError(f"Timed out waiting for {host}:{port}")


def ensure_cmd(cmd: list[str], env: dict[str, str] | None = None) -> None:
    subprocess.check_call(cmd, env=env)


def install_wheel(wheel_path: str) -> None:
    ensure_cmd([sys.executable, "-m", "pip", "install", wheel_path])


def resolve_wheel() -> str:
    explicit = os.environ.get("NEXTMINI_PY_WHEEL")
    if explicit:
        return explicit
    candidates = sorted(Path("/workspace/target/wheels").glob("nextmini_py-*.whl"))
    if not candidates:
        raise FileNotFoundError("nextmini_py wheel not found. Set NEXTMINI_PY_WHEEL or build one.")
    return str(candidates[-1])


@contextmanager
def build_lock() -> None:
    LOCK_PATH.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(LOCK_PATH, os.O_CREAT | os.O_RDWR, 0o666)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX)
        yield
    finally:
        fcntl.flock(fd, fcntl.LOCK_UN)
        os.close(fd)


def setup_virtualenv(name: str) -> tuple[str, dict[str, str]]:
    if subprocess.call(["which", "uv"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL) != 0:
        ensure_cmd([sys.executable, "-m", "pip", "install", "--no-cache-dir", "uv"])

    venv_dir = Path(f".venv_{name}")
    ensure_cmd(["uv", "venv", str(venv_dir)])
    venv_path = venv_dir.resolve()
    env = os.environ.copy()
    env["VIRTUAL_ENV"] = str(venv_path)
    env["PATH"] = f"{venv_path / 'bin'}:{env['PATH']}"

    with build_lock():
        ensure_cmd(["uv", "pip", "install", "maturin[patchelf]"], env=env)
        ensure_cmd(["maturin", "develop", "--release", "-m", "python-api/Cargo.toml"], env=env)
    python_exe = str(venv_path / "bin" / "python")
    return python_exe, env


def run_multi_group_demo(python_exe: str, role: str, config: Path, extra: list[str], env: dict[str, str] | None = None) -> None:
    cmd = [
        python_exe,
        "examples/multicast-multi-group/multi_group_demo.py",
        "--role",
        role,
        "--config",
        str(config),
        *extra,
    ]
    ensure_cmd(cmd, env=env)


def main(argv: list[str]) -> int:
    args = parse_args(argv)

    wait_for = os.environ.get("WAIT_FOR")
    if wait_for:
        attempts = int(os.environ.get("WAIT_ATTEMPTS", "60"))
        wait_for_host(wait_for, attempts)

    os.environ.setdefault("PYTHONUNBUFFERED", "1")

    if os.environ.get("SKIP_BUILD", "0") == "1":
        wheel_path = resolve_wheel()
        install_wheel(wheel_path)
        run_multi_group_demo(sys.executable, args.role, args.config, args.extra)
    else:
        python_exe, env = setup_virtualenv(args.role)
        run_multi_group_demo(python_exe, args.role, args.config, args.extra, env=env)

    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
