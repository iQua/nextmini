#!/usr/bin/env python3

from __future__ import annotations

import argparse
import concurrent.futures
import dataclasses
import os
import pathlib
import shlex
import subprocess
import sys
import typing as t


REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]


def _expand_remote_home(path: str) -> str:
    """Expand a leading ~ or ~/ to $HOME for remote bash commands."""
    if path == "~":
        return "$HOME"
    if path.startswith("~/"):
        return "$HOME/" + path[2:]
    return path


def _expand_remote_home_for_scp(path: str) -> str:
    """Convert a leading $HOME back to ~ for rsync/scp-style remote paths."""
    if path == "$HOME":
        return "~"
    if path.startswith("$HOME/"):
        return "~/" + path[len("$HOME/") :]
    return path


def _bash_dquote(expr: str) -> str:
    """Double-quote for bash while allowing $VAR expansion."""
    expr = expr.replace("\\", "\\\\").replace('"', '\\"')
    return f'"{expr}"'


@dataclasses.dataclass(frozen=True)
class Host:
    label: str
    host: str
    user: str
    port: int
    identity_file: str | None

    def display(self) -> str:
        return f"{self.label} ({self.user}@{self.host}:{self.port})"


def _parse_ssh_target(target: str, *, default_user: str, default_port: int) -> tuple[str, str, int]:
    """Parse user@host[:port] into (user, host, port)."""
    target = target.strip()
    if not target:
        raise ValueError("empty ssh target")

    user = default_user
    host_part = target
    if "@" in target:
        user, host_part = target.split("@", 1)
        user = user.strip() or default_user
        host_part = host_part.strip()

    port = default_port
    if host_part.count(":") == 1:
        maybe_host, maybe_port = host_part.rsplit(":", 1)
        if maybe_port.isdigit():
            host_part = maybe_host
            port = int(maybe_port)

    host_part = host_part.strip()
    if not host_part:
        raise ValueError(f"invalid ssh target: {target!r}")
    return user, host_part, port


def load_hosts_file(
    path: pathlib.Path,
    *,
    default_user: str,
    default_port: int,
    identity_file: str | None,
) -> list[Host]:
    """Load hosts from the bare-metal hosts.txt format: node_id|user@host|public_ip."""
    raw = path.read_text(encoding="utf-8")
    hosts: list[Host] = []
    for lineno, line in enumerate(raw.splitlines(), start=1):
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue

        parts = [p.strip() for p in stripped.split("|")]
        if len(parts) < 2:
            raise SystemExit(f"{path}:{lineno}: expected 'node_id|user@host|public_ip', got: {stripped!r}")

        label = parts[0] or f"line-{lineno}"
        ssh_target = parts[1]
        try:
            user, host, port = _parse_ssh_target(
                ssh_target,
                default_user=default_user,
                default_port=default_port,
            )
        except ValueError as exc:
            raise SystemExit(f"{path}:{lineno}: {exc}") from exc

        hosts.append(
            Host(
                label=label,
                host=host,
                user=user,
                port=port,
                identity_file=identity_file,
            )
        )

    if not hosts:
        raise SystemExit(f"{path}: no hosts found")
    return hosts


def load_inventory_file(
    path: pathlib.Path,
    *,
    default_user: str,
    default_port: int,
    identity_file: str | None,
) -> tuple[list[Host], str | None]:
    """Load hosts from an inventory.toml file.

    Supported schema (xindan-style):
    - [ssh] user/port/identity_file defaults
    - [paths] remote_repo_dir (optional)
    - [controller] host[/user/port/identity_file] (optional)
    - [[nodes]] host[/user/port/identity_file], node_id, role (optional)

    The sync/run helper only needs SSH targets; extra fields are ignored.
    """
    try:
        import tomllib  # Python 3.11+
    except ImportError:  # pragma: no cover
        try:
            import tomli as tomllib  # type: ignore[no-redef]
        except ImportError as exc:  # pragma: no cover
            raise SystemExit(
                "TOML inventory parsing requires Python 3.11+ (tomllib) or `pip install tomli`."
            ) from exc

    raw = tomllib.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise SystemExit(f"{path}: expected a TOML table at the top level")

    ssh_cfg = raw.get("ssh", {}) or {}
    if not isinstance(ssh_cfg, dict):
        raise SystemExit(f"{path}: [ssh] must be a table")
    inv_default_user = str(ssh_cfg.get("user", default_user))
    inv_default_port = int(ssh_cfg.get("port", default_port))
    inv_identity_file = ssh_cfg.get("identity_file")
    inv_identity_file = str(inv_identity_file) if inv_identity_file else identity_file

    paths_cfg = raw.get("paths", {}) or {}
    if paths_cfg and not isinstance(paths_cfg, dict):
        raise SystemExit(f"{path}: [paths] must be a table")
    remote_repo_dir = None
    if isinstance(paths_cfg, dict):
        maybe_remote = paths_cfg.get("remote_repo_dir")
        if maybe_remote:
            remote_repo_dir = str(maybe_remote)

    hosts: list[Host] = []

    controller_cfg = raw.get("controller", {}) or {}
    if controller_cfg:
        if not isinstance(controller_cfg, dict):
            raise SystemExit(f"{path}: [controller] must be a table")
        controller_host = str(controller_cfg.get("host", "")).strip()
        if controller_host:
            controller_user = str(controller_cfg.get("user", inv_default_user))
            controller_port = int(controller_cfg.get("port", inv_default_port))
            controller_identity = (
                str(controller_cfg.get("identity_file", inv_identity_file))
                if controller_cfg.get("identity_file")
                else inv_identity_file
            )
            user, host, port = _parse_ssh_target(
                controller_host,
                default_user=controller_user,
                default_port=controller_port,
            )
            hosts.append(
                Host(
                    label="controller",
                    host=host,
                    user=user,
                    port=port,
                    identity_file=controller_identity,
                )
            )

    nodes_raw = raw.get("nodes", []) or []
    if nodes_raw:
        if not isinstance(nodes_raw, list):
            raise SystemExit(f"{path}: [[nodes]] must be an array of tables")
        for idx, entry in enumerate(nodes_raw):
            if not isinstance(entry, dict):
                raise SystemExit(f"{path}: nodes[{idx}] must be a table")
            node_host = str(entry.get("host", "")).strip()
            if not node_host:
                raise SystemExit(f"{path}: nodes[{idx}].host is required")
            node_user = str(entry.get("user", inv_default_user))
            node_port = int(entry.get("port", inv_default_port))
            node_identity = str(entry.get("identity_file", inv_identity_file)) if entry.get("identity_file") else inv_identity_file
            role = str(entry.get("role", "")).strip() or "node"
            node_id = entry.get("node_id")
            label = f"{role}-{node_id}" if node_id is not None else f"{role}-{idx}"
            user, host, port = _parse_ssh_target(node_host, default_user=node_user, default_port=node_port)
            hosts.append(
                Host(
                    label=label,
                    host=host,
                    user=user,
                    port=port,
                    identity_file=node_identity,
                )
            )

    if not hosts:
        raise SystemExit(f"{path}: no SSH hosts found (expected [controller].host and/or [[nodes]].host)")

    return hosts, remote_repo_dir


def _expand_user(path: str) -> str:
    return os.path.expanduser(path)


def _ssh_base_args(host: Host, *, batch: bool) -> list[str]:
    args = ["ssh", "-p", str(host.port)]
    if host.identity_file:
        args += ["-i", _expand_user(host.identity_file)]
    # Avoid interactive host-key prompts on freshly provisioned VMs.
    args += ["-o", "StrictHostKeyChecking=accept-new"]
    if batch:
        args += ["-o", "BatchMode=yes"]
    # Keep logs readable and avoid hanging forever.
    args += ["-o", "ConnectTimeout=10"]
    return args + [f"{host.user}@{host.host}"]


def _run(
    cmd: list[str],
    *,
    capture: bool = False,
    check: bool = True,
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        cmd,
        check=check,
        text=True,
        stdout=subprocess.PIPE if capture else None,
        stderr=subprocess.STDOUT if capture else None,
    )


def ssh_run(host: Host, command: str, *, batch: bool, capture: bool = False) -> str:
    wrapped = f"set -euo pipefail\n{command}"
    args = _ssh_base_args(host, batch=batch) + ["bash", "-lc", shlex.quote(wrapped)]
    try:
        res = _run(args, capture=capture, check=True)
        return res.stdout or ""
    except subprocess.CalledProcessError as exc:
        out = exc.stdout or ""
        raise SystemExit(f"Remote command failed on {host.display()}.\nOutput:\n{out}") from exc


def rsync_repo(
    host: Host,
    *,
    remote_repo_dir: str,
    batch: bool,
    delete: bool,
    verbose: bool,
) -> None:
    ssh_cmd = _ssh_base_args(host, batch=batch)
    # rsync wants the remote shell as a string; exclude the user@host part (last element).
    ssh_shell = " ".join(shlex.quote(part) for part in ssh_cmd[:-1])

    excludes = [
        ".git/",
        "target/",
        "**/target/",
        "docs/site/",
        "docs/docs/api/rustdoc/",
        "**/__pycache__/",
        "**/.venv/",
        "**/.mypy_cache/",
        "**/.pytest_cache/",
        ".multidc_cache/",
        "**/.multidc_cache/**",
        "oracle_out/",
    ]

    cmd = ["rsync", "-az", "--stats"]
    if delete:
        cmd.append("--delete")
    for item in excludes:
        cmd += ["--exclude", item]

    dest = f"{host.user}@{host.host}:{_expand_remote_home_for_scp(remote_repo_dir)}/"
    cmd += ["-e", ssh_shell, str(REPO_ROOT) + "/", dest]

    result = _run(cmd, capture=not verbose, check=False)
    # Tolerate rsync exit code 23 (partial transfer due to error) which often happens
    # with special files or symlinks that don't affect core code.
    if result.returncode not in (0, 23):
        out = result.stdout or ""
        raise SystemExit(f"rsync failed on {host.display()} (exit={result.returncode}).\nOutput:\n{out}")


def _iter_hosts(args: argparse.Namespace) -> tuple[list[Host], str | None]:
    hosts: list[Host] = []
    inventory_remote_repo_dir: str | None = None
    if getattr(args, "inventory", None):
        inv_hosts, inventory_remote_repo_dir = load_inventory_file(
            pathlib.Path(args.inventory),
            default_user=args.user,
            default_port=args.port,
            identity_file=args.identity_file,
        )
        hosts.extend(inv_hosts)
    if args.hosts_file:
        hosts.extend(
            load_hosts_file(
                pathlib.Path(args.hosts_file),
                default_user=args.user,
                default_port=args.port,
                identity_file=args.identity_file,
            )
        )
    for raw in args.hosts or []:
        user, host, port = _parse_ssh_target(raw, default_user=args.user, default_port=args.port)
        hosts.append(
            Host(
                label=host,
                host=host,
                user=user,
                port=port,
                identity_file=args.identity_file,
            )
        )

    if not hosts:
        raise SystemExit("no hosts provided (use --inventory, --hosts-file, or --hosts)")

    seen: set[tuple[str, str, int, str | None]] = set()
    deduped: list[Host] = []
    for host in hosts:
        key = (host.user, host.host, host.port, host.identity_file)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(host)

    return deduped, inventory_remote_repo_dir


def _run_parallel(
    hosts: list[Host],
    *,
    jobs: int,
    fn: t.Callable[[Host], None],
) -> None:
    errors: list[str] = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=jobs) as pool:
        future_by_host = {pool.submit(fn, h): h for h in hosts}
        for fut in concurrent.futures.as_completed(future_by_host):
            host = future_by_host[fut]
            try:
                fut.result()
                print(f"[ok] {host.display()}", flush=True)
            except SystemExit as exc:
                errors.append(str(exc))
            except Exception as exc:  # pragma: no cover
                errors.append(f"{host.display()}: {exc}")

    if errors:
        joined = "\n\n".join(errors)
        raise SystemExit(f"{len(errors)} host(s) failed:\n\n{joined}")


def cmd_sync(args: argparse.Namespace) -> None:
    hosts, inventory_remote_repo_dir = _iter_hosts(args)

    remote_repo_dir = str(args.remote_repo_dir or inventory_remote_repo_dir or "")
    if not remote_repo_dir:
        raise SystemExit("--remote-repo-dir is required (or set [paths].remote_repo_dir in --inventory)")
    repo_expr = _expand_remote_home(remote_repo_dir)
    repo_q = _bash_dquote(repo_expr)

    def _one(host: Host) -> None:
        ssh_run(host, f"mkdir -p {repo_q}", batch=args.batch_ssh)
        rsync_repo(
            host,
            remote_repo_dir=remote_repo_dir,
            batch=args.batch_ssh,
            delete=bool(args.delete),
            verbose=bool(args.verbose),
        )

    _run_parallel(hosts, jobs=int(args.jobs), fn=_one)


def cmd_run(args: argparse.Namespace) -> None:
    hosts, _inventory_remote_repo_dir = _iter_hosts(args)
    command = str(args.cmd).strip()
    if not command:
        raise SystemExit("--cmd is required")

    def _one(host: Host) -> None:
        out = ssh_run(host, command, batch=args.batch_ssh, capture=True)
        if out.strip():
            sys.stdout.write(f"\n[{host.display()}]\n{out}\n")
            sys.stdout.flush()

    _run_parallel(hosts, jobs=int(args.jobs), fn=_one)


def cmd_sync_run(args: argparse.Namespace) -> None:
    cmd_sync(args)
    cmd_run(args)


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Parallel rsync/ssh helper for Nextmini deployments.")
    p.add_argument(
        "--inventory",
        help="Path to inventory.toml (supports [ssh]/[paths]/[controller]/[[nodes]]).",
    )
    p.add_argument("--hosts-file", help="Path to hosts.txt (node_id|user@host|public_ip).")
    p.add_argument("--hosts", action="append", help="Extra SSH targets (user@host[:port]).")
    p.add_argument("--user", default="ubuntu", help="Default SSH user when omitted.")
    p.add_argument("--port", type=int, default=22, help="Default SSH port when omitted.")
    p.add_argument("--identity-file", help="SSH identity file (passed to ssh/rsync).")
    p.add_argument("--jobs", type=int, default=8, help="Max parallelism.")
    p.add_argument("--batch-ssh", action="store_true", help="Use BatchMode=yes (non-interactive).")
    p.add_argument("--verbose", action="store_true", help="Show full rsync output (may interleave).")

    sub = p.add_subparsers(dest="cmd", required=True)

    p_sync = sub.add_parser("sync", help="rsync the local repo to all hosts.")
    p_sync.add_argument("--remote-repo-dir", help="Remote path to the Nextmini checkout.")
    p_sync.add_argument("--delete", action="store_true", help="Mirror: delete files on remote not present locally.")
    p_sync.set_defaults(fn=cmd_sync)

    p_run = sub.add_parser("run", help="Run a bash command on all hosts.")
    p_run.add_argument("--cmd", required=True, help="Command to run remotely (executed via bash -lc).")
    p_run.set_defaults(fn=cmd_run)

    p_sync_run = sub.add_parser("sync-run", help="rsync the local repo to all hosts, then run a command.")
    p_sync_run.add_argument("--remote-repo-dir", help="Remote path to the Nextmini checkout.")
    p_sync_run.add_argument(
        "--cmd",
        required=True,
        help="Command to run remotely after syncing (executed via bash -lc).",
    )
    p_sync_run.add_argument(
        "--delete",
        action="store_true",
        help="Mirror: delete files on remote not present locally.",
    )
    p_sync_run.set_defaults(fn=cmd_sync_run)

    args = p.parse_args(argv)
    t.cast(t.Callable[[argparse.Namespace], None], args.fn)(args)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
