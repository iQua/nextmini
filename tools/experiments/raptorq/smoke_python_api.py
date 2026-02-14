#!/usr/bin/env python3
from __future__ import annotations

import argparse
import inspect
import json
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


@dataclass
class CheckResult:
    name: str
    success: bool
    detail: str
    required: bool = True


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Lightweight smoke harness for Python API FEC wiring. "
            "Writes a JSON artifact containing mode/success/timing."
        )
    )
    parser.add_argument(
        "--fec",
        choices=("off", "on"),
        default="off",
        help="Requested transfer mode to validate in the artifact.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        required=True,
        help="Path to write the JSON artifact.",
    )
    parser.add_argument(
        "--assert-success",
        action="store_true",
        help="Exit non-zero if required smoke checks fail.",
    )
    parser.add_argument(
        "--strict-runtime",
        action="store_true",
        help=(
            "Require nextmini_py runtime import + Dataplane/send_data probe to succeed. "
            "Intended for CI environments that build the extension."
        ),
    )
    parser.add_argument(
        "--project-root",
        type=Path,
        default=None,
        help="Optional repo root override. Defaults to parent of tools/.",
    )
    parser.add_argument(
        "--runtime-config",
        type=Path,
        default=None,
        help=(
            "Config path used for strict runtime probing. "
            "Defaults to examples/routes/config.toml when omitted."
        ),
    )
    return parser.parse_args()


def repo_root(args: argparse.Namespace) -> Path:
    if args.project_root is not None:
        return args.project_root.resolve()
    # .../tools/experiments/raptorq/smoke_python_api.py -> repo root
    return Path(__file__).resolve().parents[3]


def check(condition: bool, name: str, detail: str, *, required: bool = True) -> CheckResult:
    return CheckResult(name=name, success=condition, detail=detail, required=required)


def signature_contains_param(signature_obj: Any, param_name: str) -> bool:
    try:
        return param_name in signature_obj.parameters
    except Exception:
        return False


def resolve_runtime_config(root: Path, override: Path | None) -> Path:
    if override is not None:
        return override.resolve()
    return root / "examples" / "routes" / "config.toml"


def run_checks(
    root: Path,
    strict_runtime: bool,
    runtime_config: Path | None,
) -> list[CheckResult]:
    results: list[CheckResult] = []

    api_file = root / "python-api/src/lib.rs"
    example_file = root / "examples/multicast-docker/scripts/multicast_node.py"
    results.append(check(api_file.exists(), "python_api_file_exists", str(api_file)))
    results.append(check(example_file.exists(), "example_file_exists", str(example_file)))
    if not api_file.exists() or not example_file.exists():
        return results

    api_src = api_file.read_text(encoding="utf-8")
    example_src = example_file.read_text(encoding="utf-8")

    results.append(
        check(
            "fec_enabled=None" in api_src,
            "api_has_fec_enabled_kwarg",
            "Expected fec_enabled keyword arg in Python API signatures.",
        )
    )
    results.append(
        check(
            "fec_symbols_per_block=None" in api_src,
            "api_has_fec_symbols_kwarg",
            "Expected fec_symbols_per_block keyword arg in send_data signature.",
        )
    )
    results.append(
        check(
            "fec_symbol_size=None" in api_src,
            "api_has_fec_symbol_size_kwarg",
            "Expected fec_symbol_size keyword arg in send_data signature.",
        )
    )
    results.append(
        check(
            "sender_fec_manifest(" in api_src and "receiver_fec_capabilities(" in api_src,
            "api_has_fec_mapping_helpers",
            "Expected sender/receiver helper mapping for FEC kwargs.",
        )
    )
    results.append(
        check(
            "--fec" in example_src,
            "example_has_fec_toggle",
            "Expected --fec CLI toggle in multicast example.",
        )
    )
    results.append(
        check(
            "fec_enabled=fec_enabled(args)" in example_src,
            "example_passes_fec_flag",
            "Expected example to pass fec_enabled through send/receive API calls.",
        )
    )

    try:
        import nextmini_py as nm  # type: ignore
    except Exception as exc:
        results.append(
            check(
                not strict_runtime,
                "extension_import_available",
                (
                    f"nextmini_py import unavailable ({exc!r}); static checks used."
                    if not strict_runtime
                    else f"nextmini_py import unavailable in strict runtime mode: {exc!r}"
                ),
                required=strict_runtime,
            )
        )
        return results

    try:
        send_sig = inspect.signature(nm.Dataplane.send_data)
        receive_sig = inspect.signature(nm.Dataplane.receive_data)
        receive_async_sig = inspect.signature(nm.Dataplane.receive_data_async)
        results.append(
            check(
                signature_contains_param(send_sig, "fec_enabled")
                and signature_contains_param(send_sig, "fec_symbols_per_block")
                and signature_contains_param(send_sig, "fec_symbol_size"),
                "extension_send_data_signature",
                f"send_data signature={send_sig}",
            )
        )
        results.append(
            check(
                signature_contains_param(receive_sig, "fec_enabled")
                and signature_contains_param(receive_async_sig, "fec_enabled"),
                "extension_receive_signature",
                f"receive_data signature={receive_sig}; receive_data_async signature={receive_async_sig}",
            )
        )
    except Exception as exc:
        results.append(
            check(
                False,
                "extension_signature_introspection",
                f"Unable to introspect extension signatures: {exc!r}",
            )
        )

    if strict_runtime:
        cfg_path = resolve_runtime_config(root, runtime_config)
        if not cfg_path.exists():
            results.append(
                check(
                    False,
                    "runtime_probe_config_exists",
                    f"Strict runtime probe config not found: {cfg_path}",
                )
            )
            return results

        try:
            dataplane = nm.Dataplane(str(cfg_path))
            payload = nm.PacketView.from_buffer(b"runtime-probe")
            started_sid = dataplane.send_data(
                1,
                "10.0.0.2",
                [1],
                payload,
                chunk_size=16,
                fec_enabled=False,
            )
            if not isinstance(started_sid, int):
                raise TypeError(f"send_data returned {type(started_sid).__name__}, expected int")
            _ = dataplane.lossless_wait(started_sid, timeout_ms=25)
            results.append(
                check(
                    True,
                    "extension_runtime_send_path",
                    f"Dataplane/send_data probe succeeded with config {cfg_path}.",
                )
            )
        except Exception as exc:
            results.append(
                check(
                    False,
                    "extension_runtime_send_path",
                    f"Dataplane/send_data probe failed: {exc!r}",
                )
            )

    return results


def main() -> int:
    args = parse_args()
    start_wall = datetime.now(timezone.utc)
    start_perf = time.perf_counter()

    root = repo_root(args)
    checks = run_checks(root, args.strict_runtime, args.runtime_config)
    required_failures = [c for c in checks if c.required and not c.success]
    success = not required_failures

    elapsed_ms = (time.perf_counter() - start_perf) * 1000.0
    end_wall = datetime.now(timezone.utc)

    artifact = {
        "mode": "python_api_smoke",
        "fec": args.fec,
        "success": success,
        "elapsed_ms": round(elapsed_ms, 3),
        "started_at": start_wall.isoformat(),
        "finished_at": end_wall.isoformat(),
        "strict_runtime": args.strict_runtime,
        "checks": [
            {
                "name": c.name,
                "success": c.success,
                "required": c.required,
                "detail": c.detail,
            }
            for c in checks
        ],
    }
    if not success:
        artifact["error"] = "one or more required smoke checks failed"

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(artifact, indent=2), encoding="utf-8")

    print(
        json.dumps(
            {
                "mode": artifact["mode"],
                "fec": artifact["fec"],
                "success": success,
                "output": str(args.output),
                "required_failures": [c.name for c in required_failures],
            }
        )
    )

    if args.assert_success and not success:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
