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
            "Lightweight smoke harness for Python API surface compatibility. "
            "Writes a JSON artifact containing mode/success/timing."
        )
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
        "--project-root",
        type=Path,
        default=None,
        help="Optional repo root override. Defaults to parent of tools/.",
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


def run_checks(root: Path) -> list[CheckResult]:
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
            "fec_enabled" not in api_src,
            "api_has_no_fec_enabled_kwarg",
            "Unexpected fec_enabled keyword arg in Python API signatures.",
        )
    )
    results.append(
        check(
            "fec_symbols_per_block" not in api_src,
            "api_has_no_fec_symbols_kwarg",
            "Unexpected fec_symbols_per_block keyword arg in send_data signature.",
        )
    )
    results.append(
        check(
            "fec_symbol_size" not in api_src,
            "api_has_no_fec_symbol_size_kwarg",
            "Unexpected fec_symbol_size keyword arg in send_data signature.",
        )
    )
    results.append(
        check(
            "sender_fec_manifest(" not in api_src and "receiver_fec_capabilities(" not in api_src,
            "api_has_no_fec_mapping_helpers",
            "Unexpected sender/receiver helper mapping for FEC kwargs.",
        )
    )
    results.append(
        check(
            "--fec" not in example_src,
            "example_has_no_fec_toggle",
            "Unexpected --fec CLI toggle in multicast example.",
        )
    )
    results.append(
        check(
            "fec_enabled=" not in example_src,
            "example_has_no_fec_kwargs",
            "Unexpected per-call FEC kwargs in multicast example send/receive calls.",
        )
    )

    try:
        import nextmini_py as nm  # type: ignore
    except Exception as exc:
        results.append(
            check(
                True,
                "extension_import_optional",
                f"nextmini_py import unavailable in this environment ({exc!r}); static checks used.",
                required=False,
            )
        )
        return results

    try:
        send_sig = inspect.signature(nm.Dataplane.send_data)
        receive_sig = inspect.signature(nm.Dataplane.receive_data)
        receive_async_sig = inspect.signature(nm.Dataplane.receive_data_async)
        results.append(
            check(
                not signature_contains_param(send_sig, "fec_enabled")
                and not signature_contains_param(send_sig, "fec_symbols_per_block")
                and not signature_contains_param(send_sig, "fec_symbol_size"),
                "extension_send_data_signature",
                f"send_data signature={send_sig}",
            )
        )
        results.append(
            check(
                not signature_contains_param(receive_sig, "fec_enabled")
                and not signature_contains_param(receive_async_sig, "fec_enabled"),
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

    return results


def main() -> int:
    args = parse_args()
    start_wall = datetime.now(timezone.utc)
    start_perf = time.perf_counter()

    root = repo_root(args)
    checks = run_checks(root)
    required_failures = [c for c in checks if c.required and not c.success]
    success = not required_failures

    elapsed_ms = (time.perf_counter() - start_perf) * 1000.0
    end_wall = datetime.now(timezone.utc)

    artifact = {
        "mode": "python_api_surface",
        "success": success,
        "timing": {
            "started_at": start_wall.isoformat(),
            "finished_at": end_wall.isoformat(),
            "elapsed_ms": round(elapsed_ms, 3),
        },
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

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(artifact, indent=2), encoding="utf-8")

    print(
        json.dumps(
            {
                "mode": artifact["mode"],
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
