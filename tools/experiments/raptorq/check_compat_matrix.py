#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ALLOWED_FEC_MODES: tuple[str, ...] = ("off", "lossless", "parity", "raptorq")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Compatibility matrix check for RaptorQ rollout gating. "
            "Provide one or more --input report files to run real checks."
        )
    )
    parser.add_argument(
        "--input",
        action="append",
        dest="inputs",
        type=Path,
        default=[],
        help=(
            "Node/session report JSON path (repeat for multiple nodes). "
            "Strict mode fails when homogeneous checks are required but no inputs are supplied."
        ),
    )
    parser.add_argument(
        "--require-homogeneous-fec",
        action="store_true",
        help="Require sender/receiver sessions to all agree on FEC mode.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        required=True,
        help="Write compatibility matrix JSON artifact to this path.",
    )
    parser.add_argument(
        "--assert-strict",
        action="store_true",
        help="Exit non-zero if required checks fail.",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Alias for --assert-strict.",
    )
    return parser.parse_args()


@dataclass
class NodeCapability:
    node_id: str
    source: str
    enabled: bool | None
    mode: str | None
    version: int | None
    valid: bool
    errors: list[str]


def _coerce_bool(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"true", "1", "yes", "on"}:
            return True
        if normalized in {"false", "0", "no", "off"}:
            return False
    return None


def _coerce_int(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        stripped = value.strip()
        if stripped.isdigit():
            return int(stripped)
    return None


def _load_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"report must be a JSON object, got {type(payload).__name__}")
    return payload


def _extract_node_capability(path: Path) -> NodeCapability:
    errors: list[str] = []
    try:
        report = _load_json(path)
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        return NodeCapability(
            node_id=path.stem,
            source=str(path),
            enabled=None,
            mode=None,
            version=None,
            valid=False,
            errors=[f"failed_to_load_report:{exc}"],
        )

    node_id = str(report.get("node_id") or report.get("node") or path.stem)
    capability = report.get("fec_capability")
    capability = capability if isinstance(capability, dict) else {}

    enabled_raw = capability.get("enabled")
    if enabled_raw is None:
        enabled_raw = report.get("fec_enabled")

    mode_raw = capability.get("mode")
    if mode_raw is None:
        mode_raw = report.get("fec_mode")

    version_raw = capability.get("version")
    if version_raw is None:
        version_raw = report.get("lossless_session_fec_version")
    if version_raw is None:
        version_raw = report.get("fec_version")

    enabled = _coerce_bool(enabled_raw)
    if enabled is None:
        errors.append("missing_or_invalid:enabled")

    mode = None
    if isinstance(mode_raw, str) and mode_raw.strip():
        normalized_mode = mode_raw.strip().lower()
        if normalized_mode in ALLOWED_FEC_MODES:
            mode = normalized_mode
        else:
            errors.append(
                f"unsupported_mode:{normalized_mode}:expected_one_of:{'|'.join(ALLOWED_FEC_MODES)}"
            )
    else:
        errors.append("missing_or_invalid:mode")

    version = _coerce_int(version_raw)
    if version is None:
        errors.append("missing_or_invalid:version")

    return NodeCapability(
        node_id=node_id,
        source=str(path),
        enabled=enabled,
        mode=mode,
        version=version,
        valid=len(errors) == 0,
        errors=errors,
    )


def _capability_signature(node: NodeCapability) -> tuple[bool | None, str | None, int | None]:
    return (node.enabled, node.mode, node.version)


def check_compatibility(inputs: list[Path], require_homogeneous_fec: bool) -> dict:
    checks: list[dict[str, Any]] = []
    mismatches: list[dict[str, Any]] = []
    nodes = [_extract_node_capability(path) for path in inputs]
    has_inputs = len(inputs) > 0

    checks.append(
        {
            "name": "input_reports_present",
            "passed": has_inputs,
            "detail": (
                f"Loaded {len(inputs)} compatibility report(s)."
                if has_inputs
                else "No compatibility reports were provided."
            ),
            "required": require_homogeneous_fec,
        }
    )

    schema_valid = has_inputs and all(node.valid for node in nodes)
    checks.append(
        {
            "name": "report_schema_valid",
            "passed": schema_valid,
            "detail": (
                "All input reports include required FEC capability fields."
                if schema_valid
                else "At least one report is missing required FEC capability fields."
            ),
            "required": require_homogeneous_fec,
        }
    )

    if not schema_valid:
        for node in nodes:
            if node.valid:
                continue
            mismatches.append(
                {
                    "type": "invalid_report",
                    "node_id": node.node_id,
                    "source": node.source,
                    "detail": ", ".join(node.errors),
                }
            )

    homogeneous = False
    if schema_valid:
        baseline = _capability_signature(nodes[0])
        homogeneous = all(_capability_signature(node) == baseline for node in nodes[1:])
        if not homogeneous:
            expected = {
                "enabled": baseline[0],
                "mode": baseline[1],
                "version": baseline[2],
            }
            for node in nodes[1:]:
                signature = _capability_signature(node)
                if signature == baseline:
                    continue
                mismatches.append(
                    {
                        "type": "fec_capability_mismatch",
                        "node_id": node.node_id,
                        "source": node.source,
                        "expected": expected,
                        "actual": {
                            "enabled": signature[0],
                            "mode": signature[1],
                            "version": signature[2],
                        },
                    }
                )

    checks.append(
        {
            "name": "homogeneous_fec_capability",
            "passed": homogeneous,
            "detail": (
                "All reports agree on FEC enabled/mode/version."
                if homogeneous
                else "Reports do not agree on FEC enabled/mode/version."
            ),
            "required": require_homogeneous_fec,
        }
    )

    return {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "input_reports": [str(path) for path in inputs],
        "nodes": [asdict(node) for node in nodes],
        "require_homogeneous_fec": require_homogeneous_fec,
        "compatibility_checks": checks,
        "mismatches": mismatches,
    }


def main() -> int:
    args = parse_args()
    strict = args.assert_strict or args.strict
    artifact = check_compatibility(args.inputs, args.require_homogeneous_fec)

    failed_checks = [c for c in artifact["compatibility_checks"] if c["required"] and not c["passed"]]
    success = len(failed_checks) == 0

    artifact["strict"] = strict
    artifact["success"] = success
    artifact["failed_checks"] = [c["name"] for c in failed_checks]
    artifact["failed_count"] = len(artifact["failed_checks"])

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(artifact, indent=2), encoding="utf-8")

    if strict and not success:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
