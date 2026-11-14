#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
from dataclasses import dataclass
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Verify namespace lossless example artifacts."
    )
    parser.add_argument("artifact_dir", type=Path)
    parser.add_argument(
        "--synthetic",
        action="store_true",
        help="Verify synthetic-payload markers and metrics instead of payload hashes.",
    )
    return parser.parse_args()


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def require_ok_status(path: Path) -> None:
    if not path.exists():
        raise SystemExit(f"missing status file: {path}")
    status = path.read_text(encoding="utf-8").strip()
    if status != "ok":
        raise SystemExit(f"status file {path} reported failure: {status}")


def verify_sidecar(path: Path, digest: str) -> None:
    sidecar = path.with_name(f"{path.name}.sha256")
    if not sidecar.exists():
        raise SystemExit(f"missing hash sidecar: {sidecar}")
    recorded = sidecar.read_text(encoding="utf-8").strip()
    if recorded != digest:
        raise SystemExit(f"hash sidecar mismatch for {path}: {recorded} != {digest}")


@dataclass(frozen=True)
class PerformanceMetrics:
    role: str
    node_id: int
    payload_bytes: int
    duration_seconds: float
    throughput_gbps: float


def parse_metrics(path: Path) -> PerformanceMetrics:
    if not path.exists():
        raise SystemExit(f"missing performance metrics file: {path}")

    values: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip() or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip()

    try:
        metrics = PerformanceMetrics(
            role=values["role"],
            node_id=int(values["node_id"]),
            payload_bytes=int(values["payload_bytes"]),
            duration_seconds=float(values["duration_seconds"]),
            throughput_gbps=float(values["throughput_gbps"]),
        )
    except KeyError as exc:
        raise SystemExit(f"performance metrics file {path} is incomplete: missing {exc}") from exc
    except ValueError as exc:
        raise SystemExit(f"performance metrics file {path} is malformed: {exc}") from exc

    if metrics.payload_bytes <= 0:
        raise SystemExit(f"performance metrics file {path} has non-positive payload_bytes")
    if metrics.duration_seconds <= 0:
        raise SystemExit(f"performance metrics file {path} has non-positive duration_seconds")
    if metrics.throughput_gbps <= 0:
        raise SystemExit(f"performance metrics file {path} has non-positive throughput_gbps")

    expected_throughput_gbps = (
        metrics.payload_bytes * 8.0 / metrics.duration_seconds / 1_000_000_000.0
    )
    if abs(metrics.throughput_gbps - expected_throughput_gbps) > 1e-6:
        raise SystemExit(
            "throughput mismatch in "
            f"{path}: {metrics.throughput_gbps} != {expected_throughput_gbps}"
        )

    return metrics


def parse_marker(path: Path) -> dict[str, str]:
    if not path.exists():
        raise SystemExit(f"missing synthetic marker: {path}")

    values: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip() or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip()
    return values


def require_synthetic_marker(path: Path, payload_bytes: int) -> None:
    values = parse_marker(path)
    if values.get("synthetic") != "true":
        raise SystemExit(f"synthetic marker {path} is not marked synthetic=true")
    try:
        marker_payload_bytes = int(values["payload_bytes"])
    except KeyError as exc:
        raise SystemExit(f"synthetic marker {path} is incomplete: missing {exc}") from exc
    except ValueError as exc:
        raise SystemExit(f"synthetic marker {path} has malformed payload_bytes: {exc}") from exc
    if marker_payload_bytes != payload_bytes:
        raise SystemExit(
            f"synthetic marker payload mismatch for {path}: "
            f"{marker_payload_bytes} != {payload_bytes}"
        )


def verify_synthetic(artifact_dir: Path) -> None:
    require_ok_status(artifact_dir / "source-1.status")
    source_metrics = parse_metrics(artifact_dir / "source-1.metrics")
    if source_metrics.role != "source":
        raise SystemExit(
            f"unexpected role in source metrics: {source_metrics.role} != source"
        )
    if source_metrics.node_id != 1:
        raise SystemExit(
            f"unexpected node_id in source metrics: {source_metrics.node_id} != 1"
        )
    require_synthetic_marker(artifact_dir / "source-1.synthetic", source_metrics.payload_bytes)

    receiver_statuses = sorted(artifact_dir.glob("receiver-*.status"))
    if not receiver_statuses:
        raise SystemExit(f"no receiver status files found in {artifact_dir}")

    receiver_metrics: list[PerformanceMetrics] = []
    for status_path in receiver_statuses:
        node_id = status_path.stem.split("-")[-1]
        require_ok_status(status_path)
        metrics = parse_metrics(artifact_dir / f"receiver-{node_id}.metrics")
        if metrics.role != "receiver":
            raise SystemExit(
                f"unexpected role in receiver-{node_id}.metrics: {metrics.role} != receiver"
            )
        if metrics.node_id != int(node_id):
            raise SystemExit(
                f"unexpected node_id in receiver-{node_id}.metrics: {metrics.node_id} != {node_id}"
            )
        if metrics.payload_bytes != source_metrics.payload_bytes:
            raise SystemExit(
                f"synthetic receiver payload mismatch for node {node_id}: "
                f"{metrics.payload_bytes} != {source_metrics.payload_bytes}"
            )
        require_synthetic_marker(
            artifact_dir / f"receiver-{node_id}.synthetic",
            source_metrics.payload_bytes,
        )
        receiver_metrics.append(metrics)

    print_performance(
        artifact_dir,
        receiver_metrics,
        f"synthetic payload_bytes={source_metrics.payload_bytes}",
    )


def print_performance(
    artifact_dir: Path,
    receiver_metrics: list[PerformanceMetrics],
    verification_label: str,
) -> None:
    receiver_throughputs = [metrics.throughput_gbps for metrics in receiver_metrics]
    slowest_receiver = min(receiver_metrics, key=lambda metrics: metrics.throughput_gbps)
    fastest_receiver = max(receiver_metrics, key=lambda metrics: metrics.throughput_gbps)
    print(f"VERIFICATION PASSED: {artifact_dir.name} {verification_label}")
    print(
        "PERFORMANCE receivers "
        f"count={len(receiver_metrics)} "
        f"min_gbps={min(receiver_throughputs):.6f} "
        f"avg_gbps={sum(receiver_throughputs) / len(receiver_throughputs):.6f} "
        f"max_gbps={max(receiver_throughputs):.6f} "
        f"slowest_node={slowest_receiver.node_id} "
        f"fastest_node={fastest_receiver.node_id}"
    )


def main() -> None:
    args = parse_args()
    artifact_dir = args.artifact_dir.resolve()
    if not artifact_dir.is_dir():
        raise SystemExit(f"artifact dir does not exist: {artifact_dir}")

    if args.synthetic:
        verify_synthetic(artifact_dir)
        return

    source = artifact_dir / "source.bin"
    if not source.exists():
        raise SystemExit(f"missing source artifact: {source}")

    receiver_bins = sorted(artifact_dir.glob("receiver-*.bin"))
    if not receiver_bins:
        raise SystemExit(f"no receiver artifacts found in {artifact_dir}")

    require_ok_status(artifact_dir / "source-1.status")
    source_digest = sha256(source)
    verify_sidecar(source, source_digest)
    source_size = source.stat().st_size
    source_metrics = parse_metrics(artifact_dir / "source-1.metrics")

    if source_metrics.role != "source":
        raise SystemExit(
            f"unexpected role in source metrics: {source_metrics.role} != source"
        )
    if source_metrics.node_id != 1:
        raise SystemExit(
            f"unexpected node_id in source metrics: {source_metrics.node_id} != 1"
        )
    if source_metrics.payload_bytes != source_size:
        raise SystemExit(
            "source metrics payload size mismatch: "
            f"{source_metrics.payload_bytes} != {source_size}"
        )

    receiver_metrics: list[PerformanceMetrics] = []

    for receiver in receiver_bins:
        node_id = receiver.stem.split("-")[-1]
        require_ok_status(artifact_dir / f"receiver-{node_id}.status")
        receiver_digest = sha256(receiver)
        verify_sidecar(receiver, receiver_digest)
        receiver_size = receiver.stat().st_size
        metrics = parse_metrics(artifact_dir / f"receiver-{node_id}.metrics")
        if receiver_size != source_size:
            raise SystemExit(
                f"size mismatch for {receiver}: {receiver_size} != {source_size}"
            )
        if receiver_digest != source_digest:
            raise SystemExit(
                f"hash mismatch for {receiver}: {receiver_digest} != {source_digest}"
            )
        if metrics.role != "receiver":
            raise SystemExit(
                f"unexpected role in {receiver}: {metrics.role} != receiver"
            )
        if metrics.node_id != int(node_id):
            raise SystemExit(
                f"unexpected node_id in {receiver}: {metrics.node_id} != {node_id}"
            )
        if metrics.payload_bytes != receiver_size:
            raise SystemExit(
                f"metrics payload size mismatch for {receiver}: "
                f"{metrics.payload_bytes} != {receiver_size}"
            )
        receiver_metrics.append(metrics)

    print_performance(artifact_dir, receiver_metrics, f"sha256={source_digest}")


if __name__ == "__main__":
    main()
