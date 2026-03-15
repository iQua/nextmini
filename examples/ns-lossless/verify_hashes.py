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


def main() -> None:
    args = parse_args()
    artifact_dir = args.artifact_dir.resolve()
    if not artifact_dir.is_dir():
        raise SystemExit(f"artifact dir does not exist: {artifact_dir}")

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

    receiver_throughputs = [metrics.throughput_gbps for metrics in receiver_metrics]
    slowest_receiver = min(receiver_metrics, key=lambda metrics: metrics.throughput_gbps)
    fastest_receiver = max(receiver_metrics, key=lambda metrics: metrics.throughput_gbps)
    aggregate_receiver_gbps = (
        len(receiver_metrics) * source_size * 8.0 / source_metrics.duration_seconds / 1_000_000_000.0
    )

    print(f"VERIFICATION PASSED: {artifact_dir.name} sha256={source_digest}")
    print(
        "PERFORMANCE "
        f"case={artifact_dir.name} "
        f"source_gbps={source_metrics.throughput_gbps:.6f} "
        f"source_duration_s={source_metrics.duration_seconds:.6f} "
        f"aggregate_receiver_gbps={aggregate_receiver_gbps:.6f}"
    )
    print(
        "PERFORMANCE receivers "
        f"count={len(receiver_metrics)} "
        f"min_gbps={min(receiver_throughputs):.6f} "
        f"avg_gbps={sum(receiver_throughputs) / len(receiver_throughputs):.6f} "
        f"max_gbps={max(receiver_throughputs):.6f} "
        f"slowest_node={slowest_receiver.node_id} "
        f"fastest_node={fastest_receiver.node_id}"
    )


if __name__ == "__main__":
    main()
