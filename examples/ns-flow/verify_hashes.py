#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Verify namespace lossless integration artifacts."
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

    for receiver in receiver_bins:
        node_id = receiver.stem.split("-")[-1]
        require_ok_status(artifact_dir / f"receiver-{node_id}.status")
        receiver_digest = sha256(receiver)
        verify_sidecar(receiver, receiver_digest)
        receiver_size = receiver.stat().st_size
        if receiver_size != source_size:
            raise SystemExit(
                f"size mismatch for {receiver}: {receiver_size} != {source_size}"
            )
        if receiver_digest != source_digest:
            raise SystemExit(
                f"hash mismatch for {receiver}: {receiver_digest} != {source_digest}"
            )

    print(f"VERIFICATION PASSED: {artifact_dir.name} sha256={source_digest}")


if __name__ == "__main__":
    main()
