from __future__ import annotations

import os
from pathlib import Path

import pytest

try:
    import nextmini_py  # noqa: F401
    from nextmini_py import Dataplane
except Exception as e:  # pragma: no cover
    nextmini_import_error = e
else:
    nextmini_import_error = None


pytestmark = pytest.mark.skipif(
    os.environ.get("ENABLE_RELIABLE_E2E") != "1" or nextmini_import_error is not None,
    reason=(
        "ENABLE_RELIABLE_E2E!=1 or nextmini_py not importable: "
        f"{nextmini_import_error!r}"
    ),
)


def test_invalid_ack_policy_raises(tmp_path: Path) -> None:
    f = tmp_path / "p.bin"
    f.write_bytes(b"x" * 1024)
    dp = Dataplane(os.environ.get("NEXTMINI_CONFIG", "node.toml"))

    with pytest.raises(Exception) as ei:
        dp.reliable_send_file_rs(
            "239.1.1.1",
            [2],
            str(f),
            chunk_size=1024,
            src_port=None,
            dst_port=None,
            ack_policy="bogus",
        )
    assert "invalid ack_policy" in str(ei.value).lower()


@pytest.mark.xfail(reason="numeric bounds to be enforced in python-api via messages::rlm::parse_ack_policy()")
@pytest.mark.parametrize("policy", ["k:0", "frac:0", "frac:1.2"])  # out-of-bounds
def test_numeric_ack_policy_bounds_future(tmp_path: Path, policy: str) -> None:
    f = tmp_path / "p.bin"
    f.write_bytes(b"x" * 1024)
    dp = Dataplane(os.environ.get("NEXTMINI_CONFIG", "node.toml"))
    with pytest.raises(Exception):
        dp.reliable_send_file_rs(
            "239.1.1.1",
            [2],
            str(f),
            chunk_size=1024,
            src_port=None,
            dst_port=None,
            ack_policy=policy,
        )


def test_zero_chunk_size_raises(tmp_path: Path) -> None:
    f = tmp_path / "p.bin"
    f.write_bytes(b"x" * 1024)
    dp = Dataplane(os.environ.get("NEXTMINI_CONFIG", "node.toml"))

    with pytest.raises(Exception) as ei:
        dp.reliable_send_file_rs(
            "239.1.1.1",
            [2],
            str(f),
            chunk_size=0,
            src_port=None,
            dst_port=None,
            ack_policy="all",
        )
    assert "chunk_size must be positive" in str(ei.value).lower()


def test_missing_file_raises(tmp_path: Path) -> None:
    dp = Dataplane(os.environ.get("NEXTMINI_CONFIG", "node.toml"))
    missing = tmp_path / "nope.bin"
    with pytest.raises(Exception) as ei:
        dp.reliable_send_file_rs(
            "239.1.1.1",
            [2],
            str(missing),
            chunk_size=1024,
            src_port=None,
            dst_port=None,
            ack_policy="all",
        )
    assert "does not exist" in str(ei.value).lower()
