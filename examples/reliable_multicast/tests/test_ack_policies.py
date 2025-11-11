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


@pytest.mark.parametrize("ack_policy", ["all", "k:2", "frac:0.66"]) 
def test_send_receive_ack_variants(tmp_path: Path, ack_policy: str) -> None:
    payload = b"abc" * 1024
    f = tmp_path / "p.bin"
    f.write_bytes(payload)

    dp = Dataplane(os.environ.get("NEXTMINI_CONFIG", "node.toml"))
    sid_send = dp.reliable_send_file_rs(
        "239.1.1.1",
        [2, 3],
        str(f),
        chunk_size=4096,
        src_port=None,
        dst_port=None,
        ack_policy=ack_policy,
    )
    assert isinstance(sid_send, int) and sid_send > 0

    sid_recv = dp.reliable_receive_file_rs(
        "239.1.1.1",
        2,
        expected_bytes=len(payload),
        chunk_size=4096,
        src_port=None,
        dst_port=None,
        sink_path=None,
    )
    assert isinstance(sid_recv, int) and sid_recv > 0

