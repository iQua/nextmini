from __future__ import annotations

import os
import tempfile
from pathlib import Path

import pytest

try:
    import nextmini_py  # noqa: F401
    from nextmini_py import Dataplane
except Exception as e:  # pragma: no cover
    nextmini_import_error = e
else:
    nextmini_import_error = None

from examples.reliable_multicast.utils.logging import console, panel, show_toml, metrics_table
from rich.syntax import Syntax


pytestmark = pytest.mark.skipif(
    os.environ.get("ENABLE_RELIABLE_E2E") != "1" or nextmini_import_error is not None,
    reason=(
        "ENABLE_RELIABLE_E2E!=1 or nextmini_py not importable: "
        f"{nextmini_import_error!r}"
    ),
)


def test_reliable_dry_run_roundtrip(tmp_path: Path) -> None:
    # Prepare a small payload file
    payload_path = tmp_path / "payload.bin"
    payload = b"hello reliable multicast\n" * 128  # ~3KB
    payload_path.write_bytes(payload)

    cfg_path = os.environ.get("NEXTMINI_CONFIG", "node.toml")
    panel("Config Path", cfg_path)
    if Path(cfg_path).exists():
        show_toml(Path(cfg_path).read_text(), title="Config")
    else:
        panel("Config Missing", f"{cfg_path!r} not found; proceeding with defaults")

    panel("Step 1 — Init Dataplane", f"Config: {cfg_path}")
    dp = Dataplane(cfg_path)

    receivers = [2, 3]
    group_ip = "239.1.1.1"
    chunk_size = 8192
    ack_policy = "all"

    call_src = f"""
sid_send = dp.reliable_send_file_rs(
    group_ip={group_ip!r}, receiver_ids={receivers!r}, tensor_path={str(payload_path)!r},
    chunk_size={chunk_size}, ack_policy={ack_policy!r}
)
"""
    console.print(Syntax(call_src, "python", theme="monokai", word_wrap=True))
    sid_send = dp.reliable_send_file_rs(
        group_ip,
        receivers,
        str(payload_path),
        chunk_size=chunk_size,
        ack_policy=ack_policy,
        src_port=None,
        dst_port=None,
    )

    call_src = f"""
sid_recv = dp.reliable_receive_file_rs(
    group_ip={group_ip!r}, source_node_id={receivers[0]!r}, expected_bytes={len(payload)},
    chunk_size={chunk_size}, sink_path=None
)
"""
    console.print(Syntax(call_src, "python", theme="monokai", word_wrap=True))
    sid_recv = dp.reliable_receive_file_rs(
        group_ip,
        receivers[0],
        expected_bytes=len(payload),
        chunk_size=chunk_size,
        sink_path=None,
        src_port=None,
        dst_port=None,
    )

    panel("Step 2 — Dry Run", "Wrappers return session IDs only until engines are wired.")
    metrics_table({
        "sid_send": sid_send,
        "sid_recv": sid_recv,
        "bytes": len(payload),
        "receivers": len(receivers),
        "group_ip": group_ip,
    })

    assert isinstance(sid_send, int) and sid_send > 0
    assert isinstance(sid_recv, int) and sid_recv > 0

