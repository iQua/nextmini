from __future__ import annotations

import os
from pathlib import Path

import pytest
from rich.table import Table
from rich.syntax import Syntax

try:
    import nextmini_py  # noqa: F401
    from nextmini_py import Dataplane
except Exception as e:  # pragma: no cover
    nextmini_import_error = e
else:
    nextmini_import_error = None

from examples.reliable_multicast.utils.logging import console, panel, metrics_table, show_toml


pytestmark = pytest.mark.skipif(
    os.environ.get("ENABLE_RELIABLE_E2E") != "1" or nextmini_import_error is not None,
    reason=(
        "ENABLE_RELIABLE_E2E!=1 or nextmini_py not importable: "
        f"{nextmini_import_error!r}"
    ),
)


def test_verbose_matrix(tmp_path: Path) -> None:
    payload = b"matrix\n" * 512
    f = tmp_path / "payload.bin"
    f.write_bytes(payload)

    cfg_path = os.environ.get("NEXTMINI_CONFIG", "node.toml")
    panel("Config Path", cfg_path)
    if Path(cfg_path).exists():
        show_toml(Path(cfg_path).read_text(), title="Config")

    dp = Dataplane(cfg_path)

    steps = Table(title="Reliable Multicast Call Trace")
    steps.add_column("Step")
    steps.add_column("Action")
    steps.add_column("Expected")
    steps.add_column("Observed")

    # Step 1: sender call
    sender_src = f"dp.reliable_send_file_rs(\n  group_ip='239.1.9.9', receiver_ids=[2,3], tensor_path='{f}',\n  chunk_size=8192, src_port=None, dst_port=None, ack_policy='all'\n)"
    console.print(Syntax(sender_src, "python", theme="monokai", word_wrap=True))
    sid_send = dp.reliable_send_file_rs(
        "239.1.9.9", [2, 3], str(f), chunk_size=8192, src_port=None, dst_port=None, ack_policy="all"
    )
    steps.add_row("1", "Start sender (stub)", "Validate inputs; return sid", f"sid_send={sid_send}")

    # Step 2: receiver call
    recv_src = f"dp.reliable_receive_file_rs(\n  group_ip='239.1.9.9', source_node_id=2, expected_bytes={len(payload)},\n  chunk_size=8192, src_port=None, dst_port=None, sink_path=None\n)"
    console.print(Syntax(recv_src, "python", theme="monokai", word_wrap=True))
    sid_recv = dp.reliable_receive_file_rs(
        "239.1.9.9", 2, expected_bytes=len(payload), chunk_size=8192, src_port=None, dst_port=None, sink_path=None
    )
    steps.add_row("2", "Start receiver (stub)", "Validate inputs; return sid", f"sid_recv={sid_recv}")

    console.print(steps)
    metrics_table({
        "bytes": len(payload),
        "receivers": 2,
        "sid_send": sid_send,
        "sid_recv": sid_recv,
    })

    assert isinstance(sid_send, int) and sid_send > 0
    assert isinstance(sid_recv, int) and sid_recv > 0

