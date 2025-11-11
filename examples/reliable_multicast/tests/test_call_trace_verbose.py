from __future__ import annotations

import os
from pathlib import Path

import pytest
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


def test_call_trace_verbose(tmp_path: Path) -> None:
    # Inputs
    payload = b"RLM-verbose-logging\n" * 256
    f = tmp_path / "payload.bin"
    f.write_bytes(payload)
    group_ip = "239.1.2.3"
    receivers = [2, 3]
    chunk_size = 4096
    ack_policy = "k:2"

    # Config preview
    cfg_path = os.environ.get("NEXTMINI_CONFIG", "node.toml")
    panel("Config Path", cfg_path)
    if Path(cfg_path).exists():
        show_toml(Path(cfg_path).read_text(), title="Config")

    # Dataplane bootstrap
    panel("Step", "Constructing Dataplane binding and attaching Python interface.")
    dp = Dataplane(cfg_path)

    # Trace: send call
    send_src = f"""
sid_send = dp.reliable_send_file_rs(
    group_ip={group_ip!r},
    receiver_ids={receivers!r},
    tensor_path={str(f)!r},
    chunk_size={chunk_size},
    src_port=None, dst_port=None,
    ack_policy={ack_policy!r},
)
"""
    console.print(Syntax(send_src, "python", theme="monokai", word_wrap=True))
    panel(
        "Function Under Test",
        "Dataplane.reliable_send_file_rs — validates inputs and returns a session id (stub until engines wired).",
    )
    sid_send = dp.reliable_send_file_rs(
        group_ip,
        receivers,
        str(f),
        chunk_size=chunk_size,
        src_port=None,
        dst_port=None,
        ack_policy=ack_policy,
    )

    # Trace: receive call
    recv_src = f"""
sid_recv = dp.reliable_receive_file_rs(
    group_ip={group_ip!r},
    source_node_id={receivers[0]!r},
    expected_bytes={len(payload)},
    chunk_size={chunk_size},
    src_port=None, dst_port=None,
    sink_path=None,
)
"""
    console.print(Syntax(recv_src, "python", theme="monokai", word_wrap=True))
    panel(
        "Function Under Test",
        "Dataplane.reliable_receive_file_rs — validates inputs and returns a session id (stub until engines wired).",
    )
    sid_recv = dp.reliable_receive_file_rs(
        group_ip,
        receivers[0],
        expected_bytes=len(payload),
        chunk_size=chunk_size,
        src_port=None,
        dst_port=None,
        sink_path=None,
    )

    # Observed output
    panel("Observation", "Engines not yet wired — both calls return stub session IDs.")
    metrics_table({
        "sid_send": sid_send,
        "sid_recv": sid_recv,
        "bytes": len(payload),
        "receivers": len(receivers),
        "ack_policy": ack_policy,
    })

    assert isinstance(sid_send, int) and sid_send > 0
    assert isinstance(sid_recv, int) and sid_recv > 0

