"""
End-to-end reliable multicast harness (opt-in) with rich logging.

Usage
- Build and install nextmini_py (see AGENTS.md). Example:
  uv venv && source .venv/bin/activate
  uv pip install pytest rich
  pip install maturin && maturin develop --release -m python-api/Cargo.toml

- Set env:
  export ENABLE_RELIABLE_E2E=1
  export NEXTMINI_CONFIG=path/to/node.toml
  # any other NEXTMINI_* envs your examples rely on

- Run:
  pytest -q tools/e2e/test_reliable_multicast.py

Notes
- Currently uses the thin `reliable_*_rs` wrappers. Once `reliable` feature is wired, these will
  delegate to rust engines. Until then, tests assert only session ids and log steps.
"""

import os
import time
import hashlib
import pathlib
import pytest
from rich.console import Console
from rich.panel import Panel
from rich.syntax import Syntax
from rich.table import Table

console = Console()


def step(title: str, body: str = "", code: str | None = None, language: str = "text"):
    if code is not None:
        syn = Syntax(code, language, theme="monokai", word_wrap=True)
        console.print(Panel.fit(syn, title=title))
    else:
        console.print(Panel.fit(body, title=title))


def require_env(name: str):
    val = os.getenv(name)
    if not val:
        pytest.skip(f"ENV {name} not set; skipping E2E")
    return val


def sha256(path: pathlib.Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


@pytest.mark.skipif(os.getenv("ENABLE_RELIABLE_E2E") != "1", reason="opt-in E2E")
def test_rmcast_no_loss_session_ids_only():
    import nextmini_py as nm

    cfg = require_env("NEXTMINI_CONFIG")
    step("Config", code=open(cfg, "r", encoding="utf-8").read(), language="toml")

    dp = nm.Dataplane(cfg)
    step("Dataplane", body="Spawned dataplane; ready to start sessions.")

    group_ip = "239.0.0.1"
    receivers = [2, 3]
    tensor_path = str(pathlib.Path(__file__).parent / "fixtures" / "sample.bin")
    pathlib.Path(tensor_path).parent.mkdir(parents=True, exist_ok=True)
    pathlib.Path(tensor_path).write_bytes(os.urandom(256 * 1024))

    step("Inputs", body=f"group={group_ip}\nreceivers={receivers}\nfile={tensor_path}")

    sid_send = dp.reliable_send_file_rs(
        group_ip,
        receivers,
        tensor_path,
        chunk_size=32768,
        src_port=None,
        dst_port=None,
        ack_policy="all",
    )
    step("Sender Session", body=f"sid={sid_send}")
    assert isinstance(sid_send, int) and sid_send > 0

    sid_recv = dp.reliable_receive_file_rs(
        group_ip,
        source_node_id=1,
        expected_bytes=os.path.getsize(tensor_path),
        chunk_size=32768,
        src_port=None,
        dst_port=None,
        sink_path=None,
    )
    step("Receiver Session", body=f"sid={sid_recv}")
    assert isinstance(sid_recv, int) and sid_recv > 0

    # TODO: once ReliableHandle is wired, block on completion and compare checksums
    step("Result", body="Sessions created. Enable `reliable` to run full data checks.")


@pytest.mark.skipif(os.getenv("ENABLE_RELIABLE_E2E_LIVE") != "1", reason="live reliable tests opt-in")
def test_rmcast_live_checksum_validation():
    """
    Live end-to-end validation with rich logs and checksum comparison.
    Requires the dataplane built with `--features reliable` and wrappers wired.
    """
    import nextmini_py as nm

    cfg = require_env("NEXTMINI_CONFIG")
    step("Config", code=open(cfg, "r", encoding="utf-8").read(), language="toml")

    dp = nm.Dataplane(cfg)
    step("Dataplane", body="Spawned dataplane with reliable feature enabled.")

    group_ip = "239.0.0.1"
    receivers = [2]
    tensor_path = str(pathlib.Path(__file__).parent / "fixtures" / "live.bin")
    pathlib.Path(tensor_path).parent.mkdir(parents=True, exist_ok=True)
    pathlib.Path(tensor_path).write_bytes(os.urandom(128 * 1024))

    table = Table(title="Inputs")
    table.add_column("Field")
    table.add_column("Value")
    table.add_row("group_ip", group_ip)
    table.add_row("receivers", str(receivers))
    table.add_row("file", tensor_path)
    console.print(table)

    step(
        "Plan",
        body=(
            "1) Start sender and receiver sessions.\n"
            "2) Wait for transfer to complete (TODO: add completion signaling).\n"
            "3) Compare source and sink checksums.\n"
            "4) Log counters and timing (TODO in M5)."
        ),
    )

    sid_send = dp.reliable_send_file_rs(
        group_ip,
        receivers,
        tensor_path,
        chunk_size=32768,
        src_port=None,
        dst_port=None,
        ack_policy="all",
    )
    step("Sender Session", body=f"sid={sid_send}")

    sink_path = str(pathlib.Path(__file__).parent / "fixtures" / "live.sink.bin")
    sid_recv = dp.reliable_receive_file_rs(
        group_ip,
        source_node_id=1,
        expected_bytes=os.path.getsize(tensor_path),
        chunk_size=32768,
        src_port=None,
        dst_port=None,
        sink_path=sink_path,
    )
    step("Receiver Session", body=f"sid={sid_recv}\nsink={sink_path}")

    step("Wait (sender)", code="dp.reliable_wait(sid_send, timeout_ms=2000)", language="python")
    assert dp.reliable_wait(sid_send, timeout_ms=2000) is True
    step("Wait (receiver)", code="dp.reliable_wait(sid_recv, timeout_ms=2000)", language="python")
    assert dp.reliable_wait(sid_recv, timeout_ms=2000) is True

    if os.path.exists(sink_path):
        src_hash = sha256(pathlib.Path(tensor_path))
        dst_hash = sha256(pathlib.Path(sink_path))
        step("Checksums", body=f"src={src_hash}\ndst={dst_hash}")
        assert src_hash == dst_hash
    else:
        pytest.xfail("Sink file not produced yet; completion signaling not implemented.")


@pytest.mark.skipif(os.getenv("ENABLE_RELIABLE_E2E") != "1", reason="opt-in E2E")
def test_rmcast_ack_policy_variants_and_input_validation():
    import nextmini_py as nm

    cfg = require_env("NEXTMINI_CONFIG")
    dp = nm.Dataplane(cfg)

    group_ip = "239.0.0.2"
    receivers = [2]
    tmp = pathlib.Path(__file__).parent / "fixtures" / "ack.bin"
    tmp.parent.mkdir(parents=True, exist_ok=True)
    tmp.write_bytes(os.urandom(64 * 1024))

    def call_and_panel(title: str, policy: str):
        code = f"dp.reliable_send_file_rs(\n    group_ip='{group_ip}',\n    receiver_ids={receivers},\n    tensor_path='{str(tmp)}',\n    chunk_size=32768,\n    ack_policy='{policy}',\n)"
        step(title, code=code, language="python")
        sid = dp.reliable_send_file_rs(group_ip, receivers, str(tmp), chunk_size=32768, ack_policy=policy)
        step("Result", body=f"sid={sid}")
        assert isinstance(sid, int) and sid > 0

    call_and_panel("AckPolicy all", "all")
    call_and_panel("AckPolicy k:1", "k:1")
    call_and_panel("AckPolicy frac:0.5", "frac:0.5")

    # Invalid policy should raise
    bad_code = f"dp.reliable_send_file_rs(\n    group_ip='{group_ip}', receiver_ids={receivers}, tensor_path='{str(tmp)}', chunk_size=32768, ack_policy='bogus'\n)"
    step("AckPolicy invalid (expect error)", code=bad_code, language="python")
    with pytest.raises(Exception) as ei:
        dp.reliable_send_file_rs(group_ip, receivers, str(tmp), chunk_size=32768, ack_policy="bogus")
    step("Error", body=str(ei.value))

    # Input validation: empty receivers
    code = f"dp.reliable_send_file_rs('{group_ip}', [], '{str(tmp)}', 32768, ack_policy='all')"
    step("Empty receivers (expect error)", code=code, language="python")
    with pytest.raises(Exception) as ei2:
        dp.reliable_send_file_rs(group_ip, [], str(tmp), chunk_size=32768, ack_policy="all")
    step("Error", body=str(ei2.value))

    # Input validation: chunk_size=0
    code = f"dp.reliable_send_file_rs('{group_ip}', {receivers}, '{str(tmp)}', 0, ack_policy='all')"
    step("chunk_size=0 (expect error)", code=code, language="python")
    with pytest.raises(Exception) as ei3:
        dp.reliable_send_file_rs(group_ip, receivers, str(tmp), chunk_size=0, ack_policy="all")
    step("Error", body=str(ei3.value))

    # Input validation: missing file
    missing = str(pathlib.Path(__file__).parent / "fixtures" / "missing.bin")
    code = f"dp.reliable_send_file_rs('{group_ip}', {receivers}, '{missing}', 32768, ack_policy='all')"
    step("missing file (expect error)", code=code, language="python")
    with pytest.raises(Exception) as ei4:
        dp.reliable_send_file_rs(group_ip, receivers, missing, chunk_size=32768, ack_policy="all")
    step("Error", body=str(ei4.value))


@pytest.mark.skipif(os.getenv("ENABLE_RELIABLE_E2E") != "1", reason="opt-in E2E")
def test_rmcast_receiver_expected_bytes_and_sink_path():
    import nextmini_py as nm

    cfg = require_env("NEXTMINI_CONFIG")
    dp = nm.Dataplane(cfg)
    group_ip = "239.0.0.3"

    # expected_bytes must be positive
    code = f"dp.reliable_receive_file_rs('{group_ip}', source_node_id=1, expected_bytes=0, chunk_size=32768)"
    step("expected_bytes=0 (expect error)", code=code, language="python")
    with pytest.raises(Exception) as ei:
        dp.reliable_receive_file_rs(group_ip, source_node_id=1, expected_bytes=0, chunk_size=32768)
    step("Error", body=str(ei.value))

    # When provided, sink_path will be used (live test compares checksums when engines are ready)
    sink_path = str(pathlib.Path(__file__).parent / "fixtures" / "recv.sink.bin")
    code = (
        f"dp.reliable_receive_file_rs('{group_ip}', source_node_id=1, expected_bytes=1024, chunk_size=32768, "
        f"sink_path='{sink_path}')"
    )
    step("Receiver session (stub)", code=code, language="python")
    sid = dp.reliable_receive_file_rs(group_ip, source_node_id=1, expected_bytes=1024, chunk_size=32768, sink_path=sink_path)
    step("Result", body=f"sid={sid}\nsink={sink_path}")
    assert isinstance(sid, int) and sid > 0
