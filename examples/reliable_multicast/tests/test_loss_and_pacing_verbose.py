from __future__ import annotations

import os
from pathlib import Path

import pytest
from rich.syntax import Syntax
from examples.reliable_multicast.utils.logging import console, panel, metrics_table, show_toml


pytestmark = pytest.mark.skipif(
    os.environ.get("ENABLE_RELIABLE_E2E") != "1",
    reason="ENABLE_RELIABLE_E2E!=1",
)


@pytest.mark.xfail(reason="Live loss scenario pending sender/receiver engines + completion")
def test_loss_scenario_verbose(tmp_path: Path) -> None:
    payload = b"loss\n" * 1024
    f = tmp_path / "payload.bin"
    f.write_bytes(payload)

    cfg_path = os.environ.get("NEXTMINI_CONFIG", "node.toml")
    panel("Config Path", cfg_path)
    if Path(cfg_path).exists():
        show_toml(Path(cfg_path).read_text(), title="Config")

    panel(
        "Scenario",
        "Loss: In live mode, apply tc netem (e.g., 3-5% drop) or enable in-process drop hooks; here we trace calls only.",
    )
    console.print(Syntax("tc qdisc add dev <nic> root netem loss 3%", "bash", theme="monokai"))

    # Placeholder until engines wire up; mirrors the CLI runner logic
    panel(
        "Expected Behavior (Live)",
        "Sender emits DATA; receivers SACK gaps; sender performs targeted REPAIR; final checksum matches.",
    )
    metrics_table({"expected_loss": "~3%", "receivers": 2, "status": "pending live wiring"})


@pytest.mark.xfail(reason="Live pacing scenario pending data_bucket wiring + completion")
def test_pacing_scenario_verbose(tmp_path: Path) -> None:
    payload = b"pacing\n" * 1024
    f = tmp_path / "payload.bin"
    f.write_bytes(payload)

    cfg_path = os.environ.get("NEXTMINI_CONFIG", "node.toml")
    panel("Config Path", cfg_path)
    if Path(cfg_path).exists():
        show_toml(Path(cfg_path).read_text(), title="Config")

    panel(
        "Scenario",
        "Pacing: In live mode, set [reliable.data_bucket] and higher WRR for control; here we trace calls only.",
    )
    example_toml = """
[reliable]
default_chunk_size = 32768
control_weight = 10
sack_interval_ms = 25
nack_interval_ms = 50
ack_policy = "all"

[reliable.data_bucket]
rate = 50_000_000  # bytes/sec
burst = 200_000    # bytes
"""
    console.print(Syntax(example_toml, "toml", theme="monokai", word_wrap=True))

    panel(
        "Expected Behavior (Live)",
        "Data pacing observed (bucket drain); control frames not starved; completion with matching checksum.",
    )
    metrics_table({"target_rate_bps": 50_000_000, "receivers": 2, "status": "pending live wiring"})

