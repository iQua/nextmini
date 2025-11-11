from __future__ import annotations

import os
import pytest
from rich.syntax import Syntax
from examples.reliable_multicast.utils.logging import console, panel


pytestmark = pytest.mark.skipif(
    os.environ.get("ENABLE_RELIABLE_E2E") != "1",
    reason="ENABLE_RELIABLE_E2E!=1",
)


@pytest.mark.xfail(reason="Will switch to actual frame bytes once engines emit frames")
def test_control_frames_hex_placeholders() -> None:
    panel(
        "Note",
        "The following hex dumps are placeholders to illustrate rich logging. They will be replaced with live-captured frames.",
    )

    manifest_hex = (
        "52 4C 4D 31 01 02 01 00 00 00 00 00 00 00 00 07 00 00 00 11 "
        "00 00 80 00 00 00 00 10 01 00 00 00 00 00 00 00"
    )
    ack_hex = "52 4C 4D 31 01 02 03 00 00 00 00 00 00 00 00 07 00 00 00 08 00 00 00 00 00 00 00 77"
    eot_hex = (
        "52 4C 4D 31 01 02 06 00 00 00 00 00 00 00 00 07 00 00 00 09 "
        "00 00 00 00 00 00 04 00"
    )

    console.print(Syntax(manifest_hex, "text", theme="monokai", word_wrap=True))
    console.print(Syntax(ack_hex, "text", theme="monokai", word_wrap=True))
    console.print(Syntax(eot_hex, "text", theme="monokai", word_wrap=True))

