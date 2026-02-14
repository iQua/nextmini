from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[4]
SCRIPT = REPO_ROOT / "tools" / "experiments" / "raptorq" / "run_smoke.py"


def run_script(args: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(SCRIPT), *args],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )


class RunSmokeTests(unittest.TestCase):
    def test_raptorq_mode_writes_expected_top_level_metrics(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            out = root / "smoke.json"
            proc = run_script(
                [
                    "--mode",
                    "raptorq",
                    "--loss",
                    "0.10",
                    "--assert-metrics",
                    "--output",
                    str(out),
                ]
            )
            self.assertEqual(proc.returncode, 0, proc.stderr)
            artifact = json.loads(out.read_text(encoding="utf-8"))
            self.assertTrue(artifact["success"])
            for key in (
                "mode",
                "loss",
                "success",
                "completion_ms",
                "p95_ms",
                "p99_ms",
                "overhead",
                "cpu_pct",
                "mem_mb",
            ):
                self.assertIn(key, artifact)

    def test_unicast_mode_is_rejected_by_cli(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            out = root / "smoke.json"
            proc = run_script(
                [
                    "--mode",
                    "unicast",
                    "--loss",
                    "0.10",
                    "--output",
                    str(out),
                ]
            )
            self.assertNotEqual(proc.returncode, 0)


if __name__ == "__main__":
    unittest.main()
