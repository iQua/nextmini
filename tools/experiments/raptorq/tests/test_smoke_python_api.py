from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[4]
SCRIPT = REPO_ROOT / "tools" / "experiments" / "raptorq" / "smoke_python_api.py"


def run_script(args: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(SCRIPT), *args],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )


class SmokePythonApiTests(unittest.TestCase):
    def test_default_mode_emits_expected_top_level_fields(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            out = Path(td) / "smoke_py.json"
            proc = run_script(["--fec", "off", "--output", str(out)])
            self.assertEqual(proc.returncode, 0, proc.stderr)

            artifact = json.loads(out.read_text(encoding="utf-8"))
            for key in ("mode", "fec", "success", "elapsed_ms", "checks"):
                self.assertIn(key, artifact)
            self.assertEqual(artifact["mode"], "python_api_smoke")
            self.assertEqual(artifact["fec"], "off")

    def test_strict_runtime_fails_when_runtime_probe_config_missing(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            out = Path(td) / "smoke_py.json"
            missing_cfg = Path(td) / "does_not_exist.toml"
            proc = run_script(
                [
                    "--fec",
                    "off",
                    "--strict-runtime",
                    "--runtime-config",
                    str(missing_cfg),
                    "--assert-success",
                    "--output",
                    str(out),
                ]
            )
            self.assertEqual(proc.returncode, 1, proc.stderr)

            artifact = json.loads(out.read_text(encoding="utf-8"))
            self.assertFalse(artifact["success"])
            failed_required = {
                check["name"]
                for check in artifact["checks"]
                if check.get("required") and not check.get("success")
            }
            self.assertTrue(
                "runtime_probe_config_exists" in failed_required
                or "extension_import_available" in failed_required,
                failed_required,
            )


if __name__ == "__main__":
    unittest.main()
