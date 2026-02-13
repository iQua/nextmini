from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[4]
SCRIPT = REPO_ROOT / "tools" / "experiments" / "raptorq" / "check_compat_matrix.py"


def run_script(args: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(SCRIPT), *args],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )


class CheckCompatMatrixTests(unittest.TestCase):
    def write_report(self, root: Path, name: str, enabled: bool, mode: str, version: int) -> Path:
        path = root / f"{name}.json"
        payload = {
            "node_id": name,
            "fec_capability": {
                "enabled": enabled,
                "mode": mode,
                "version": version,
            },
        }
        path.write_text(json.dumps(payload), encoding="utf-8")
        return path

    def test_strict_passes_when_reports_are_homogeneous(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            n1 = self.write_report(root, "node-a", True, "raptorq", 2)
            n2 = self.write_report(root, "node-b", True, "raptorq", 2)
            out = root / "compat.json"

            proc = run_script(
                [
                    "--input",
                    str(n1),
                    "--input",
                    str(n2),
                    "--require-homogeneous-fec",
                    "--assert-strict",
                    "--output",
                    str(out),
                ]
            )
            self.assertEqual(proc.returncode, 0, proc.stderr)

            artifact = json.loads(out.read_text(encoding="utf-8"))
            self.assertTrue(artifact["success"])
            self.assertEqual(artifact["failed_count"], 0)

    def test_strict_fails_when_reports_disagree(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            n1 = self.write_report(root, "node-a", True, "raptorq", 2)
            n2 = self.write_report(root, "node-b", False, "off", 2)
            out = root / "compat.json"

            proc = run_script(
                [
                    "--input",
                    str(n1),
                    "--input",
                    str(n2),
                    "--require-homogeneous-fec",
                    "--assert-strict",
                    "--output",
                    str(out),
                ]
            )
            self.assertEqual(proc.returncode, 1, proc.stderr)

            artifact = json.loads(out.read_text(encoding="utf-8"))
            self.assertFalse(artifact["success"])
            self.assertIn("homogeneous_fec_capability", artifact["failed_checks"])
            self.assertGreaterEqual(len(artifact["mismatches"]), 1)

    def test_strict_fails_without_input_reports(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            out = root / "compat.json"

            proc = run_script(
                [
                    "--require-homogeneous-fec",
                    "--assert-strict",
                    "--output",
                    str(out),
                ]
            )
            self.assertEqual(proc.returncode, 1, proc.stderr)

            artifact = json.loads(out.read_text(encoding="utf-8"))
            self.assertFalse(artifact["success"])
            self.assertIn("input_reports_present", artifact["failed_checks"])


if __name__ == "__main__":
    unittest.main()
