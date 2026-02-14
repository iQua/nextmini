from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from textwrap import dedent


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


def write_contract_fixture(
    root: Path,
    *,
    api_src: str,
    example_src: str,
    extra_tool_src: str | None = None,
) -> None:
    api_file = root / "python-api" / "src" / "lib.rs"
    api_file.parent.mkdir(parents=True, exist_ok=True)
    api_file.write_text(api_src, encoding="utf-8")

    example_file = root / "examples" / "multicast-docker" / "scripts" / "multicast_node.py"
    example_file.parent.mkdir(parents=True, exist_ok=True)
    example_file.write_text(example_src, encoding="utf-8")

    if extra_tool_src is None:
        return

    tool_file = root / "tools" / "experiments" / "raptorq" / "surface_probe.py"
    tool_file.parent.mkdir(parents=True, exist_ok=True)
    tool_file.write_text(extra_tool_src, encoding="utf-8")


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

    def test_contract_guardrail_passes_for_clean_surface(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td) / "repo"
            write_contract_fixture(
                root,
                api_src=dedent(
                    """
                    #[pyo3(signature=(group_id, dest_ip, receiver_ids, buffer, chunk_size=8500))]
                    fn send_data() {}
                    """
                ),
                example_src=dedent(
                    """
                    def parse_args():
                        return "--fec"

                    def run(dataplane, payload):
                        return dataplane.send_data(1, "10.0.0.2", [1], payload, chunk_size=16)
                    """
                ),
                extra_tool_src=dedent(
                    """
                    def helper(dataplane, payload):
                        return dataplane.receive_data(1, "10.0.0.2", 1, len(payload), chunk_size=16)
                    """
                ),
            )
            out = Path(td) / "smoke_py.json"
            proc = run_script(
                [
                    "--fec",
                    "off",
                    "--project-root",
                    str(root),
                    "--assert-success",
                    "--output",
                    str(out),
                ]
            )
            self.assertEqual(proc.returncode, 0, proc.stderr)

            artifact = json.loads(out.read_text(encoding="utf-8"))
            checks = {check["name"]: check for check in artifact["checks"]}
            self.assertTrue(checks["api_omits_legacy_sender_kwargs"]["success"])
            self.assertTrue(checks["surface_omits_legacy_fec_kwargs"]["success"])

    def test_contract_guardrail_fails_when_api_reintroduces_legacy_kwargs(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td) / "repo"
            write_contract_fixture(
                root,
                api_src=dedent(
                    """
                    #[pyo3(signature=(group_id, dest_ip, receiver_ids, buffer, fec_enabled = None))]
                    fn send_data() {}
                    """
                ),
                example_src=dedent(
                    """
                    def parse_args():
                        return "--fec"
                    """
                ),
            )
            out = Path(td) / "smoke_py.json"
            proc = run_script(
                [
                    "--fec",
                    "off",
                    "--project-root",
                    str(root),
                    "--assert-success",
                    "--output",
                    str(out),
                ]
            )
            self.assertEqual(proc.returncode, 1, proc.stderr)

            artifact = json.loads(out.read_text(encoding="utf-8"))
            failed_required = {
                check["name"]
                for check in artifact["checks"]
                if check.get("required") and not check.get("success")
            }
            self.assertIn("api_omits_legacy_sender_kwargs", failed_required)

    def test_contract_guardrail_fails_when_tools_callsites_use_legacy_kwargs(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td) / "repo"
            write_contract_fixture(
                root,
                api_src=dedent(
                    """
                    #[pyo3(signature=(group_id, dest_ip, receiver_ids, buffer, chunk_size=8500))]
                    fn send_data() {}
                    """
                ),
                example_src=dedent(
                    """
                    def parse_args():
                        return "--fec"
                    """
                ),
                extra_tool_src=dedent(
                    """
                    def helper(dataplane, payload):
                        return dataplane.send_data(
                            1,
                            "10.0.0.2",
                            [1],
                            payload,
                            chunk_size=16,
                            fec_enabled=False,
                        )
                    """
                ),
            )
            out = Path(td) / "smoke_py.json"
            proc = run_script(
                [
                    "--fec",
                    "off",
                    "--project-root",
                    str(root),
                    "--assert-success",
                    "--output",
                    str(out),
                ]
            )
            self.assertEqual(proc.returncode, 1, proc.stderr)

            artifact = json.loads(out.read_text(encoding="utf-8"))
            failed_required = {
                check["name"]
                for check in artifact["checks"]
                if check.get("required") and not check.get("success")
            }
            self.assertIn("surface_omits_legacy_fec_kwargs", failed_required)


if __name__ == "__main__":
    unittest.main()
