from __future__ import annotations

import importlib.util
import pathlib
import sys
import tempfile
import unittest


TOOLS_DIR = pathlib.Path(__file__).parents[1] / "tools"
sys.path.insert(0, str(TOOLS_DIR))
SPEC = importlib.util.spec_from_file_location("cpu_monitor", TOOLS_DIR / "cpu_monitor.py")
assert SPEC is not None and SPEC.loader is not None
cpu_monitor = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(cpu_monitor)


class CpuMonitorTests(unittest.TestCase):
    def test_summary_marks_peak_over_ceiling_invalid(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            samples = pathlib.Path(directory) / "cpu.csv"
            samples.write_text(
                "timestamp_utc,utilization_pct\n"
                "2026-08-06T00:00:00Z,50\n"
                "2026-08-06T00:00:01Z,80\n",
                encoding="utf-8",
            )
            report = cpu_monitor.summarize(
                samples, 75.0, calibration=False, metrics_dir=None
            )

        self.assertFalse(report["valid"])
        self.assertEqual(report["invalid_reason"], "cpu_ceiling_exceeded")
        self.assertEqual(report["peak_utilization_pct"], 80.0)

    def test_calibration_records_receiver_capability(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            samples = root / "cpu.csv"
            samples.write_text(
                "timestamp_utc,utilization_pct\n2026-08-06T00:00:00Z,25\n",
                encoding="utf-8",
            )
            metrics = root / "metrics"
            metrics.mkdir()
            (metrics / "receiver-4.metrics").write_text(
                "node_id=4\nduration_seconds=2\nthroughput_gbps=1.25\n",
                encoding="utf-8",
            )
            report = cpu_monitor.summarize(
                samples, 75.0, calibration=True, metrics_dir=metrics
            )

        self.assertTrue(report["valid"])
        self.assertEqual(report["mode"], "calibration")
        self.assertEqual(report["capability"]["min_receiver_throughput_gbps"], 1.25)


if __name__ == "__main__":
    unittest.main()
