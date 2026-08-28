from __future__ import annotations

import importlib.util
import pathlib
import sys
import tempfile
import unittest


TOOLS_DIR = pathlib.Path(__file__).parents[1] / "tools"
sys.path.insert(0, str(TOOLS_DIR))
SPEC = importlib.util.spec_from_file_location(
    "compare_results", TOOLS_DIR / "compare_results.py"
)
assert SPEC is not None and SPEC.loader is not None
compare_results = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(compare_results)


def complementary_solution() -> dict:
    return {
        "T_opt": 0.2,
        "scenario": {
            "source": 1,
            "receivers": [4, 5],
            "edges": [
                {"src": 1, "dst": 2, "bw": 2.0},
                {"src": 2, "dst": 4, "bw": 1.0},
                {"src": 2, "dst": 5, "bw": 0.1},
                {"src": 1, "dst": 3, "bw": 2.0},
                {"src": 3, "dst": 4, "bw": 0.1},
                {"src": 3, "dst": 5, "bw": 1.0},
            ],
        },
        "trees": [
            {"tree_id": 0, "edges": [[1, 2], [2, 4], [2, 5]]},
            {"tree_id": 1, "edges": [[1, 3], [3, 4], [3, 5]]},
        ],
    }


class CompareResultsTests(unittest.TestCase):
    def test_metrics_and_structured_log_counters_are_reported(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            for receiver in (4, 5):
                (root / f"receiver-{receiver}.metrics").write_text(
                    f"role=receiver\nnode_id={receiver}\npayload_bytes=125000\n"
                    "duration_seconds=1.000000000\nthroughput_gbps=0.001000000\n",
                    encoding="utf-8",
                )
            log = root / "dataplane.log"
            log.write_text(
                "admitted_packets=7 stall_entries=2 "
                "Child-scoped fan-out dispatcher statistics\n",
                encoding="utf-8",
            )

            report = compare_results.compare(complementary_solution(), root, [log])

        self.assertAlmostEqual(report["min_receiver_achieved_mbit"], 1.0)
        self.assertAlmostEqual(report["pi_star_mbit"], 0.2)
        self.assertAlmostEqual(report["achieved_to_pi_star"], 5.0)
        self.assertEqual(report["log_counters"]["maxima"]["admitted_packets"], 7)
        self.assertEqual(report["log_counters"]["event_counts"]["fanout_statistics"], 1)


if __name__ == "__main__":
    unittest.main()
