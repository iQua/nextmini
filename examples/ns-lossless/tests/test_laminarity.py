from __future__ import annotations

import importlib.util
import pathlib
import sys
import unittest


TOOLS_DIR = pathlib.Path(__file__).parents[1] / "tools"
sys.path.insert(0, str(TOOLS_DIR))
SPEC = importlib.util.spec_from_file_location(
    "check_laminarity", TOOLS_DIR / "check_laminarity.py"
)
assert SPEC is not None and SPEC.loader is not None
check_laminarity = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(check_laminarity)


class LaminarityTests(unittest.TestCase):
    def test_canonical_family_is_laminar_and_reports_r(self) -> None:
        solution = {
            "scenario": {
                "source": 1,
                "edges": [
                    {"src": 1, "dst": 2, "bw": 2},
                    {"src": 1, "dst": 3, "bw": 2},
                    {"src": 8, "dst": 9, "bw": 4},
                ],
            },
            "trees": [
                {"tree_id": 0, "weight": 1, "edges": [[1, 2], [8, 9]]},
                {"tree_id": 1, "weight": 1, "edges": [[1, 2], [8, 9]]},
                {"tree_id": 2, "weight": 1, "edges": [[1, 3], [8, 9]]},
                {"tree_id": 3, "weight": 1, "edges": [[1, 3], [8, 9]]},
            ],
        }

        report = check_laminarity.analyze(solution)

        self.assertTrue(report["laminar"])
        self.assertEqual(report["r"], 2)
        self.assertEqual(
            report["canonical_classification"],
            {"all-trees": 1, "root-group": 2},
        )

    def test_overlapping_non_nested_incidence_is_not_laminar(self) -> None:
        solution = {
            "scenario": {
                "source": 1,
                "edges": [
                    {"src": 4, "dst": 5, "bw": 2},
                    {"src": 6, "dst": 7, "bw": 2},
                ],
            },
            "trees": [
                {"tree_id": 0, "weight": 1, "edges": [[4, 5]]},
                {"tree_id": 1, "weight": 1, "edges": [[4, 5], [6, 7]]},
                {"tree_id": 2, "weight": 1, "edges": [[6, 7]]},
            ],
        }

        report = check_laminarity.analyze(solution)

        self.assertFalse(report["laminar"])
        self.assertEqual(report["r"], 2)
        self.assertEqual(len(report["violations"]), 1)


if __name__ == "__main__":
    unittest.main()
