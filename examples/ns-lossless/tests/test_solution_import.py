from __future__ import annotations

import importlib.util
import json
import pathlib
import tempfile
import unittest


NS_DIR = pathlib.Path(__file__).parents[1]


def load_module(name: str, path: pathlib.Path):
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


generator = load_module("ns_generate", NS_DIR / "generate.py")
solution_edges = load_module("solution_edges", NS_DIR / "tools" / "solution_edges.py")


class SolutionImportTests(unittest.TestCase):
    def sample_solution(self) -> dict:
        return {
            "scenario": {
                "source": 1,
                "receivers": [4],
                "forwarding_nodes": [2, 3],
                "edges": [
                    {
                        "src": 1,
                        "dst": 2,
                        "bw": 120,
                        "delay_ms": 8,
                        "jitter_ms": 1.5,
                        "loss_pct": 0.2,
                    },
                    {"src": 2, "dst": 4, "bw": 80},
                    {"src": 1, "dst": 3, "bw": 100},
                    {"src": 3, "dst": 4, "bw": 70},
                ],
            },
            "trees": [
                {"tree_id": 9, "edges": [[1, 2], [2, 4]]},
                {"tree_id": 42, "edges": [[1, 3], [3, 4]]},
            ],
        }

    def test_import_densifies_tree_ids_without_changing_tree_order(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = pathlib.Path(directory) / "solution.json"
            path.write_text(json.dumps(self.sample_solution()), encoding="utf-8")
            imported = generator.load_imported_topology(path)

        self.assertEqual([tree_id for tree_id, _ in imported["trees"]], [0, 1])
        self.assertEqual(
            imported["tree_id_mapping"],
            [
                {"solver_tree_id": 9, "runtime_tree_id": 0},
                {"solver_tree_id": 42, "runtime_tree_id": 1},
            ],
        )

    def test_all_edge_profile_shapes_source_and_propagates_conditions(self) -> None:
        rows = solution_edges.select_edges(
            self.sample_solution(), "solution-edge-rates", 0.0
        )

        self.assertEqual([(row["src"], row["dst"]) for row in rows], [(1, 2), (2, 4), (1, 3), (3, 4)])
        self.assertEqual(rows[0]["dev"], "veth1a")
        self.assertEqual(rows[0]["rate_kbit"], 120_000)
        self.assertEqual(rows[0]["delay_ms"], 8.0)
        self.assertEqual(rows[0]["jitter_ms"], 1.5)
        self.assertEqual(rows[0]["loss_pct"], 0.2)

    def test_worker_feasibility_requires_one_lane_beyond_dense_tree_ids(self) -> None:
        degraded = generator.worker_feasibility(2, [0, 1])
        feasible = generator.worker_feasibility(3, [0, 1])

        self.assertFalse(degraded["feasible"])
        self.assertIn("SharedQueue", degraded["warning"])
        self.assertEqual(degraded["required_minimum"], 3)
        self.assertTrue(feasible["feasible"])
        self.assertIsNone(feasible["warning"])

    def test_node_budget_plan_targets_namespace_egress_and_host_ingress(self) -> None:
        solution = self.sample_solution()
        solution["scenario"]["edges"].append({"src": 2, "dst": 3, "bw": 60})
        solution["scenario"]["node_caps"] = {
            "egress": {"1": 50, "2": 90},
            "ingress": {"4": 100},
        }

        budgets, leaves = solution_edges.select_node_budget_plan(solution)

        self.assertEqual(
            [(row["budget_id"], row["dev"], row["rate_kbit"]) for row in budgets],
            [
                ("node-1-egress", "veth0b", 50_000),
                ("node-2-egress", "veth1b", 90_000),
                ("node-4-ingress", "veth3a", 100_000),
            ],
        )
        self.assertEqual(
            [(row["budget_id"], row["src"], row["dst"]) for row in leaves],
            [
                ("node-1-egress", 1, 2),
                ("node-1-egress", 1, 3),
                ("node-2-egress", 2, 4),
                ("node-4-ingress", 2, 4),
                ("node-4-ingress", 3, 4),
            ],
        )
        self.assertEqual(leaves[0]["delay_ms"], 8.0)
        self.assertEqual(leaves[2]["delay_ms"], 0.0)
        self.assertEqual(
            [
                (
                    row["budget_id"],
                    row["guaranteed_kbit"],
                    row["ceil_kbit"],
                )
                for row in leaves
            ],
            [
                ("node-1-egress", 25_000, 50_000),
                ("node-1-egress", 25_000, 50_000),
                ("node-2-egress", 80_000, 80_000),
                ("node-4-ingress", 50_000, 80_000),
                ("node-4-ingress", 50_000, 70_000),
            ],
        )
        self.assertLessEqual(
            sum(
                int(row["guaranteed_kbit"])
                for row in leaves
                if row["budget_id"] == "node-4-ingress"
            ),
            100_000,
        )

if __name__ == "__main__":
    unittest.main()
