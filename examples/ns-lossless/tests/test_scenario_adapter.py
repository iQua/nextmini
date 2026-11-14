from __future__ import annotations

import importlib.util
import json
import pathlib
import unittest


TOOLS_DIR = pathlib.Path(__file__).parents[1] / "tools"
SPEC = importlib.util.spec_from_file_location(
    "scenario_adapter", TOOLS_DIR / "scenario_adapter.py"
)
assert SPEC is not None and SPEC.loader is not None
scenario_adapter = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(scenario_adapter)


class ScenarioAdapterTests(unittest.TestCase):
    def test_replayed_probe_preserves_optional_edge_conditions(self) -> None:
        adapted = scenario_adapter.adapt_payload(
            {
                "scenario": {
                    "name": "replay",
                    "source": 1,
                    "receivers": [4],
                    "forwarding_nodes": [2],
                    "edges": [
                        {
                            "src": 1,
                            "dst": 2,
                            "bw": 120,
                            "delay_ms": 11,
                            "jitter_ms": 2,
                            "loss_pct": 0.25,
                        },
                        {"src": 2, "dst": 4, "bw": 80},
                    ],
                }
            }
        )

        self.assertEqual(adapted["scenario"]["name"], "replay")
        self.assertEqual(adapted["scenario"]["forwarding_nodes"], [4, 2])
        self.assertTrue(adapted["scenario"]["receiver_relays"])
        self.assertEqual(
            adapted["scenario"]["edges"][0],
            {
                "src": 1,
                "dst": 2,
                "bw": 120.0,
                "delay_ms": 11.0,
                "jitter_ms": 2.0,
                "loss_pct": 0.25,
            },
        )

    def test_compact_scenario_expands_relay_uplinks_and_downlinks(self) -> None:
        adapted = scenario_adapter.adapt_payload(
            {
                "name": "tiny",
                "source": 1,
                "receivers": [4, 5, 6],
                "forwarding_nodes": [2, 3],
                "uplink_bw": 120,
                "downlink_bw": 75,
                "heterogeneity": {"delay_ms": 7, "loss_pct": 0.1},
            }
        )

        edges = adapted["scenario"]["edges"]
        self.assertEqual(adapted["scenario"]["forwarding_nodes"], [4, 5, 6, 2, 3])
        self.assertEqual(len(edges), 14)
        self.assertEqual(edges[0]["src"], 1)
        self.assertEqual(edges[0]["bw"], 120.0)
        self.assertEqual(edges[-1], {
            "src": 6,
            "dst": 5,
            "bw": 75.0,
            "delay_ms": 7.0,
            "loss_pct": 0.1,
        })
        receiver_pairs = {
            (edge["src"], edge["dst"])
            for edge in edges
            if edge["src"] in {4, 5, 6}
        }
        self.assertEqual(
            receiver_pairs,
            {(4, 5), (4, 6), (5, 4), (5, 6), (6, 4), (6, 5)},
        )

    def test_scipy_inventory_selects_supported_path_solver(self) -> None:
        inventory = scenario_adapter.render_inventory(backend="scipy", max_tree_hops=3)
        self.assertIn('variant = "path_based"', inventory)
        self.assertIn('backend = "scipy"', inventory)

    def test_node_caps_are_emitted_in_solver_format_with_scaled_role_coverage(
        self,
    ) -> None:
        scale = 0.5
        adapted = scenario_adapter.adapt_payload(
            {
                "scenario": {
                    "name": "node-cap-relays",
                    "source": 1,
                    "receivers": [2, 3],
                    "forwarding_nodes": [4, 5],
                    "edges": [
                        {"src": 1, "dst": 4, "bw": 24_000 * scale},
                        {"src": 1, "dst": 5, "bw": 24_000 * scale},
                        {"src": 4, "dst": 2, "bw": 250 * scale},
                        {"src": 4, "dst": 3, "bw": 250 * scale},
                        {"src": 5, "dst": 2, "bw": 250 * scale},
                        {"src": 5, "dst": 3, "bw": 250 * scale},
                    ],
                    "node_caps": {
                        "egress": {"4": 162 * scale, "5": 162 * scale},
                        "ingress": {"2": 250 * scale, "3": 250 * scale},
                    },
                }
            }
        )
        scenario = adapted["scenario"]
        self.assertEqual(scenario["source"], 1)
        self.assertEqual(scenario["receivers"], [2, 3])
        self.assertEqual(scenario["forwarding_nodes"], [2, 3, 4, 5])
        self.assertEqual(
            scenario["node_caps"],
            {
                "egress": {"4": 81.0, "5": 81.0},
                "ingress": {"2": 125.0, "3": 125.0},
            },
        )
        self.assertNotIn("1", scenario["node_caps"]["egress"])
        self.assertEqual({edge["src"] for edge in scenario["edges"][:2]}, {1})
        self.assertTrue(all(edge["bw"] == 12_000.0 for edge in scenario["edges"][:2]))
        self.assertTrue(all(edge["bw"] == 125.0 for edge in scenario["edges"][2:]))
        self.assertEqual(
            {(edge["src"], edge["dst"], edge["bw"]) for edge in scenario["edges"][-2:]},
            {(2, 3, 125.0), (3, 2, 125.0)},
        )
        self.assertNotIn("node_capacity_model", scenario)
        self.assertFalse(any(edge.get("virtual") for edge in scenario["edges"]))

        round_trip = json.loads(json.dumps(adapted))
        self.assertEqual(round_trip["scenario"]["node_caps"], scenario["node_caps"])

    def test_direct_scenario_emits_source_wan_cap(self) -> None:
        scale = 0.5
        scenario = scenario_adapter.adapt_payload(
            {
                "scenario": {
                    "name": "node-cap-direct",
                    "source": 1,
                    "receivers": [2, 3],
                    "edges": [
                        {"src": 1, "dst": 2, "bw": 250 * scale},
                        {"src": 1, "dst": 3, "bw": 250 * scale},
                    ],
                    "node_caps": {
                        "egress": {"1": 241 * scale},
                        "ingress": {"2": 250 * scale, "3": 250 * scale},
                    },
                }
            }
        )["scenario"]

        self.assertEqual(scenario["node_caps"]["egress"], {"1": 120.5})
        self.assertEqual(
            scenario["node_caps"]["ingress"], {"2": 125.0, "3": 125.0}
        )
        self.assertEqual(scenario["forwarding_nodes"], [2, 3])
        self.assertEqual(
            {(edge["src"], edge["dst"], edge["bw"]) for edge in scenario["edges"][-2:]},
            {(2, 3, 125.0), (3, 2, 125.0)},
        )

    def test_receiver_relays_can_be_explicitly_disabled(self) -> None:
        scenario = scenario_adapter.adapt_payload(
            {
                "scenario": {
                    "source": 1,
                    "receivers": [2, 3],
                    "forwarding_nodes": [2, 3, 4],
                    "receiver_relays": False,
                    "edges": [
                        {"src": 1, "dst": 4, "bw": 1000},
                        {"src": 4, "dst": 2, "bw": 100},
                        {"src": 4, "dst": 3, "bw": 100},
                    ],
                }
            }
        )["scenario"]

        self.assertFalse(scenario["receiver_relays"])
        self.assertEqual(scenario["forwarding_nodes"], [4])
        self.assertFalse(
            any(
                edge["src"] in {2, 3} and edge["dst"] in {2, 3}
                for edge in scenario["edges"]
            )
        )


if __name__ == "__main__":
    unittest.main()
