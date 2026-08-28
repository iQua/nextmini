from __future__ import annotations

import importlib.util
import pathlib
import unittest


TOOLS_DIR = pathlib.Path(__file__).parents[1] / "tools"
SPEC = importlib.util.spec_from_file_location(
    "generate_relay_sweep", TOOLS_DIR / "generate_relay_sweep.py"
)
assert SPEC is not None and SPEC.loader is not None
generate_relay_sweep = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(generate_relay_sweep)


class GenerateRelaySweepTests(unittest.TestCase):
    def test_family_is_nested_and_receiver_relays_are_complete(self) -> None:
        kwargs = {
            "seed": 7,
            "receiver_count": 3,
            "k_values": [0, 1, 2],
            "cross_dc_min_mbit": 50.0,
            "cross_dc_max_mbit": 200.0,
            "feed_min_mbit": 900.0,
            "feed_max_mbit": 1100.0,
            "node_cap_mbit": 1000.0,
        }
        family, scenarios = generate_relay_sweep.generate_family(**kwargs)
        repeated_family, repeated_scenarios = generate_relay_sweep.generate_family(
            **kwargs
        )

        self.assertEqual(family, repeated_family)
        self.assertEqual(scenarios, repeated_scenarios)
        self.assertEqual(family["relay_order"], [5, 6])

        expected_receiver_pairs = {
            (src, dst) for src in (2, 3, 4) for dst in (2, 3, 4) if src != dst
        }
        for k, wrapped in scenarios.items():
            scenario = wrapped["scenario"]
            receivers = set(scenario["receivers"])
            receiver_pairs = {
                (edge["src"], edge["dst"])
                for edge in scenario["edges"]
                if edge["src"] in receivers and edge["dst"] in receivers
            }
            self.assertEqual(receiver_pairs, expected_receiver_pairs)
            self.assertTrue(scenario["receiver_relays"])
            self.assertTrue(receivers <= set(scenario["forwarding_nodes"]))
            self.assertEqual(
                set(scenario["node_caps"]["egress"]),
                {str(node) for node in ([1] if k == 0 else [5, 6][:k]) + [2, 3, 4]},
            )

        k1_edges = scenarios[1]["scenario"]["edges"]
        k2_edges = scenarios[2]["scenario"]["edges"]
        k1_by_pair = {(edge["src"], edge["dst"]): edge["bw"] for edge in k1_edges}
        k2_by_pair = {(edge["src"], edge["dst"]): edge["bw"] for edge in k2_edges}
        self.assertTrue(k1_by_pair.items() <= k2_by_pair.items())

        cross_dc = [
            edge["bw"]
            for edge in k2_edges
            if edge["src"] != 1
        ]
        feeds = [edge["bw"] for edge in k2_edges if edge["src"] == 1]
        self.assertTrue(all(50.0 <= capacity <= 200.0 for capacity in cross_dc))
        self.assertTrue(all(900.0 <= capacity <= 1100.0 for capacity in feeds))


if __name__ == "__main__":
    unittest.main()
