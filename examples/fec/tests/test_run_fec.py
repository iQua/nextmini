from __future__ import annotations

import hashlib
import importlib.util
import sys
import tempfile
import textwrap
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
RUN_FEC_PATH = ROOT / "examples" / "fec" / "run_fec.py"


def load_run_fec_module():
    spec = importlib.util.spec_from_file_location("run_fec", RUN_FEC_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to load module from {RUN_FEC_PATH}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def write_inventory(path: Path) -> None:
    path.write_text(
        textwrap.dedent(
            """\
            [controller]
            host = "boston.csl.toronto.edu"
            user = "xindan"
            identity_file = "~/.ssh/no-key"
            no_sudo = true

            [paths]
            remote_repo_dir = "~/skyrocket/nextmini"

            [[nodes]]
            role = "trainer"
            node_id = 1
            host = "206.12.89.244"
            user = "ubuntu"
            identity_file = "~/.ssh/id_rsa_ns-test"

            [[nodes]]
            role = "worker"
            node_id = 2
            host = "206.12.102.201"
            user = "ubuntu"
            identity_file = "~/.ssh/no-key"
            rank = 0

            [[nodes]]
            role = "worker"
            node_id = 3
            host = "206.12.97.30"
            user = "ubuntu"
            identity_file = "~/.ssh/no-key"
            rank = 1

            [[nodes]]
            role = "worker"
            node_id = 4
            host = "34.30.74.243"
            user = "no-passphrase-key"
            identity_file = "~/.ssh/no-key"
            rank = 2

            [[nodes]]
            role = "relay"
            node_id = 5
            host = "206.12.92.2"
            user = "ubuntu"
            identity_file = "~/.ssh/no-key"

            [[nodes]]
            role = "relay"
            node_id = 6
            host = "206.12.100.198"
            user = "ubuntu"
            identity_file = "~/.ssh/no-key"
            """
        ),
        encoding="utf-8",
    )


class RunFecTests(unittest.TestCase):
    def test_validate_block_size_rejects_oversized_plain_payload_but_allows_large_fec_blocks(
        self,
    ) -> None:
        mod = load_run_fec_module()

        with self.assertRaises(ValueError):
            mod.validate_block_size(65_536)

        self.assertEqual(mod.validate_block_size(8_192), 8_192)
        self.assertEqual(
            mod.validate_block_size(
                128 * 1024,
                fec_enabled=True,
                symbols_per_block=16,
            ),
            128 * 1024,
        )

    def test_load_inventory_accepts_relays(self) -> None:
        mod = load_run_fec_module()
        with tempfile.TemporaryDirectory() as tmpdir:
            inventory_path = Path(tmpdir) / "inventory.toml"
            write_inventory(inventory_path)
            inventory = mod.load_inventory(inventory_path)

        self.assertEqual(inventory.trainer.node_id, 1)
        self.assertEqual([node.node_id for node in inventory.workers], [2, 3, 4])
        self.assertEqual([node.node_id for node in inventory.relays], [5, 6])

    def test_compute_relay_trees_uses_two_relays_per_tree(self) -> None:
        mod = load_run_fec_module()

        plain = mod.compute_relay_trees(
            source_node_id=1,
            receiver_ids=[2, 3, 4],
            relay_ids=[5, 6],
            tree_ids=[0],
        )
        self.assertEqual(
            plain,
            [(0, [(1, 5), (5, 6), (6, 2), (6, 3), (6, 4)])],
        )

        fec = mod.compute_relay_trees(
            source_node_id=1,
            receiver_ids=[2, 3, 4],
            relay_ids=[5, 6, 7, 8],
            tree_ids=[0, 1],
        )
        self.assertEqual(
            fec,
            [
                (0, [(1, 5), (5, 6), (6, 2), (6, 3), (6, 4)]),
                (1, [(1, 7), (7, 8), (8, 2), (8, 3), (8, 4)]),
            ],
        )

    def test_render_node_config_switches_fec_by_mode(self) -> None:
        mod = load_run_fec_module()

        plain_text = mod.render_node_config(
            node_id=2,
            controller_addr="ws://boston.csl.toronto.edu:3000",
            tree_ids=[0],
            block_size=65_536,
            symbols_per_block=32,
            fec_enabled=False,
        )
        fec_text = mod.render_node_config(
            node_id=2,
            controller_addr="ws://boston.csl.toronto.edu:3000",
            tree_ids=[0, 1],
            block_size=65_536,
            symbols_per_block=32,
            fec_enabled=True,
        )

        plain_cfg = mod.parse_toml_text(plain_text)
        fec_cfg = mod.parse_toml_text(fec_text)

        self.assertEqual(plain_cfg["feature"], "sequential")
        self.assertTrue(plain_cfg["channel_backpressure"])
        self.assertFalse(plain_cfg["enable_local_interface"])
        self.assertFalse(plain_cfg["lossless_runtime_config"]["fec_enabled"])
        self.assertEqual(plain_cfg["lossless_runtime_config"]["fec_default_tree_ids"], [0])

        self.assertTrue(fec_cfg["lossless_runtime_config"]["fec_enabled"])
        self.assertEqual(fec_cfg["lossless_runtime_config"]["fec_default_tree_ids"], [0, 1])
        self.assertEqual(
            fec_cfg["lossless_runtime_config"]["fec_default_symbols_per_block"],
            32,
        )

    def test_write_deterministic_payload_is_stable(self) -> None:
        mod = load_run_fec_module()

        with tempfile.TemporaryDirectory() as tmpdir:
            payload_a = Path(tmpdir) / "payload-a.bin"
            payload_b = Path(tmpdir) / "payload-b.bin"
            mod.write_deterministic_payload(payload_a, 1_048_707)
            mod.write_deterministic_payload(payload_b, 1_048_707)

            self.assertEqual(payload_a.stat().st_size, 1_048_707)
            self.assertEqual(payload_b.stat().st_size, 1_048_707)
            self.assertEqual(
                hashlib.sha256(payload_a.read_bytes()).hexdigest(),
                hashlib.sha256(payload_b.read_bytes()).hexdigest(),
            )

    def test_image_refs_split_local_boston_push_from_public_worker_pull(self) -> None:
        mod = load_run_fec_module()

        refs = mod.build_image_refs("fec-dev-20260319-032539")

        self.assertEqual(
            refs["controller_local"],
            "127.0.0.1:5000/nextmini-controller:fec-dev-20260319-032539",
        )
        self.assertEqual(
            refs["controller_public"],
            "boston.csl.toronto.edu:5000/nextmini-controller:fec-dev-20260319-032539",
        )
        self.assertEqual(
            refs["fec_local"],
            "127.0.0.1:5000/nextmini-fec:fec-dev-20260319-032539",
        )
        self.assertEqual(
            refs["fec_public"],
            "boston.csl.toronto.edu:5000/nextmini-fec:fec-dev-20260319-032539",
        )

    def test_plain_case_uses_active_node_count_not_full_inventory(self) -> None:
        mod = load_run_fec_module()

        with tempfile.TemporaryDirectory() as tmpdir:
            inventory_path = Path(tmpdir) / "inventory.toml"
            write_inventory(inventory_path)
            inventory = mod.load_inventory(inventory_path)

            run_dir, _plan = mod.generate_case(
                inventory,
                mode="plain",
                payload_size=1024,
                tree_ids=None,
                receiver_ids=None,
                relay_ids=None,
                group_label=None,
                out_dir=Path(tmpdir) / "plain-run",
                block_size=8_192,
                symbols_per_block=32,
            )

            controller_cfg = mod._load_toml(run_dir / "controller-config.toml")
            node_cfg = mod._load_toml(run_dir / "node-1.toml")

        self.assertEqual(controller_cfg["topology"]["n_nodes"], 6)
        self.assertEqual(node_cfg["n_nodes"], 6)

    def test_build_state_only_includes_active_plan_nodes(self) -> None:
        mod = load_run_fec_module()

        with tempfile.TemporaryDirectory() as tmpdir:
            inventory_path = Path(tmpdir) / "inventory.toml"
            write_inventory(inventory_path)
            inventory = mod.load_inventory(inventory_path)

            run_dir, plan = mod.generate_case(
                inventory,
                mode="plain",
                payload_size=1024,
                tree_ids=None,
                receiver_ids=[2, 3],
                relay_ids=[5, 6],
                group_label="active-nodes",
                out_dir=Path(tmpdir) / "plain-run",
                block_size=8_192,
                symbols_per_block=32,
            )
            state = mod.build_state(
                inventory,
                run_dir=run_dir,
                plan=plan,
                image_tag="fec-test",
            )

        self.assertEqual(sorted(int(node_id) for node_id in state["nodes"]), [1, 2, 3, 5, 6])

    def test_noncontiguous_active_node_ids_use_active_count_for_n_nodes(self) -> None:
        mod = load_run_fec_module()

        with tempfile.TemporaryDirectory() as tmpdir:
            inventory_path = Path(tmpdir) / "inventory.toml"
            write_inventory(inventory_path)
            inventory = mod.load_inventory(inventory_path)

            run_dir, _plan = mod.generate_case(
                inventory,
                mode="plain",
                payload_size=1024,
                tree_ids=None,
                receiver_ids=[2, 3],
                relay_ids=[5, 6],
                group_label="active-count",
                out_dir=Path(tmpdir) / "plain-run",
                block_size=8_192,
                symbols_per_block=32,
            )

            controller_cfg = mod._load_toml(run_dir / "controller-config.toml")
            node_cfg = mod._load_toml(run_dir / "node-5.toml")

        self.assertEqual(controller_cfg["topology"]["n_nodes"], 5)
        self.assertEqual(node_cfg["n_nodes"], 5)

    def test_remove_local_run_dir_only_allows_generated_children(self) -> None:
        mod = load_run_fec_module()

        with tempfile.TemporaryDirectory() as tmpdir:
            generated_root = Path(tmpdir) / "generated"
            generated_root.mkdir()
            run_dir = generated_root / "sample-run"
            run_dir.mkdir()
            outside_dir = Path(tmpdir) / "outside-run"
            outside_dir.mkdir()

            original_root = mod.GENERATED_ROOT
            mod.GENERATED_ROOT = generated_root
            try:
                removed = mod.remove_local_run_dir(run_dir)
                self.assertEqual(removed, run_dir.resolve())
                self.assertFalse(run_dir.exists())

                with self.assertRaises(SystemExit):
                    mod.remove_local_run_dir(outside_dir)
            finally:
                mod.GENERATED_ROOT = original_root


if __name__ == "__main__":
    unittest.main()
