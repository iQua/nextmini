from __future__ import annotations

import importlib.util
import sys
import tempfile
import textwrap
import types
import unittest
from argparse import Namespace
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
MULTICAST_NODE_PATH = (
    ROOT / "examples" / "multicast-docker" / "scripts" / "multicast_node.py"
)


def load_multicast_node_module():
    sys.modules.setdefault("nextmini_py", types.SimpleNamespace())
    spec = importlib.util.spec_from_file_location("multicast_node", MULTICAST_NODE_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to load module from {MULTICAST_NODE_PATH}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def write_controller_config(path: Path) -> None:
    path.write_text(
        textwrap.dedent(
            """\
            protocol = "tcp"

            [routing]
            protocol = "shortest_path"

            [topology]
            n_nodes = 6
            edges = [
              [1, 5],
              [5, 2],
              [5, 3],
              [5, 4],
              [1, 6],
              [6, 2],
              [6, 3],
              [6, 4],
            ]
            """
        ),
        encoding="utf-8",
    )


class MulticastNodeTests(unittest.TestCase):
    def test_validate_chunk_size_rejects_oversized_plain_payload_but_allows_large_fec_blocks(
        self,
    ) -> None:
        mod = load_multicast_node_module()

        with self.assertRaises(SystemExit):
            mod.validate_chunk_size(65_536)

        self.assertEqual(mod.validate_chunk_size(8_192), 8_192)
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "node.toml"
            config_path.write_text(
                textwrap.dedent(
                    """\
                    [lossless_runtime_config]
                    fec_default_symbols_per_block = 16
                    """
                ),
                encoding="utf-8",
            )
            self.assertEqual(
                mod.validate_chunk_size(
                    128 * 1024,
                    fec="on",
                    config_path=config_path,
                ),
                128 * 1024,
            )

    def test_controller_topology_plan_supports_single_tree_relay_path(self) -> None:
        mod = load_multicast_node_module()
        with tempfile.TemporaryDirectory() as tmpdir:
            controller_config = Path(tmpdir) / "controller-config.toml"
            write_controller_config(controller_config)
            trees = mod.compute_group_route_trees(
                controller_config=controller_config,
                source_node_id=1,
                receiver_ids=[2, 3, 4],
                tree_ids=[0],
            )

        self.assertEqual(
            trees,
            [(0, [(1, 5), (5, 2), (5, 3), (5, 4)])],
        )

    def test_controller_topology_plan_supports_two_distinct_trees(self) -> None:
        mod = load_multicast_node_module()
        with tempfile.TemporaryDirectory() as tmpdir:
            controller_config = Path(tmpdir) / "controller-config.toml"
            write_controller_config(controller_config)
            trees = mod.compute_group_route_trees(
                controller_config=controller_config,
                source_node_id=1,
                receiver_ids=[2, 3, 4],
                tree_ids=[0, 1],
            )

        self.assertEqual(
            trees,
            [
                (0, [(1, 5), (5, 2), (5, 3), (5, 4)]),
                (1, [(1, 6), (6, 2), (6, 3), (6, 4)]),
            ],
        )

    def test_receiver_skips_metadata_wait_when_expected_bytes_is_known(self) -> None:
        mod = load_multicast_node_module()
        with tempfile.TemporaryDirectory() as tmpdir:
            args = Namespace(
                tensor_path=None,
                expected_bytes=1024,
                artifact_dir=Path(tmpdir),
                group_timeout=1,
                quiet=True,
            )
            mod.load_tensor_metadata_if_needed(args)

        self.assertEqual(args.expected_bytes, 1024)

    def test_source_records_metadata_before_waiting_for_receivers(self) -> None:
        mod = load_multicast_node_module()

        class FakePacketView:
            def __init__(self, payload: bytes) -> None:
                self._payload = payload

            def read(self) -> bytes:
                return self._payload

        class FakePacketBuilder:
            def __init__(self, size: int) -> None:
                self._payload = bytearray()
                self.size = size

            def write(self, payload: bytes) -> None:
                self._payload.extend(payload)

            def freeze(self) -> FakePacketView:
                return FakePacketView(bytes(self._payload))

        class FakeDataplane:
            def __init__(self, _config: str) -> None:
                self.node_id = 1

            def wait_for_topology_ready(self, timeout_ms: int) -> bool:
                return True

            def create_group(self, group_label: str) -> None:
                return None

            def group_is_ready(self, timeout_ms: int) -> tuple[int, str, None]:
                return (7, "239.255.0.1", None)

            def set_group_routes(self, group_id: int, edges: list[tuple[int, int]]) -> None:
                return None

            def wait_for_group_routes(
                self, group_id: int, source_node_id: int, timeout_ms: int
            ) -> bool:
                return True

            def send_data(
                self,
                group_id: int,
                group_ip: str,
                receiver_ids: list[int],
                view: FakePacketView,
                block_size: int,
                src_port: int | None,
                dst_port: int | None,
            ) -> int:
                return 11

            def lossless_wait(self, sid: int, timeout_ms: int) -> bool:
                return True

        mod.nm = types.SimpleNamespace(
            Dataplane=FakeDataplane,
            PacketBuilder=FakePacketBuilder,
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            artifact_dir = Path(tmpdir) / "artifacts"
            config_path = Path(tmpdir) / "node-1.toml"
            payload_path = Path(tmpdir) / "payload.bin"
            config_path.write_text(
                textwrap.dedent(
                    """\
                    node_id = 1
                    [lossless_runtime_config]
                    fec_default_tree_ids = [0]
                    """
                ),
                encoding="utf-8",
            )
            payload_path.write_bytes(b"hello-world")

            args = Namespace(
                config=config_path,
                controller_config=None,
                group_label="fec-test",
                chunk_size=1024,
                fec="off",
                receive_timeout_ms=1000,
                group_timeout=2,
                payload_count=None,
                expected_bytes=None,
                source_node_id=1,
                node_id=None,
                src_port=None,
                dst_port=None,
                receiver_ids="2,3",
                tensor_path=payload_path,
                generate_tensor=False,
                artifact_dir=artifact_dir,
                sink_path=None,
                quiet=True,
            )

            original_wait = mod.wait_for_receivers_ready

            def checking_wait(args_obj, receiver_ids):
                metadata = artifact_dir / mod.METADATA_FILE
                self.assertTrue(metadata.exists())
                self.assertEqual(
                    int(mod.json.loads(metadata.read_text())["bytes"]),
                    len(payload_path.read_bytes()),
                )

            mod.wait_for_receivers_ready = checking_wait
            try:
                mod.run_source(args)
            finally:
                mod.wait_for_receivers_ready = original_wait

    def test_source_throughput_timer_starts_after_send_session_registration(self) -> None:
        mod = load_multicast_node_module()

        class FakePacketView:
            def __init__(self, payload: bytes) -> None:
                self._payload = payload

            def read(self) -> bytes:
                return self._payload

        class FakePacketBuilder:
            def __init__(self, size: int) -> None:
                self._payload = bytearray()
                self.size = size

            def write(self, payload: bytes) -> None:
                self._payload.extend(payload)

            def freeze(self) -> FakePacketView:
                return FakePacketView(bytes(self._payload))

        state = {"send_called": False}

        class FakeDataplane:
            def __init__(self, _config: str) -> None:
                self.node_id = 1

            def wait_for_topology_ready(self, timeout_ms: int) -> bool:
                return True

            def create_group(self, group_label: str) -> None:
                return None

            def group_is_ready(self, timeout_ms: int) -> tuple[int, str, None]:
                return (7, "239.255.0.1", None)

            def set_group_routes(self, group_id: int, edges: list[tuple[int, int]]) -> None:
                return None

            def wait_for_group_routes(
                self, group_id: int, source_node_id: int, timeout_ms: int
            ) -> bool:
                return True

            def send_data(
                self,
                group_id: int,
                group_ip: str,
                receiver_ids: list[int],
                view: FakePacketView,
                block_size: int,
                src_port: int | None,
                dst_port: int | None,
            ) -> int:
                state["send_called"] = True
                return 11

            def lossless_wait(self, sid: int, timeout_ms: int) -> bool:
                return True

        mod.nm = types.SimpleNamespace(
            Dataplane=FakeDataplane,
            PacketBuilder=FakePacketBuilder,
        )

        logs: list[str] = []
        original_log = mod.log
        original_perf_counter = mod.time.perf_counter
        original_wait = mod.wait_for_receivers_ready

        counter_values = iter((10.0, 11.0))

        def fake_log(message: str, quiet: bool = False) -> None:
            logs.append(message)

        def fake_perf_counter() -> float:
            self.assertTrue(state["send_called"])
            return next(counter_values)

        mod.log = fake_log
        mod.time.perf_counter = fake_perf_counter
        mod.wait_for_receivers_ready = lambda args_obj, receiver_ids: None

        try:
            with tempfile.TemporaryDirectory() as tmpdir:
                artifact_dir = Path(tmpdir) / "artifacts"
                config_path = Path(tmpdir) / "node-1.toml"
                payload_path = Path(tmpdir) / "payload.bin"
                config_path.write_text(
                    textwrap.dedent(
                        """\
                        node_id = 1
                        [lossless_runtime_config]
                        fec_default_tree_ids = [0]
                        """
                    ),
                    encoding="utf-8",
                )
                payload_path.write_bytes(b"hello-world")

                args = Namespace(
                    config=config_path,
                    controller_config=None,
                    group_label="fec-test",
                    chunk_size=1024,
                    fec="off",
                    receive_timeout_ms=1000,
                    group_timeout=2,
                    payload_count=None,
                    expected_bytes=None,
                    source_node_id=1,
                    node_id=None,
                    src_port=None,
                    dst_port=None,
                    receiver_ids="2,3",
                    tensor_path=payload_path,
                    generate_tensor=False,
                    artifact_dir=artifact_dir,
                    sink_path=None,
                    quiet=False,
                )

                mod.run_source(args)
        finally:
            mod.log = original_log
            mod.time.perf_counter = original_perf_counter
            mod.wait_for_receivers_ready = original_wait

        self.assertIn("Transfer completed in 1.000 seconds.", "\n".join(logs))

    def test_receiver_prefers_ns_lossless_steady_state_metric_when_available(self) -> None:
        mod = load_multicast_node_module()

        class FakePacketView:
            def __init__(self, payload: bytes) -> None:
                self._payload = payload

            def read(self) -> bytes:
                return self._payload

        class FakeDataplane:
            def __init__(self, _config: str) -> None:
                self.node_id = 2

            def wait_for_topology_ready(self, timeout_ms: int) -> bool:
                return True

            def join_group(self, group_id: int) -> None:
                return None

            def receive_data(
                self,
                group_id: int,
                source_node_id: int,
                src_port: int | None = None,
                dst_port: int | None = None,
            ) -> int:
                return 17

            def lossless_wait(self, sid: int, timeout_ms: int) -> bool:
                return True

            def receiver_first_completed_block_offset_ms(self, sid: int) -> float:
                return 250.0

            def receiver_steady_state_duration_ms(self, sid: int) -> float:
                return 500.0

            def get_data_buffer(self, sid: int) -> FakePacketView:
                return FakePacketView(b"x" * 1024)

        mod.nm = types.SimpleNamespace(Dataplane=FakeDataplane)

        logs: list[str] = []
        original_log = mod.log
        original_wait_for_group_info = mod.wait_for_group_info

        def fake_log(message: str, quiet: bool = False) -> None:
            logs.append(message)

        mod.log = fake_log
        mod.wait_for_group_info = lambda args_obj, timeout: 7

        try:
            with tempfile.TemporaryDirectory() as tmpdir:
                artifact_dir = Path(tmpdir) / "artifacts"
                config_path = Path(tmpdir) / "node-2.toml"
                config_path.write_text("node_id = 2\n", encoding="utf-8")

                args = Namespace(
                    config=config_path,
                    controller_config=None,
                    group_label="fec-test",
                    chunk_size=1024,
                    fec="off",
                    receive_timeout_ms=1000,
                    group_timeout=2,
                    payload_count=None,
                    expected_bytes=1024,
                    source_node_id=1,
                    node_id=2,
                    src_port=None,
                    dst_port=None,
                    receiver_ids="",
                    tensor_path=None,
                    generate_tensor=False,
                    artifact_dir=artifact_dir,
                    sink_path=None,
                    quiet=False,
                )

                mod.run_receiver(args)
        finally:
            mod.log = original_log
            mod.wait_for_group_info = original_wait_for_group_info

        combined_logs = "\n".join(logs)
        self.assertIn(
            "First completed block arrived 0.250s after receiver session start.",
            combined_logs,
        )
        self.assertIn("Reception completed in 0.500s.", combined_logs)
        self.assertIn("Reception full-session wall time was", combined_logs)


if __name__ == "__main__":
    unittest.main()
