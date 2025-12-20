#!/usr/bin/env python3
"""Compute multicast DAG edges for Nextmini (LP / heuristics).

This module is designed to be importable and runnable as:

```bash
python -m examples.lp.main --controller-config examples/rl/configs-docker/controller-config.toml --src 1 --dests 2,3
```

Optional: apply the computed edges to a live controller *from the source node* using the Python
bindings:

```bash
python -m examples.lp.main \
  --controller-config examples/rl/configs-docker/controller-config.toml \
  --src 1 --dests 2,3 \
  --apply --node-config examples/rl/configs-docker/trainer-config.toml --group-id 7
```
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from .solver import build_graph_from_controller_config, compute_mflow_tree_edges


def _parse_node_list(spec: str) -> list[int]:
    items = [s.strip() for s in spec.split(",")]
    out: list[int] = []
    for item in items:
        if not item:
            continue
        out.append(int(item))
    return out


def main() -> int:
    parser = argparse.ArgumentParser(description="Compute multicast DAG edges for Nextmini")
    parser.add_argument(
        "--controller-config",
        type=str,
        required=True,
        help="Path to controller-config.toml (topology source)",
    )
    parser.add_argument("--src", type=int, required=True, help="Source node ID (e.g. trainer)")
    parser.add_argument(
        "--dests",
        type=str,
        required=True,
        help="Comma-separated destination node IDs (e.g. 2,3)",
    )
    parser.add_argument(
        "--default-capacity",
        type=int,
        default=None,
        help="Optional default link capacity override (Mbps) for the LP graph",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print edges as JSON (list of [u,v])",
    )

    # Optional apply step (requires nextmini_py and must run as the source node).
    parser.add_argument("--apply", action="store_true", help="Apply edges via nextmini_py.set_group_routes()")
    parser.add_argument("--node-config", type=str, help="Path to the SOURCE node's dataplane config.toml")
    parser.add_argument("--group-id", type=int, help="Existing multicast group_id to override")

    args = parser.parse_args()

    destinations = _parse_node_list(args.dests)
    if not destinations:
        print("Error: --dests must contain at least one node id", file=sys.stderr)
        return 2

    graph = build_graph_from_controller_config(
        args.controller_config, default_capacity=args.default_capacity
    )

    edges, throughput = compute_mflow_tree_edges(graph, src=args.src, destinations=destinations)

    if args.json:
        print(json.dumps([[a, b] for (a, b) in edges]))
    else:
        print(f"edges={edges}")
        if throughput is not None:
            print(f"throughput={throughput}")

    if args.apply:
        if args.node_config is None or args.group_id is None:
            print("Error: --apply requires --node-config and --group-id", file=sys.stderr)
            return 2

        node_cfg_path = Path(args.node_config)
        if not node_cfg_path.exists():
            print(f"Error: node config not found: {node_cfg_path}", file=sys.stderr)
            return 2

        try:
            import nextmini_py as nm  # type: ignore
        except ImportError as exc:
            raise SystemExit(
                "nextmini_py is not installed. Build/install it first, e.g.:\n"
                "  maturin develop --release -m python-api/Cargo.toml -F python-extension\n"
            ) from exc

        dp = nm.Dataplane(str(node_cfg_path))
        dp.set_group_routes(args.group_id, edges)
        print(f"Applied override: group_id={args.group_id} edges={len(edges)}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())


