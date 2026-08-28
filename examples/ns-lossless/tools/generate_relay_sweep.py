#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import pathlib
import random
from typing import Any


SOURCE_NODE_ID = 1


def parse_k_values(raw: str) -> list[int]:
    values = [int(value) for value in raw.split(",")]
    if not values or any(value < 0 for value in values):
        raise argparse.ArgumentTypeError("K values must be non-negative integers")
    if len(values) != len(set(values)):
        raise argparse.ArgumentTypeError("K values must be unique")
    return values


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate a nested fixed-seed WAN family for a source-end relay sweep."
    )
    parser.add_argument("--output-dir", type=pathlib.Path, required=True)
    parser.add_argument("--seed", type=int, default=0)
    parser.add_argument("--receivers", type=int, default=6)
    parser.add_argument("--k-values", type=parse_k_values, default=parse_k_values("0,1,2,3,4,5"))
    parser.add_argument("--cross-dc-min-mbit", type=float, default=50.0)
    parser.add_argument("--cross-dc-max-mbit", type=float, default=200.0)
    parser.add_argument("--feed-min-mbit", type=float, default=900.0)
    parser.add_argument("--feed-max-mbit", type=float, default=1100.0)
    parser.add_argument("--node-cap-mbit", type=float, default=1000.0)
    return parser.parse_args()


def sampled_capacity(rng: random.Random, low: float, high: float) -> float:
    return round(rng.uniform(low, high), 6)


def validate_parameters(
    *,
    receiver_count: int,
    k_values: list[int],
    cross_dc_min_mbit: float,
    cross_dc_max_mbit: float,
    feed_min_mbit: float,
    feed_max_mbit: float,
    node_cap_mbit: float,
) -> None:
    if receiver_count < 2:
        raise ValueError("receiver count must be at least two")
    if not k_values:
        raise ValueError("K values must not be empty")
    if cross_dc_min_mbit <= 0 or cross_dc_min_mbit > cross_dc_max_mbit:
        raise ValueError("cross-DC capacity range must be positive and ordered")
    if feed_min_mbit <= 0 or feed_min_mbit > feed_max_mbit:
        raise ValueError("feed capacity range must be positive and ordered")
    if node_cap_mbit <= 0:
        raise ValueError("node cap must be positive")


def generate_family(
    *,
    seed: int,
    receiver_count: int,
    k_values: list[int],
    cross_dc_min_mbit: float,
    cross_dc_max_mbit: float,
    feed_min_mbit: float,
    feed_max_mbit: float,
    node_cap_mbit: float,
) -> tuple[dict[str, Any], dict[int, dict[str, Any]]]:
    validate_parameters(
        receiver_count=receiver_count,
        k_values=k_values,
        cross_dc_min_mbit=cross_dc_min_mbit,
        cross_dc_max_mbit=cross_dc_max_mbit,
        feed_min_mbit=feed_min_mbit,
        feed_max_mbit=feed_max_mbit,
        node_cap_mbit=node_cap_mbit,
    )

    rng = random.Random(seed)
    receivers = list(range(2, 2 + receiver_count))
    max_relays = max(k_values)
    relays = list(range(2 + receiver_count, 2 + receiver_count + max_relays))

    source_direct = {
        receiver: sampled_capacity(rng, cross_dc_min_mbit, cross_dc_max_mbit)
        for receiver in receivers
    }
    receiver_links = {
        (src, dst): sampled_capacity(rng, cross_dc_min_mbit, cross_dc_max_mbit)
        for src in receivers
        for dst in receivers
        if src != dst
    }
    source_feeds = {
        relay: sampled_capacity(rng, feed_min_mbit, feed_max_mbit)
        for relay in relays
    }
    relay_links = {
        (relay, receiver): sampled_capacity(
            rng, cross_dc_min_mbit, cross_dc_max_mbit
        )
        for relay in relays
        for receiver in receivers
    }

    family = {
        "seed": seed,
        "source": SOURCE_NODE_ID,
        "receivers": receivers,
        "relay_order": relays,
        "k_values": k_values,
        "cross_dc_range_mbit": [cross_dc_min_mbit, cross_dc_max_mbit],
        "source_relay_feed_range_mbit": [feed_min_mbit, feed_max_mbit],
        "node_cap_mbit": node_cap_mbit,
        "source_direct_mbit": {str(dst): bw for dst, bw in source_direct.items()},
        "source_relay_feed_mbit": {str(dst): bw for dst, bw in source_feeds.items()},
        "relay_receiver_mbit": {
            f"{src}->{dst}": bw for (src, dst), bw in relay_links.items()
        },
        "receiver_receiver_mbit": {
            f"{src}->{dst}": bw for (src, dst), bw in receiver_links.items()
        },
    }

    scenarios: dict[int, dict[str, Any]] = {}
    for k in k_values:
        active_relays = relays[:k]
        edges: list[dict[str, int | float]] = []
        if k == 0:
            edges.extend(
                {"src": SOURCE_NODE_ID, "dst": dst, "bw": source_direct[dst]}
                for dst in receivers
            )
        else:
            edges.extend(
                {"src": SOURCE_NODE_ID, "dst": relay, "bw": source_feeds[relay]}
                for relay in active_relays
            )
            edges.extend(
                {"src": relay, "dst": receiver, "bw": relay_links[(relay, receiver)]}
                for relay in active_relays
                for receiver in receivers
            )
        edges.extend(
            {"src": src, "dst": dst, "bw": receiver_links[(src, dst)]}
            for src in receivers
            for dst in receivers
            if src != dst
        )

        egress_nodes = (
            [SOURCE_NODE_ID, *receivers]
            if k == 0
            else [*active_relays, *receivers]
        )
        scenarios[k] = {
            "scenario": {
                "name": f"fixed-seed-realistic-relay-sweep-k{k}",
                "source": SOURCE_NODE_ID,
                "receivers": receivers,
                "forwarding_nodes": [*receivers, *active_relays],
                "receiver_relays": True,
                "edges": edges,
                "node_caps": {
                    "egress": {str(node): node_cap_mbit for node in egress_nodes},
                    "ingress": {
                        str(receiver): node_cap_mbit for receiver in receivers
                    },
                },
            }
        }
    return family, scenarios


def write_json(path: pathlib.Path, payload: dict[str, Any]) -> str:
    encoded = (json.dumps(payload, indent=2, sort_keys=True) + "\n").encode()
    path.write_bytes(encoded)
    return hashlib.sha256(encoded).hexdigest()


def main() -> None:
    args = parse_args()
    family, scenarios = generate_family(
        seed=args.seed,
        receiver_count=args.receivers,
        k_values=args.k_values,
        cross_dc_min_mbit=args.cross_dc_min_mbit,
        cross_dc_max_mbit=args.cross_dc_max_mbit,
        feed_min_mbit=args.feed_min_mbit,
        feed_max_mbit=args.feed_max_mbit,
        node_cap_mbit=args.node_cap_mbit,
    )
    args.output_dir.mkdir(parents=True, exist_ok=True)
    hashes = {"family.json": write_json(args.output_dir / "family.json", family)}
    for k, scenario in scenarios.items():
        name = f"k{k}-raw-scenario.json"
        hashes[name] = write_json(args.output_dir / name, scenario)
    write_json(args.output_dir / "sha256.json", hashes)
    print(json.dumps(hashes, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
