#!/usr/bin/env python3
"""
Generate ring.txt for baseline nodes before building Docker image.

Usage:
    python3 generate_ring.py --num-nodes 10
"""

import argparse


def main():
    parser = argparse.ArgumentParser(description="Generate ring.txt")
    parser.add_argument("--num-nodes", type=int, default=10, help="Number of nodes")
    parser.add_argument("--port", type=int, default=9000, help="Ring port")
    args = parser.parse_args()

    with open("ring.txt", "w") as f:
        for i in range(1, args.num_nodes + 1):
            f.write(f"baseline-ring-{i}.internal:{args.port}\n")

    print(f"Generated ring.txt for {args.num_nodes} nodes")
    print(f"   Port: {args.port}")


if __name__ == "__main__":
    main()
