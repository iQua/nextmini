import argparse
import time

try:
    import nextmini_py as nm
except ImportError as exc:
    raise SystemExit(
        "nextmini_py is not installed. Build the wheel with:\n"
        "  maturin build --release -m python-api/Cargo.toml\n"
        "  pip install target/wheels/nextmini_py-*.whl"
    ) from exc


def main() -> int:
    parser = argparse.ArgumentParser(description="Nextmini relay/idle node (no ML).")
    parser.add_argument("--config", required=True, help="Nextmini node config TOML path")
    parser.add_argument("--once", action="store_true", help="Exit after topology is ready.")
    args = parser.parse_args()

    dp = nm.Dataplane(args.config)
    node_id = int(dp.node_id)
    print(f"Relay node {node_id} starting; waiting for topology ready...", flush=True)
    dp.wait_for_topology_ready()
    print(f"Relay node {node_id} topology ready.", flush=True)

    if args.once:
        return 0

    try:
        while True:
            time.sleep(3600)
    except KeyboardInterrupt:
        return 0


if __name__ == "__main__":
    raise SystemExit(main())

