# Build & Run (binaries)

This page is for running the controller and dataplane as native binaries (no Docker for Nextmini itself).

## Prerequisites

- Rust toolchain (stable)
- A local Postgres instance for the controller
- Root/CAP_NET_ADMIN when using the TUN interface

## 1) Build

From the repo root:

```bash
cargo build --workspace
```

If you only want the binaries:

```bash
cargo build --release -p controller
cargo build --release -p nextmini
```

## 2) Start Postgres

```bash
./start-database.sh
```

## 3) Run the controller

The controller always loads `config.toml` from its working directory.

```bash
cd examples/bare-metal/controller
RUST_LOG=info ../../../target/release/controller
```

## 4) Run a dataplane node

Run as root (or with CAP_NET_ADMIN) if `enable_local_interface = true` (TUN mode):

```bash
cd examples/bare-metal/dataplane
sudo -E RUST_LOG=info ../../../target/release/nextmini --config-path node.toml
```

Notes:

- You can specify the controller address either in the config (`controller_addr = "ws://..."`) or as a positional argument.
- For fully scripted native deployment across multiple hosts, use the bare-metal example scripts: [Bare Metal deployment](../examples/bare-metal.md).

