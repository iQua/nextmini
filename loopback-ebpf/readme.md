# Loopback eBPF

This document describes how to build and run the loopback-ebpf example.

The 'bpf_link_create' problem maybe encoutered, this is likely due to some operation denied by kernel. At the time of implementation, this example can be run successfully on macOS environment.

### Prerequisites

- May need to install extra cargo toolchains

### Running the example

Under current directory, simply run:

```sh
cargo build --release; docker compose up --build
```
