---
title: "Nextmini: a High-Performance Network Emulation and Experimentation Testbed"
description: ""
---


*Nextmini* is a high-performance network emulation testbed, written in the Rust programming language. It is first and foremost designed to run as a network emulation testbed within Docker containers in the same compute cluster, but it can also run natively and across geographically distributed datacenters. Similar to conventional Virtual Private Networks (VPNs), Nextmini leverages the cross-platform [TUN interface](https://en.wikipedia.org/wiki/TUN/TAP) and behaves as a virtual network device to distributed workloads. This allows distributed workloads, such as distributed machine learning workloads, to leverage the full power of the emulation testbed obliviously. As its name suggested, it is designed to supersede many of the core use cases of [Mininet](https://mininet.org), and extend it with the ability to scale up even further across multiple physical machines, and to run any distributed workload on the testbed.

Though Nextmini runs natively on Linux, the easiest way to get started with Nextmini is to run it within Docker containers. The Docker image is built atop the latest distribution of Alpine Linux and contains all the necessary dependencies to run Nextmini.

## Core capabilities

Thanks to the Rust architecture and recent protocol updates, current behavior includes a control plane/dataplane split, explicit route orchestration, and an integrated lossless/FEC transport path. The controller now manages topology, routing, multicast state, and flow policy and pushes these updates to each dataplane node over websocket connections. The dataplane applies scheduling and forwarding in user space, and now handles both unicast and multicast trees in a single table implementation.

Flow transport for controller-installed traffic is configurable. The default is `tcp` while `lossless_unicast` enables a Rust-native lossless sender/receiver path that can use RaptorQ-backed FEC for recovery behavior when configured. Per-flow pacing controls remain based on `flow_rate` and `flow_len` where applicable, and are enforced through the dataplane scheduler.

The transport stack between nodes now supports multiple options at the link layer (`tcp`, `udp`, and `quic`), while flow semantics can differ from the base transport where needed. Multicast control flow is source-scoped and tree-aware, with explicit DAG route installation from the controller and group membership managed through existing websocket messages.

Python integrations are first-class through `nextmini_py`, which provides the same bindings used by examples and tooling. This includes direct dataplane startup, flow registration, and multicast group operations while preserving the same runtime messaging model.

## Where to go next

Visit [Design](/docs/design/architecture) for the architectural design in this project, [Examples](/docs/examples/simple) for some end-to-end example scenarios, and [Testing](/docs/testing/cargo-test) for development notes on testing.
