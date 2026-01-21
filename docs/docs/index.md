# Nextmini

*Nextmini* is a high-performance network emulation testbed, written in Rust. It is designed to run inside Docker containers in the same compute cluster, but it can also run natively and across geographically distributed datacenters. Similar to conventional Virtual Private Networks (VPNs), Nextmini leverages the cross-platform [TUN interface](https://en.wikipedia.org/wiki/TUN/TAP) and behaves as a virtual network device to distributed workloads. This allows distributed workloads, such as distributed machine learning workloads, to use the emulation testbed without application changes. As its name suggests, it is designed to supersede many of the core use cases of [Mininet](https://mininet.org) while scaling across multiple physical machines.

Thanks to the Rust programming language, Nextmini provides three core features to be highly performant, capable of satisfying modern network emulation needs:

- **High performance, fully asynchronous architecture.** Based on the highly efficient [`tokio`](https://tokio.rs) library, Nextmini runs in userspace, and firmly embraces the `async/await` pattern throughout its design, ensuring _multi-Gbps_ throughput by taking full advantage of the abundance of compute cores in modern compute clusters.

- **Multi-path routing.** Nextmini supports multi-path routing obliviously, with each TCP flow traversing a different route in an emulated or real-world network.

- **Built-in performance monitoring and hot reconfiguration**. Nextmini is designed to operate in both emulated and real-world network environments. It provides the capability of both emulating and monitoring network performance at per-flow granularity, and of reconfiguring routes on-the-fly to adapt to changing network conditions.

Though Nextmini runs natively on Linux, the easiest way to get started with Nextmini is to run it within Docker containers. The Docker image is built atop the latest distribution of Alpine Linux and contains all the necessary dependencies to run Nextmini.

## Start here

- Fastest path: [Quick Start](getting-started/quickstart.md)
- How to pick an example: [Examples overview](examples/overview.md)
- All config knobs: [Configuration Reference](design/config-reference.md)
- Native binaries and Postgres setup: [Build & run (native)](getting-started/build-and-run.md)
- Docs tooling: [Docs](getting-started/docs-and-api.md)

## How the docs are organized

- **Getting Started**: basics, quick start, and docs tooling
- **Design**: architecture and core subsystems
- **Examples**: runnable scenarios
- **Deployment**: multi-host and production-like setups
- **Testing**: how to run the test suite
