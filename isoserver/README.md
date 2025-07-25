# How to Run Nextmini node with Isolated Network Namespace

## Prerequisites

- Ubuntu 24.04
- Rust, Cargo and other test tools (iperf3, ifconfig ...) pre-installed

## Getting Started

**Step 1 : Build the Project**

You need to ensure `isoserver` is added as a member in the workspace before proceeding.

```bash
cd nextmini/isoserver; cargo build --release
```

**Step 2 : Run Dataplane Nodes in Namespaces**

```bash
cd .. ; sudo env "RUST_LOG=info" ./target/release/isoserver
```

**Step 3 : Start Controller and Database**

In a new terminal, start controller and database with the following:

```bash
cd nextmini/isoserver/controller_standalone; docker compose up --build
```

## Running Tests

To run network tests such as `iperf3`, you can enter into namespace's terminal with:

```bash
nsenter -t <child_PID> -n bash
```

where <child_PID> is the process ID, provided at the start of the terminal logs, of the target node.

## Cleanup

To stop the dataplane nodes, simply press `CTRL_C` in Step2's terminal. It takes quite amount of time to clear up all the `veths` created. You can check the status with `ifconfig`.
