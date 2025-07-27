# How to Run Nextmini node with Isolated Network Namespace

## Prerequisites

- Ubuntu 24.04
- Rust, Cargo and other test tools (iperf3, ifconfig ...) pre-installed

## Getting Started

**Step 1 : Build the Project**

You need to ensure `namespace_init` is added as a member in the workspace before proceeding.

```bash
cd nextmini/namespace_init; cargo build --release
```

**Step 2 : Run Dataplane Nodes in Namespaces**

```bash
cd ..
sudo env "RUST_LOG=warn" ./target/release/namespace_init
```

**Step 3 : Start Controller and Database**

In a new terminal, start controller and database with the following:

```bash
cd nextmini/namespace_init/controller_standalone; docker compose up --build
```

## Running Tests

You can enter into namespace's terminal by running:

```bash
nsenter -t <child_PID> -n bash
```

where <child_PID> is the process ID, provided at the start of the terminal logs, of the target node.

Then, you can conduct network tests such as `iperf3`.

## Cleanup

To stop the dataplane nodes, simply press `CTRL_C` in Step2's terminal.

To delete all veths, run:

```bash
sudo bash -c 'for veth in $(ifconfig | grep "^veth" | cut -d" " -f1); do ip link delete "$veth"; done'
```

You can check the status with `ifconfig`.
