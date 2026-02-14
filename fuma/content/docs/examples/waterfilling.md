
## Water-Filling Routing

> **Warning**
> The following instructions have not been verified to work correctly.


The water-filling routing example, written in Python, showcases run-time route adaptation based on live performance measurements. To start the experiment with water-filling routing, open a terminal and run the following:

```bash
cd ./examples/routing/waterfilling && docker compose build && docker compose up
```

Before starting to build the docker image, it is recommended to start from a clean slate:

```bash
docker system prune -a
```

This will remove all stopped containers, all unused networks and volumes, and all build cache. If you wish to remove all existing volumes at the same time, run:

```bash
docker system prune -a --volumes -f
```

To reset the environment and start from a clean state, run:

```bash
docker compose down
docker compose build --no-cache
```

_Note:_ Port `5432` is the default for PostgreSQL. On macOS, running a local PostgreSQL instance may conflict with Docker containers using the same port. To avoid issues, do not run another PostgreSQL server on macOS while using Docker.

This will start a Nextmini network with 4 nodes and a controller. We are interested in having `node1` as the data source and `node2` as data destination. We configure 3 paths between the two nodes, 1→2, 1→3→2, and 1→4→2. In addition, we leverage Nextmini's link-rate control feature to manually set link 1→2 to have a bandwidth of 10 Mbps, link 3→2 20 Mbps, and link 4→2 30 Mbps. This effectively limits the bandwidth for the three paths to 10 Mbps, 20 Mbps, and 30 Mbps respectively. Details regarding how these are configured are contained in the `controller-config.toml` file.

_Running the workload._ We can now generate arbitrary data with `iperf3` workloads. In this case, we use 6 iperf connections each with 10 Mbps bandwidth using the UDP protocol (TCP won't allow us to set the bandwidth). Manually setting up these iperf connections can be a hassle, so we included two shell scripts to automatically set them up. To execute them, in separate terminals, run the following commands respectively.

In a new terminal, start the iperf3 servers on node2 by running:

```bash
docker exec -it node2 /bin/bash -c "./iperf3_s.sh"
```

In another terminal, start the iperf3 clients on node1 by running:

```bash
docker exec -it node1 /bin/bash -c "./iperf3_c.sh"
```

**Monitoring the throughput.** To monitor the network throughput, first make sure you have `uv` installed first:

```sh
curl -LsSf https://astral.sh/uv/install.sh | sh
```

And make sure that `$HOME/.cargo/bin` is in the `$PATH` by revising `~/.zshrc` accordingly:

```sh
export PATH=$HOME/.cargo/bin:$PATH
```

Later on, whenever one needs to update the version of `uv`, the following command can be used:

```sh
uv self update
```

They use the following command for live data monitoring, you will see a command-line dashboard for live data monitoring:

```bash
cd ./tools/monitor && uv run dashboard.py
```

Observe the traffic in each path, and note how they are not distributed evenly according to the bandwidth limit we set for each path.

**Running the algorithm.** To run the water-filling algorithm, open one more terminal and run:

```bash
cd ./tools/routing && uv run waterfilling.py
```

By default, the waterfilling algorithm will run in 2-second intervals, and print the output in each round. Once convergence is reached, the algorithm will stop printing.

Once the water-filling algorithm converges, observe the flow in each link from the dashboard again. Now, each path should have around 10 Mbps, 20 Mbps, and 30 Mbps of traffic in them respectively.

_Known caveat._ Perhaps due to the design of the water-filling algorithm, the converged values may be 10 Mbps, 20 Mbps, and 10 Mbps in some of the runs.
