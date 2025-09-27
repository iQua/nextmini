# Description

This example aims to test the starting time and memory usage when spawning different number of nodes via the namespace feature of Nextmini.

## How to run

**Step 0 : Change the number of nodes**

To change the number of nodes, you need to update three config files.

First, change the `n_nodes` field in `net-config.toml` by:

```bash
vi nextmini/namespace_init/net-config.toml
```

Then, change the `n_nodes` field in the `controller-config.toml` by:

```bash
vi nextmini/namespace_init/controller_standalone/controller-config.toml
```

Finally, change the `n_nodes` field in the `config.toml` by:

```bash
vi nextmini/namespace_init/config.toml
```

If the number of nodes is greated than 500, it is recommended that the `controller_service_rate` field in the `config.toml` is set to a smaller value, such as 6, so that nodes requests towards the controller can be spread out over time.

**Step 1 : Start Controller and Database**

Start controller and database with the following:

```bash
cd nextmini/namespace_init/controller_standalone; docker compose up --build
```

**Step 2 : Build the Project**

You need to ensure `namespace_init` is added as a member in the workspace before proceeding. Run the following in a new terminal to build the project.

```bash
cd nextmini/namespace_init; cargo build --release
```

**Step 3 : Run Dataplane Nodes in Namespaces**

```bash
cd nextmini
sudo ./target/release/namespace_init
```

**Step 4 : Observe the Results**

You can see results similar to the following logged out at the terminal running the controller:

```bash
controller  | 2025-07-27T17:21:16.925666Z  WARN controller: All 128 nodes are now connected. Sending node addresses, link rates and flows to all nodes.
controller  | 2025-07-27T17:21:17.299502Z  WARN controller: It took 38.181973 seconds for all nodes to fully connect to the controller.
```

Now, in a new terminal, you can use `free -h` to check the memory usage.

**Step 5 : Cleanup**

To cleanup, simply press `CTRL_C` at the two terminals in Step 1 and Step 2. Then, in the terminal running `namespace_init` run the following command to clear the created veths.

```bash
sudo bash -c 'for veth in $(ifconfig | grep "^veth" | cut -d" " -f1); do ip link delete "$veth"; done'; echo "Cleaned up veths successfully"
```

Check if all veth pairs cleaned up:

```bash
ip link show | grep veth | wc -l
```

## No persistent tcp connections and no routes

The namespace example can be tested with arbutus c16-180-576.

You can change the `n_nodes` field to the number to test. Also, remove the `[routing]` section since we are testing pure start up speed and memory usage of nodes.


---

## Test memory before build controller and postgreSQL

Open a terminal and enter:

```bash
free -h
```


```text
ubuntu@ns-test024:~/nextmini/namespace_init/controller_standalone$ free -h
               total        used        free      shared  buff/cache   available
Mem:           176Gi       3.3Gi       162Gi       1.2Mi        12Gi       173Gi
Swap:             0B          0B          0B
```

## Test memory after building up controller and postgreSQL

Enter the following command:
```bash
docker compose build
docker compose up
```

Open an another terminal and enter `free -h`:
```text
ubuntu@ns-test024:~/nextmini$ free -h
               total        used        free      shared  buff/cache   available
Mem:           176Gi       4.1Gi       161Gi        15Mi        12Gi       172Gi
Swap:             0B          0B          0B
```

Then 0.4Gi is used for controller and postgreSQL.

## Build namespace related code

Open another terminal and enter:
```bash
cd nextmini/namespace_init; cargo build --release
```

## Run Dataplane Nodes in Namespaces

Open another terminal and enter:
```bash
cd nextmini
sudo ./target/release/namespace_init
```


## Database limit

### [How to set the max_connections higher](https://stackoverflow.com/questions/2757549/org-postgresql-util-psqlexception-fatal-sorry-too-many-clients-already)

To change the default max_connections 100 to higher, enter the following command in `docker-compose.yml` before creating controller and postgreSQL:
```bash
command: postgres -c max_connections=600 -c shared_preload_libraries=pg_stat_statements
```

Check the max_connections of postgreSQL after creating controller and postgreSQL:
```bash
docker exec postgres psql -U pgusr -d nextmini -c "SHOW max_connections;"
```

Check how many idle connections:
```bash
docker exec postgres psql -U pgusr -d nextmini -c "
SELECT
    state,
    count(*) as connection_count,
    application_name
FROM pg_stat_activity
GROUP BY state, application_name
ORDER BY connection_count DESC;"
```

## 600 nodes

```text
controller  | 2025-09-26T22:40:24.693768Z  INFO controller::new_node: All dataplane nodes have connected. It takes 412.26 seconds since the first node arrived.
```

### increase APR

```bash
sudo sysctl net.ipv4.neigh.default.gc_thresh1=2048
sudo sysctl net.ipv4.neigh.default.gc_thresh2=4096
sudo sysctl net.ipv4.neigh.default.gc_thresh3=8192
```

###  Watch APR

```bash
watch -n 1 'echo "=== $(date +%H:%M:%S) ==="; echo "ARP: $(arp -a | wc -l)/$(cat /proc/sys/net/ipv4/neigh/default/gc_thresh1)"; echo "Veth UP: $(ip link show | grep "veth.*state UP" | wc -l)"; echo "Controller CPU: $(docker stats controller --no-stream | grep controller | awk '\''{print $3}'\'')"'
```

## Check the missing nodes

```bash
docker exec postgres psql -U pgusr -d nextmini -c "
WITH RECURSIVE expected_nodes AS (
    SELECT 1 as node_id
    UNION ALL
    SELECT node_id + 1
    FROM expected_nodes
    WHERE node_id < 600
)
SELECT en.node_id as missing_node_id
FROM expected_nodes en
LEFT JOIN nodes n ON en.node_id = n.id
WHERE n.id IS NULL
ORDER BY en.node_id
LIMIT 10;"
```
