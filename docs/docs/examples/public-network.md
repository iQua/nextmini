# Manual deployment

This example (`examples/public-network`) runs the controller + Postgres on one VM and a small number of dataplane nodes on other VMs, using public IP addresses.

## What it’s for

- Small multi-VM deployments where you want to control placement manually
- Debugging connectivity and routing across real networks

## Prerequisites

- Linux hosts/VMs with Docker Engine installed (this scenario uses `network_mode: host`)

## Files

The example directory provides separate compose/config files for the controller VM and for each dataplane VM.

## High-level steps

1. Pick a controller VM and start Postgres + controller there.
2. On each dataplane VM, edit the compose file to point at the controller’s public IP:

   - `ws://<controller_public_ip>:3000`

3. Start each dataplane compose file and wait for all nodes to connect.

## Notes

- If you use both private and public NICs, ensure `private_network_name` / `private_network_interface` are set consistently across nodes so the controller can decide which address family to hand out for neighbor connections.
- Expect to adjust firewall rules and security groups to allow controller WebSocket (3000) and dataplane node-to-node ports (typically 8080 plus `max_server_port` if using MAX/proxy flows).
