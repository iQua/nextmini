# multi-dc (legacy)

`examples/multi-dc` contains historical notes and manifests for deploying Nextmini across multiple datacenters using Docker Swarm.

This setup is **not maintained as a primary path** and typically requires environment-specific tuning (ports, firewalls, swarm placement constraints, VM images).

If you are starting fresh, prefer:

- The single-machine Docker examples (to validate configs and routing)
- [Public network deployment (no swarm)](public-network.md) (small multi-VM setups)

If you still want to use `examples/multi-dc`, treat it as a reference and expect to edit the manifests for your environment.

