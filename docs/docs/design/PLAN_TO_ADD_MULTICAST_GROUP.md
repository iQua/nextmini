# Multicast groups – rollout status

The detailed design and API reference for multicast groups now live in [`multicast-groups.md`](multicast-groups.md). This
page only tracks rollout health and serves as a quick checklist for teams enabling the feature.

## Current status

- ✅ Controller persists groups/members/routes and streams `GroupCreated`, `InstallGroupDirectory`, and
  `InstallGroupRoutes` messages to connected dataplanes.
- ✅ Dataplanes install directory + route updates, fan out packets per hop, and expose join/leave helpers through
  `nextmini_py`.
- ✅ Example + design docs updated (see `docs/docs/examples/multicast-flow.md` and `docs/docs/design/multicast-groups.md`).
- ☐ CI automation: Postgres-backed integration test + docker-compose harness still need to run in CI once shared hosts
  ship the required dependencies.

## Rollout checklist

1. Update controller config with the multicast pool you plan to use (`multicast_pool_base` / `multicast_pool_mask`).
2. Deploy the upgraded controller before rolling out dataplanes so websocket messages are understood everywhere.
3. Rebuild and roll dataplanes so they install the directory/route payloads and expose the Python APIs.
4. For receiver workloads, call `join_group` + `wait_for_local_membership` (or the CLI equivalent) before expecting
   traffic, then `leave_group` during teardown.
5. Monitor controller logs for `GroupCreated`/`InstallGroupRoutes` lines during the first deployment and confirm
   dataplanes log the matching installs.
6. Keep `docs/docs/examples/multicast-flow.md` handy as a regression walkthrough whenever you touch controller or
   dataplane code in this area.

## Pending work

- Run the Postgres-backed integration script under CI once the multi-node harness is back online.
- Add operator tooling (`controller groups list`, idle-group cleanup) once telemetry requirements solidify.
- Document performance/scale test results after the first production bake.
