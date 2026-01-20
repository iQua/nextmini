# Running ns-flow

Quick start:

```bash
sudo ./examples/ns-flow/run.sh
```

`run.sh` waits for the controller to seed routes + flows in Postgres before starting the dataplane.

Regenerate configs (optional; `run.sh` does this by default):

```bash
python3 examples/ns-flow/generate.py --n-nodes 136 --n-flows 180 --src 1 --dst 136
```

Verify all flows finished:

```bash
docker exec postgres psql -U pgusr -d nextmini -c "SELECT COUNT(*) AS total, COUNT(*) FILTER (WHERE is_finished) AS finished FROM flows;"
```

Cleanup:

```bash
sudo ./examples/ns-flow/cleanup.sh
```
