# FEC WAN Experiment

This folder contains the WAN runner for comparing plain multicast against FEC multicast on the Boston/Arbutus testbed.

The runner lives in `examples/fec/run_fec.py`. It generates configs and payloads locally, stages them to the remote hosts, launches the controller on Boston, launches node containers on the selected VMs, fetches artifacts, verifies the received payloads, and cleans up the run by default.

## What The Runner Assumes

- You can SSH from the machine running `run_fec.py` to Boston and to every selected VM.
- Docker is installed on Boston and on every participating VM.
- The Boston user can run Docker without `sudo`.
- Every VM that needs to pull images already trusts the Boston registry at `boston.csl.toronto.edu:5000`.

Important: the runner does **not** configure Docker daemon trust for you. It starts a plain `registry:2` container on Boston and then assumes the other VMs can pull from it.

## How Image Publishing Works

`prepare-images` does the image build and publish flow:

1. Rsyncs the repository to the Boston controller host.
2. Starts a Docker registry on Boston if one is not already running.
3. Builds the controller image on Boston and tags it as:
   - `127.0.0.1:5000/nextmini-controller:<tag>`
4. Builds the FEC image on Boston and tags it as:
   - `127.0.0.1:5000/nextmini-fec:<tag>`
5. Pushes both images into the Boston registry.
6. Smoke-tests the FEC image by pulling `boston.csl.toronto.edu:5000/nextmini-fec:<tag>` on the first worker and importing `nextmini_py`.

At runtime:

- Boston pulls the controller image via `127.0.0.1:5000/...`
- Worker and relay VMs pull the node image via `boston.csl.toronto.edu:5000/...`

That split is intentional:

- local Boston push/pull uses `127.0.0.1:5000`
- remote VM pull uses `boston.csl.toronto.edu:5000`

## Letting VMs Trust The Boston Registry

The Boston registry is a plain private registry, not a TLS-backed public registry. Each VM that pulls from it must allow Docker to use `boston.csl.toronto.edu:5000` as an insecure registry.

If the VM does not already trust it, configure Docker on that VM with something like:

```bash
sudo mkdir -p /etc/docker
sudo tee /etc/docker/daemon.json >/dev/null <<'JSON'
{
  "insecure-registries": [
    "boston.csl.toronto.edu:5000"
  ]
}
JSON
sudo systemctl restart docker
```

If the VM already has `/etc/docker/daemon.json`, merge the `insecure-registries` entry instead of overwriting the file.

On Arbutus, you may also already be using:

```json
{
  "data-root": "/mnt/docker"
}
```

In that case, merge both settings:

```json
{
  "data-root": "/mnt/docker",
  "insecure-registries": [
    "boston.csl.toronto.edu:5000"
  ]
}
```

After restarting Docker, verify the setting:

```bash
docker info | rg -A2 "Insecure Registries"
docker pull boston.csl.toronto.edu:5000/nextmini-fec:<tag>
```

If that pull fails, `run_fec.py run` will fail later when it tries to launch the node container.

## Inventory

The default inventory is `examples/fec/inventory.toml`.

It defines:

- the Boston controller host
- the remote repository path on Boston
- one trainer node
- worker nodes with contiguous `rank` values
- relay nodes

The runner selects a subset of those nodes for each run through `--receiver-ids`, `--relay-ids`, and `--tree-ids`.

## Typical Workflow

Validate the inventory first:

```bash
python examples/fec/run_fec.py validate \
  --inventory examples/fec/inventory.toml
```

Build and publish images to Boston:

```bash
python examples/fec/run_fec.py prepare-images \
  --inventory examples/fec/inventory.toml \
  --image-tag fec-dev-20260319-184222
```

If you omit `--image-tag`, the runner generates one and saves it in `examples/fec/.last-image-tag`. Later `run` commands reuse that saved tag automatically unless you pass a new one.

Run a plain experiment:

```bash
python examples/fec/run_fec.py run \
  --inventory examples/fec/inventory.toml \
  --mode plain \
  --payload-size 100MiB \
  --receiver-ids 2,3 \
  --relay-ids 5,6 \
  --tree-ids 0 \
  --block-size 8192
```

Run an FEC experiment:

```bash
python examples/fec/run_fec.py run \
  --inventory examples/fec/inventory.toml \
  --mode fec \
  --payload-size 100MiB \
  --receiver-ids 2,3 \
  --relay-ids 5,6,7,8 \
  --tree-ids 0,1 \
  --block-size 131072 \
  --symbols-per-block 16
```

For the `ns-lossless`-style apples-to-apples comparison we used:

- plain block size: `8192`
- FEC block size: `131072`
- FEC symbols per block: `16`
- effective FEC symbol size: `131072 / 16 = 8192`

## Other Useful Commands

Plan the relay trees without launching anything:

```bash
python examples/fec/run_fec.py plan \
  --inventory examples/fec/inventory.toml \
  --mode fec \
  --receiver-ids 2,3 \
  --relay-ids 5,6,7,8 \
  --tree-ids 0,1
```

Generate configs and payload locally without launching:

```bash
python examples/fec/run_fec.py generate \
  --inventory examples/fec/inventory.toml \
  --mode fec \
  --payload-size 100MiB \
  --receiver-ids 2,3 \
  --relay-ids 5,6,7,8 \
  --tree-ids 0,1 \
  --block-size 131072 \
  --symbols-per-block 16
```

Fetch artifacts again for an existing run:

```bash
python examples/fec/run_fec.py fetch-artifacts \
  --run-dir examples/fec/generated/<run-name>
```

Stop containers for an existing run:

```bash
python examples/fec/run_fec.py down \
  --run-dir examples/fec/generated/<run-name>
```

Remove local generated run directories:

```bash
python examples/fec/run_fec.py clean-generated \
  --run-dir examples/fec/generated/<run-name>

python examples/fec/run_fec.py clean-generated --all
```

## Where Results Go

Each run creates a directory under `examples/fec/generated/<timestamp>-<mode>/` with:

- generated configs
- payload file
- `remote-state.json`
- fetched logs
- receiver artifacts
- `verification.json`

By default, successful runs clean up the remote containers after verification. Use `--keep-containers` if you want the remote containers to stay up for debugging.
