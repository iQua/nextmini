## Testing Mininet Examples with Docker

**Step 1: Starting the Docker environment**

Run the following command within the `mininet-docker/` directory:

```bash
docker compose build; docker compose up
```

Open another terminal:

```bash
docker exec -it mininet-container /bin/bash
```

Then run the following command to test the basic Mininet functionality:

```bash
sudo mn --switch ovsbr --test pingall
```

**Step 2: Runing the examples**

All the examples are in the `/opt/mininet-examples` directory within the Docker container:

```bash
cd /opt/mininet-examples; ls
```

To run an example:

```bash
python3 multiping.py
```

You can change `multiping.py` to any python scripts included in the `/opt/mininet-examples` directory.

In addition, examples that are outdated, unrelated to Nextmini, or failed to run are placed inside the `/opt/extra_examples` directory. If you would like to run more examples, you can clone the official Mininet repository by:

```bash
git clone https://github.com/mininet/mininet
```

Then, go to the `examples` directory to access additional examples:

```bash
cd mininet/examples
```

By copying any of the Python scripts to `./nextmini/mininet-examples/mininet-docker/examples`, you may can test newly added examples inside a Docker container.
