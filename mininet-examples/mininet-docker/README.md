## Test mininet examples with docker

**Step 1 : Start the docker environment**

Firstly, run the following commands :

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

**Step 2 : Run examples**

By running the following commands, you will see all examples presented for running.

```bash
cd /opt/mininet-examples; ls
```

To run an example:

```bash
python3 multiping.py
```

You can change `multiping.py` to any python scripts presented in the `/opt/mininet-examples` folder.

## Complementary Information

If you would like to run more examples, you can clone the official mininet repository in a new terminal by:

```bash
git clone https://github.com/mininet/mininet
```

Then, go to the examples folder for extra use cases:

```bash
cd mininet/examples
```

By moving any python scripts to the `./nextmini/mininet-examples/mininet-docker/examples`, you may repeat the above _Step 1_ and _Step 2_ to test newly added examples inside docker environment.
