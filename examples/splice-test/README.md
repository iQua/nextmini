To test splice-test with customized configurations, you can enter the following command:

```bash
cd examples/splice-test
```

Then, you can generate a custom configuration file using the `nodes.py` script. For example, to create a configuration with 3 nodes, run:

```bash
python nodes.py -n 3
```

Then the corresponding `client.rs`, `controller-config.toml`, and `docker-compose.yml` files will be updated in the current directory.
