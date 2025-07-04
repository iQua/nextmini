To run it,

```bash
docker compose up -d --build
```

To test with proxy:

```bash
docker exec test-client iperf3 -c tcp-proxy -p 8080 -t 10
```

To test with server:

```bash
docker exec test-client iperf3 -c iperf-server -p 5201 -t 10
```
