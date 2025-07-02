To run the Aya eBPF test container, you can use the following commands:

```bash
cd nextmini/aya && docker cp target/release/aya aya-ebpf-test:/usr/local/bin/aya && docker restart aya-ebpf-test
```

Then you can run the test container with:

```bash
sleep 3 && docker exec -d aya-ebpf-test python3 -m http.server 8080 && timeout 10 bash -c 'docker exec aya-ebpf-test /usr/local/bin/aya &' && sleep 3 && docker exec aya-ebpf-test curl 127.0.0.1:8080 >/dev/null
```
