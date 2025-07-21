Test record:

```text
external_client  | Starting wget client via SOCKS5 proxy 172.16.8.5:8081 -> http://172.16.8.8:8080/file.txt
external_client  | [22:54:36] Downloading http://172.16.8.8:8080/file.txt to /downloads/file.txt
external_client  | [22:54:36] Download failed
external_server  | Launching simple HTTP server on port 8080 with file.txt available...
external_client  | [22:54:39] Downloading http://172.16.8.8:8080/file.txt to /downloads/file.txt
external_server  | 172.16.8.4 - - [21/Jul/2025 22:54:39] "GET /file.txt HTTP/1.1" 200 -
external_client  | [22:54:39] Download completed. File content: Hello from Nextmini!
```
