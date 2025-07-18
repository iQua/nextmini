# SOCKS5 Proxy and TCP/HTTP Server-Client System

This project consists of three Rust programs that implement a SOCKS5 proxy, an HTTP/TCP echo server, and a SOCKS5 client. The proxy forwards connections from the client (or tools like curl) to the server, which responds to HTTP requests or echoes TCP data.

## Files

- `proxy.rs`: A SOCKS5 proxy server that listens on `127.0.0.1:1080` and forwards TCP connections to a target server specified by the client.
- `server.rs`: An HTTP/TCP server that listens on `127.0.0.1:8080`. It responds to HTTP GET requests with a simple HTML page and echoes non-HTTP data for TCP clients.
- `client.rs`: A SOCKS5 client that connects to the proxy, requests a connection to `127.0.0.1:8080`, sends a test message, and prints the response.

## Prerequisites

- Rust installed (includes `rustc` and `cargo`).
- A terminal to run the programs.
- curl (optional, for testing HTTP requests).

## Setup

1. **Save the files**:

   - Ensure you have `proxy.rs`, `server.rs`, and `client.rs` in the same directory.
   - The code is available in the project repository or provided separately.

2. **Compile the programs**: Open a terminal in the project directory and run:

   ```bash
   rustc proxy.rs -o proxy
   rustc server.rs -o server
   rustc client.rs -o client
   ```

   This generates three executables: `proxy`, `server`, and `client`.

## Usage

The programs work together to demonstrate a SOCKS5 proxy forwarding TCP connections. The server also supports HTTP for curl compatibility.

### Running the Programs

1. **Start the proxy** (in a terminal):

   ```bash
   ./proxy
   ```

   Output: `SOCKS5 proxy listening on 127.0.0.1:1080`

2. **Start the server** (in a separate terminal):

   ```bash
   ./server
   ```

   Output: `HTTP server listening on 127.0.0.1:8080`

3. **Run the client** (in another terminal):

   ```bash
   ./client
   ```

   Output: `Received from server: Hello, Server!`

   - The client connects to the proxy, requests a connection to `127.0.0.1:8080`, sends "Hello, Server!", and prints the echoed response.

4. **Test with curl**:

   - Via the proxy:

     ```bash
     curl --socks5 localhost:1080 http://127.0.0.1:8080
     ```

     Output:

     ```html
     <html><body><h1>Hello from Rust HTTP Server!</h1></body></html>
     ```
   - Directly (bypassing the proxy):

     ```bash
     curl http://127.0.0.1:8080
     ```

     Output: Same as above.

### How It Works

- **Proxy (**`proxy.rs`**)**:

  - Implements SOCKS5 protocol (no authentication, CONNECT command only).
  - Listens on `127.0.0.1:1080`.
  - Parses client requests to determine the target address (e.g., `127.0.0.1:8080` or `example.com:80`) and forwards data bidirectionally.
  - Supports IPv4, IPv6, and domain names.

- **Server (**`server.rs`**)**:

  - Listens on `127.0.0.1:8080`.
  - Handles HTTP GET requests (e.g., from curl) with a static HTML response.
  - Echoes non-HTTP data for TCP clients (e.g., `client.rs`).
  - Uses threads for simplicity; not optimized for high load.

- **Client (**`client.rs`**)**:

  - Connects to the proxy at `127.0.0.1:1080`.
  - Sends a SOCKS5 request to connect to `127.0.0.1:8080`.
  - Sends "Hello, Server!" and prints the echoed response.

### Testing Notes

- **Order**: Start `proxy` and `server` before `client` or curl.
- **Ports**: Ensure `127.0.0.1:1080` (proxy) and `127.0.0.1:8080` (server) are free.
- **Curl**: Works with the server via the proxy or directly, as the server handles HTTP GET requests.
- **External servers**: Use curl with the proxy to connect to external HTTP servers, e.g.:

  ```bash
  curl --socks5 localhost:1080 https://example.com
  ```

## Limitations

- **Proxy**: Supports only SOCKS5 CONNECT command, no authentication, and minimal error handling.
- **Server**: Handles only HTTP GET requests with a static response; non-HTTP data is echoed as-is.
- **Client**: Hardcoded to connect to `127.0.0.1:8080` via the proxy.
- **Performance**: Uses threads, not async I/O, so not suitable for high concurrency. Consider Tokio for production use.

## Potential Improvements

- Add SOCKS5 authentication to `proxy.rs`.
- Enhance `server.rs` to support other HTTP methods or dynamic content.
- Make `client.rs` configurable for different target addresses.
- Use async I/O for better scalability.

## License

### This project is provided as-is for educational purposes. No warranty is implied.
