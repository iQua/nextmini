
use std::process::{Command, Stdio};
use std::time::Duration;

use tokio::io::{self, AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_splice::zero_copy_bidirectional;
use tracing::{error, info, instrument};

const LOCAL_GATEWAY_ADDR: &str = "127.0.0.1:1080";
const DATAPLANE_PROXY_DNS: &str = "dataplane:8081";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    info!("---------------------------------------------------------");
    info!("Client started. Waiting 60 seconds for routes to be inserted.");
    info!("Please run `uv run insert_route.py` on the manager node now.");
    info!("---------------------------------------------------------");

    tokio::time::sleep(Duration::from_secs(60)).await;

    info!("Starting local gateway and curl client...");

    tokio::spawn(async {
        if let Err(e) = start_local_gateway().await {
            error!("Local gateway failed: {}", e);
        }
    });

    tokio::time::sleep(Duration::from_secs(1)).await;
    run_curl_command();

    Ok(())
}

async fn start_local_gateway() -> io::Result<()> {
    info!("Local gateway listening on {}", LOCAL_GATEWAY_ADDR);
    let listener = TcpListener::bind(LOCAL_GATEWAY_ADDR).await?;

    loop {
        let (inbound, client_addr) = listener.accept().await?;
        info!("Accepted connection from curl: {}", client_addr);

        tokio::spawn(async move {
            if let Err(e) = handle_client_connection(inbound).await {
                error!("Error handling client connection: {}", e);
            }
        });
    }
}

#[instrument(skip(inbound))]
async fn handle_client_connection(mut inbound: TcpStream) -> io::Result<()> {
    // SOCKS5 handshake with local curl
    let mut auth_buf = [0u8; 2]; // [VER, NMETHODS]
    inbound.read_exact(&mut auth_buf).await?;
    let nmethods = auth_buf[1] as usize;
    let mut methods_buf = vec![0; nmethods];
    inbound.read_exact(&mut methods_buf).await?;
    inbound.write_all(&[0x05, 0x00]).await?; // Respond "No Auth"

    // Read the connection request from curl to get the target address
    let mut req_header = [0u8; 4]; // [VER, CMD, RSV, ATYP]
    inbound.read_exact(&mut req_header).await?;
    let atyp = req_header[3];

    let (target_addr, target_port) = match atyp {
        0x01 => { // IPv4 Address
            let mut addr_buf = [0u8; 4];
            inbound.read_exact(&mut addr_buf).await?;
            let mut port_buf = [0u8; 2];
            inbound.read_exact(&mut port_buf).await?;
            let addr = std::net::Ipv4Addr::from(addr_buf);
            let port = u16::from_be_bytes(port_buf);
            (addr.to_string(), port)
        }
        0x03 => { // Domain name
            let mut domain_len_buf = [0u8; 1];
            inbound.read_exact(&mut domain_len_buf).await?;
            let domain_len = domain_len_buf[0] as usize;
            let mut domain_buf = vec![0; domain_len];
            inbound.read_exact(&mut domain_buf).await?;
            let mut port_buf = [0u8; 2];
            inbound.read_exact(&mut port_buf).await?;
            let addr = String::from_utf8(domain_buf).unwrap();
            let port = u16::from_be_bytes(port_buf);
            (addr, port)
        }
        _ => return Err(io::Error::new(io::ErrorKind::Other, "Unsupported address type")),
    };

    info!("Curl requests to connect to: {}:{}", target_addr, target_port);

    info!("Connecting to the actual dataplane proxy...");
    let mut outbound = connect_to_dataplane_proxy(&target_addr, target_port).await?;

    // Respond to curl that connection is successful
    let response = [0x05, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00];
    inbound.write_all(&response).await?;

    info!("Splicing connections...");
    let (from_client, from_server) = zero_copy_bidirectional(&mut inbound, &mut outbound).await?;
    info!("Splicing complete. Bytes: {}/{}", from_client, from_server);

    Ok(())
}

async fn connect_to_dataplane_proxy(target_host: &str, target_port: u16) -> io::Result<TcpStream> {
    let mut stream = TcpStream::connect(DATAPLANE_PROXY_DNS).await?;
    stream.set_nodelay(true)?;

    // SOCKS5 Auth with dataplane
    stream.write_all(&[0x05, 0x01, 0x00]).await?;
    stream.flush().await?;
    let mut auth_resp = [0u8; 2];
    stream.read_exact(&mut auth_resp).await?;
    if auth_resp[0] != 0x05 || auth_resp[1] != 0x00 {
        return Err(io::Error::new(io::ErrorKind::Other, "Dataplane auth failed"));
    }

    // SOCKS5 Connect to dataplane, asking it to connect to the final target.
    // We need to handle both IP addresses and domain names correctly.
    if let Ok(ip) = target_host.parse::<std::net::Ipv4Addr>() {
        // Target is an IPv4 address
        info!("Forwarding request for IP target: {}", ip);
        stream.write_all(&[0x05, 0x01, 0x00, 0x01]).await?; // ATYP: IPv4
        stream.write_all(&ip.octets()).await?;
    } else {
        // Target is a domain name
        info!("Forwarding request for Domain target: {}", target_host);
        stream.write_all(&[0x05, 0x01, 0x00, 0x03]).await?; // ATYP: Domain name
        stream.write_all(&[target_host.len() as u8]).await?;
        stream.write_all(target_host.as_bytes()).await?;
    }

    // Write the port
    stream.write_all(&target_port.to_be_bytes()).await?;
    stream.flush().await?;

    // Read response from dataplane
    let mut conn_resp = [0u8; 10]; // This is a simplification, assumes IPv4 response
    stream.read_exact(&mut conn_resp).await?;
    if conn_resp[0] != 0x05 || conn_resp[1] != 0x00 {
        error!("Dataplane proxy connection failed, resp: {:?}", &conn_resp);
        return Err(io::Error::new(io::ErrorKind::Other, "Dataplane conn failed"));
    }

    info!("SOCKS5 tunnel to dataplane for target {} established.", target_host);
    Ok(stream)
}

fn run_curl_command() {
    info!("Executing curl command...");
    let mut child = Command::new("curl")
        .arg("--verbose")
        .arg("--socks5")
        .arg(LOCAL_GATEWAY_ADDR)
        .arg("http://curl-server:8080/")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("Failed to execute curl");

    let output = child.wait_with_output().expect("Failed to wait on curl");

    info!("Curl command finished with status: {}", output.status);
    if !output.stdout.is_empty() {
        info!("Curl stdout:\n{}", String::from_utf8_lossy(&output.stdout));
    }
    if !output.stderr.is_empty() {
        info!("Curl stderr:\n{}", String::from_utf8_lossy(&output.stderr));
    }
} 