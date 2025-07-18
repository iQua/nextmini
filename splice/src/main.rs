use tokio::io;
use tokio::net::TcpListener;

#[cfg(target_os = "linux")]
use tokio_splice::zero_copy_bidirectional;

#[cfg(target_os = "linux")]
async fn handle_client(mut client: tokio::net::TcpStream) -> io::Result<()> {
    let mut backend = tokio::net::TcpStream::connect("iperf-server:5201").await?;

    let (tx_bytes, rx_bytes) = zero_copy_bidirectional(&mut client, &mut backend).await?;
    println!("Zero-Copy transfer: {} {} bytes", tx_bytes, rx_bytes);
    Ok(())
}

#[tokio::main]
async fn main() -> io::Result<()> {
    #[cfg(target_os = "linux")]
    println!("Zero-Copy TCP Proxy starting on 0.0.0.0:8080 → iperf-server:5201");

    let listener = TcpListener::bind("0.0.0.0:8080").await?;

    loop {
        let (client, addr) = listener.accept().await?;
        println!("Client connected: {}", addr);

        tokio::spawn(async move {
            if let Err(e) = handle_client(client).await {
                eprintln!("Error handling {}: {}", addr, e);
            } else {
                println!("Client {} finished", addr);
            }
        });
    }
}
