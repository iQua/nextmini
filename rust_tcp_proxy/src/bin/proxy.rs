use tokio::net::{TcpListener, TcpStream};
use tokio::io;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let listen_addr = "127.0.0.1:8080";
    let server_addr = "127.0.0.1:8081";

    println!("Proxy listening on {}", listen_addr);
    println!("Proxying to {}", server_addr);

    let listener = TcpListener::bind(listen_addr).await?;

    while let Ok((inbound, _)) = listener.accept().await {
        let server_addr = server_addr.to_string();
        tokio::spawn(async move {
            if let Err(e) = proxy(inbound, &server_addr).await {
                eprintln!("Failed to proxy connection: {}", e);
            }
        });
    }

    Ok(())
}

async fn proxy(mut inbound: TcpStream, server_addr: &str) -> Result<(), Box<dyn std::error::Error>> {
    let mut outbound = TcpStream::connect(server_addr).await?;

    let (mut ri, mut wi) = inbound.split();
    let (mut ro, mut wo) = outbound.split();

    let client_to_server = io::copy(&mut ri, &mut wo);
    let server_to_client = io::copy(&mut ro, &mut wi);

    tokio::try_join!(client_to_server, server_to_client)?;

    Ok(())
} 