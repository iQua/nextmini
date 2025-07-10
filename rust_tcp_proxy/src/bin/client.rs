use tokio::net::TcpStream;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut stream = TcpStream::connect("127.0.0.1:8080").await?;
    println!("Connected to proxy");

    let msg = "Hello, world!";
    stream.write_all(msg.as_bytes()).await?;
    println!("Sent: {}", msg);

    let mut buf = [0; 1024];
    let n = stream.read(&mut buf).await?;

    let response = &buf[..n];
    println!("Received: {}", String::from_utf8_lossy(response));

    Ok(())
} 