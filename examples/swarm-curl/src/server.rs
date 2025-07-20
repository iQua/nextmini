use tokio::io::{AsyncWriteExt, BufReader, AsyncBufReadExt};
use tokio::net::{TcpListener, TcpStream};
use tracing::{error, info, instrument};

#[tokio::main]
async fn main() -> std::io::Result<()> {
    tracing_subscriber::fmt::init();

    let listener = TcpListener::bind("0.0.0.0:8080").await?;
    info!("Server listening on port 8080");

    loop {
        let (stream, addr) = listener.accept().await?;
        info!("Accepted connection from: {}", addr);

        tokio::spawn(async move {
            if let Err(e) = handle_client(stream).await {
                error!("Failed to handle client: {}", e);
            }
        });
    }
}

#[instrument(skip(stream))]
async fn handle_client(mut stream: TcpStream) -> std::io::Result<()> {
    let (reader, mut writer) = stream.split();
    let mut buf_reader = BufReader::new(reader);
    let mut request_line = String::new();

    // Read the request line to make sure we're responding to a valid HTTP request
    // This makes the server slightly more robust.
    match buf_reader.read_line(&mut request_line).await {
        Ok(0) | Err(_) => {
            // Connection closed or error, do nothing.
            return Ok(());
        }
        Ok(_) => {
            info!("Received request: {}", request_line.trim());
        }
    }

    let response = "HTTP/1.1 200 OK\r\nContent-Length: 11\r\nConnection: close\r\n\r\nHello World";
    
    writer.write_all(response.as_bytes()).await?;
    writer.flush().await?;
    
    info!("Response sent and stream flushed for {}", stream.peer_addr()?);
    Ok(())
}
