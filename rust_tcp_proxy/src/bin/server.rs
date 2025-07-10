use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind("127.0.0.1:8081").await?;
    println!("Server listening on 127.0.0.1:8081");

    loop {
        let (socket, _) = listener.accept().await?;
        tokio::spawn(async move {
            handle_connection(socket).await;
        });
    }
}

async fn handle_connection(mut socket: TcpStream) {
    let mut buf = [0; 1024];
    loop {
        match socket.read(&mut buf).await {
            Ok(0) => {
                println!("Client closed connection");
                return;
            }
            Ok(n) => {
                println!("Received {} bytes", n);
                if let Err(e) = socket.write_all(&buf[0..n]).await {
                    eprintln!("failed to write to socket; err = {:?}", e);
                    return;
                }
            }
            Err(e) => {
                eprintln!("failed to read from socket; err = {:?}", e);
                return;
            }
        }
    }
} 