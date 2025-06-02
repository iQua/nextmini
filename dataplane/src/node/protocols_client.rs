use bytes::Bytes;
use std::net::SocketAddr;
use std::path::Path;
use std::time::Duration;

use tokio::{io::AsyncWriteExt, net::TcpStream};
use tracing::info;

use s2n_quic::stream::BidirectionalStream;
use s2n_quic::{Client, client::Connect};

pub async fn connect_quic_node(local_id: usize, addr: &str, node_id: usize) -> BidirectionalStream {
    let client = Client::builder()
        .with_tls((Path::new("server_cert.pem"), Path::new("server_key.pem")))
        .expect("Failed to set TLS configuration")
        .with_io("0.0.0.0:0")
        .expect("Failed to bind the client")
        .start()
        .expect("Failed to start client");

    let mut retry_count = 0;
    const MAX_RETRY: usize = 10;

    let mut connection = loop {
        let addr: SocketAddr = addr.parse().unwrap();
        let connect = Connect::new(addr).with_server_name("Strato");

        match client.connect(connect).await {
            Ok(mut connection) => {
                connection
                    .keep_alive(true)
                    .expect("Unable to keep the connection alive");
                break connection;
            }
            Err(e) => {
                info!(
                    "Failed to initiate quic connection to {addr}, error: {e} retrying in 1 second"
                );
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }

        retry_count += 1;

        if retry_count >= MAX_RETRY {
            panic!(
                "Maximum retry reached to establish a QUIC connection to {}. Aborting.",
                addr
            );
        }
    };

    let mut stream = connection
        .open_bidirectional_stream()
        .await
        .expect("Failed to establish handshake stream");

    info!("Connecting to node {} with QUIC...", node_id);

    stream
        .send(Bytes::copy_from_slice(&local_id.to_be_bytes()))
        .await
        .expect("Failed to send local node id to the node");

    info!("Connected to node {}.", node_id);

    stream
}
