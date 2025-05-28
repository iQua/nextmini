use crate::node::protocols_io::ProtocolWriterMessage;
use tokio::sync::mpsc;
pub struct TcpServer {
    context: Context,
    processor_manager: Arc<RwLock<ProcessorManager>>,
}

impl TcpServer {
    pub fn new(context: Context, processor_manager: Arc<RwLock<ProcessorManager>>) -> Self {
        Self {
            context,
            processor_manager,
        }
    }

    pub async fn start_listening(&mut self, addr: &String) {
        let listener = match TcpListener::bind(addr).await {
            Ok(listener) => listener,
            Err(e) => {
                error!("Failed to bind to address {}: {}", addr, e);
                return;
            }
        };

        let mut node_id_buf: [u8; 8] = [0; 8];

        loop {
            let mut stream = match listener.accept().await {
                Ok((stream, socket_addr)) => {
                    info!("Connection accepted from {:?}.", socket_addr);
                    stream
                }
                Err(e) => {
                    error!("Failed to accept TCP connection: {}", e);
                    continue;
                }
            };

            if let Err(e) = stream.read_exact(&mut node_id_buf).await {
                error!("Failed to read node ID: {}", e);
                continue;
            }

            let mut cursor = Cursor::new(&node_id_buf);

            let node_id = match cursor.read_u64().await {
                Ok(id) => id as usize,
                Err(e) => {
                    error!("Failed to parse node ID: {}", e);
                    continue;
                }
            };

            info!("Incoming connection from node {}...", node_id);

            self.context.add_tcp_node(node_id, stream).await;
            self.processor_manager
                .write()
                .await
                .update_processors()
                .await;

            info!("Connected to node {}.", node_id);
        }
    }
}

pub struct TcpReader {
    stream: ReadHalf<TcpStream>,
}

impl TcpReader {
    pub fn new(stream: ReadHalf<TcpStream>) -> Self {
        Self { stream }
    }

    pub async fn recv(&mut self, buf: &mut PacketBuf) -> usize {
        if let Err(e) = self.stream.read_exact(&mut buf[0..4]).await {
            eprintln!("Failed to read TCP header: {}", e);
            return 0;
        }

        let msg_len = buf[2] as usize * 256 + buf[3] as usize;

        if let Err(e) = self.stream.read_exact(&mut buf[4..msg_len]).await {
            eprintln!("Failed to read TCP data: {}", e);
            return 0;
        }

        msg_len
    }
}



/// Actor Model Implementation

struct TcpProtocolWriter {
    receiver: mpsc::Receiver<ProtocolWriterMessage>,
    writer: TcpWriter,
}

impl TcpProtocolWriter {
    async fn run(&mut self) {
        while let Some(message) = self.receiver.recv().await {
            match message {
                ProtocolWriterMessage::Send(data) => {
                    (&mut self.writer).send(&data).await;
                }
                ProtocolWriterMessage::Shutdown => break,
            }
        }
    }
}


#[derive(Clone)]
pub struct TcpProtocolWriterHandle {
    sender: mpsc::Sender<ProtocolWriterMessage>,
}

impl TcpProtocolWriterHandle {
    pub fn new_tcp(tcp_writer: TcpWriter) -> Self {
        let (sender, receiver) = mpsc::channel(100);

        let mut actor = TcpProtocolWriter {
            receiver,
            writer: tcp_writer,
        };

        tokio::spawn(async move {
            actor.run().await;
        });

        Self { sender }
    }
    pub async fn send(&self, data: &[u8]) {
        if let Err(_) = self
            .sender
            .send(ProtocolWriterMessage::Send(data.to_vec()))
            .await
        {
            error!("Failed to send data through protocol writer channel");
        }
    }
}

