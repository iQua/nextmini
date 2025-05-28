use crate::node::protocols_io::ProtocolWriterMessage;
use tokio::sync::mpsc;
#[derive(Clone)]
pub struct UdpReader {
    sock: Arc<UdpSocket>,
}

impl UdpReader {
    pub fn new(sock: Arc<UdpSocket>) -> Self {
        Self { sock }
    }

    pub async fn recv(&self, buf: &mut PacketBuf) -> usize {
        match self.sock.recv(&mut buf[..]).await {
            Ok(n) => n,
            Err(e) => panic!("{e}"),
        }
    }
}


/// Actor Model Implementation
struct UdpProtocolWriter {
    receiver: mpsc::Receiver<ProtocolWriterMessage>,
    writer: UdpWriter,
}

impl UdpProtocolWriter {
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
pub struct UdpProtocolWriterHandle {
    sender: mpsc::Sender<ProtocolWriterMessage>,
}

impl UdpProtocolWriterHandle {
        pub fn new_udp(udp_writer: UdpWriter) -> Self {
        let (sender, receiver) = mpsc::channel(100);

        let mut actor = UdpProtocolWriter {
            receiver,
            writer: udp_writer,
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
