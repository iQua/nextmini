// Context is designed as a cloneable singleton that holds data shared between different components
// of the dataplane.

use std::sync::Arc;

use tokio::net::TcpStream;
use tokio::net::UdpSocket;
use tokio::sync::RwLock;
use tokio::sync::mpsc;

use fxhash::FxHashMap;
use s2n_quic::stream::BidirectionalStream;

use crate::node::config::LocalConfig;
use crate::node::local_interface::{TunReader, TunWriter};
use crate::node::metrics::MetricsTx;
use crate::node::node_interface::{
    NodeSender, create_quic_node_interfaces, create_tcp_node_interfaces, create_udp_node_receiver,
    create_udp_node_sender,
};
use crate::node::packet::Packet;
use crate::node::scheduler::SchedulingDiscipline;
use crate::node::utils::RateLimiter;
use crate::node::{INTERNAL_Q_SIZE, NodeId, ProcessorChannel, RateLimiterMap};

#[derive(Clone)]
pub struct Context {
    // The node ID
    pub local_id: NodeId,

    // Local configurations
    config: LocalConfig,

    // Writers to the TUN interface, as shared writable vectors, where each queue corresponds to one writer
    tun_writers: Arc<RwLock<Vec<TunWriter>>>,

    // A vector of processor channels, each including a sender and a receiver for an mpsc channel
    processor_channels: Arc<RwLock<Vec<ProcessorChannel>>>,

    // Hashmap from node IDs to NodeSenders
    processor_senders: Arc<RwLock<FxHashMap<NodeId, NodeSender>>>,

    // Performance metrics
    metrics_tx: MetricsTx,

    // Hashmap from node IDs to rate limiters
    link_rate_limiters: Arc<RwLock<RateLimiterMap>>,

    // UdpSocket
    udp_socket: Option<Arc<UdpSocket>>,

    // The scheduling discipline
    pub scheduler_type: SchedulingDiscipline,
}

impl Context {
    pub fn new(
        config: LocalConfig,
        local_id: NodeId,
        metrics: MetricsTx,
        link_rate_limiters: Arc<RwLock<RateLimiterMap>>,
        scheduler_type: SchedulingDiscipline,
    ) -> Self {
        Self {
            local_id,
            config,
            processor_channels: Arc::new(RwLock::new(Vec::new())),
            processor_senders: Arc::new(RwLock::new(FxHashMap::default())),
            tun_writers: Arc::new(RwLock::new(Vec::new())),
            metrics_tx: metrics,
            link_rate_limiters,
            udp_socket: None,
            scheduler_type,
        }
    }

    // Create and start the single TUN device
    pub async fn start_tun_device(&mut self, tun_queues: Vec<Arc<tun_rs::AsyncDevice>>) {
        let senders_to_proc = self.get_processor_txs().await;
        let mut tun_writers = self.tun_writers.write().await;

        for dev in tun_queues.iter() {
            // tun_writers is a vector of queues for one TUN device
            // dev.clone() does not clone the device, it simply creates a new reference to it
            let writer = TunWriter::new(dev.clone());
            tun_writers.push(writer);

            // Start a new Tokio task for reading continuously from this TUN device
            let mut reader = TunReader::new(dev.clone(), senders_to_proc.clone());

            tokio::spawn(async move {
                reader.start_reading().await;
            });
        }
    }

    // Get a reference of the TUN writer for the corresponding queue ID
    pub async fn get_tun_writer(&self, queue_id: usize) -> TunWriter {
        self.tun_writers
            .read()
            .await
            .get(queue_id)
            .expect(
                "Error: Attempting to obtain a TUN queue that doesn't exist. \\
                        Are TUN queues created successfully?",
            )
            .clone()
    }

    // metrics_tx is an mpsc unbounded sender that can be cloned.
    pub fn get_metrics_tx(&self) -> MetricsTx {
        self.metrics_tx.clone()
    }

    // Obtains the transmitters from processor mpsc channels.
    // Note: This can be called multiple times (i.e. every time when a new node is added),
    // but it will only create the channels once.
    async fn get_processor_txs(&self) -> Vec<mpsc::Sender<Packet>> {
        let mut channels = self.processor_channels.write().await;
        let mut txs = vec![];

        if channels.is_empty() {
            // Channels don't exist yet, create a new mpsc channel for each queue
            for i in 0..self.config.num_packet_processors {
                let (tx, rx) = mpsc::channel(INTERNAL_Q_SIZE);
                channels.insert(i, (tx.clone(), Some(rx)));
                txs.push(tx);
            }
        } else {
            // Channels already exist, just get the txs
            for i in 0..self.config.num_packet_processors {
                let tx = match channels.get_mut(i) {
                    Some(channel) => channel.0.clone(),
                    None => {
                        panic!("Error: Trying to obtain a processor channel that doesn't exist.");
                    }
                };

                txs.push(tx);
            }
        }

        txs
    }

    // Obtains the receivers from processor mpsc channels.
    // Note: This should only be called once to initialize the processor manager.
    pub async fn get_processor_rxs(&self) -> Vec<mpsc::Receiver<Packet>> {
        let mut channels = self.processor_channels.write().await;

        let mut rxs = vec![];
        if channels.is_empty() {
            // channels don't exist yet, create a new mpsc channel for each queue
            for i in 0..self.config.num_packet_processors {
                let (tx, rx) = mpsc::channel(INTERNAL_Q_SIZE);
                // inserts None as the rx since we take rx into a vector to be returned
                channels.insert(i, (tx, None));
                rxs.push(rx);
            }
        } else {
            // channels already exist, just get the rxs
            for i in 0..self.config.num_packet_processors {
                let rx = match channels.get_mut(i) {
                    Some(channel) => {
                        // channel.1.take() can only be called once. Calling it more than once should
                        // result an error by design, since an mpsc channel should only have one receiver
                        channel
                            .1
                            .take()
                            .expect("Error: Attempting to add node with duplicate id")
                    }
                    None => {
                        panic!("Error: Trying to obtain a processor channel that doesn't exist.");
                    }
                };

                rxs.push(rx);
            }
        }

        rxs
    }

    // Adds a NodeSender to the global hashmap, associated with the node ID
    pub async fn register_sender(&self, node_id: NodeId, node_sender: NodeSender) {
        self.processor_senders
            .write()
            .await
            .insert(node_id, node_sender);
    }

    // the link rate limiters need to be guarded for writing to prevent simutaneous edits from the controller
    async fn get_link_rate_limiter(&self, node_id: NodeId) -> Arc<RwLock<Option<RateLimiter>>> {
        let mut link_rate_limiter = self.link_rate_limiters.write().await;

        let limiter = match link_rate_limiter.get(&node_id) {
            None => {
                let new_limiter = Arc::new(RwLock::new(None));
                link_rate_limiter.insert(node_id, new_limiter.clone());
                new_limiter
            }
            Some(limiter) => limiter.clone(),
        };

        drop(link_rate_limiter);

        limiter
    }

    pub async fn add_tcp_node(&self, node_id: NodeId, stream: TcpStream) {
        assert!(
            self.local_id != node_id,
            "Error: Attempt to add a new TCP node with the same id as the local id {}",
            self.local_id
        );

        // gets the receiver txs from processor channels
        let senders_to_proc = self.get_processor_txs().await;

        // gets the link rate limiter for this node ID
        let rate_limiter = self.get_link_rate_limiter(node_id).await;

        // creates TCP node interfaces
        let (mut node_receiver, node_sender) = create_tcp_node_interfaces(
            stream,
            node_id,
            rate_limiter,
            senders_to_proc,
            self.scheduler_type,
        );

        let metrics_tx = self.metrics_tx.clone();

        tokio::spawn(async move {
            node_receiver.start_receiving(metrics_tx).await;
        });

        self.register_sender(node_id, node_sender).await;
    }

    pub async fn start_udp_receiver(&mut self) {
        let sock = self.udp_socket.take().unwrap();

        // gets the receiver txs from processor channels
        let senders_to_proc = self.get_processor_txs().await;

        let metrics_tx = self.metrics_tx.clone();

        // uses the local ID for remote_node_id here because UDP is connectionless, so we cannot
        // identify which node the packet is from directly. It can, however, still be inferred
        // through the flow ID and the associated route.
        let mut receiver = create_udp_node_receiver(sock.clone(), self.local_id, senders_to_proc);

        // puts back the UDP socket
        self.udp_socket = Some(sock);

        tokio::spawn(async move {
            receiver.start_receiving(metrics_tx).await;
        });
    }

    pub async fn init_udp_socket(&mut self, port: String) {
        let sock = UdpSocket::bind(format!("0.0.0.0:{port}"))
            .await
            .unwrap_or_else(|_| panic!("Failed to bind to local port: {port}"));

        self.udp_socket = Some(Arc::new(sock));
    }

    pub async fn add_udp_node(&mut self, node_id: NodeId, addr: String) {
        assert!(
            self.local_id != node_id,
            "Attempt to add node with the same id as local id {}",
            self.local_id
        );

        // gets the link rate limiter for this node ID
        let rate_limiter = self.get_link_rate_limiter(node_id).await;

        let sock = self.udp_socket.take().unwrap();
        let node_sender =
            create_udp_node_sender(sock.clone(), addr, rate_limiter, self.scheduler_type);

        // puts back the UDP socket
        self.udp_socket = Some(sock);

        // registers the UDP sender with its associated node ID
        self.register_sender(node_id, node_sender).await;
    }

    pub async fn add_quic_node(&self, node_id: NodeId, stream: BidirectionalStream) {
        assert!(
            self.local_id != node_id,
            "Error: Attempt to add node with the same id as local id {}",
            self.local_id
        );

        // gets the link rate limiter for this node ID
        let rate_limiter = self.get_link_rate_limiter(node_id).await;

        // gets the senders from processor channels
        let senders_to_proc = self.get_processor_txs().await;

        // creates node interfaces
        let (mut node_receiver, node_sender) = create_quic_node_interfaces(
            stream,
            node_id,
            rate_limiter,
            senders_to_proc,
            self.scheduler_type,
        )
        .await;

        let metrics_tx = self.metrics_tx.clone();

        tokio::spawn(async move {
            node_receiver.start_receiving(metrics_tx).await;
        });

        self.register_sender(node_id, node_sender).await;
    }

    // For asynchronously copying the processor senders, potentially creating new streams in
    // the process
    pub async fn reproduce_senders(&self) -> FxHashMap<NodeId, NodeSender> {
        let mut new_sender_map = FxHashMap::default();
        let senders = self.processor_senders.read().await;

        for (node_id, node_sender) in senders.iter() {
            new_sender_map.insert(*node_id, node_sender.reproduce().await);
        }

        new_sender_map
    }
}
