use std::net::Ipv4Addr;
use std::sync::Arc;

use ahash::AHashMap;
use bytes::Bytes;
use tokio::sync::{Mutex, mpsc, oneshot, watch};
use tokio::task::JoinHandle;
use tracing::warn;

use nextmini_messages::TokenBucketSpec;
use nextmini_messages::lossless_session::LosslessSessionManifest;

use crate::node::config::LosslessConfig;
use crate::node::processor::ProcessorHandle;
use crate::node::session::api::{Command, InboundFrame, SessionId};
use crate::node::session::plan::BlockPlan;
use crate::node::session::{fec_policy, receiver, sender};

pub use crate::node::session::fec_policy::PreflightError;

#[derive(Clone, Debug)]
pub struct CommonConfig {
    pub session_id: SessionId,
    pub dest_ip: Ipv4Addr,
    pub block_size: usize,
    pub src_port: u16,
    pub dst_port: u16,
    pub data_bucket: Option<TokenBucketSpec>,
    pub local_node_id: usize,
    pub user_space_base_addr: Ipv4Addr,
    pub local_netmask: Ipv4Addr,
}

#[derive(Clone, Debug)]
pub struct SenderRequest {
    pub common: CommonConfig,
    pub receiver_ids: Vec<usize>,
    pub total_bytes: u64,
    pub source_buffer: Bytes,
    pub ready_grace_ms: u64,
}

#[derive(Clone, Debug)]
pub struct ReceiverRequest {
    pub common: CommonConfig,
    pub source_node_id: usize,
    pub expected_bytes: u64,
    pub sink_buffer: Option<Arc<Mutex<Vec<u8>>>>,
}

#[derive(Clone, Debug)]
pub struct SenderConfig {
    pub common: CommonConfig,
    pub receiver_ids: Vec<usize>,
    pub total_bytes: u64,
    pub source_buffer: Bytes,
    pub manifest: LosslessSessionManifest,
    pub ready_grace_ms: u64,
    pub topology_ready: Option<watch::Receiver<bool>>,
}

#[derive(Clone, Debug)]
pub struct ReceiverConfig {
    pub common: CommonConfig,
    pub source_node_id: usize,
    pub expected_bytes: u64,
    pub sink_buffer: Option<Arc<Mutex<Vec<u8>>>>,
    pub fec_enabled: bool,
}

#[derive(Clone, Debug)]
pub struct LosslessRuntimeHandle {
    command_tx: mpsc::UnboundedSender<Command>,
}

impl LosslessRuntimeHandle {
    pub fn new(processors: ProcessorHandle, config: LosslessConfig) -> Self {
        let (command_tx, command_rx) = mpsc::unbounded_channel();
        let runtime = LosslessRuntime::new(processors, config, command_rx);

        tokio::spawn(async move {
            let mut runtime = runtime;
            runtime.run().await;
        });

        Self { command_tx }
    }

    pub async fn start_sender(&self, cfg: SenderRequest) -> Result<SessionId, PreflightError> {
        let (reply_tx, reply_rx) = oneshot::channel();

        if self
            .command_tx
            .send(Command::StartSender {
                cfg,
                reply: reply_tx,
            })
            .is_err()
        {
            return Err(PreflightError::RuntimeChannelClosed);
        }

        reply_rx
            .await
            .unwrap_or(Err(PreflightError::RuntimeChannelClosed))
    }

    pub async fn start_receiver(&self, cfg: ReceiverRequest) -> SessionId {
        let (reply_tx, reply_rx) = oneshot::channel();
        let _ = self.command_tx.send(Command::StartReceiver {
            cfg,
            reply: reply_tx,
        });
        reply_rx.await.expect("The session ID.")
    }

    pub fn stop(&self, session: SessionId) {
        let _ = self.command_tx.send(Command::Stop { session });
    }

    pub fn deliver(&self, session: SessionId, frame: InboundFrame) {
        let _ = self.command_tx.send(Command::Deliver { session, frame });
    }

    pub async fn wait_completion(&self, session: SessionId) -> bool {
        let (reply_tx, reply_rx) = oneshot::channel();
        let _ = self.command_tx.send(Command::Wait {
            session,
            reply: reply_tx,
        });
        reply_rx.await.unwrap_or(false)
    }

    #[allow(dead_code)]
    pub async fn allocate_session_id(&self) -> SessionId {
        let (reply_tx, reply_rx) = oneshot::channel();
        let _ = self
            .command_tx
            .send(Command::AllocateSession { reply: reply_tx });
        reply_rx.await.expect("The session ID.")
    }

    pub fn set_topology_ready(&self, ready: bool) {
        let _ = self.command_tx.send(Command::SetTopologyReady { ready });
    }
}

struct LosslessRuntime {
    processors: ProcessorHandle,
    config: LosslessConfig,
    tasks: AHashMap<SessionId, JoinHandle<()>>,
    inputs: AHashMap<SessionId, mpsc::Sender<InboundFrame>>,
    next_session_id: SessionId,
    topology_ready_tx: watch::Sender<bool>,
    topology_ready: bool,
    command_rx: mpsc::UnboundedReceiver<Command>,
}

impl LosslessRuntime {
    fn new(
        processors: ProcessorHandle,
        config: LosslessConfig,
        command_rx: mpsc::UnboundedReceiver<Command>,
    ) -> Self {
        let (topology_ready_tx, _) = watch::channel(false);

        Self {
            processors,
            config,
            tasks: AHashMap::default(),
            inputs: AHashMap::default(),
            next_session_id: 1,
            topology_ready_tx,
            topology_ready: false,
            command_rx,
        }
    }

    async fn run(&mut self) {
        while let Some(cmd) = self.command_rx.recv().await {
            match cmd {
                Command::StartSender { cfg, reply } => {
                    let sid = self.spawn_sender(cfg);
                    let _ = reply.send(sid);
                }
                Command::StartReceiver { cfg, reply } => {
                    let sid = self.spawn_receiver(cfg);
                    let _ = reply.send(sid);
                }
                Command::Stop { session } => {
                    self.stop(session).await;
                }
                Command::Deliver { session, frame } => {
                    self.deliver_frame(session, frame).await;
                }
                Command::Wait { session, reply } => {
                    self.handle_wait(session, reply);
                }
                Command::AllocateSession { reply } => {
                    let sid = self.allocate_session_id();
                    let _ = reply.send(sid);
                }
                Command::SetTopologyReady { ready } => {
                    self.set_topology_ready(ready);
                }
            }
        }
    }

    async fn deliver_frame(&mut self, session: SessionId, frame: InboundFrame) {
        if let Some(tx) = self.input_sender(session) {
            if tx.send(frame).await.is_err() {
                warn!(
                    session_id = session,
                    "Lossless runtime: receiver dropped inbound frame."
                );
            }
        } else {
            warn!(
                session_id = session,
                "Lossless runtime: no receiver for inbound frame."
            );
        }
    }

    fn handle_wait(&mut self, session: SessionId, reply: oneshot::Sender<bool>) {
        let handle = self.take_task(session);

        tokio::spawn(async move {
            if let Some(handle) = handle {
                let _ = handle.await;
                let _ = reply.send(true);
            } else {
                let _ = reply.send(false);
            }
        });
    }

    fn take_task(&mut self, sid: SessionId) -> Option<JoinHandle<()>> {
        self.tasks.remove(&sid)
    }

    fn spawn_sender(&mut self, req: SenderRequest) -> Result<SessionId, PreflightError> {
        let sid = req.common.session_id;
        let block_size = fec_policy::validate_block_size(req.common.block_size)?;
        let plan = BlockPlan::new(req.total_bytes, req.common.block_size)
            .map_err(|_| PreflightError::InvalidBlockSize {
                value: req.common.block_size,
            })?;
        let policy = fec_policy::derive_sender_policy(&self.config)?;
        let manifest = LosslessSessionManifest {
            block_size,
            total_bytes: req.total_bytes,
            total_blocks: plan.total_blocks(),
            mode: policy.mode,
        };
        manifest
            .validate()
            .expect("runtime-derived manifest must validate");

        let mut cfg = SenderConfig {
            common: req.common,
            receiver_ids: req.receiver_ids,
            total_bytes: req.total_bytes,
            source_buffer: req.source_buffer,
            manifest,
            ready_grace_ms: req.ready_grace_ms,
            topology_ready: None,
        };
        if !self.topology_ready {
            cfg.topology_ready = Some(self.topology_ready_tx.subscribe());
        }

        let processors = self.processors.clone();
        let (tx, rx) = mpsc::channel(1024);
        self.inputs.insert(sid, tx);

        let sender_handle = tokio::spawn(sender::run(cfg, rx, processors));
        self.tasks.insert(sid, sender_handle);

        Ok(sid)
    }

    fn spawn_receiver(&mut self, req: ReceiverRequest) -> SessionId {
        let sid = req.common.session_id;
        let cfg = ReceiverConfig {
            common: req.common,
            source_node_id: req.source_node_id,
            expected_bytes: req.expected_bytes,
            sink_buffer: req.sink_buffer,
            fec_enabled: self.config.fec_enabled,
        };
        let processors = self.processors.clone();

        let (tx, rx) = mpsc::channel(1024);
        self.inputs.insert(sid, tx);

        let receiver_handle = tokio::spawn(receiver::run(cfg, rx, processors));
        self.tasks.insert(sid, receiver_handle);

        sid
    }

    async fn stop(&mut self, sid: SessionId) {
        if let Some(handle) = self.tasks.remove(&sid) {
            handle.abort();
        }
        self.inputs.remove(&sid);
    }

    fn input_sender(&self, sid: SessionId) -> Option<mpsc::Sender<InboundFrame>> {
        self.inputs.get(&sid).cloned()
    }

    fn allocate_session_id(&mut self) -> SessionId {
        let sid = self.next_session_id;
        self.next_session_id = self.next_session_id.wrapping_add(1).max(1);
        sid
    }

    fn set_topology_ready(&mut self, ready: bool) {
        self.topology_ready = ready;
        let _ = self.topology_ready_tx.send(ready);
    }
}
