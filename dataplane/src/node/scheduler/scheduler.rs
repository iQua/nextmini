use std::sync::Arc;

use clap::ValueEnum;
use serde::Deserialize;
use tokio::sync::{Notify, mpsc};
use tracing::{debug, error};

use crate::node::FlowId;
use nextmini_messages::TokenBucketSpec;

use crate::node::config::LocalConfig;
use crate::node::drop::{CapacityUnit, DropStrategy, PacketDrop, Red, TailDrop};
use crate::node::network::interface::NetworkInterfaceHandle;
use crate::node::packet::Packet;
use crate::node::scheduler::fifo::FifoQueue;
use crate::node::scheduler::queue::SchedulerQueue;
use crate::node::scheduler::reader::SchedulerReader;
use crate::node::scheduler::writer::SchedulerWriter;
use crate::node::scheduler::wrr::WrrQueue;

/// The scheduling discipline.
#[allow(unused)]
#[derive(Clone, Copy, Debug, PartialEq, Deserialize, ValueEnum, Default)]
#[serde(rename_all = "lowercase")]
pub enum SchedulingDiscipline {
    #[default]
    Fifo,
    Wrr,
}

/// The types of messages sent to the scheduler.
pub enum SchedulerReaderMessage {
    InboundPacket(Packet),
}

/// The rate limit is to be sent by the processor, and in the unit of bytes per second.
pub enum SchedulerWriterMessage {
    RateLimit(TokenBucketSpec),
    SetFlowWeight(FlowId, usize),
}

/// The handle for the scheduler actor, which is between the processors and the network interface.
#[derive(Clone, Debug)]
pub struct SchedulerHandle {
    reader_sender: mpsc::Sender<SchedulerReaderMessage>,
    writer_sender: mpsc::UnboundedSender<SchedulerWriterMessage>,
}

impl SchedulerHandle {
    pub fn new(config: LocalConfig, net_interface: NetworkInterfaceHandle) -> Self {
        let (reader_sender, reader_receiver) = mpsc::channel(config.channel_capacity);
        let (writer_sender, writer_receiver) = mpsc::unbounded_channel();

        let scheduler = Scheduler::new(config, net_interface, reader_receiver, writer_receiver);
        scheduler.run();

        Self {
            reader_sender,
            writer_sender,
        }
    }

    /// Sends a packet to the scheduler.
    pub fn send(&self, packet: Packet) {
        if let Err(e) = self
            .reader_sender
            .try_send(SchedulerReaderMessage::InboundPacket(packet))
        {
            error!(
                "SchedulerHandle: Error sending a packet to the scheduler: {}.",
                e
            );
        }
    }

    /// Limits the rate of sending packets the outbound network connection, in bytes/second.
    pub fn limit_rate(&self, spec: TokenBucketSpec) {
        if let Err(e) = self
            .writer_sender
            .send(SchedulerWriterMessage::RateLimit(spec))
        {
            error!(
                "SchedulerHandle: Error sending a rate limit to the scheduler: {}.",
                e
            );
        }
    }

    /// TO BE IMPLEMENTED : Pending changes according to flow spec
    pub fn set_flow_weight(&self, flow_id: FlowId, weight: usize) {
        if let Err(e) = self
            .writer_sender
            .send(SchedulerWriterMessage::SetFlowWeight(flow_id, weight))
        {
            error!(
                "SchedulerHandle: Error sending a flow weight to the scheduler: {}.",
                e
            );
        }
    }
}

pub struct Scheduler {
    config: LocalConfig,
}

impl Scheduler {
    pub fn new(
        config: LocalConfig,
        net_interface: NetworkInterfaceHandle,
        reader_receiver: mpsc::Receiver<SchedulerReaderMessage>,
        writer_receiver: mpsc::UnboundedReceiver<SchedulerWriterMessage>,
    ) -> Self {
        let capacity = config.queue_capacity;
        let capacity_unit = CapacityUnit::Packets;

        let packet_drop: Box<dyn PacketDrop + Send + Sync> = match config.scheduler_drop_strategy {
            DropStrategy::TailDrop => Box::new(TailDrop::new(capacity, capacity_unit)),
            DropStrategy::Red => Box::new(Red::new(capacity, capacity_unit, 0.7, 0.9, 0.8)),
        };

        let queue_strategy: Arc<dyn SchedulerQueue + Send + Sync> = match config.scheduler_type {
            SchedulingDiscipline::Fifo => Arc::new(FifoQueue::new(capacity)),
            SchedulingDiscipline::Wrr => Arc::new(WrrQueue::new(capacity)),
        };

        let queues_not_empty = Arc::new(Notify::new());

        let mut reader = SchedulerReader::new(
            queue_strategy.clone(),
            packet_drop,
            queues_not_empty.clone(),
            capacity,
            reader_receiver,
            config.scheduler_type,
        );

        let mut writer = SchedulerWriter::new(
            queue_strategy,
            net_interface,
            queues_not_empty,
            writer_receiver,
        );

        tokio::task::spawn(async move {
            let _ = reader.run().await;
        });

        tokio::task::spawn(async move {
            let _ = writer.run().await;
        });

        Self { config }
    }

    pub fn run(&self) {
        // This method is intentionally left empty as the actual run logic is handled in the
        // FifoReader and FifoWriter tasks spawned above.
        debug!(
            "A {:?} scheduler has just been started.",
            self.config.scheduler_type
        );
    }
}
