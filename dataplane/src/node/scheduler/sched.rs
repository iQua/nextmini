use std::sync::Arc;

use tokio::sync::{Notify, Semaphore, mpsc};
use tracing::error;

use nextmini_messages::{SchedulingDiscipline, TokenBucketSpec};

use crate::node::FlowId;
use crate::node::config::LocalConfig;
use crate::node::network::interface::NetworkInterfaceHandle;
use crate::node::packet::Packet;
use crate::node::scheduler::drop::{CapacityUnit, DropStrategy, PacketDrop, Red, TailDrop};
use crate::node::scheduler::fifo::FifoQueue;
use crate::node::scheduler::queue::SchedulerQueue;
use crate::node::scheduler::reader::SchedulerReader;
use crate::node::scheduler::writer::SchedulerWriter;
use crate::node::scheduler::wrr::WrrQueue;

/// The types of messages sent to the scheduler.
pub enum SchedulerReaderMessage {
    InboundPacket(Packet),
}

/// The rate limit is to be sent by the processor, and in the unit of bytes per second.
pub enum SchedulerWriterMessage {
    RateLimit(TokenBucketSpec),
    SetFlowWeight(FlowId, usize),
    /// Link-probe packets sent directly to the network interface.
    LinkProbePackets(Vec<Packet>),
}

/// The handle for the scheduler actor, which is between the processors and the network interface.
#[derive(Clone, Debug)]
pub struct SchedulerHandle {
    reader_sender: mpsc::Sender<SchedulerReaderMessage>,
    writer_sender: mpsc::UnboundedSender<SchedulerWriterMessage>,
    backpressure: bool,
}

impl SchedulerHandle {
    pub fn new(config: LocalConfig, net_interface: NetworkInterfaceHandle) -> Self {
        let (reader_sender, reader_receiver) = mpsc::channel(config.channel_capacity);
        let (writer_sender, writer_receiver) = mpsc::unbounded_channel();
        let backpressure = config.channel_backpressure;

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

        // When channel backpressure is enabled, also apply it to the scheduler queue so we block
        // instead of dropping when the queue reaches capacity.
        let capacity_semaphore = if config.channel_backpressure && capacity > 0 {
            Some(Arc::new(Semaphore::new(capacity)))
        } else {
            None
        };

        let mut reader = SchedulerReader::new(
            queue_strategy.clone(),
            packet_drop,
            queues_not_empty.clone(),
            capacity,
            capacity_semaphore.clone(),
            reader_receiver,
            config.scheduler_type,
        );

        let mut writer = SchedulerWriter::new(
            queue_strategy,
            net_interface,
            queues_not_empty,
            capacity_semaphore,
            writer_receiver,
        );

        tokio::task::spawn(async move {
            let _ = reader.run().await;
        });

        tokio::task::spawn(async move {
            let _ = writer.run().await;
        });

        Self {
            reader_sender,
            writer_sender,
            backpressure,
        }
    }

    #[cfg(test)]
    pub(crate) fn new_for_test(
        reader_sender: mpsc::Sender<SchedulerReaderMessage>,
        backpressure: bool,
    ) -> Self {
        let (writer_sender, mut writer_receiver) = mpsc::unbounded_channel();
        tokio::task::spawn(async move { while writer_receiver.recv().await.is_some() {} });
        Self {
            reader_sender,
            writer_sender,
            backpressure,
        }
    }

    /// Sends a packet to the scheduler.
    pub async fn send(&self, packet: Packet) {
        let msg = SchedulerReaderMessage::InboundPacket(packet);

        if self.backpressure {
            if let Err(e) = self.reader_sender.send(msg).await {
                error!("SchedulerHandle: reader channel closed; dropping packet: {e}");
            }
        } else if let Err(e) = self.reader_sender.try_send(msg) {
            error!("SchedulerHandle: Error sending a packet to the scheduler: {e}.");
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

    /// Sends link-probe packets directly to the network interface.
    pub fn send_link_probe_packets(&self, packets: Vec<Packet>) {
        let _ = self
            .writer_sender
            .send(SchedulerWriterMessage::LinkProbePackets(packets));
    }

    /// Sets the weight of a flow.
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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn scheduler_test_handle_keeps_writer_channel_open() {
        let (reader_sender, _reader_receiver) = mpsc::channel(1);
        let handle = SchedulerHandle::new_for_test(reader_sender, true);

        tokio::task::yield_now().await;

        assert!(!handle.writer_sender.is_closed());
    }
}
