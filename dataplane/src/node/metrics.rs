use chrono::Utc;
use fxhash::FxHashMap;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};
use tokio::time::{Duration, sleep};

use nextmini_messages::{DataplaneToController, Metric};

use crate::node::{FlowId, NodeId};
use crate::node::controller::ControllerHandle;
pub struct Collector {
    controller_handle: ControllerHandle, // Should be changed to controller handle
    receiver: UnboundedReceiver<MetricsCollectorMessage>,
    collection_rate: u64,
}

impl Collector {
    pub fn new(
        controller_handle: ControllerHandle,
        receiver: UnboundedReceiver<MetricsCollectorMessage>,
        collection_rate: u64,
    ) -> Self {
        Self {
            controller_handle,
            receiver,
            collection_rate,
        }
    }


    pub async fn run(&mut self) {
        loop {
            sleep(Duration::from_secs(self.collection_rate)).await;

            let mut data = FxHashMap::default();

            while let Ok(msg) = self.receiver.try_recv() {
                match msg {
                    MetricsCollectorMessage::Metrics(flow_id, node_id, n_bytes) => {
                        let flow_data = data.entry(flow_id).or_insert_with(|| (node_id, 0));
                        flow_data.1 += n_bytes;
                    }
                }
            }

            let now = Utc::now();
            let mut metrics_array = Vec::new();

            for (flow_id, value) in data.iter() {
                let bps = (8.0 * value.1 as f64 / self.collection_rate as f64) as usize;

                    metrics_array.push(Metric {
                        flow_id: flow_id.to_be_bytes(),
                        bps,
                        src_node_id: Some(value.0),
                        time_read: now,
                    });
            }

            let msg = DataplaneToController::Metrics {
                metrics: metrics_array,
            };
            self.controller_handle.send(msg).unwrap();
        }
    }
}

/// Actor Model Implementation

pub enum MetricsCollectorMessage {
    Metrics(FlowId, NodeId, usize), // (flow_id, node_id, packet_size)
}

#[derive(Clone)]
pub struct MetricsCollectorHandle {
    sender: UnboundedSender<MetricsCollectorMessage>,
}

impl MetricsCollectorHandle {
    pub fn new(
        collection_rate: u64,
        controller_handle: ControllerHandle,  
    ) -> Self {
        let (sender, receiver) = unbounded_channel();
        let mut collector = Collector::new(
            controller_handle,
            receiver,
            collection_rate,
        );
        tokio::spawn(async move {
            collector.run().await;
        });
        Self { sender }
    }
    pub async fn collect_metrics(&self, flow_id: FlowId, local_id: NodeId, packet_size: usize) {
        let _ = self.sender
                    .send(MetricsCollectorMessage::Metrics(flow_id, local_id, packet_size))
                    .unwrap();
    }
}