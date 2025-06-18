use ahash::AHashMap;
use chrono::Utc;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};
use tokio::time::{Duration, interval};

use nextmini_messages::{DataplaneToController, Metric};

use crate::node::controller_interface::ControllerInterfaceHandle;
use crate::node::{FlowId, NodeId};

pub enum CollectorMessage {
    Metric(FlowId, NodeId, NodeId, usize), // sending content may change change according to where handle is actually used
}

#[derive(Clone)]
pub struct CollectorHandle {
    sender: UnboundedSender<CollectorMessage>,
}

impl CollectorHandle {
    pub fn new(controller_interface: ControllerInterfaceHandle) -> Self {
        let (sender, receiver) = unbounded_channel();
        let mut collector = Collector {
            receiver,
            controller_interface: controller_interface.clone(),
        };

        tokio::spawn(async move {
            collector.run().await;
        });

        Self { sender }
    }

    pub fn send(
        &self,
        flow_id: FlowId,
        local_node_id: NodeId,
        remote_node_id: NodeId,
        bytes: usize,
    ) {
        self.sender
            .send(CollectorMessage::Metric(
                flow_id,
                local_node_id,
                remote_node_id,
                bytes,
            ))
            .unwrap();
    }
}

pub struct Collector {
    controller_interface: ControllerInterfaceHandle,
    receiver: UnboundedReceiver<CollectorMessage>,
}

impl Collector {
    pub async fn run(&mut self) {
        // Transmit metrics every 5 seconds
        let mut metrics_tick = interval(Duration::from_secs(5));

        // HashMap: flow_id -> (node_id, total_bytes, remote_node_id)
        let mut data = AHashMap::default();

        loop {
            tokio::select! {
                // receives new metrics data
                Some(CollectorMessage::Metric(flow_id, local_node_id, remote_node_id, bytes)) = self.receiver.recv() => {
                    let flow_data = data.entry(flow_id).or_insert_with(|| (local_node_id, remote_node_id, 0));
                    flow_data.2 += bytes;
                }

                // timer tick: calculate bandwidth metrics and transmit to controller
                _ = metrics_tick.tick() => {
                    if !data.is_empty() {
                        let now = Utc::now();
                        let mut metrics_array = Vec::new();

                        const COLLECTION_INTERVAL_SECS: f64 = 5.0;

                        for (flow_id, value) in data.iter() {
                            let bps = (8.0 * value.2 as f64 / COLLECTION_INTERVAL_SECS) as usize;

                            metrics_array.push(Metric {
                                flow_id: flow_id.to_be_bytes(),
                                bps,
                                local_node_id: value.0,
                                remote_node_id: value.1,
                                time_read: now,
                            });
                        }

                        if !metrics_array.is_empty() {
                            let msg = DataplaneToController::Metrics {
                                metrics: metrics_array,
                            };

                            self.controller_interface.send_metrics(msg).await;
                        }

                        data.clear();
                    }
                }
            }
        }
    }
}
