use ahash::AHashMap;
use chrono::Utc;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};
use tokio::time::{Duration, interval};

use nextmini_messages::{DataplaneToController, Metric};

use crate::node::controller_interface::ControllerInterfaceHandle;
use crate::node::{FlowId, NodeId};

pub struct Collector {
    controller_interface: ControllerInterfaceHandle,
    metrics_tx: UnboundedSender<(FlowId, NodeId, usize)>,
    metrics_rx: UnboundedReceiver<(FlowId, NodeId, usize)>,
}

impl Collector {
    pub fn new(controller_interface: ControllerInterfaceHandle) -> Self {
        let (metrics_tx, metrics_rx) = unbounded_channel();
        Self {
            metrics_tx,
            metrics_rx,
            controller_interface,
        }
    }

    pub fn get_metrics_tx(&self) -> UnboundedSender<(FlowId, NodeId, usize)> {
        self.metrics_tx.clone()
    }

    pub async fn run(&mut self) {
        // Transmit metrics every 5 seconds
        let mut metrics_tick = interval(Duration::from_secs(5));

        // HashMap: flow_id -> (node_id, total_bytes)
        let mut data = AHashMap::default();

        loop {
            tokio::select! {
                // receives new metrics data
                Some((flow_id, node_id, n_bytes)) = self.metrics_rx.recv() => {
                    let flow_data = data.entry(flow_id).or_insert_with(|| (node_id, 0));
                    flow_data.1 += n_bytes;
                }

                // timer tick: calculate bandwidth metrics and transmit to controller
                _ = metrics_tick.tick() => {
                    if !data.is_empty() {
                        let now = Utc::now();
                        let mut metrics_array = Vec::new();

                        const COLLECTION_INTERVAL_SECS: f64 = 5.0;

                        for (flow_id, value) in data.iter() {
                            let bps = (8.0 * value.1 as f64 / COLLECTION_INTERVAL_SECS) as usize;

                            metrics_array.push(Metric {
                                flow_id: flow_id.to_be_bytes(),
                                bps,
                                src_node_id: Some(value.0),
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
