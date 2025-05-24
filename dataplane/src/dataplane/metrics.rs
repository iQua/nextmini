use chrono::Utc;
use fxhash::FxHashMap;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::time::{Duration, sleep};

use nextmini-messages::{DataplaneToController, Metric};

use crate::dataplane::{FlowId, NodeId};

pub type MetricsTx = UnboundedSender<(FlowId, NodeId, usize)>;

pub struct Collector {
    controller_tx: UnboundedSender<DataplaneToController>,
    metrics_tx: UnboundedSender<(FlowId, NodeId, usize)>,
    metrics_rx: UnboundedReceiver<(FlowId, NodeId, usize)>,
    collection_rate: u64,
}

impl Collector {
    pub fn new(
        controller_tx: UnboundedSender<DataplaneToController>,
        collection_rate: u64,
    ) -> Self {
        let (metrics_tx, metrics_rx) = tokio::sync::mpsc::unbounded_channel();
        Self {
            metrics_tx,
            metrics_rx,
            controller_tx,
            collection_rate,
        }
    }

    pub fn get_metrics_tx(&self) -> UnboundedSender<(FlowId, NodeId, usize)> {
        self.metrics_tx.clone()
    }

    pub async fn run(&mut self) {
        loop {
            sleep(Duration::from_secs(self.collection_rate)).await;

            let mut data = FxHashMap::default();

            while let Ok((flow_id, node_id, n_bytes)) = self.metrics_rx.try_recv() {
                let flow_data = data.entry(flow_id).or_insert_with(|| (node_id, 0));
                // let socket_data = flow_data.entry(stream_id).or_insert_with(|| (node_id, 0));

                flow_data.1 += n_bytes;
            }

            let now = Utc::now();
            let mut metrics_array = Vec::new();

            for (flow_id, value) in data.iter() {
                // for (sock_id, value) in entry.iter() {
                    let bps = (8.0 * value.1 as f64 / self.collection_rate as f64) as usize;

                    metrics_array.push(Metric {
                        flow_id: flow_id.to_be_bytes(),
                        bps,
                        src_node_id: Some(value.0),
                        // stream_id: Some(format!("{}:{}", sock_id.0, sock_id.1)),
                        time_read: now,
                    });
                // }
            }

            let msg = DataplaneToController::Metrics {
                metrics: metrics_array,
            };
            self.controller_tx.send(msg).unwrap();
        }
    }
}
