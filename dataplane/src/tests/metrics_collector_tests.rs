use approx::assert_relative_eq;
use nextmini_messages::DataplaneToController;

use crate::dataplane::metrics::Collector;
use crate::dataplane::{FlowId, NodeId, SocketId};

#[tokio::test]
async fn tests_new() {
    let (tx, _) = tokio::sync::mpsc::unbounded_channel();
    let _ = Collector::new(tx, 1);
}

#[tokio::test]
async fn test_collector_run() {
    // Set up the controller channel
    let (controller_tx, mut controller_rx) =
        tokio::sync::mpsc::unbounded_channel::<DataplaneToController>();
    let collection_rate = 1; // 1 second
    let mut collector = Collector::new(controller_tx, collection_rate);
    let metrics_tx = collector.get_metrics_tx();

    // Spawn the collector's run method in a background task
    let collector_handle = tokio::spawn(async move {
        collector.run().await;
    });

    // Define metric data
    let flow_id: FlowId = 1;
    let socket_id: SocketId = (1, 1);
    let socket_id2: SocketId = (1, 2);
    let node_id: NodeId = 1;
    let n_bytes = 500;

    // Send metrics: 2 messages for "1:1" and 2 for "1:2", each with 500 bytes
    metrics_tx
        .send((flow_id, socket_id, node_id, n_bytes))
        .unwrap();
    metrics_tx
        .send((flow_id, socket_id, node_id, n_bytes))
        .unwrap();
    metrics_tx
        .send((flow_id, socket_id2, node_id, n_bytes))
        .unwrap();
    metrics_tx
        .send((flow_id, socket_id2, node_id, n_bytes))
        .unwrap();

    // Wait longer than the collection rate to ensure processing
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    // Receive and verify the message from controller_rx
    let received = controller_rx.recv().await.unwrap();

    // Calculate expected bps
    let total_bytes_per_stream = (n_bytes * 2) as f64; // Two metrics per stream
    let expected_bps = 8.0 * total_bytes_per_stream / (collection_rate as f64); // 8 bits per byte

    // Check metrics
    let metrics = match received {
        DataplaneToController::Metrics { metrics } => metrics,
        _ => panic!("Expected Metrics variant"),
    };

    assert!(!metrics.is_empty(), "Metrics should not be empty");
    assert_eq!(metrics.len(), 2); // Expect two (flow_id, stream_id) pairs

    // Verify metrics for stream_id "1:1"
    let metric1 = metrics
        .iter()
        .find(|m| m.stream_id.as_ref().map(|s| s.as_str()) == Some("1:1"))
        .expect("stream_id 1:1 not found");
    assert_eq!(metric1.src_node_id, Some(node_id));
    assert_relative_eq!(metric1.bps as f64, expected_bps, epsilon = 1e-6);

    // Verify metrics for stream_id "1:2"
    let metric2 = metrics
        .iter()
        .find(|m| m.stream_id.as_ref().map(|s| s.as_str()) == Some("1:2"))
        .expect("stream_id 1:2 not found");
    assert_eq!(metric2.src_node_id, Some(node_id));
    assert_relative_eq!(metric2.bps as f64, expected_bps, epsilon = 1e-6);

    // Ensure no unexpected metrics
    assert!(metrics.iter().all(|m| {
        m.stream_id.as_ref().map(|s| s.as_str()) == Some("1:1")
            || m.stream_id.as_ref().map(|s| s.as_str()) == Some("1:2")
    }));

    // Clean up by aborting the collector task
    collector_handle.abort();
}
