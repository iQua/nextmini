use crate::configs;
use crate::dataplane::controller_interface::Controller;
use futures::{SinkExt, StreamExt};
use serde_json::{Value, json};
use std::time::Duration;
use tokio::sync::oneshot;
use tokio_tungstenite::tungstenite::Message;

// Helper function to create a server with a cancellation channel
async fn create_install_flow_server() -> oneshot::Sender<()> {
    let (shutdown_tx, mut shutdown_rx) = oneshot::channel();

    tokio::spawn(async move {
        let listener = match tokio::net::TcpListener::bind("127.0.0.1:6688").await {
            Err(e) => panic!("{e}"),
            Ok(listener) => listener,
        };

        let accept = async {
            let (stream, _) = listener.accept().await.expect("Should receive connection");
            let mut ws_stream = tokio_tungstenite::accept_async(stream)
                .await
                .expect("Should be able to accept");

            let startup = ws_stream
                .next()
                .await
                .expect("Should receive a message")
                .expect("Invalid message");

            match startup {
                Message::Text(msg) => {
                    let msg: Value = serde_json::from_str(&msg).expect("Invalid message");
                    assert_eq!(msg["type"], 0, "Expected: 0, Got: {}", msg["type"]);
                }
                _ => panic!("Invalid message"),
            }

            // Send a start up control message
            let ctrl_msg = json!({
                "type": 0,
                "node_id": 0,
                "addr": [10,0,0,1],
                "net_mask": [255,255,255,0],
                "session_id": [0,0,0,1],
                "num_paths": 1,
                "protocol": "tcp",
            });

            ws_stream
                .send(Message::Text(ctrl_msg.to_string().into()))
                .await
                .expect("Should be able to write");

            //install a flow
            let ctrl_msg1 = get_dummy_json_flow();

            ws_stream
                .send(Message::Text(ctrl_msg1.to_string().into()))
                .await
                .expect("Should be able to write");

            ws_stream
        };

        tokio::select! {
            _ = &mut shutdown_rx => {
                return;
            }
            stream = accept => {
                let _ws_stream = stream;
                // Keep stream alive until shutdown
                let _ = shutdown_rx.await;
            }
        }
    });

    shutdown_tx
}

#[tokio::test]
async fn test_controller_interface_new() {
    let server_shutdown = create_install_flow_server().await;
    tokio::time::sleep(Duration::from_millis(100)).await;

    let configs = configs::new();
    let (shutdown_tx, _) = tokio::sync::watch::channel(false);
    let controller = Controller::connect(configs, shutdown_tx).await;
    drop(controller);
    let _ = server_shutdown.send(());
}

#[tokio::test]
async fn test_controller_interface_split() {
    let server_shutdown = create_install_flow_server().await;
    tokio::time::sleep(Duration::from_millis(100)).await;

    let configs = configs::new();
    let (shutdown_tx, _) = tokio::sync::watch::channel(false);
    let controller = Controller::connect(configs, shutdown_tx).await;

    let (sender, receiver) = controller.split().await;
    drop(sender);
    drop(receiver);
    let _ = server_shutdown.send(());
}

#[tokio::test]
async fn test_controller_interface_install_flow() {
    let server_shutdown = create_install_flow_server().await;
    tokio::time::sleep(Duration::from_millis(100)).await;

    let config = configs::new();
    let (shutdown_tx, _) = tokio::sync::watch::channel(false);
    let controller = Controller::connect(config, shutdown_tx).await;

    let processor_manager = controller.get_processor_manager();
    let processor_manager = processor_manager.read().await;
    let routing_table = processor_manager.routing_table.clone();
    drop(processor_manager);

    let (sender, mut receiver) = controller.split().await;
    tokio::spawn(async move {
        receiver.run().await;
    });

    tokio::time::sleep(Duration::from_millis(100)).await;

    let flow_id = 42;
    let expected_next_hop = 1; // First route's next_hop from dummy flow
    if let Some(next_hop) = routing_table.next_hop(&flow_id) {
        assert_eq!(
            *next_hop, expected_next_hop,
            "Flow was not installed correctly"
        );
    } else {
        panic!("Flow was not installed");
    }

    drop(sender);
    let _ = server_shutdown.send(());
}

#[tokio::test]
async fn test_controller_interface_send() {
    let server_shutdown = create_install_flow_server().await;
    tokio::time::sleep(Duration::from_millis(100)).await;

    let configs = configs::new();
    let (shutdown_tx, _) = tokio::sync::watch::channel(false);
    let controller = Controller::connect(configs, shutdown_tx).await;

    let (mut sender, receiver) = controller.split().await;
    let sender_tx = sender.get_tx();

    tokio::spawn(async move {
        sender.run().await;
    });

    let msg = json!({
        "type": "dummy"
    });

    sender_tx
        .send(msg)
        .expect("Failed to send message to the controller sender");

    tokio::time::sleep(Duration::from_millis(50)).await;
    drop(receiver);
    let _ = server_shutdown.send(());
}

#[tokio::test]
async fn controller_interface_send_startup() {
    let server_shutdown = create_install_flow_server().await;
    tokio::time::sleep(Duration::from_millis(100)).await;

    let configs = configs::new();
    let (shutdown_tx, _) = tokio::sync::watch::channel(false);
    let controller = Controller::connect(configs, shutdown_tx).await;
    drop(controller);
    let _ = server_shutdown.send(());
}

#[tokio::test]
async fn controller_interface_send_startup_real_server() {
    let server_shutdown = create_install_flow_server().await;
    tokio::time::sleep(Duration::from_millis(100)).await;

    let mut configs = configs::new();
    configs.server_addr = "ws://127.0.0.1:6688".to_string();
    let (shutdown_tx, _) = tokio::sync::watch::channel(false);
    let mut controller = Controller::connect(configs, shutdown_tx).await;
    let mut collector = controller.take_metrics_collector();

    let collector_handle = tokio::spawn(async move {
        collector.run().await;
    });

    let (mut sender, mut receiver) = controller.split().await;

    let sender_handle = tokio::spawn(async move {
        sender.run().await;
    });

    let receiver_handle = tokio::spawn(async move {
        receiver.run().await;
    });

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Cleanup
    collector_handle.abort();
    sender_handle.abort();
    receiver_handle.abort();
    let _ = server_shutdown.send(());
}

fn get_dummy_json_flow() -> Value {
    json!({
        "type": 2,
        "flows": [
            {
                "flow_id": [0, 0, 0, 0, 0, 0, 0, 42],
                "weight": 10,
                "routes": [
                    {
                        "weight": 10,
                        "next_hop": 1,
                    },
                    {
                        "weight": 10,
                        "next_hop": 2,
                    },
                ],
            }
        ]
    })
}
