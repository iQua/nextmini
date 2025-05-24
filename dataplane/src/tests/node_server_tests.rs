use std::collections::HashMap;
use std::sync::Arc;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::RwLock;

use crate::configs::{self, ControllerConfigs};
use crate::dataplane::local_interface;
use crate::dataplane::metrics::Collector;
use crate::dataplane::node_interface::NodeManager;
use crate::dataplane::node_server::TcpNodeServer;

#[tokio::test]
async fn test_node_server_new() {
    let local_configs = configs::new();
    let controller_configs = ControllerConfigs {
        node_id: 1,
        controller_addr: "127.0.0.1:8080".to_string(),
        session_id: [0, 0, 0, 1],
        strato_address: (10, 0, 0, 1),
        strato_mask: (255, 255, 255, 0),
        num_interfaces: 1,
        protocol: nextmini - messages::Protocol::Tcp,
        scheduler_type: crate::dataplane::scheduler::SchedulingDiscipline::Fifo,
    };
    let tun_device =
        local_interface::create_tun_device(local_configs.clone(), controller_configs.clone()).await;
    let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
    let metrics_collector = Collector::new(tx, local_configs.metrics_collection_interval);
    let context = NodeManager::new(
        &local_configs,
        1,
        tun_device,
        metrics_collector,
        Arc::new(RwLock::new(HashMap::new())),
    );
    let _node_server = TcpNodeServer::new(context.clone(), [0, 0, 0, 1]);
}

#[tokio::test]
async fn test_node_server_start_listening() {
    let local_configs = configs::new();
    let controller_configs = ControllerConfigs {
        node_id: 2,
        controller_addr: "127.0.0.1:8080".to_string(),
        session_id: [0, 0, 0, 1],
        strato_address: (10, 0, 0, 1),
        strato_mask: (255, 255, 255, 0),
        num_interfaces: 1,
        protocol: nextmini - messages::Protocol::Tcp,
        scheduler_type: crate::dataplane::scheduler::SchedulingDiscipline::Fifo,
    };
    let tun_device =
        local_interface::create_tun_device(local_configs.clone(), controller_configs.clone()).await;
    let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
    let metrics_collector: Collector =
        Collector::new(tx, local_configs.metrics_collection_interval);
    let context = NodeManager::new(
        &local_configs,
        2,
        tun_device,
        metrics_collector,
        Arc::new(RwLock::new(HashMap::new())),
    );
    let session_id = [0, 0, 0, 1];
    let mut node_server = TcpNodeServer::new(context.clone(), session_id);

    let handle = tokio::spawn(async move {
        node_server
            .start_listening(&"127.0.0.1:9861".to_string())
            .await;
    });

    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await; //wait for the server to be up

    //Mock a tcp node initiating a connection to node server
    let mut stream = tokio::net::TcpStream::connect("127.0.0.1:9861")
        .await
        .expect("Failed to connect to server");

    //Write session id to the fake tcp node stream
    stream
        .write(&session_id)
        .await
        .expect("Failed to send session id");

    let node_id: u64 = 1;
    stream
        .write(&node_id.to_be_bytes())
        .await
        .expect("Failed to send node id");

    //Write some data to the fake tcp node stream
    let dummy_ip_frame: Vec<u8> = vec![
        69, 0, 0, 60, 152, 244, 64, 0, 64, 6, 141, 197, 10, 0, 0, 1, 10, 0, 0, 2, 227, 140, 31,
        144, 173, 151, 10, 67, 0, 0, 0, 0, 160, 2, 114, 16, 38, 110, 0, 0, 2, 4, 5, 180, 4, 2, 8,
        10, 121, 193, 102, 198, 0, 0, 0, 0, 1, 3, 3, 7,
    ];
    stream
        .write(&dummy_ip_frame[..])
        .await
        .expect("Failed to write to server");

    let mut packet = context.recv().await;
    while packet.packet_size != 60 {
        packet = context.recv().await;
    }

    assert!(
        packet.packet_size == 60,
        "Expected 60, but got {:?}",
        packet.packet_size
    );
    assert!(
        packet.buf[..packet.packet_size] == dummy_ip_frame[..],
        "Expected {:?}, Got {:?}",
        dummy_ip_frame,
        packet.buf
    );
    handle.abort();
}

