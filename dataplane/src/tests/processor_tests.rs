use std::collections::HashMap;
use std::sync::Arc;
use crate::dataplane::node_server;
#[cfg(test)]
use crate::dataplane::processor;
use crate::dataplane::routes;
use crate::dataplane::node_interface;
use crate::dataplane::routes::RoutingTable;
use tokio::net::TcpStream;
use tokio::sync::RwLock;
use crate::dataplane::node_interface::NodeManager;
use crate::dataplane::local_interface;
use crate::dataplane::metrics::Collector;
use crate::dataplane::utils::RateLimiter;
use crate::configs;

use tokio::{io::{AsyncReadExt, AsyncWriteExt}};


#[tokio::test]
async fn test_processor_new(){
    let configs = configs::new();
    let tun_devs = local_interface::create_tun_devices(&configs, 1, (10,0,0,1), (255,255,255,0));
    let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
    let metrics_collector = Collector::new(tx, 1);
    let context = NodeManager::new(&configs, 1, tun_devs, metrics_collector,Arc::new(RwLock::new(HashMap::new())));
    let table  = routes::RoutingTable::new();
    let router = routes::Router::new(
        Arc::new(RwLock::new(table)),
    );
    let _ = processor::Processor::new(
        context.clone(),
        router,
    );
}