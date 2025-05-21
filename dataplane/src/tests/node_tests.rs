use core::panic;
use std::collections::HashMap;
use std::sync::Arc;
use crate::dataplane::node_interface::{self, NodeManager};
use crate::dataplane::local_interface;
use crate::dataplane::metrics::{self, Collector};
use crate::dataplane::Packet;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::{mpsc, RwLock};
use tokio_tungstenite::tungstenite::handshake::server;
use crate::configs;

const MTU: usize = 1400;
const RECEIVE_BUF_SIZE: usize = MTU + 4;

#[tokio::test]
async fn create_new_tcp_node(){
    //test to see if we can create a new tcp node
    let _server_handle = tokio::spawn(async move{
        let listener = match tokio::net::TcpListener::bind("127.0.0.1:6688").await{
            Err(e) => panic!("{e}" ),
            Ok(listener) => listener,
        };
        let (mut _stream, _) = listener.accept().await.expect("Should receive connection");     
    });
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await; //wait for the server to be up
    let stream = tokio::net::TcpStream::connect("127.0.0.1:6688").await.expect("Should be able to connect");
    let (_receiver, _sender) =  node_interface::create_tcp_node_interfaces(stream,Arc::new(RwLock::new(None)));
}

#[tokio::test]
async fn tcp_node_test_receive(){
    let server_handle = tokio::spawn(async move{
        let listener = match tokio::net::TcpListener::bind("127.0.0.1:6688").await{
            Err(e) => panic!("{e}" ),
            Ok(listener) => listener,
        };
        let (mut stream, _) = listener.accept().await.expect("Should receive connection");
        let dummy_ip_frame: Vec<u8> = vec![69,0,0,60,152,244,64,0,64,6,141,197,10,0,0,1,10,0,0,2,227,140,31,144,173,151,10,67,0,0,0,0,160,2,114,16,38,110,0,0,2,4,5,180,4,2,8,10,121,193,102,198,0,0,0,0,1,3,3,7];

        if let Err(e) = stream.write_all(&dummy_ip_frame[..]).await {
            eprintln!("failed to write to socket; err = {:?}", e);
            return;
        }
       tokio::time::sleep(tokio::time::Duration::from_secs(1000)).await;
    });
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await; //wait for the server to be up
    let stream = tokio::net::TcpStream::connect("127.0.0.1:6688").await.expect("Should be able to connect");
    let (mut receiver, _sender) =  node_interface::create_tcp_node_interfaces(stream, Arc::new(RwLock::new(None)));

    let (tx,mut rx) = mpsc::channel(512);
    let receiver_handle = tokio::spawn(async move{
        receiver.start_receiving(tx, Arc::new(RwLock::new(metrics::Meter::new()))).await;
    });

    let res = match rx.recv().await{
        Some(res) => res,
        _ => panic!("The channel is closed"),
    };

    let dummy_ip_frame: Vec<u8> = vec![69,0,0,60,152,244,64,0,64,6,141,197,10,0,0,1,10,0,0,2,227,140,31,144,173,151,10,67,0,0,0,0,160,2,114,16,38,110,0,0,2,4,5,180,4,2,8,10,121,193,102,198,0,0,0,0,1,3,3,7];
    assert!(res.packet_size == 60); //17 is the message size here
    
    assert!(&res.buf[..60] == &dummy_ip_frame[..]);
    receiver_handle.abort();
    server_handle.abort();
}

#[tokio::test]
async fn tcp_node_test_send(){
    let _server_handle = tokio::spawn(async move{
        let listener = match tokio::net::TcpListener::bind("127.0.0.1:6688").await{
            Err(e) => panic!("{e}" ),
            Ok(listener) => listener,
        };
        let (mut stream, _) = listener.accept().await.expect("Should receive connection");
        let mut buf = [0; 60];

        // read data from the socket and write the data back.
        let _n = match stream.read_exact(&mut buf).await {
            // socket closed
            Ok(n) if n == 0 => return,
            Ok(n) => n,
            Err(e) => {
                eprintln!("failed to read from socket; err = {:?}", e);
                return;
            }
        };

        // Write the data back
        if let Err(e) = stream.write_all(&buf[..]).await {
            eprintln!("failed to write to socket; err = {:?}", e);
            return;
        }
        tokio::time::sleep(tokio::time::Duration::from_secs(1000)).await;
    });
    
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await; //wait for the server to be up
    let stream = tokio::net::TcpStream::connect("127.0.0.1:6688").await.expect("Should be able to connect");
    let (mut receiver, mut sender) =  node_interface::create_tcp_node_interfaces(stream, Arc::new(RwLock::new(None)));

    let (s_tx,s_rx) = mpsc::channel(512);
    let _sender_handle = tokio::spawn(async move{
        sender.start_sending(s_rx).await;
    });

    let (r_tx, mut r_rx) = mpsc::channel(512);
    let receiver_handle = tokio::spawn(async move{
        receiver.start_receiving(r_tx, Arc::new(RwLock::new(metrics::Meter::new()))).await;
    });

    let dummy_ip_frame: Vec<u8> = vec![69,0,0,60,152,244,64,0,64,6,141,197,10,0,0,1,10,0,0,2,227,140,31,144,173,151,10,67,0,0,0,0,160,2,114,16,38,110,0,0,2,4,5,180,4,2,8,10,121,193,102,198,0,0,0,0,1,3,3,7];
    let mut buf:PacketBuf = [0; RECEIVE_BUF_SIZE];
    buf[..60].copy_from_slice(&dummy_ip_frame[..]);
    s_tx.send(Packet::new(60, buf)).await.expect("Should suceed sending");

    let ans = r_rx.recv().await.expect("Should receive an answer");

    assert!(ans.packet_size == 60); //17 is the message size here
    assert!(&ans.buf[..60] == &dummy_ip_frame[..]);
    receiver_handle.abort();
}


#[tokio::test]
async fn test_context_new(){
    let configs = configs::new();
    let tun_devs = local_interface::create_tun_devices(&configs, 1, (10,0,0,1), (255,255,255,0));
    let (tx, _) = tokio::sync::mpsc::unbounded_channel();
    let metrics_collector = Collector::new(tx, 1);
    let _context = NodeManager::new(&configs, 1, tun_devs, metrics_collector, Arc::new(RwLock::new(HashMap::new())));
}

#[tokio::test]
async fn test_context_add_tcp_node(){
    let server_handle = tokio::spawn(async move{
        let listener = match tokio::net::TcpListener::bind("127.0.0.1:6688").await{
            Err(e) => panic!("{e}" ),
            Ok(listener) => listener,
        };
        let (mut _stream, _) = listener.accept().await.expect("Should receive connection");
        tokio::time::sleep(tokio::time::Duration::from_secs(1000)).await;
    });
    
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await; //wait for the server to be up
    // Create a node manager
    let configs = configs::new();
    let tun_devs = local_interface::create_tun_devices(&configs, 1, (10,0,0,1), (255,255,255,0));
    let (tx, _) = tokio::sync::mpsc::unbounded_channel();
    let metrics_collector = Collector::new(tx, 1);
    let context = NodeManager::new(&configs, 2, tun_devs, metrics_collector,Arc::new(RwLock::new(HashMap::new())));
    let stream = tokio::net::TcpStream::connect("127.0.0.1:6688").await.expect("Should be able to connect");
    
    context.add_node(1, stream).await;
    server_handle.abort();
}

#[tokio::test]
async fn test_context_recv(){
    let server_handle = tokio::spawn(async move{
        let listener = match tokio::net::TcpListener::bind("127.0.0.1:6688").await{
            Err(e) => panic!("{e}" ),
            Ok(listener) => listener,
        };
        let (mut stream, _) = listener.accept().await.expect("Should receive connection");
        let dummy_ip_frame: Vec<u8> = vec![69,0,0,60,152,244,64,0,64,6,141,197,10,0,0,1,10,0,0,2,227,140,31,144,173,151,10,67,0,0,0,0,160,2,114,16,38,110,0,0,2,4,5,180,4,2,8,10,121,193,102,198,0,0,0,0,1,3,3,7];

        if let Err(e) = stream.write_all(&dummy_ip_frame[..]).await {
            eprintln!("failed to write to socket; err = {:?}", e);
            return;
        }
       tokio::time::sleep(tokio::time::Duration::from_secs(1000)).await;
    });
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await; //wait for the server to be up
    let stream = tokio::net::TcpStream::connect("127.0.0.1:6688").await.expect("Should be able to connect");
    let configs = configs::new();
    let tun_devs = local_interface::create_tun_devices(&configs, 1, (10,0,0,1), (255,255,255,0));

    // Create a node manager
    let (tx, _) = tokio::sync::mpsc::unbounded_channel();
    let metrics_collector = Collector::new(tx, 1);
    let context = NodeManager::new(&configs, 2, tun_devs, metrics_collector,Arc::new(RwLock::new(HashMap::new())));

    context.add_node(1, stream).await;
    let packet = context.recv().await;

    let dummy_ip_frame: Vec<u8> = vec![69,0,0,60,152,244,64,0,64,6,141,197,10,0,0,1,10,0,0,2,227,140,31,144,173,151,10,67,0,0,0,0,160,2,114,16,38,110,0,0,2,4,5,180,4,2,8,10,121,193,102,198,0,0,0,0,1,3,3,7];
    assert!(packet.packet_size == 60); //17 is the message size here
    assert!(&packet.buf[..60] == &dummy_ip_frame[..]);
    server_handle.abort();
}

#[tokio::test]
async fn test_node_mannager_send(){
    let _server_handle = tokio::spawn(async move{
        let listener = match tokio::net::TcpListener::bind("127.0.0.1:6688").await{
            Err(e) => panic!("{e}" ),
            Ok(listener) => listener,
        };
        let (mut stream, _) = listener.accept().await.expect("Should receive connection");
        let mut buf = [0; 60];

        // read data from the socket and write the data back.
        let _n = match stream.read_exact(&mut buf).await {
            // socket closed
            Ok(n) if n == 0 => return,
            Ok(n) => n,
            Err(e) => {
                eprintln!("failed to read from socket; err = {:?}", e);
                return;
            }
        };

        // Write the data back
        if let Err(e) = stream.write_all(&buf[..]).await {
            eprintln!("failed to write to socket; err = {:?}", e);
            return;
        }
        tokio::time::sleep(tokio::time::Duration::from_secs(1000)).await;
    });
    
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await; //wait for the server to be up
    let stream = tokio::net::TcpStream::connect("127.0.0.1:6688").await.expect("Should be able to connect");
    let configs = configs::new();
    let tun_devs = local_interface::create_tun_devices(&configs, 1, (10,0,0,1), (255,255,255,0));
    
    // create a node manager
    let (tx, _) = tokio::sync::mpsc::unbounded_channel();
    let metrics_collector = Collector::new(tx, 1);
    let context = NodeManager::new(&configs, 2, tun_devs, metrics_collector, Arc::new(RwLock::new(HashMap::new())));

    context.add_node(1, stream).await;

    let dummy_ip_frame: Vec<u8> = vec![69,0,0,60,152,244,64,0,64,6,141,197,10,0,0,1,10,0,0,2,227,140,31,144,173,151,10,67,0,0,0,0,160,2,114,16,38,110,0,0,2,4,5,180,4,2,8,10,121,193,102,198,0,0,0,0,1,3,3,7];
    let mut buf:PacketBuf = [0; RECEIVE_BUF_SIZE];
    buf[..60].copy_from_slice(&dummy_ip_frame[..]);
    context.send(1, Packet::new(60, buf)).await;

    let packet = context.recv().await;

    assert!(packet.packet_size == 60);
    assert!(&packet.buf[..60] == &dummy_ip_frame[..]);
}