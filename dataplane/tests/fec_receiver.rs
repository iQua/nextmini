use std::sync::Arc;
use std::time::Duration;

use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use tokio::sync::{Mutex, mpsc};

use nextmini::node::config::LocalConfig;
use nextmini::node::processor::ProcessorHandle;
use nextmini::node::session::api::InboundFrame;
use nextmini::node::session::fec::{BlockParams, Encoder, block_seed};
use nextmini::node::session::receiver;
use nextmini::node::session::runtime::{CommonConfig, ReceiverConfig};
use nextmini_messages::TokenBucketSpec;
use nextmini_messages::lossless_session::{
    self, FecCapabilities, FecManifest, LosslessSessionControl,
};

const SESSION_ID: u64 = 0xA55A;
const SOURCE_NODE_ID: usize = 11;
const RECEIVER_NODE_ID: usize = 12;
const SYMBOLS_PER_BLOCK: usize = 32;
const SYMBOL_SIZE: usize = 4096;
const REPAIR_PER_BLOCK: usize = 64;
const IID_LOSS_RATE: f64 = 0.10;
const PAYLOAD_BYTES: usize = 64 * 1024 * 1024;

fn test_processor_handle() -> ProcessorHandle {
    let cfg = LocalConfig {
        node_id: RECEIVER_NODE_ID,
        num_packet_processors: 1,
        channel_capacity: 1024,
        ..Default::default()
    };
    ProcessorHandle::new(cfg)
}

fn build_payload(size: usize) -> Vec<u8> {
    let mut payload = vec![0u8; size];
    for (idx, byte) in payload.iter_mut().enumerate() {
        *byte = ((idx as u64 * 31 + 17) & 0xFF) as u8;
    }
    payload
}

#[tokio::test(flavor = "multi_thread")]
async fn receiver_recovers_under_10pct_loss() {
    let payload = build_payload(PAYLOAD_BYTES);
    let sink = Arc::new(Mutex::new(Vec::with_capacity(payload.len())));
    let manifest = FecManifest::new_raptorq(SYMBOLS_PER_BLOCK as u16, SYMBOL_SIZE as u16);

    let common_cfg = CommonConfig {
        session_id: SESSION_ID,
        dest_ip: std::net::Ipv4Addr::new(10, 0, 0, 1),
        chunk_size: SYMBOL_SIZE,
        src_port: 4500,
        dst_port: 4600,
        data_bucket: Some(TokenBucketSpec {
            rate: PAYLOAD_BYTES,
            bucket_size: PAYLOAD_BYTES,
        }),
        local_node_id: RECEIVER_NODE_ID,
        user_space_base_addr: std::net::Ipv4Addr::new(10, 0, 0, 0),
        local_netmask: std::net::Ipv4Addr::new(255, 255, 255, 0),
    };
    let receiver_cfg = ReceiverConfig {
        common: common_cfg,
        source_node_id: SOURCE_NODE_ID,
        expected_bytes: payload.len() as u64,
        sink_buffer: Some(sink.clone()),
        fec_capabilities: FecCapabilities::default(),
    };

    let (tx, rx) = mpsc::channel::<InboundFrame>(2048);
    let receiver_task = tokio::spawn(receiver::run(receiver_cfg, rx, test_processor_handle()));

    let manifest_frame = lossless_session::encode_control(
        SESSION_ID,
        &LosslessSessionControl::FecManifest {
            chunk_size: SYMBOL_SIZE as u32,
            total_bytes: payload.len() as u64,
            fec: manifest,
        },
    );
    tx.send(InboundFrame {
        bytes: manifest_frame,
        peer_id: Some(SOURCE_NODE_ID),
    })
    .await
    .expect("manifest frame should be delivered");

    let total_chunks = (payload.len() as u64).div_ceil(SYMBOL_SIZE as u64);
    let total_blocks = total_chunks.div_ceil(SYMBOLS_PER_BLOCK as u64);
    let mut rng = SmallRng::seed_from_u64(0x5EED);

    for block_id in 0..total_blocks {
        let mut source_symbols = Vec::with_capacity(SYMBOLS_PER_BLOCK);
        let block_base_chunk = block_id as usize * SYMBOLS_PER_BLOCK;
        for esi in 0..SYMBOLS_PER_BLOCK {
            let chunk_zero = block_base_chunk + esi;
            let byte_start = chunk_zero * SYMBOL_SIZE;

            let mut symbol = vec![0u8; SYMBOL_SIZE];
            if byte_start < payload.len() {
                let byte_end = (byte_start + SYMBOL_SIZE).min(payload.len());
                symbol[..byte_end - byte_start].copy_from_slice(&payload[byte_start..byte_end]);
            }
            source_symbols.push(symbol);
        }

        let params = BlockParams::new(
            SYMBOLS_PER_BLOCK,
            SYMBOL_SIZE,
            block_seed(SESSION_ID, block_id),
        );
        let mut encoder =
            Encoder::from_block(params, &source_symbols).expect("encoder should build for block");

        let mut symbols = encoder.emit_systematic();
        symbols.extend(encoder.emit_repair(REPAIR_PER_BLOCK));

        for symbol in symbols {
            if rng.random::<f64>() < IID_LOSS_RATE {
                continue;
            }
            let frame = lossless_session::encode_fec_data_default_tree(
                SESSION_ID,
                block_id,
                symbol.esi,
                &symbol.payload,
            );
            tx.send(InboundFrame {
                bytes: frame,
                peer_id: Some(SOURCE_NODE_ID),
            })
            .await
            .expect("fec frame should be delivered");
        }
    }

    let eot_frame = lossless_session::encode_control(
        SESSION_ID,
        &LosslessSessionControl::Eot {
            last_index: total_chunks,
        },
    );
    tx.send(InboundFrame {
        bytes: eot_frame,
        peer_id: Some(SOURCE_NODE_ID),
    })
    .await
    .expect("eot frame should be delivered");

    drop(tx);

    tokio::time::timeout(Duration::from_secs(30), receiver_task)
        .await
        .expect("receiver should finish")
        .expect("receiver task should not panic");

    let recovered = sink.lock().await;
    assert_eq!(
        recovered.len(),
        payload.len(),
        "recovered payload length must match expected object length"
    );
    assert_eq!(
        recovered.as_slice(),
        payload.as_slice(),
        "recovered payload content must match source object under configured IID loss"
    );
}
