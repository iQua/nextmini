use tokio::time::Duration;
use tokio::time::Instant as AsyncInstant;

use tracing::error;

use crate::node::network_interface::NetworkInterfaceHandle;
use crate::node::packet::Packet;

/// The specification of a token bucket.
#[derive(Clone)]
pub struct TokenBucketSpec {
    pub rate: usize,
    pub bucket_size: usize,
}

/// A token bucket traffic shaping algorithm.
pub struct TokenBucket {
    spec: TokenBucketSpec,
    tokens: usize,
    last_update: AsyncInstant,
}

impl TokenBucket {
    pub fn new(spec: TokenBucketSpec) -> Self {
        TokenBucket {
            spec,
            tokens: 0,
            last_update: AsyncInstant::now(),
        }
    }

    pub async fn send(&mut self, net_interface: &mut NetworkInterfaceHandle, packets: Vec<Packet>) {
        self.tokens += ((AsyncInstant::now() - self.last_update).as_secs_f64()
            * self.spec.rate as f64) as usize;

        // if the token bucket is full, discard all extra tokens
        if self.tokens > self.spec.bucket_size {
            self.tokens = self.spec.bucket_size;
        }

        // separate packets into immediate and delayed
        let mut packets_permitted: Vec<Packet> = Vec::new();
        let mut packets_delayed: Vec<Packet> = Vec::new();

        for packet in packets {
            if self.tokens >= packet.packet_size {
                // deducts the corresponding amount of tokens from the token bucket
                self.tokens -= packet.packet_size;
                // prepares this packet for sending
                packets_permitted.push(packet);
            } else {
                // not enough tokens, need to wait
                packets_delayed.push(packet);
            }
        }

        // sends the permitted packets in one batch
        if !packets_permitted.is_empty() {
            if let Err(e) = net_interface.send(packets_permitted).await {
                error!("TokenBucket: Error sending a batch of packets: {}", e);
            }
        }

        // then sends the delayed packets one by one
        let mut wait_time = Duration::from_secs_f64(0.0);

        for packet in packets_delayed {
            // calculates the wait time for this packet
            let seconds_required = if self.tokens < packet.packet_size {
                (packet.packet_size - self.tokens) as f64 / self.spec.rate as f64
            } else {
                0.0
            };

            wait_time += Duration::from_secs_f64(seconds_required);

            // only needs to sleep if more than 5 milliseconds, due to the time resolution of tokio
            if wait_time > Duration::from_millis(5) {
                tokio::time::sleep(wait_time).await;
                wait_time = Duration::from_secs_f64(0.0);
            }

            // updates tokens based on the time passed
            self.tokens += ((AsyncInstant::now() - self.last_update).as_secs_f64()
                * self.spec.rate as f64) as usize;

            if self.tokens > self.spec.bucket_size {
                self.tokens = self.spec.bucket_size;
            }

            // consumes tokens and sends the packet
            self.tokens = self.tokens.saturating_sub(packet.packet_size);

            if let Err(e) = net_interface.send(vec![packet]).await {
                error!("TokenBucket: Error sending a packet: {}", e);
            }

            self.last_update = AsyncInstant::now();
        }

        self.last_update = AsyncInstant::now();
    }
}
