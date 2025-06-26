use tokio::time::Duration;
use tokio::time::Instant as AsyncInstant;

use tracing::error;

use nextmini_messages::TokenBucketSpec;

use crate::node::network::interface::NetworkInterfaceHandle;
use crate::node::packet::Packet;

/// A token bucket traffic shaping algorithm.
pub struct TokenBucket {
    spec: TokenBucketSpec,
    tokens: usize,
    last_update: AsyncInstant,
}

impl TokenBucket {
    pub fn new(spec: TokenBucketSpec) -> Self {
        TokenBucket {
            tokens: spec.bucket_size,
            spec,
            last_update: AsyncInstant::now(),
        }
    }

    /// Updates tokens in the token bucket based on elapsed time.
    fn update_tokens(&mut self) {
        let now = AsyncInstant::now();
        self.tokens += ((now - self.last_update).as_secs_f64() * self.spec.rate as f64) as usize;

        // if the token bucket is full, discard all extra tokens
        if self.tokens > self.spec.bucket_size {
            self.tokens = self.spec.bucket_size;
        }

        self.last_update = now;
    }

    /// Deducts the corresponding amount of tokens from the token bucket.
    fn consume_tokens(&mut self, packet: &Packet) {
        self.tokens -= packet.packet_size;
    }

    pub async fn send(&mut self, net_interface: &mut NetworkInterfaceHandle, packets: Vec<Packet>) {
        self.update_tokens();

        // separate packets into immediate and delayed
        let mut packets_permitted: Vec<Packet> = Vec::new();
        let mut packets_delayed: Vec<Packet> = Vec::new();

        for packet in packets {
            if self.tokens >= packet.packet_size {
                self.consume_tokens(&packet);

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
        for packet in packets_delayed {
            // check if packet is larger than bucket size (impossible to send)
            if packet.packet_size > self.spec.bucket_size {
                error!(
                    "TokenBucket: Packet size ({}) exceeds the bucket size ({}). Dropped.",
                    packet.packet_size, self.spec.bucket_size
                );

                continue;
            }

            // a retry loop to handle timing precision issues
            const MAX_RETRIES: usize = 3;
            let mut retry_count = 0;

            loop {
                self.update_tokens();

                if self.tokens >= packet.packet_size {
                    self.consume_tokens(&packet);

                    if let Err(e) = net_interface.send(vec![packet]).await {
                        error!("TokenBucket: Error sending a packet: {}", e);
                    }

                    break;
                }

                // calculates the wait time for the remaining tokens needed
                let tokens_needed = packet.packet_size - self.tokens;
                let seconds_required = tokens_needed as f64 / self.spec.rate as f64;
                let wait_time = Duration::from_secs_f64(seconds_required);

                tokio::time::sleep(wait_time).await;

                retry_count += 1;
                if retry_count >= MAX_RETRIES {
                    error!(
                        "TokenBucket: Failed to send packet after {} retries. Packet size: {}, available tokens: {}",
                        MAX_RETRIES, packet.packet_size, self.tokens
                    );

                    break;
                }
            }
        }
    }
}
