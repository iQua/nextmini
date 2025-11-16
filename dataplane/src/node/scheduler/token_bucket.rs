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

    /// Wait for enough tokens to send `bytes` worth of data and then
    /// consume those tokens.
    ///
    /// This is a generic pacing primitive that can be reused by components
    /// that don't own a `NetworkInterfaceHandle` (e.g. the reliable sender).
    pub async fn wait_for_bytes(&mut self, bytes: usize) {
        if bytes == 0 {
            return;
        }

        if bytes > self.spec.bucket_size {
            error!(
                "TokenBucket: requested size ({}) exceeds the bucket size ({}). Skipping pacing.",
                bytes, self.spec.bucket_size
            );
            return;
        }

        self.update_tokens();

        if self.tokens >= bytes {
            self.tokens -= bytes;
            return;
        }

        const MAX_RETRIES: usize = 3;
        let mut retry_count = 0;

        loop {
            self.update_tokens();

            if self.tokens >= bytes {
                self.tokens -= bytes;
                break;
            }

            let tokens_needed = bytes - self.tokens;
            let rate = self.spec.rate.max(1) as f64;
            let seconds_required = tokens_needed as f64 / rate;
            let wait_time = Duration::from_secs_f64(seconds_required);

            tokio::time::sleep(wait_time).await;

            retry_count += 1;
            if retry_count >= MAX_RETRIES {
                error!(
                    "TokenBucket: Failed to acquire {} tokens after {} retries. Available tokens: {}. Skipping further pacing for this request.",
                    bytes, MAX_RETRIES, self.tokens
                );
                break;
            }
        }
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
        if !packets_permitted.is_empty()
            && let Err(e) = net_interface.send(packets_permitted).await
        {
            error!("TokenBucket: Error sending a batch of packets: {}", e);
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

#[cfg(test)]
mod tests {
    use super::*;

    use tokio::time::{self, Duration};

    fn make_packet(size: usize) -> Packet {
        let mut packet = Packet::from_vec(vec![0; size.max(1)]);
        packet.flow_id = 1;
        packet.packet_size = size;
        packet
    }

    #[test]
    fn update_tokens_refills_bucket_without_exceeding_capacity() {
        let spec = TokenBucketSpec {
            rate: 100,
            bucket_size: 500,
        };
        let mut bucket = TokenBucket::new(spec);

        // Consume 400 tokens, leaving 100 remaining.
        bucket.consume_tokens(&make_packet(400));
        assert_eq!(bucket.tokens, 100);

        // Pretend 4 seconds have elapsed.
        bucket.last_update = bucket
            .last_update
            .checked_sub(Duration::from_secs(4))
            .expect("last_update is always after the zero instant");
        bucket.update_tokens();

        assert_eq!(
            bucket.tokens, 500,
            "Token bucket should refill to its capacity after sufficient time"
        );
    }

    #[test]
    fn update_tokens_without_time_progress_does_not_add_tokens() {
        let spec = TokenBucketSpec {
            rate: 100,
            bucket_size: 500,
        };
        let mut bucket = TokenBucket::new(spec);

        bucket.consume_tokens(&make_packet(200));
        assert_eq!(bucket.tokens, 300);

        // Update tokens multiple times without advancing time.
        bucket.update_tokens();
        bucket.update_tokens();

        assert_eq!(
            bucket.tokens, 300,
            "Token count should remain unchanged if no time has advanced"
        );
    }

    #[tokio::test]
    async fn wait_for_bytes_respects_rate_with_virtual_time() {
        time::pause();

        let spec = TokenBucketSpec {
            rate: 1_000,
            bucket_size: 1_000,
        };
        let mut bucket = TokenBucket::new(spec);

        bucket.wait_for_bytes(1_000).await;
        assert_eq!(
            bucket.tokens, 0,
            "bucket should be empty after consuming exactly its capacity"
        );

        let mut bucket_for_wait = bucket;
        let waiter = tokio::spawn(async move {
            bucket_for_wait.wait_for_bytes(500).await;
        });

        tokio::task::yield_now().await;
        assert!(
            !waiter.is_finished(),
            "wait_for_bytes should not complete without any time passing"
        );

        time::advance(Duration::from_millis(400)).await;
        tokio::task::yield_now().await;
        assert!(
            !waiter.is_finished(),
            "wait_for_bytes should still be pending after only 400ms of simulated time"
        );

        time::advance(Duration::from_millis(200)).await;
        tokio::task::yield_now().await;
        assert!(
            waiter.is_finished(),
            "wait_for_bytes should complete once enough simulated time has passed"
        );

        waiter.await.unwrap();
    }
}
