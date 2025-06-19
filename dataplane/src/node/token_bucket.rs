use tokio::time::Duration;
use tokio::time::Instant as AsyncInstant;

use crate::node::network_interface::NetworkInterfaceHandle;
use crate::node::packet::Packet;

/// The specification of a token bucket.
struct TokenBucketSpec {
    pub rate: usize,
    pub bucket_size: usize,
}

/// A token bucket traffic shaping algorithm.
struct TokenBucket {
    spec: TokenBucketSpec,
    tokens: usize,
    last_update: AsyncInstant,
}

impl TokenBucket {
    fn new(rate: usize, bucket_size: usize) -> Self {
        TokenBucket {
            TokenBucketSpec {rate, bucket_size},
            tokens: 0,
            last_update: AsyncInstant::now(),
        }
    }

    fn send(&mut self, net_interface: NetworkInterfaceHandle, packets: Vec<Packet>) -> bool {
        self.tokens += ((AsyncInstant::now() - self.last_update).as_secs_f64() * self.spec.rate as f64) as usize;

        // if the token bucket is full, discard all extra tokens
        if self.tokens > self.spec.bucket_size {
            self.tokens = self.spec.bucket_size;
        }

        // sends the packets according to the current status of the token bucket
        // to be implemented

        self.last_update = AsyncInstant::now();
    }
}
