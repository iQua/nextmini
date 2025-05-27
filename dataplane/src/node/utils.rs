use std::sync::Arc;

use tokio::sync::RwLock;
use tokio::time::Duration;
use tokio::time::Instant as AsyncInstant;

/// This struct represents a leaky bucket used in leaky bucket rate limiter algorithms.
struct LeakyBucket {
    rate: f64,
    tokens: f64,
    time_to_wait: Duration,
    last_update: AsyncInstant,
    init: bool,
}

impl LeakyBucket {
    fn new(rate: f64) -> Self {
        LeakyBucket {
            rate,
            tokens: 0.0,
            time_to_wait: Duration::from_secs(0),
            last_update: AsyncInstant::now(),
            init: true,
        }
    }

    fn consume(&mut self, tokens: f64) -> bool {
        if self.init {
            self.last_update = AsyncInstant::now();
            self.init = false;
        }
        self.tokens += (AsyncInstant::now() - self.last_update).as_secs_f64() * self.rate;
        self.last_update = AsyncInstant::now();

        // If the sender has been idle for more than 1 second, we reset the bucket
        if self.tokens > self.rate {
            self.tokens = 0.0;
        }

        if self.tokens >= 0.0 {
            // Bucket is filled up, we can send a packet now
            self.tokens -= tokens;
            true
        } else {
            false
        }
    }

    fn time_to_wait(&mut self) -> Duration {
        self.tokens += (AsyncInstant::now() - self.last_update).as_secs_f64() * self.rate;
        self.last_update = AsyncInstant::now();

        let time_to_wait = if self.tokens >= 0.0 {
            Duration::from_secs(0)
        } else {
            let seconds_required = -self.tokens / self.rate;
            Duration::from_secs_f64(seconds_required)
        };

        // Only return time to wait if it is greater than 1 millisecond. This is because tokio's
        // sleep only have a highest time resolution of 1 millisecond. If smaller than 1 millisecond,
        // then just save the sleep for later
        self.time_to_wait += time_to_wait;
        if self.time_to_wait > Duration::from_millis(5) {
            let ret = self.time_to_wait;
            self.time_to_wait = Duration::from_secs(0);

            ret
        } else {
            Duration::from_secs(0)
        }
    }
}

#[derive(Clone)]
pub struct RateLimiter {
    bucket: Arc<RwLock<LeakyBucket>>,
}

impl RateLimiter {
    pub fn new(rate: f64) -> Self {
        let bucket = Arc::new(RwLock::new(LeakyBucket::new(rate)));
        RateLimiter { bucket }
    }

    pub async fn consume(&self, tokens: f64) {
        let mut bucket = self.bucket.write().await;
        while !bucket.consume(tokens) {
            tokio::time::sleep(bucket.time_to_wait()).await;
        }
    }
}

#[tokio::test]
async fn test_bucket() {
    let mut bucket = LeakyBucket::new(10.0);
    // consume 1 tokens
    assert!(bucket.consume(1.0));

    // consume 1 tokens again, should fail
    assert!(!bucket.consume(1.0));

    //sleep for 100 seconds for one token
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // consume 1 tokens, should succeed
    assert!(bucket.consume(1.0));

    // consume 1 token, should fail
    assert!(!bucket.consume(1.0));
}

#[tokio::test]
async fn test_rate_limiter() {
    //create a rate limiter with 10 tokens per second
    let rate_limiter = RateLimiter::new(10.0);

    //record the current time
    let start = AsyncInstant::now();

    // consume 3 tokens
    rate_limiter.consume(3.0).await;

    //record the current time
    let end = AsyncInstant::now();

    //check if the time difference is less than 100ms (it should be instant)
    assert!(end - start < Duration::from_millis(100));

    // The bucket is now in deficit. Consume 1 token, it should take ~ 400 ms
    let start = AsyncInstant::now();

    rate_limiter.consume(1.0).await;

    let end = AsyncInstant::now();
    assert!(end - start > Duration::from_millis(300));
}
