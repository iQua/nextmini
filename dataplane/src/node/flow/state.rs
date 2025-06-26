use std::time::Instant as StdInstant;

use tracing::info;

#[derive(Debug, Clone)]
pub struct ConnectionState {
    pub connected: bool,
    pub start_time: StdInstant,
    pub time_last_updated: StdInstant,
    pub bytes_last_updated: u64,
    pub bytes_total: u64,
}

// Implements test_throughput for both client and server side.
impl ConnectionState {
    // node_type: node as client or server
    // bytes_added: bytes received or sent
    pub fn test_throughput(&mut self, node_type: &str, id: usize, bytes_added: u64) {
        if self.bytes_total == 0 {
            self.start_time = StdInstant::now();
            info!("{} {} started transferring data", node_type, id);
        }

        self.bytes_total += bytes_added;
        self.bytes_last_updated += bytes_added;

        let now = StdInstant::now();
        let elapsed_time = now.duration_since(self.time_last_updated).as_secs_f64();

        if elapsed_time > 1.0 {
            let throughput =
                (self.bytes_last_updated as f64 * 8.0) / (elapsed_time * 1_000_000_000.0);

            println!(
                "{} {} throughput: {:.3} Gbps ({} bytes in {:.3}s)",
                node_type, id, throughput, self.bytes_last_updated, elapsed_time
            );

            self.bytes_last_updated = 0;
            self.time_last_updated = now;
        }
    }
}
