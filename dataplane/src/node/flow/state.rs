use std::time::Instant as StdInstant;

use tracing::info;

#[derive(Debug, Clone)]
pub struct ConnectionState {
    pub start_time: StdInstant,
    pub time_last_updated: StdInstant,
    pub bytes_last_updated: u64,
    pub bytes_total: u64,
}

// Measures and reports throughput for both the client and the server.
impl ConnectionState {
    pub fn test_throughput(
        &mut self,
        node_role: &str, // the node serves as "client" or "server"
        local_id: usize,
        remote_node_id: usize,
        port: u16,
        bytes_added: u64,
    ) {
        if self.bytes_total == 0 {
            self.start_time = StdInstant::now();
            info!(
                "A user-space TCP {} on port {} has started transferring data to node {}.",
                node_role, port, remote_node_id
            );
        }

        self.bytes_total += bytes_added;
        self.bytes_last_updated += bytes_added;

        let now = StdInstant::now();
        let elapsed_time = now.duration_since(self.time_last_updated).as_secs_f64();

        if elapsed_time > 1.0 {
            let throughput =
                (self.bytes_last_updated as f64 * 8.0) / (elapsed_time * 1_000_000_000.0);

            info!(
                "Throughput at the {} (node {}) on port {} to node {}: {:.3} Gbps ({} bytes in {:.3}s)",
                node_role,
                local_id,
                port,
                remote_node_id,
                throughput,
                self.bytes_last_updated,
                elapsed_time
            );

            self.bytes_last_updated = 0;
            self.time_last_updated = now;
        }
    }
}
