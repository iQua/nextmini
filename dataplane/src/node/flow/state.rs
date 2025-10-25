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
    pub fn update(&mut self, local_id: usize, remote_node_id: usize, bytes_added: u64) {
        if self.bytes_total == 0 {
            self.start_time = StdInstant::now();
            info!(
                "A user-space flow has started transferring data from node {} to node {}.",
                local_id, remote_node_id
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
                "Throughput from node {} to node {}: {:.3} Gbps ({} bytes in {:.3}s)",
                local_id, remote_node_id, throughput, self.bytes_last_updated, elapsed_time
            );

            self.bytes_last_updated = 0;
            self.time_last_updated = now;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn update_initializes_counters_on_first_transfer() {
        let initial_start = StdInstant::now() - Duration::from_secs(10);
        let mut state = ConnectionState {
            start_time: initial_start,
            time_last_updated: StdInstant::now(),
            bytes_last_updated: 0,
            bytes_total: 0,
        };

        state.update(3, 7, 512);

        assert_eq!(state.bytes_total, 512);
        assert_eq!(state.bytes_last_updated, 512);
        assert!(
            state.start_time >= initial_start,
            "first update should refresh the start timestamp when data begins flowing"
        );
    }

    #[test]
    fn update_resets_window_after_reporting() {
        let initial_start = StdInstant::now() - Duration::from_secs(5);
        let initial_last_update = StdInstant::now() - Duration::from_secs(2);
        let mut state = ConnectionState {
            start_time: initial_start,
            time_last_updated: initial_last_update,
            bytes_last_updated: 128,
            bytes_total: 1024,
        };

        state.update(1, 2, 256);

        assert_eq!(state.bytes_total, 1280);
        assert_eq!(
            state.bytes_last_updated, 0,
            "bytes_last_updated should reset after throughput reporting window elapses"
        );
        assert!(
            state.time_last_updated >= initial_last_update,
            "time_last_updated should advance when the reporting window flushes"
        );
        assert_eq!(
            state.start_time, initial_start,
            "start_time should remain unchanged after the first update"
        );
    }
}
