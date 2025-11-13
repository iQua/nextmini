use std::collections::{BTreeMap, BTreeSet};
use std::time::{Duration, Instant};

use ahash::AHashMap;

use super::session::PgmccConfig;

const MIN_LOSS_PROB: f64 = 1e-6;

#[derive(Debug)]
struct PerReceiverStats {
    last_ack_index: u64,
    last_ack_time: Option<Instant>,
    rtt: f64,
    loss_p: f64,
    acked_chunks: u64,
    acked_since_loss_sample: u64,
    last_loss_sample: Instant,
    lost_chunks: BTreeSet<u64>,
}

impl PerReceiverStats {
    fn new() -> Self {
        Self {
            last_ack_index: 0,
            last_ack_time: None,
            rtt: 0.0,
            loss_p: 0.0,
            acked_chunks: 0,
            acked_since_loss_sample: 0,
            last_loss_sample: Instant::now(),
            lost_chunks: BTreeSet::new(),
        }
    }
}

/// Result of a PGMCC recompute: identifies the ACKer and pacing knobs.
pub struct PgmccUpdate {
    pub acker: usize,
    pub window_chunks: usize,
    pub rate_bytes_per_s: f64,
}

/// Tracks per-receiver RTT/loss estimates and derives a congestion window based
/// on the slowest (ACKer) receiver in the session.
pub struct PgmccController {
    cfg: PgmccConfig,
    per_receiver: AHashMap<usize, PerReceiverStats>,
    acker: Option<usize>,
    cwnd_chunks: f64,
    last_update: Instant,
}

impl PgmccController {
    pub fn new(cfg: PgmccConfig, receivers: Vec<usize>) -> Self {
        let mut per_receiver = AHashMap::with_capacity(receivers.len());
        for id in receivers {
            per_receiver.insert(id, PerReceiverStats::new());
        }
        let now = Instant::now();
        let interval = Duration::from_millis(cfg.feedback_interval_ms.max(1));
        let last_update = now.checked_sub(interval).unwrap_or(now);
        Self {
            cfg,
            per_receiver,
            acker: None,
            cwnd_chunks: 0.0,
            last_update,
        }
    }

    fn stats_mut(&mut self, node: usize) -> &mut PerReceiverStats {
        self.per_receiver
            .entry(node)
            .or_insert_with(PerReceiverStats::new)
    }

    pub fn set_cwnd(&mut self, cwnd: f64) {
        self.cwnd_chunks = cwnd.max(self.cfg.min_cwnd_chunks as f64);
    }

    pub fn on_ack(
        &mut self,
        from_node: usize,
        ack_up_to: u64,
        send_times: &BTreeMap<u64, Instant>,
        now: Instant,
    ) {
        let min_rtt_ms = self.cfg.min_rtt_ms;
        let rtt_alpha = self.cfg.rtt_alpha;
        let feedback_interval = Duration::from_millis(self.cfg.feedback_interval_ms);
        let loss_alpha = self.cfg.loss_alpha;
        let stats = self.stats_mut(from_node);
        if ack_up_to <= stats.last_ack_index {
            return;
        }
        let delta = ack_up_to - stats.last_ack_index;
        stats.last_ack_index = ack_up_to;
        stats.acked_chunks = stats.acked_chunks.saturating_add(delta);
        stats.acked_since_loss_sample = stats.acked_since_loss_sample.saturating_add(delta);
        stats.last_ack_time = Some(now);

        if let Some(sent) = send_times.get(&ack_up_to) {
            let mut sample = now.saturating_duration_since(*sent).as_secs_f64();
            let min_rtt = (min_rtt_ms as f64) / 1000.0;
            if sample <= 0.0 {
                sample = min_rtt;
            } else {
                sample = sample.max(min_rtt);
            }
            stats.rtt = if stats.rtt == 0.0 {
                sample
            } else {
                (1.0 - rtt_alpha) * stats.rtt + rtt_alpha * sample
            };
        }

        // Drop tracked loss indices that are now acknowledged.
        let ack_cut = ack_up_to;
        stats.lost_chunks.retain(|idx| *idx > ack_cut);

        // Periodically decay the loss estimate if no new losses arrive.
        if stats.acked_since_loss_sample > 0
            && now.duration_since(stats.last_loss_sample) >= feedback_interval
        {
            Self::apply_loss_sample(loss_alpha, stats, 0, now);
        }
    }

    pub fn on_sack(&mut self, from_node: usize, base: u64, runs: &[(u16, u16)]) {
        let loss_alpha = self.cfg.loss_alpha;
        let stats = self.stats_mut(from_node);
        let mut new_losses = 0u64;
        for (delta, len) in runs {
            let start = base + (*delta as u64);
            let end = start + (*len as u64);
            for idx in start..end {
                if stats.lost_chunks.insert(idx) {
                    new_losses += 1;
                }
            }
        }
        if new_losses > 0 {
            Self::apply_loss_sample(loss_alpha, stats, new_losses, Instant::now());
        }
    }

    pub fn on_repair(&mut self, from_node: usize, indices: &[u64]) {
        let loss_alpha = self.cfg.loss_alpha;
        let stats = self.stats_mut(from_node);
        let mut new_losses = 0u64;
        for idx in indices {
            if stats.lost_chunks.insert(*idx) {
                new_losses += 1;
            }
        }
        if new_losses > 0 {
            Self::apply_loss_sample(loss_alpha, stats, new_losses, Instant::now());
        }
    }

    pub fn maybe_recompute(
        &mut self,
        now: Instant,
        base_window: usize,
        chunk_size: usize,
    ) -> Option<PgmccUpdate> {
        let interval = Duration::from_millis(self.cfg.feedback_interval_ms);
        if now.duration_since(self.last_update) < interval {
            return None;
        }
        self.last_update = now;

        let mut best: Option<(usize, f64, f64, f64)> = None; // (receiver, cwnd, rate, rtt)
        let mut current: Option<(usize, f64, f64, f64)> = None;

        for (id, stats) in self.per_receiver.iter() {
            if stats.rtt <= 0.0 {
                continue;
            }
            let raw_cwnd = tcp_friendly_cwnd(stats.rtt, stats.loss_p, chunk_size);
            if !raw_cwnd.is_finite() {
                continue;
            }
            let clamped = raw_cwnd
                .max(self.cfg.min_cwnd_chunks as f64)
                .min(self.cfg.max_cwnd_chunks as f64);
            let rate = if stats.rtt > 0.0 {
                (clamped * chunk_size as f64) / stats.rtt
            } else {
                0.0
            };
            let candidate = (*id, clamped, rate, stats.rtt);
            if Some(*id) == self.acker {
                current = Some(candidate);
            }
            match &best {
                Some((_, best_cwnd, _, _)) if clamped >= *best_cwnd => {}
                _ => best = Some(candidate),
            }
        }

        let (cand_id, cand_cwnd, cand_rate, cand_rtt) = best?;
        let (winner_id, winner_cwnd, winner_rate, winner_rtt) = match (self.acker, current) {
            (Some(cur_id), Some(current_metrics)) if cur_id != cand_id => {
                let (cur_id, cur_cwnd, cur_rate, cur_rtt) = current_metrics;
                let hysteresis = 1.0 - self.cfg.acker_hysteresis_pct.clamp(0.0, 1.0);
                if cand_cwnd < cur_cwnd * hysteresis {
                    (cand_id, cand_cwnd, cand_rate, cand_rtt)
                } else {
                    (cur_id, cur_cwnd, cur_rate, cur_rtt)
                }
            }
            _ => (cand_id, cand_cwnd, cand_rate, cand_rtt),
        };

        self.acker = Some(winner_id);
        let mut window = winner_cwnd.min(base_window as f64);
        if !window.is_finite() || window <= 0.0 {
            window = base_window as f64;
        }
        self.cwnd_chunks = window;
        let rate = if winner_rtt > 0.0 {
            (window * chunk_size as f64) / winner_rtt
        } else {
            winner_rate
        };
        Some(PgmccUpdate {
            acker: winner_id,
            window_chunks: window.max(1.0).round() as usize,
            rate_bytes_per_s: rate.max(0.0),
        })
    }

    fn apply_loss_sample(loss_alpha: f64, stats: &mut PerReceiverStats, losses: u64, now: Instant) {
        if losses == 0 && stats.acked_since_loss_sample == 0 {
            return;
        }
        let total = (stats.acked_since_loss_sample + losses).max(1);
        let sample = (losses as f64) / (total as f64);
        if stats.loss_p == 0.0 {
            stats.loss_p = sample.max(MIN_LOSS_PROB);
        } else {
            stats.loss_p =
                ((1.0 - loss_alpha) * stats.loss_p + loss_alpha * sample).max(MIN_LOSS_PROB);
        }
        stats.acked_since_loss_sample = 0;
        stats.last_loss_sample = now;
    }
}

fn tcp_friendly_cwnd(rtt_s: f64, loss_p: f64, chunk_size: usize) -> f64 {
    if rtt_s <= 0.0 {
        return f64::INFINITY;
    }
    let p = loss_p.max(MIN_LOSS_PROB);
    if !p.is_finite() {
        return 1.0;
    }
    let s = chunk_size.max(1) as f64;
    let b = 1.0;
    let t_rto = 4.0 * rtt_s;

    // Padhye/TFRC throughput approximation.
    let term1 = (2.0 * b * p / 3.0).sqrt();
    let term2 = t_rto * (3.0 * (b * p / 8.0).sqrt() * p * (1.0 + 32.0 * p * p));
    let denom = rtt_s * (term1 + term2);
    if denom <= 0.0 {
        return f64::INFINITY;
    }
    let throughput = s / denom;
    (throughput * rtt_s / s).max(0.0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    fn test_config() -> PgmccConfig {
        PgmccConfig {
            min_cwnd_chunks: 4,
            max_cwnd_chunks: 64,
            init_cwnd_chunks: 16,
            rtt_alpha: 0.5,
            loss_alpha: 0.5,
            min_rtt_ms: 5,
            feedback_interval_ms: 10,
            acker_hysteresis_pct: 0.1,
        }
    }

    #[test]
    fn pgmcc_reduces_window_when_loss_increases() {
        let cfg = test_config();
        let mut controller = PgmccController::new(cfg.clone(), vec![1]);
        controller.set_cwnd(cfg.init_cwnd_chunks as f64);
        let mut send_times = BTreeMap::new();
        send_times.insert(1, Instant::now() - Duration::from_millis(20));
        controller.on_ack(1, 1, &send_times, Instant::now());
        controller.last_update = Instant::now() - Duration::from_millis(cfg.feedback_interval_ms);
        let update = controller
            .maybe_recompute(Instant::now(), 128, 1024)
            .expect("initial update");
        assert_eq!(update.window_chunks, cfg.max_cwnd_chunks);

        controller.on_sack(1, 1, &[(0, 1)]);
        controller.last_update = Instant::now() - Duration::from_millis(cfg.feedback_interval_ms);
        let after_loss = controller
            .maybe_recompute(Instant::now(), 128, 1024)
            .expect("update after loss");
        assert!(after_loss.window_chunks < update.window_chunks);
    }

    #[test]
    fn pgmcc_picks_slowest_receiver_as_acker() {
        let cfg = test_config();
        let mut controller = PgmccController::new(cfg.clone(), vec![1, 2]);
        controller.set_cwnd(cfg.init_cwnd_chunks as f64);
        let base_time = Instant::now();
        let mut send_times = BTreeMap::new();
        send_times.insert(1, base_time - Duration::from_millis(10));
        send_times.insert(2, base_time - Duration::from_millis(50));
        controller.on_ack(1, 1, &send_times, Instant::now());
        controller.on_sack(1, 1, &[]);
        controller.on_ack(2, 1, &send_times, Instant::now());
        controller.on_sack(2, 1, &[(0, 1)]);
        controller.last_update = Instant::now() - Duration::from_millis(cfg.feedback_interval_ms);
        let update = controller
            .maybe_recompute(Instant::now(), 128, 1024)
            .expect("update");
        assert_eq!(update.acker, 2);
    }
}
