use std::collections::VecDeque;
use std::time::{Duration, Instant};

use ahash::AHashMap;

use nextmini_messages::rlm::{RlmControl, TfmccDataHeader};

use super::session::TfmccConfig;

const MAX_LOSS_SAMPLES: usize = 32;
const MIN_RTT_S: f64 = 0.001;

#[derive(Debug)]
struct LossHistory {
    intervals: VecDeque<f64>,
    have_loss: bool,
}

impl LossHistory {
    fn new() -> Self {
        Self {
            intervals: VecDeque::with_capacity(MAX_LOSS_SAMPLES),
            have_loss: false,
        }
    }

    fn record(&mut self, interval: f64) {
        if !interval.is_finite() || interval <= 0.0 {
            return;
        }
        if self.intervals.len() >= MAX_LOSS_SAMPLES {
            self.intervals.pop_front();
        }
        self.intervals.push_back(interval);
        self.have_loss = true;
    }

    fn loss_event_rate(&self) -> Option<f64> {
        if self.intervals.is_empty() {
            return None;
        }
        let sum: f64 = self.intervals.iter().copied().sum();
        if sum <= 0.0 {
            return None;
        }
        Some(1.0 / (sum / self.intervals.len() as f64))
    }

    fn have_loss(&self) -> bool {
        self.have_loss
    }
}

#[derive(Debug)]
pub struct TfmccReceiver {
    cfg: TfmccConfig,
    receiver_id: u32,
    packet_bits: f64,
    session_start: Instant,
    feedback_interval: Duration,
    expected_seqno: u64,
    packets_since_loss: f64,
    loss_history: LossHistory,
    rtt_s: f64,
    have_rtt: bool,
    x_r_bps: f64,
    last_header: Option<TfmccDataHeader>,
    last_ts_i_ms: u32,
    last_feedback_sent: Option<Instant>,
    last_sent_round: u8,
    pending_rtt: Option<(u32, Instant)>,
}

impl TfmccReceiver {
    pub fn new(receiver_id: u32, cfg: TfmccConfig, chunk_size: usize) -> Self {
        let packet_bits = (chunk_size.max(1) as f64) * 8.0;
        let feedback_interval = Duration::from_millis(cfg.feedback_interval_ms.max(10));
        Self {
            cfg,
            receiver_id,
            packet_bits,
            session_start: Instant::now(),
            feedback_interval,
            expected_seqno: 0,
            packets_since_loss: 0.0,
            loss_history: LossHistory::new(),
            rtt_s: 0.0,
            have_rtt: false,
            x_r_bps: 0.0,
            last_header: None,
            last_ts_i_ms: 0,
            last_feedback_sent: None,
            last_sent_round: 0,
            pending_rtt: None,
        }
    }

    pub fn on_data_header(&mut self, header: &TfmccDataHeader, now: Instant) {
        self.last_header = Some(*header);
        self.last_ts_i_ms = header.ts_i_ms;
        if let Some((stamp, sent)) = self.pending_rtt
            && header.receiver_id == self.receiver_id && stamp == header.tr_r_echo_ms {
                let sample = now.saturating_duration_since(sent).as_secs_f64();
                self.update_rtt(sample);
                self.pending_rtt = None;
            }
    }

    pub fn on_chunk(&mut self, seqno: u64) {
        if self.expected_seqno == 0 {
            self.expected_seqno = seqno.saturating_add(1);
            self.packets_since_loss = 1.0;
            return;
        }
        if seqno >= self.expected_seqno {
            if seqno > self.expected_seqno {
                self.loss_history
                    .record(self.packets_since_loss.max(1.0));
                self.packets_since_loss = 0.0;
            }
            self.expected_seqno = seqno.saturating_add(1);
        }
        self.packets_since_loss += 1.0;
        self.recompute_rate();
    }

    pub fn maybe_feedback(&mut self, now: Instant) -> Option<RlmControl> {
        let header = self.last_header?;
        let due_time = self
            .last_feedback_sent
            .map(|t| now.duration_since(t) >= self.feedback_interval)
            .unwrap_or(true);
        let slower = self.x_r_bps > 0.0
            && self.x_r_bps + 1.0 < header.x_supp_bits_per_s as f64;
        if !due_time && !slower {
            return None;
        }
        self.last_feedback_sent = Some(now);
        self.last_sent_round = header.fb_nr;
        let tr_r_ms = self.elapsed_ms(now);
        if self.pending_rtt.is_none() {
            self.pending_rtt = Some((tr_r_ms, now));
        }
        let bits_per_s = self
            .x_r_bps
            .clamp(self.cfg.min_rate_bps, self.cfg.max_rate_bps)
            .max(1.0);
        Some(RlmControl::TfmccFeedback {
            receiver_id: self.receiver_id,
            have_rtt: self.have_rtt,
            have_loss: self.loss_history.have_loss(),
            receiver_leave: false,
            tr_r_ms,
            ts_i_echo_ms: self.last_ts_i_ms,
            fb_nr_echo: header.fb_nr,
            x_r_bits_per_s: bits_per_s as u32,
        })
    }

    fn elapsed_ms(&self, now: Instant) -> u32 {
        now.saturating_duration_since(self.session_start)
            .as_millis()
            .min(u32::MAX as u128) as u32
    }

    fn update_rtt(&mut self, sample: f64) {
        if !sample.is_finite() || sample <= 0.0 {
            return;
        }
        self.rtt_s = if !self.have_rtt {
            sample.max(MIN_RTT_S)
        } else {
            (1.0 - self.cfg.rate_smooth_alpha) * self.rtt_s
                + self.cfg.rate_smooth_alpha * sample.max(MIN_RTT_S)
        };
        self.have_rtt = true;
        self.recompute_rate();
    }

    fn recompute_rate(&mut self) {
        if !self.have_rtt {
            self.x_r_bps = self.cfg.initial_rate_bps;
            return;
        }
        let rtt = self.rtt_s.max(MIN_RTT_S);
        let p = self
            .loss_history
            .loss_event_rate()
            .unwrap_or_else(|| if self.loss_history.have_loss() { 1e-6 } else { 0.0 });
        let rate = if p <= 0.0 {
            self.cfg.max_rate_bps
        } else {
            let sqrt_term = (2.0 * p / 3.0).sqrt();
            let denom = sqrt_term
                + 12.0 * (3.0 * p / 8.0).sqrt() * p * (1.0 + 32.0 * p * p);
            if denom <= 0.0 {
                self.cfg.max_rate_bps
            } else {
                (self.packet_bits * 1.0) / (rtt * denom)
            }
        };
        self.x_r_bps = rate
            .clamp(self.cfg.min_rate_bps, self.cfg.max_rate_bps)
            .max(self.cfg.min_rate_bps);
    }
}

#[derive(Debug)]
struct ReceiverInfo {
    x_r_bps: f64,
    rtt_s: f64,
    last_feedback_at: Instant,
}

impl ReceiverInfo {
    fn new(now: Instant) -> Self {
        Self {
            x_r_bps: 0.0,
            rtt_s: 0.0,
            last_feedback_at: now,
        }
    }
}

#[derive(Debug)]
pub struct TfmccSender {
    cfg: TfmccConfig,
    packet_bits: f64,
    session_start: Instant,
    feedback_interval: Duration,
    fb_nr: u8,
    last_round_start: Instant,
    current_rate_bps: f64,
    r_max_s: f64,
    clr_id: Option<u32>,
    receivers: AHashMap<u32, ReceiverInfo>,
    last_echo: Option<(u32, u32)>,
}

impl TfmccSender {
    pub fn new(cfg: TfmccConfig, chunk_size: usize, session_start: Instant) -> Self {
        let packet_bits = (chunk_size.max(1) as f64) * 8.0;
        let rate = cfg
            .initial_rate_bps
            .clamp(cfg.min_rate_bps, cfg.max_rate_bps)
            .max(cfg.min_rate_bps);
        let interval = Duration::from_millis(cfg.feedback_interval_ms.max(10));
        Self {
            cfg,
            packet_bits,
            session_start,
            feedback_interval: interval,
            fb_nr: 0,
            last_round_start: session_start,
            current_rate_bps: rate,
            r_max_s: 0.0,
            clr_id: None,
            receivers: AHashMap::default(),
            last_echo: None,
        }
    }

    pub fn on_feedback(&mut self, feedback: &RlmControl, now: Instant) {
        if let RlmControl::TfmccFeedback {
            receiver_id,
            have_rtt,
            receiver_leave,
            tr_r_ms,
            ts_i_echo_ms,
            x_r_bits_per_s,
            ..
        } = feedback
        {
            self.last_echo = Some((*receiver_id, *tr_r_ms));
            let rtt_sample = if *have_rtt {
                self.sample_rtt(*ts_i_echo_ms, now)
            } else {
                None
            };
            let mut candidate = (*x_r_bits_per_s).max(1) as f64;
            {
                let info = self
                    .receivers
                    .entry(*receiver_id)
                    .or_insert_with(|| ReceiverInfo::new(now));
                info.x_r_bps = candidate;
                info.last_feedback_at = now;
                if let Some(sample) = rtt_sample {
                    info.rtt_s = if info.rtt_s == 0.0 {
                        sample
                    } else {
                        (1.0 - self.cfg.rate_smooth_alpha) * info.rtt_s
                            + self.cfg.rate_smooth_alpha * sample
                    };
                    self.r_max_s = self.r_max_s.max(info.rtt_s.max(MIN_RTT_S));
                }
                candidate = info.x_r_bps;
            }
            if *receiver_leave && self.clr_id == Some(*receiver_id) {
                self.clr_id = None;
                self.current_rate_bps =
                    (self.current_rate_bps * 0.5).max(self.cfg.min_rate_bps);
            }
            let candidate = self.clamp_rate(candidate);
            self.apply_candidate_rate(*receiver_id, candidate);
        }
    }

    pub fn on_tick(&mut self, now: Instant) {
        if now.duration_since(self.last_round_start) >= self.feedback_interval {
            self.fb_nr = self.fb_nr.wrapping_add(1);
            self.last_round_start = now;
        }
        if let Some(clr) = self.clr_id
            && let Some(info) = self.receivers.get(&clr)
                && now.duration_since(info.last_feedback_at) >= self.feedback_interval * 3 {
                    self.current_rate_bps =
                        (self.current_rate_bps * 0.5).max(self.cfg.min_rate_bps);
                    self.clr_id = None;
                }
    }

    pub fn current_rate_bytes_per_s(&self) -> f64 {
        (self.current_rate_bps / 8.0).max(0.0)
    }

    pub fn build_data_header(&mut self, now: Instant) -> TfmccDataHeader {
        self.on_tick(now);
        self.current_rate_bps = self.clamp_rate(self.current_rate_bps);
        let ts_i_ms = self.elapsed_ms(now);
        let (receiver_id, tr_r_echo_ms) = self
            .last_echo
            .or_else(|| self.clr_id.map(|id| (id, 0)))
            .unwrap_or((0, 0));
        let is_clr = self.clr_id == Some(receiver_id) && receiver_id != 0;
        let r_max_ms = (self.r_max_s * 1000.0)
            .round()
            .clamp(0.0, u16::MAX as f64) as u16;
        TfmccDataHeader {
            x_supp_bits_per_s: self.current_rate_bps.max(1.0) as u32,
            ts_i_ms,
            receiver_id,
            tr_r_echo_ms,
            fb_nr: self.fb_nr,
            is_clr,
            r_max_ms,
        }
    }

    fn apply_candidate_rate(&mut self, receiver_id: u32, candidate: f64) {
        if let Some(clr) = self.clr_id {
            if clr == receiver_id {
                if candidate < self.current_rate_bps {
                    self.current_rate_bps = candidate;
                } else {
                    let inc = self.additive_increase();
                    self.current_rate_bps = (self.current_rate_bps + inc).min(candidate);
                }
                self.current_rate_bps = self.clamp_rate(self.current_rate_bps);
                return;
            }
            let hysteresis = self.current_rate_bps
                * (1.0 - self.cfg.clr_hysteresis_pct.clamp(0.0, 1.0));
            if candidate < hysteresis {
                self.clr_id = Some(receiver_id);
                self.current_rate_bps = candidate;
            }
        } else {
            self.clr_id = Some(receiver_id);
            self.current_rate_bps = candidate;
        }
        self.current_rate_bps = self.clamp_rate(self.current_rate_bps);
    }

    fn clamp_rate(&self, rate: f64) -> f64 {
        rate.clamp(self.cfg.min_rate_bps, self.cfg.max_rate_bps)
    }

    fn additive_increase(&self) -> f64 {
        if self.r_max_s <= 0.0 {
            return self.packet_bits;
        }
        (self.packet_bits * self.cfg.max_increase_per_rtt_pkts).max(0.0) / self.r_max_s
    }

    fn sample_rtt(&self, ts_i_echo_ms: u32, now: Instant) -> Option<f64> {
        let send_time = self
            .session_start
            .checked_add(Duration::from_millis(ts_i_echo_ms as u64))?;
        Some(now.saturating_duration_since(send_time).as_secs_f64())
    }

    fn elapsed_ms(&self, now: Instant) -> u32 {
        now.saturating_duration_since(self.session_start)
            .as_millis()
            .min(u32::MAX as u128) as u32
    }
}
