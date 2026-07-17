//! General-purpose, dynamically written TCP byte-stream state machines.
//!
//! The original [`super::tcp_source::TCPPacketSource`] models a finite, known flow pulled from an
//! application actor. This module supplies the socket-facing mechanics needed by relays and other
//! applications that produce bytes over time: finite send/receive buffers, advertised receive
//! windows, read credit, zero-window persist, and TCP_NODELAY-equivalent packetization. It is kept
//! independent of nexosim actors so callers can embed one state machine in any model.

use std::collections::BTreeMap;

use thiserror::Error;

use crate::flows::cc::{AckEvent, CongestionControl, CongestionEvent};
use crate::flows::packet::{EcnField, Packet, TCPAck};
use crate::flows::reno::TCPReno;

/// Bytes charged to every modeled TCP data segment and pure ACK for IPv4 + TCP headers.
pub const TCP_IP_HEADER_BYTES: usize = 40;

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct TcpSocketConfig {
    pub mss: usize,
    pub send_buffer_bytes: usize,
    pub receive_buffer_bytes: usize,
    pub nodelay: bool,
    pub initial_rto_seconds: f64,
    pub min_rto_seconds: f64,
    pub max_rto_seconds: f64,
    pub persist_interval_seconds: f64,
}

impl Default for TcpSocketConfig {
    fn default() -> Self {
        Self {
            mss: 512,
            send_buffer_bytes: 64 * 1024,
            receive_buffer_bytes: 64 * 1024,
            nodelay: true,
            initial_rto_seconds: 1.0,
            min_rto_seconds: 1.0,
            max_rto_seconds: 60.0,
            persist_interval_seconds: 1.0,
        }
    }
}

impl TcpSocketConfig {
    pub fn validate(self) -> Result<Self, TcpSocketError> {
        if self.mss == 0 {
            return Err(TcpSocketError::ZeroMss);
        }
        if self.send_buffer_bytes == 0 {
            return Err(TcpSocketError::ZeroSendBuffer);
        }
        if self.receive_buffer_bytes == 0 {
            return Err(TcpSocketError::ZeroReceiveBuffer);
        }
        for (name, value) in [
            ("initial_rto_seconds", self.initial_rto_seconds),
            ("min_rto_seconds", self.min_rto_seconds),
            ("max_rto_seconds", self.max_rto_seconds),
            ("persist_interval_seconds", self.persist_interval_seconds),
        ] {
            if !value.is_finite() || value <= 0.0 {
                return Err(TcpSocketError::InvalidPositiveTime { name, value });
            }
        }
        if self.min_rto_seconds > self.initial_rto_seconds
            || self.initial_rto_seconds > self.max_rto_seconds
        {
            return Err(TcpSocketError::InvalidRtoBounds);
        }
        Ok(self)
    }
}

#[derive(Debug, Error, PartialEq)]
pub enum TcpSocketError {
    #[error("TCP MSS must be nonzero")]
    ZeroMss,
    #[error("TCP send-buffer capacity must be nonzero")]
    ZeroSendBuffer,
    #[error("TCP receive-buffer capacity must be nonzero")]
    ZeroReceiveBuffer,
    #[error("{name} must be finite and positive, got {value}")]
    InvalidPositiveTime { name: &'static str, value: f64 },
    #[error("TCP RTO bounds must satisfy min <= initial <= max")]
    InvalidRtoBounds,
    #[error("sequence-space arithmetic overflow")]
    SequenceOverflow,
    #[error("packet belongs to flow {actual}, expected {expected}")]
    WrongFlow { expected: usize, actual: usize },
    #[error("packet is not a TCP acknowledgment")]
    MissingAcknowledgment,
    #[error("acknowledgment {acknowledged} exceeds highest sent sequence {highest_sent}")]
    AcknowledgmentBeyondSent {
        acknowledged: usize,
        highest_sent: usize,
    },
    #[error(
        "TCP data packet is only {wire_bytes} bytes, below the {TCP_IP_HEADER_BYTES}-byte header"
    )]
    TruncatedTcpPacket { wire_bytes: usize },
    #[error("simulation time must be finite and nonnegative, got {0}")]
    InvalidSimulationTime(f64),
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct TcpSocketMetrics {
    pub application_bytes_admitted: usize,
    pub application_bytes_delivered: usize,
    pub data_segments_sent: usize,
    pub retransmitted_segments: usize,
    pub persist_probes_sent: usize,
    pub wire_bytes_sent: usize,
    pub zero_window_updates: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WriteAdmission {
    pub accepted_bytes: usize,
    pub blocked_bytes: usize,
}

#[derive(Clone, Copy, Debug)]
struct OutstandingSegment {
    len: usize,
    first_sent_at: f64,
    last_sent_at: f64,
    deadline: f64,
}

/// Sender half of a persistent byte-stream socket.
pub struct TcpSocketSender {
    flow_id: usize,
    priority: u8,
    config: TcpSocketConfig,
    congestion_control: Box<dyn CongestionControl + Send + Sync>,
    send_unacknowledged: usize,
    next_sequence: usize,
    application_write_end: usize,
    peer_receive_window: usize,
    outstanding: BTreeMap<usize, OutstandingSegment>,
    duplicate_acks: usize,
    rto_seconds: f64,
    smoothed_rtt: Option<f64>,
    rtt_variance: f64,
    next_persist_at: Option<f64>,
    metrics: TcpSocketMetrics,
}

impl std::fmt::Debug for TcpSocketSender {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TcpSocketSender")
            .field("flow_id", &self.flow_id)
            .field("send_unacknowledged", &self.send_unacknowledged)
            .field("next_sequence", &self.next_sequence)
            .field("application_write_end", &self.application_write_end)
            .field("peer_receive_window", &self.peer_receive_window)
            .field("outstanding", &self.outstanding)
            .finish()
    }
}

impl TcpSocketSender {
    pub fn new(
        flow_id: usize,
        priority: u8,
        config: TcpSocketConfig,
        congestion_control: Box<dyn CongestionControl + Send + Sync>,
    ) -> Result<Self, TcpSocketError> {
        let config = config.validate()?;
        Ok(Self {
            flow_id,
            priority,
            config,
            congestion_control,
            send_unacknowledged: 0,
            next_sequence: 0,
            application_write_end: 0,
            peer_receive_window: config.receive_buffer_bytes,
            outstanding: BTreeMap::new(),
            duplicate_acks: 0,
            rto_seconds: config.initial_rto_seconds,
            smoothed_rtt: None,
            rtt_variance: 0.0,
            next_persist_at: None,
            metrics: TcpSocketMetrics::default(),
        })
    }

    pub fn new_reno(
        flow_id: usize,
        priority: u8,
        config: TcpSocketConfig,
    ) -> Result<Self, TcpSocketError> {
        Self::new(flow_id, priority, config, Box::new(TCPReno::new()))
    }

    pub fn new_window_scaled_reno(
        flow_id: usize,
        priority: u8,
        config: TcpSocketConfig,
    ) -> Result<Self, TcpSocketError> {
        let maximum_window = config.receive_buffer_bytes;
        let mss = config.mss;
        Self::new(
            flow_id,
            priority,
            config,
            Box::new(TCPReno::with_max_cwnd_and_mss(maximum_window, mss)),
        )
    }

    pub fn metrics(&self) -> TcpSocketMetrics {
        self.metrics
    }

    pub fn send_buffer_capacity(&self) -> usize {
        self.config.send_buffer_bytes
    }

    pub fn send_buffered_bytes(&self) -> usize {
        self.application_write_end
            .saturating_sub(self.send_unacknowledged)
    }

    pub fn writable_bytes(&self) -> usize {
        self.config
            .send_buffer_bytes
            .saturating_sub(self.send_buffered_bytes())
    }

    pub fn bytes_in_flight(&self) -> usize {
        self.next_sequence.saturating_sub(self.send_unacknowledged)
    }

    pub fn peer_receive_window(&self) -> usize {
        self.peer_receive_window
    }

    pub fn highest_admitted_sequence(&self) -> usize {
        self.application_write_end
    }

    /// Attempts a nonblocking application write into the finite socket send buffer.
    pub fn admit_application_write(
        &mut self,
        requested_bytes: usize,
    ) -> Result<WriteAdmission, TcpSocketError> {
        let accepted_bytes = requested_bytes.min(self.writable_bytes());
        self.application_write_end = self
            .application_write_end
            .checked_add(accepted_bytes)
            .ok_or(TcpSocketError::SequenceOverflow)?;
        self.metrics.application_bytes_admitted = self
            .metrics
            .application_bytes_admitted
            .checked_add(accepted_bytes)
            .ok_or(TcpSocketError::SequenceOverflow)?;
        Ok(WriteAdmission {
            accepted_bytes,
            blocked_bytes: requested_bytes - accepted_bytes,
        })
    }

    /// Returns all segments immediately eligible under `min(cwnd, rwnd)`.
    pub fn poll_transmit(&mut self, now: f64) -> Result<Vec<Packet>, TcpSocketError> {
        validate_time(now)?;
        let window = self
            .congestion_control
            .get_cwnd()
            .min(self.peer_receive_window);
        let window_end = self
            .send_unacknowledged
            .checked_add(window)
            .ok_or(TcpSocketError::SequenceOverflow)?;
        let send_end = self.application_write_end.min(window_end);
        let mut packets = Vec::new();

        while self.next_sequence < send_end {
            let available = send_end - self.next_sequence;
            let payload_bytes = available.min(self.config.mss);
            if !self.config.nodelay
                && payload_bytes < self.config.mss
                && self.next_sequence > self.send_unacknowledged
            {
                break;
            }

            let packet = self.build_data_packet(self.next_sequence, payload_bytes, now)?;
            self.record_new_segment(self.next_sequence, payload_bytes, now)?;
            self.next_sequence = self
                .next_sequence
                .checked_add(payload_bytes)
                .ok_or(TcpSocketError::SequenceOverflow)?;
            packets.push(packet);
        }

        if self.peer_receive_window == 0 && self.next_sequence < self.application_write_end {
            self.next_persist_at
                .get_or_insert(now + self.config.persist_interval_seconds);
        } else {
            self.next_persist_at = None;
        }
        Ok(packets)
    }

    /// Applies a cumulative ACK/window update and returns newly eligible data segments.
    pub fn receive_ack(
        &mut self,
        ack_packet: &Packet,
        now: f64,
    ) -> Result<Vec<Packet>, TcpSocketError> {
        validate_time(now)?;
        if ack_packet.flow_id != self.flow_id {
            return Err(TcpSocketError::WrongFlow {
                expected: self.flow_id,
                actual: ack_packet.flow_id,
            });
        }
        let ack = ack_packet
            .ack
            .ok_or(TcpSocketError::MissingAcknowledgment)?;
        if ack.sequence_num > self.next_sequence {
            return Err(TcpSocketError::AcknowledgmentBeyondSent {
                acknowledged: ack.sequence_num,
                highest_sent: self.next_sequence,
            });
        }

        let prior_window = self.peer_receive_window;
        self.peer_receive_window = ack.advertised_window;
        if ack.advertised_window == 0 && prior_window != 0 {
            self.metrics.zero_window_updates += 1;
        }

        if ack.sequence_num > self.send_unacknowledged {
            let acknowledged_bytes = ack.sequence_num - self.send_unacknowledged;
            let sample_sent_at = self
                .outstanding
                .iter()
                .filter(|(start, segment)| start.saturating_add(segment.len) <= ack.sequence_num)
                .map(|(_, segment)| segment.first_sent_at)
                .next_back();
            self.trim_acknowledged_segments(ack.sequence_num);
            self.send_unacknowledged = ack.sequence_num;
            self.duplicate_acks = 0;

            let sample_rtt = sample_sent_at.map_or(self.rto_seconds, |sent_at| now - sent_at);
            self.update_rto(sample_rtt);
            self.congestion_control.ack_received(AckEvent::new_basic(
                ack.sequence_num,
                sample_rtt.max(f64::EPSILON),
                now,
                acknowledged_bytes,
            ));
        } else if ack.sequence_num == self.send_unacknowledged {
            self.duplicate_acks = self.duplicate_acks.saturating_add(1);
        }

        if self.peer_receive_window == 0 && self.next_sequence < self.application_write_end {
            self.next_persist_at
                .get_or_insert(now + self.config.persist_interval_seconds);
        } else {
            self.next_persist_at = None;
        }
        self.poll_transmit(now)
    }

    /// Processes retransmission and zero-window-persist deadlines.
    pub fn timer_tick(&mut self, now: f64) -> Result<Vec<Packet>, TcpSocketError> {
        validate_time(now)?;
        let mut packets = Vec::new();
        let expired = self
            .outstanding
            .iter()
            .find(|(_, segment)| segment.deadline <= now)
            .map(|(&sequence, _)| sequence);
        if let Some(sequence) = expired {
            let bytes_in_flight = self.bytes_in_flight();
            self.congestion_control.timeout_event(CongestionEvent {
                now,
                flight_size_bytes: bytes_in_flight,
            });
            let (payload_bytes, next_deadline) = {
                let segment = self
                    .outstanding
                    .get_mut(&sequence)
                    .ok_or(TcpSocketError::SequenceOverflow)?;
                segment.last_sent_at = now;
                segment.deadline = now + self.rto_seconds;
                (segment.len, segment.deadline)
            };
            let packet = self.build_data_packet(sequence, payload_bytes, now)?;
            self.metrics.retransmitted_segments += 1;
            self.metrics.data_segments_sent += 1;
            self.metrics.wire_bytes_sent = self
                .metrics
                .wire_bytes_sent
                .checked_add(packet.size)
                .ok_or(TcpSocketError::SequenceOverflow)?;
            packets.push(packet);
            self.rto_seconds = (next_deadline - now)
                .mul_add(2.0, 0.0)
                .min(self.config.max_rto_seconds);
        }

        if self.peer_receive_window == 0
            && self.next_sequence < self.application_write_end
            && self.next_persist_at.is_some_and(|deadline| deadline <= now)
        {
            let packet = self.build_data_packet(self.next_sequence, 1, now)?;
            self.metrics.persist_probes_sent += 1;
            self.metrics.data_segments_sent += 1;
            self.metrics.wire_bytes_sent = self
                .metrics
                .wire_bytes_sent
                .checked_add(packet.size)
                .ok_or(TcpSocketError::SequenceOverflow)?;
            packets.push(packet);
            self.next_persist_at = Some(now + self.config.persist_interval_seconds);
        }
        Ok(packets)
    }

    fn build_data_packet(
        &self,
        sequence: usize,
        payload_bytes: usize,
        now: f64,
    ) -> Result<Packet, TcpSocketError> {
        let wire_bytes = payload_bytes
            .checked_add(TCP_IP_HEADER_BYTES)
            .ok_or(TcpSocketError::SequenceOverflow)?;
        let mut packet = Packet::new(wire_bytes, sequence, self.flow_id, now);
        packet.set_priority(self.priority);
        packet.ecn = EcnField::Ect0;
        Ok(packet)
    }

    fn record_new_segment(
        &mut self,
        sequence: usize,
        payload_bytes: usize,
        now: f64,
    ) -> Result<(), TcpSocketError> {
        self.outstanding.insert(
            sequence,
            OutstandingSegment {
                len: payload_bytes,
                first_sent_at: now,
                last_sent_at: now,
                deadline: now + self.rto_seconds,
            },
        );
        self.congestion_control.packet_sent(payload_bytes, now);
        self.metrics.data_segments_sent += 1;
        self.metrics.wire_bytes_sent = self
            .metrics
            .wire_bytes_sent
            .checked_add(
                payload_bytes
                    .checked_add(TCP_IP_HEADER_BYTES)
                    .ok_or(TcpSocketError::SequenceOverflow)?,
            )
            .ok_or(TcpSocketError::SequenceOverflow)?;
        Ok(())
    }

    fn trim_acknowledged_segments(&mut self, acknowledged: usize) {
        let mut retained = BTreeMap::new();
        for (&start, segment) in &self.outstanding {
            let end = start.saturating_add(segment.len);
            if end <= acknowledged {
                continue;
            }
            if start < acknowledged {
                retained.insert(
                    acknowledged,
                    OutstandingSegment {
                        len: end - acknowledged,
                        ..*segment
                    },
                );
            } else {
                retained.insert(start, *segment);
            }
        }
        self.outstanding = retained;
    }

    fn update_rto(&mut self, sample_rtt: f64) {
        if !sample_rtt.is_finite() || sample_rtt <= 0.0 {
            return;
        }
        match self.smoothed_rtt {
            None => {
                self.smoothed_rtt = Some(sample_rtt);
                self.rtt_variance = sample_rtt / 2.0;
            }
            Some(smoothed) => {
                self.rtt_variance = 0.75 * self.rtt_variance + 0.25 * (smoothed - sample_rtt).abs();
                self.smoothed_rtt = Some(0.875 * smoothed + 0.125 * sample_rtt);
            }
        }
        let smoothed = self.smoothed_rtt.unwrap_or(sample_rtt);
        self.rto_seconds = (smoothed + 4.0 * self.rtt_variance)
            .clamp(self.config.min_rto_seconds, self.config.max_rto_seconds);
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DeliveredBytes {
    pub stream_offset: usize,
    pub byte_count: usize,
}

#[derive(Clone, Debug)]
pub struct ReceiveOutcome {
    pub acknowledgment: Packet,
    pub delivered: Option<DeliveredBytes>,
}

/// Receiver half of a persistent byte-stream socket.
#[derive(Debug)]
pub struct TcpSocketReceiver {
    flow_id: usize,
    priority: u8,
    receive_buffer_bytes: usize,
    next_sequence_expected: usize,
    application_read_sequence: usize,
    application_read_credit: usize,
    received_ranges: BTreeMap<usize, usize>,
    last_creation_time: f64,
    metrics: TcpSocketMetrics,
}

impl TcpSocketReceiver {
    pub fn new(
        flow_id: usize,
        priority: u8,
        config: TcpSocketConfig,
    ) -> Result<Self, TcpSocketError> {
        let config = config.validate()?;
        Ok(Self {
            flow_id,
            priority,
            receive_buffer_bytes: config.receive_buffer_bytes,
            next_sequence_expected: 0,
            application_read_sequence: 0,
            application_read_credit: 0,
            received_ranges: BTreeMap::new(),
            last_creation_time: 0.0,
            metrics: TcpSocketMetrics::default(),
        })
    }

    pub fn metrics(&self) -> TcpSocketMetrics {
        self.metrics
    }

    pub fn receive_buffered_bytes(&self) -> usize {
        self.received_ranges
            .iter()
            .map(|(&start, &end)| end.saturating_sub(start.max(self.application_read_sequence)))
            .sum()
    }

    pub fn advertised_window(&self) -> usize {
        self.receive_buffer_bytes
            .saturating_sub(self.receive_buffered_bytes())
    }

    pub fn next_sequence_expected(&self) -> usize {
        self.next_sequence_expected
    }

    pub fn application_read_sequence(&self) -> usize {
        self.application_read_sequence
    }

    pub fn receive_segment(
        &mut self,
        packet: &Packet,
        now: f64,
    ) -> Result<ReceiveOutcome, TcpSocketError> {
        validate_time(now)?;
        if packet.flow_id != self.flow_id {
            return Err(TcpSocketError::WrongFlow {
                expected: self.flow_id,
                actual: packet.flow_id,
            });
        }
        if packet.ack.is_some() || packet.size < TCP_IP_HEADER_BYTES {
            return Err(TcpSocketError::TruncatedTcpPacket {
                wire_bytes: packet.size,
            });
        }
        self.last_creation_time = packet.creation_time;
        let payload_bytes = packet.size - TCP_IP_HEADER_BYTES;
        let segment_end = packet
            .packet_id
            .checked_add(payload_bytes)
            .ok_or(TcpSocketError::SequenceOverflow)?;
        let window_end = self
            .application_read_sequence
            .checked_add(self.receive_buffer_bytes)
            .ok_or(TcpSocketError::SequenceOverflow)?;
        let accepted_start = packet.packet_id.max(self.application_read_sequence);
        let accepted_end = segment_end.min(window_end);
        if accepted_start < accepted_end {
            self.insert_range(accepted_start, accepted_end);
        }

        let prior_ack = self.next_sequence_expected;
        self.advance_cumulative_ack();
        let delivered = self.deliver_readable();
        let newly_acknowledged = self.next_sequence_expected - prior_ack;
        Ok(ReceiveOutcome {
            acknowledgment: self.build_acknowledgment(
                packet.packet_id,
                newly_acknowledged,
                packet.creation_time,
                now,
            ),
            delivered,
        })
    }

    /// Makes application read capacity available and emits a TCP window update.
    pub fn grant_read_credit(
        &mut self,
        byte_count: usize,
        now: f64,
    ) -> Result<ReceiveOutcome, TcpSocketError> {
        validate_time(now)?;
        self.application_read_credit = self.application_read_credit.saturating_add(byte_count);
        let delivered = self.deliver_readable();
        Ok(ReceiveOutcome {
            acknowledgment: self.build_acknowledgment(
                self.next_sequence_expected,
                0,
                self.last_creation_time,
                now,
            ),
            delivered,
        })
    }

    fn insert_range(&mut self, start: usize, end: usize) {
        let mut merged_start = start;
        let mut merged_end = end;
        let overlapping: Vec<(usize, usize)> = self
            .received_ranges
            .range(..=end)
            .filter_map(|(&range_start, &range_end)| {
                (range_end >= start).then_some((range_start, range_end))
            })
            .collect();
        for (range_start, range_end) in overlapping {
            self.received_ranges.remove(&range_start);
            merged_start = merged_start.min(range_start);
            merged_end = merged_end.max(range_end);
        }
        self.received_ranges.insert(merged_start, merged_end);
    }

    fn advance_cumulative_ack(&mut self) {
        for (&start, &end) in &self.received_ranges {
            if start > self.next_sequence_expected {
                break;
            }
            self.next_sequence_expected = self.next_sequence_expected.max(end);
        }
    }

    fn deliver_readable(&mut self) -> Option<DeliveredBytes> {
        let available = self
            .next_sequence_expected
            .saturating_sub(self.application_read_sequence);
        let byte_count = available.min(self.application_read_credit);
        if byte_count == 0 {
            return None;
        }
        let stream_offset = self.application_read_sequence;
        self.application_read_sequence += byte_count;
        self.application_read_credit -= byte_count;
        self.metrics.application_bytes_delivered += byte_count;
        self.discard_delivered_ranges();
        Some(DeliveredBytes {
            stream_offset,
            byte_count,
        })
    }

    fn discard_delivered_ranges(&mut self) {
        let delivered_through = self.application_read_sequence;
        let mut retained = BTreeMap::new();
        for (&start, &end) in &self.received_ranges {
            if end <= delivered_through {
                continue;
            }
            retained.insert(start.max(delivered_through), end);
        }
        self.received_ranges = retained;
    }

    fn build_acknowledgment(
        &self,
        packet_id: usize,
        acknowledged_size: usize,
        creation_time: f64,
        now: f64,
    ) -> Packet {
        let mut acknowledgment = Packet::new(TCP_IP_HEADER_BYTES, packet_id, self.flow_id, now);
        acknowledgment.creation_time = creation_time;
        acknowledgment.set_priority(self.priority);
        acknowledgment.ack = Some(TCPAck {
            sequence_num: self.next_sequence_expected,
            acknowledged_size,
            advertised_window: self.advertised_window(),
            ece: false,
        });
        acknowledgment
    }
}

fn validate_time(now: f64) -> Result<(), TcpSocketError> {
    if !now.is_finite() || now < 0.0 {
        return Err(TcpSocketError::InvalidSimulationTime(now));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config() -> TcpSocketConfig {
        TcpSocketConfig {
            mss: 512,
            send_buffer_bytes: 2_048,
            receive_buffer_bytes: 1_024,
            nodelay: true,
            initial_rto_seconds: 1.0,
            min_rto_seconds: 0.1,
            max_rto_seconds: 60.0,
            persist_interval_seconds: 0.25,
        }
    }

    #[test]
    fn scaled_socket_constructor_selects_window_scaled_reno() {
        let scaled = TcpSocketSender::new_window_scaled_reno(3, 0, config())
            .expect("window-scaled Reno socket");
        let reno = scaled
            .congestion_control
            .as_any()
            .downcast_ref::<TCPReno>()
            .expect("Reno controller");
        assert_eq!(reno.get_cwnd(), 2 * config().mss);
    }

    #[test]
    fn finite_send_buffer_admits_only_available_bytes() {
        let mut sender = TcpSocketSender::new_reno(1, 0, config()).expect("valid socket");
        let admission = sender
            .admit_application_write(3_000)
            .expect("sequence space");
        assert_eq!(admission.accepted_bytes, 2_048);
        assert_eq!(admission.blocked_bytes, 952);
        assert_eq!(sender.writable_bytes(), 0);
    }

    #[test]
    fn sender_gates_data_by_minimum_of_cwnd_and_receive_window() {
        let mut sender = TcpSocketSender::new_reno(1, 0, config()).expect("valid socket");
        sender
            .admit_application_write(2_048)
            .expect("sequence space");
        sender.peer_receive_window = 600;

        let packets = sender.poll_transmit(0.0).expect("transmit");
        assert_eq!(packets.len(), 2);
        assert_eq!(packets[0].size, 512 + TCP_IP_HEADER_BYTES);
        assert_eq!(packets[1].size, 88 + TCP_IP_HEADER_BYTES);
        assert_eq!(sender.bytes_in_flight(), 600);
    }

    #[test]
    fn receive_credit_bounds_delivery_and_reopens_window() {
        let mut receiver = TcpSocketReceiver::new(1, 0, config()).expect("valid socket");
        let packet = Packet::new(512 + TCP_IP_HEADER_BYTES, 0, 1, 0.0);

        let received = receiver
            .receive_segment(&packet, 0.1)
            .expect("valid segment");
        assert_eq!(received.delivered, None);
        assert_eq!(receiver.advertised_window(), 512);

        let update = receiver.grant_read_credit(256, 0.2).expect("valid time");
        assert_eq!(
            update.delivered,
            Some(DeliveredBytes {
                stream_offset: 0,
                byte_count: 256,
            })
        );
        assert_eq!(receiver.advertised_window(), 768);
        assert_eq!(
            update
                .acknowledgment
                .ack
                .expect("window update")
                .advertised_window,
            768
        );
    }

    #[test]
    fn out_of_order_segment_never_advances_ack_across_gap() {
        let mut receiver = TcpSocketReceiver::new(1, 0, config()).expect("valid socket");
        let later = Packet::new(512 + TCP_IP_HEADER_BYTES, 512, 1, 0.0);
        let first = Packet::new(512 + TCP_IP_HEADER_BYTES, 0, 1, 0.0);

        let gap_ack = receiver
            .receive_segment(&later, 0.1)
            .expect("later segment")
            .acknowledgment;
        assert_eq!(gap_ack.ack.expect("ack").sequence_num, 0);

        let joined_ack = receiver
            .receive_segment(&first, 0.2)
            .expect("first segment")
            .acknowledgment;
        assert_eq!(joined_ack.ack.expect("ack").sequence_num, 1_024);
    }

    #[test]
    fn zero_window_persist_sends_one_byte_probe() {
        let mut sender = TcpSocketSender::new_reno(1, 0, config()).expect("valid socket");
        sender.admit_application_write(512).expect("sequence space");
        sender.peer_receive_window = 0;
        assert!(sender.poll_transmit(0.0).expect("transmit").is_empty());
        assert!(sender.timer_tick(0.24).expect("timer").is_empty());

        let probe = sender.timer_tick(0.25).expect("timer");
        assert_eq!(probe.len(), 1);
        assert_eq!(probe[0].packet_id, 0);
        assert_eq!(probe[0].size, TCP_IP_HEADER_BYTES + 1);
        assert_eq!(sender.metrics().persist_probes_sent, 1);
    }

    #[test]
    fn nodelay_controls_sub_mss_send_with_data_in_flight() {
        let mut delayed_config = config();
        delayed_config.nodelay = false;
        let mut sender = TcpSocketSender::new_reno(1, 0, delayed_config).expect("valid socket");
        sender.admit_application_write(600).expect("sequence space");

        let packets = sender.poll_transmit(0.0).expect("transmit");
        assert_eq!(packets.len(), 1);
        assert_eq!(sender.bytes_in_flight(), 512);

        let mut immediate = TcpSocketSender::new_reno(2, 0, config()).expect("valid socket");
        immediate
            .admit_application_write(600)
            .expect("sequence space");
        assert_eq!(immediate.poll_transmit(0.0).expect("transmit").len(), 2);
    }

    #[test]
    fn wire_accounting_charges_tcp_ip_header_per_segment() {
        let mut sender = TcpSocketSender::new_reno(1, 0, config()).expect("valid socket");
        sender.admit_application_write(600).expect("sequence space");
        let packets = sender.poll_transmit(0.0).expect("transmit");
        assert_eq!(packets.iter().map(|packet| packet.size).sum::<usize>(), 680);
        assert_eq!(sender.metrics().wire_bytes_sent, 680);
    }
}
