use days::flows::packet::Packet;
use days::flows::tcp_socket::{TcpSocketConfig, TcpSocketError, TcpSocketSender};
use nexosim::ports::Output;
use nexosim::time::MonotonicTime;

use crate::metrics::{MailboxTracker, TrackedPacket};

pub(crate) const FLOW_HOP_1: usize = 10_001;
pub(crate) const FLOW_HOP_2: usize = 10_002;
pub(crate) const FLOW_SOURCE_RELAY_A: usize = 20_001;
pub(crate) const FLOW_RELAY_A_RECEIVER_1: usize = 20_002;
pub(crate) const FLOW_RELAY_A_RELAY_B: usize = 20_003;
pub(crate) const FLOW_RELAY_B_RECEIVER_2: usize = 20_004;
pub(crate) const FLOW_RELAY_B_RECEIVER_3: usize = 20_005;

#[derive(Clone, Copy, Debug)]
pub(crate) struct SocketPairConfig {
    pub(crate) socket: TcpSocketConfig,
    congestion_control: TcpCongestionControl,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) enum TcpCongestionControl {
    #[default]
    Reno,
    WindowScaledReno,
}

impl SocketPairConfig {
    pub(crate) fn new(
        mss: usize,
        send_buffer_bytes: usize,
        receive_buffer_bytes: usize,
        initial_rto_ns: u64,
        persist_interval_ns: u64,
    ) -> Result<Self, TcpSocketError> {
        let socket = TcpSocketConfig {
            mss,
            send_buffer_bytes,
            receive_buffer_bytes,
            nodelay: true,
            initial_rto_seconds: seconds_from_ns(initial_rto_ns),
            min_rto_seconds: seconds_from_ns(initial_rto_ns),
            max_rto_seconds: seconds_from_ns(initial_rto_ns.saturating_mul(64)),
            persist_interval_seconds: seconds_from_ns(persist_interval_ns),
        }
        .validate()?;
        Ok(Self {
            socket,
            congestion_control: TcpCongestionControl::Reno,
        })
    }

    pub(crate) fn with_congestion_control(
        mut self,
        congestion_control: TcpCongestionControl,
    ) -> Self {
        self.congestion_control = congestion_control;
        self
    }

    pub(crate) fn sender(
        self,
        flow_id: usize,
        priority: u8,
    ) -> Result<TcpSocketSender, TcpSocketError> {
        match self.congestion_control {
            TcpCongestionControl::Reno => TcpSocketSender::new_reno(flow_id, priority, self.socket),
            TcpCongestionControl::WindowScaledReno => {
                TcpSocketSender::new_window_scaled_reno(flow_id, priority, self.socket)
            }
        }
    }
}

pub(crate) async fn emit_packets(
    packets: Vec<Packet>,
    output: &mut Output<TrackedPacket>,
    target_mailbox: &'static str,
    tracker: &MailboxTracker,
) {
    for packet in packets {
        output
            .send(TrackedPacket::enqueue(packet, target_mailbox, tracker))
            .await;
    }
}

pub(crate) fn now_ns<M: nexosim::model::Model>(context: &nexosim::model::Context<M>) -> u64 {
    let nanos = context
        .time()
        .duration_since(MonotonicTime::EPOCH)
        .as_nanos();
    u64::try_from(nanos).unwrap_or(u64::MAX)
}

pub(crate) fn seconds_from_ns(nanoseconds: u64) -> f64 {
    nanoseconds as f64 / 1_000_000_000.0
}
