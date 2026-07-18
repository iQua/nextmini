use std::time::Duration;

use days::flows::packet::Packet;
use days::flows::tcp_socket::{TcpSocketReceiver, TcpSocketSender};
use nexosim::model::{
    BuildContext, Context, InitializedModel, Model, ModelRegistry, ProtoModel, SchedulableId,
};
use nexosim::ports::Output;

use crate::determinism::CounterPrf;
use crate::metrics::{MailboxTracker, Recorder, TrackedPacket};
use crate::transport::{SocketPairConfig, emit_packets, now_ns, seconds_from_ns};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum BackgroundTrafficKind {
    Bulk,
    HeavyTailedOnOff,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct BackgroundFlowConfig {
    pub(crate) component: &'static str,
    pub(crate) mailbox: &'static str,
    pub(crate) flow_id: usize,
    pub(crate) data_path_mailbox: &'static str,
    pub(crate) ack_path_mailbox: &'static str,
    pub(crate) kind: BackgroundTrafficKind,
    pub(crate) timer_interval_ns: u64,
    pub(crate) on_base_ns: u64,
    pub(crate) off_base_ns: u64,
    /// Optional application offered-rate ceiling in bits/s. TCP congestion and receive windows
    /// remain authoritative; this only paces admission into the finite send buffer.
    pub(crate) application_rate_bps: Option<u64>,
    pub(crate) prf: CounterPrf,
}

pub(crate) struct BackgroundFlow {
    config: BackgroundFlowConfig,
    sender: TcpSocketSender,
    receiver: TcpSocketReceiver,
    active: bool,
    transition_at_ns: u64,
    transition_index: u64,
    rate_credit_bit_ns: u128,
    last_credit_ns: u64,
    pub(crate) data_output: Output<TrackedPacket>,
    pub(crate) ack_output: Output<TrackedPacket>,
    recorder: Recorder,
    mailbox_tracker: MailboxTracker,
}

impl BackgroundFlow {
    const START_SID: SchedulableId<Self, ()> = SchedulableId::__from_decorated(0);
    const TIMER_SID: SchedulableId<Self, ()> = SchedulableId::__from_decorated(1);

    pub(crate) fn new(
        config: BackgroundFlowConfig,
        socket: SocketPairConfig,
        recorder: Recorder,
        mailbox_tracker: MailboxTracker,
    ) -> Result<Self, days::flows::tcp_socket::TcpSocketError> {
        let active = true;
        let transition_at_ns = match config.kind {
            BackgroundTrafficKind::HeavyTailedOnOff => config.on_base_ns,
            BackgroundTrafficKind::Bulk => u64::MAX,
        };
        Ok(Self {
            config,
            sender: socket.sender(config.flow_id, 0)?,
            receiver: TcpSocketReceiver::new(config.flow_id, 0, socket.socket)?,
            active,
            transition_at_ns,
            transition_index: 0,
            rate_credit_bit_ns: 0,
            last_credit_ns: 0,
            data_output: Output::default(),
            ack_output: Output::default(),
            recorder,
            mailbox_tracker,
        })
    }

    pub(crate) async fn network_packet(&mut self, tracked: TrackedPacket, context: &Context<Self>) {
        self.recorder
            .count_event_class("dispatch_background_network_packet");
        let packet = tracked.arrive(self.config.mailbox);
        let now = now_ns(context);
        if packet.ack.is_some() {
            match self.sender.receive_ack(&packet, seconds_from_ns(now)) {
                Ok(mut packets) => {
                    if self.active {
                        self.refill_after_ack(now);
                        match self.sender.poll_transmit(seconds_from_ns(now)) {
                            Ok(new_packets) => packets.extend(new_packets),
                            Err(error) => self.fail("poll after ACK", error),
                        }
                    }
                    self.emit_data(packets).await;
                }
                Err(error) => self.fail("receive ACK", error),
            }
        } else {
            self.receive_data(packet, now).await;
        }
    }

    async fn start(&mut self, _: (), context: &Context<Self>) {
        self.recorder.count_event_class("dispatch_background_start");
        self.mailbox_tracker.dequeue(self.config.mailbox);
        let now = now_ns(context);
        match self
            .receiver
            .grant_read_credit(self.receiver_capacity(), seconds_from_ns(now))
        {
            Ok(outcome) => self.emit_ack(vec![outcome.acknowledgment]).await,
            Err(error) => self.fail("initial read credit", error),
        }
        self.refill_sender(now);
        match self.sender.poll_transmit(seconds_from_ns(now)) {
            Ok(packets) => self.emit_data(packets).await,
            Err(error) => self.fail("initial transmit", error),
        }
        self.schedule_timer(context);
    }

    async fn timer(&mut self, _: (), context: &Context<Self>) {
        self.recorder.count_event_class("dispatch_background_timer");
        self.mailbox_tracker.dequeue(self.config.mailbox);
        let now = now_ns(context);
        self.advance_on_off(now);
        let mut packets = match self.sender.timer_tick(seconds_from_ns(now)) {
            Ok(packets) => packets,
            Err(error) => {
                self.fail("timer tick", error);
                Vec::new()
            }
        };
        if self.active {
            self.refill_sender(now);
            match self.sender.poll_transmit(seconds_from_ns(now)) {
                Ok(new_packets) => packets.extend(new_packets),
                Err(error) => self.fail("timer transmit", error),
            }
        }
        self.emit_data(packets).await;
        self.schedule_timer(context);
    }

    async fn receive_data(&mut self, packet: Packet, now: u64) {
        let outcome = match self.receiver.receive_segment(&packet, seconds_from_ns(now)) {
            Ok(outcome) => outcome,
            Err(error) => {
                self.fail("receive data", error);
                return;
            }
        };
        let mut acknowledgments = vec![outcome.acknowledgment];
        let mut delivered = outcome.delivered;
        while let Some(bytes) = delivered {
            self.recorder.record(
                now,
                self.config.component,
                "background_bytes_delivered",
                self.config.flow_id,
                bytes.stream_offset,
                bytes.byte_count,
                usize::from(self.config.kind == BackgroundTrafficKind::HeavyTailedOnOff),
            );
            match self
                .receiver
                .grant_read_credit(bytes.byte_count, seconds_from_ns(now))
            {
                Ok(outcome) => {
                    acknowledgments.push(outcome.acknowledgment);
                    delivered = outcome.delivered;
                }
                Err(error) => {
                    self.fail("return read credit", error);
                    break;
                }
            }
        }
        self.emit_ack(acknowledgments).await;
    }

    fn refill_sender(&mut self, now: u64) {
        let writable = self.sender.writable_bytes();
        if writable == 0 {
            return;
        }
        let admitted = if let Some(rate_bps) = self.config.application_rate_bps {
            let elapsed = now.saturating_sub(self.last_credit_ns);
            self.last_credit_ns = now;
            self.rate_credit_bit_ns = self
                .rate_credit_bit_ns
                .saturating_add(u128::from(rate_bps).saturating_mul(u128::from(elapsed)));
            let maximum_credit =
                (self.sender.send_buffer_capacity() as u128).saturating_mul(8_000_000_000);
            self.rate_credit_bit_ns = self.rate_credit_bit_ns.min(maximum_credit);
            let available =
                usize::try_from(self.rate_credit_bit_ns / 8_000_000_000).unwrap_or(usize::MAX);
            let admitted = writable.min(available);
            self.rate_credit_bit_ns = self
                .rate_credit_bit_ns
                .saturating_sub((admitted as u128).saturating_mul(8_000_000_000));
            admitted
        } else {
            writable
        };
        if admitted == 0 {
            return;
        }
        if let Err(error) = self.sender.admit_application_write(admitted) {
            self.fail("application write", error);
        }
    }

    fn refill_after_ack(&mut self, now: u64) {
        if self.config.application_rate_bps.is_none() {
            self.refill_sender(now);
        }
    }

    fn advance_on_off(&mut self, now: u64) {
        if self.config.kind != BackgroundTrafficKind::HeavyTailedOnOff {
            return;
        }
        while now >= self.transition_at_ns {
            self.active = !self.active;
            self.transition_index = self.transition_index.saturating_add(1);
            let base = if self.active {
                self.config.on_base_ns
            } else {
                self.config.off_base_ns
            };
            let draw = self.config.prf.draw_u64(
                if self.active {
                    "on_duration"
                } else {
                    "off_duration"
                },
                self.config.flow_id as u64,
                0,
                self.transition_index,
            );
            let exponent = draw.trailing_zeros().min(6);
            let duration = base.saturating_mul(1_u64 << exponent);
            self.transition_at_ns = self.transition_at_ns.saturating_add(duration);
            self.recorder.record(
                now,
                self.config.component,
                if self.active {
                    "background_on"
                } else {
                    "background_off"
                },
                self.config.flow_id,
                self.transition_index as usize,
                0,
                duration as usize,
            );
        }
        if !self.active {
            self.rate_credit_bit_ns = 0;
            self.last_credit_ns = now;
        }
    }

    async fn emit_data(&mut self, packets: Vec<Packet>) {
        emit_packets(
            packets,
            &mut self.data_output,
            self.config.data_path_mailbox,
            &self.mailbox_tracker,
        )
        .await;
    }

    async fn emit_ack(&mut self, packets: Vec<Packet>) {
        emit_packets(
            packets,
            &mut self.ack_output,
            self.config.ack_path_mailbox,
            &self.mailbox_tracker,
        )
        .await;
    }

    fn receiver_capacity(&self) -> usize {
        self.sender.send_buffer_capacity()
    }

    fn fail(&self, operation: &str, error: impl std::fmt::Display) {
        self.recorder.fail(format_args!(
            "{} {operation}: {error}",
            self.config.component
        ));
    }

    fn schedule_timer(&self, context: &Context<Self>) {
        self.mailbox_tracker.enqueue(self.config.mailbox);
        if let Err(error) = context.schedule_event(
            Duration::from_nanos(self.config.timer_interval_ns),
            &Self::TIMER_SID,
            (),
        ) {
            self.mailbox_tracker.dequeue(self.config.mailbox);
            self.fail("schedule timer", error);
        }
    }
}

impl Model for BackgroundFlow {
    type Env = ();

    fn register_schedulables(
        context: &mut BuildContext<impl ProtoModel<Model = Self>>,
    ) -> ModelRegistry {
        let mut registry = ModelRegistry::default();
        registry.add(context.register_schedulable(Self::start));
        registry.add(context.register_schedulable(Self::timer));
        registry
    }

    async fn init(self, context: &Context<Self>, _: &mut Self::Env) -> InitializedModel<Self> {
        self.mailbox_tracker.enqueue(self.config.mailbox);
        if let Err(error) = context.schedule_event(Duration::from_nanos(1), &Self::START_SID, ()) {
            self.mailbox_tracker.dequeue(self.config.mailbox);
            self.fail("schedule start", error);
        }
        self.into()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn flow(application_rate_bps: Option<u64>) -> BackgroundFlow {
        let config = BackgroundFlowConfig {
            component: "background-test",
            mailbox: "background-test",
            flow_id: 1,
            data_path_mailbox: "network",
            ack_path_mailbox: "network",
            kind: BackgroundTrafficKind::Bulk,
            timer_interval_ns: 10_000_000,
            on_base_ns: 100_000_000,
            off_base_ns: 150_000_000,
            application_rate_bps,
            prf: CounterPrf::new(7, "background-test"),
        };
        let socket = SocketPairConfig::new(512, 64 * 1024, 64 * 1024, 1_000_000, 1_000_000)
            .expect("socket geometry");
        BackgroundFlow::new(
            config,
            socket,
            Recorder::new("background-test", 7),
            MailboxTracker::default(),
        )
        .expect("background flow")
    }

    #[test]
    fn paced_application_credit_is_not_refilled_by_ack_arrivals() {
        let mut flow = flow(Some(8_000));
        flow.refill_sender(1_000_000_000);
        assert_eq!(flow.sender.metrics().application_bytes_admitted, 1_000);

        flow.refill_after_ack(1_500_000_000);
        assert_eq!(
            flow.sender.metrics().application_bytes_admitted,
            1_000,
            "ACK-clock timing must not fragment paced writes into tiny segments"
        );

        flow.refill_sender(2_000_000_000);
        assert_eq!(flow.sender.metrics().application_bytes_admitted, 2_000);
    }
}
