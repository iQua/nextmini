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
    pub(crate) prf: CounterPrf,
}

pub(crate) struct BackgroundFlow {
    config: BackgroundFlowConfig,
    sender: TcpSocketSender,
    receiver: TcpSocketReceiver,
    active: bool,
    transition_at_ns: u64,
    transition_index: u64,
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
            sender: TcpSocketSender::new_reno(config.flow_id, 0, socket.socket)?,
            receiver: TcpSocketReceiver::new(config.flow_id, 0, socket.socket)?,
            active,
            transition_at_ns,
            transition_index: 0,
            data_output: Output::default(),
            ack_output: Output::default(),
            recorder,
            mailbox_tracker,
        })
    }

    pub(crate) async fn network_packet(&mut self, tracked: TrackedPacket, context: &Context<Self>) {
        let packet = tracked.arrive(self.config.mailbox);
        let now = now_ns(context);
        if packet.ack.is_some() {
            match self.sender.receive_ack(&packet, seconds_from_ns(now)) {
                Ok(mut packets) => {
                    if self.active {
                        self.refill_sender();
                        match self.sender.poll_transmit(seconds_from_ns(now)) {
                            Ok(new_packets) => packets.extend(new_packets),
                            Err(error) => self.recorder.fail(error),
                        }
                    }
                    self.emit_data(packets).await;
                }
                Err(error) => self.recorder.fail(error),
            }
        } else {
            self.receive_data(packet, now).await;
        }
    }

    async fn start(&mut self, _: (), context: &Context<Self>) {
        self.mailbox_tracker.dequeue(self.config.mailbox);
        let now = now_ns(context);
        match self
            .receiver
            .grant_read_credit(self.receiver_capacity(), seconds_from_ns(now))
        {
            Ok(outcome) => self.emit_ack(vec![outcome.acknowledgment]).await,
            Err(error) => self.recorder.fail(error),
        }
        self.refill_sender();
        match self.sender.poll_transmit(seconds_from_ns(now)) {
            Ok(packets) => self.emit_data(packets).await,
            Err(error) => self.recorder.fail(error),
        }
        self.schedule_timer(context);
    }

    async fn timer(&mut self, _: (), context: &Context<Self>) {
        self.mailbox_tracker.dequeue(self.config.mailbox);
        let now = now_ns(context);
        self.advance_on_off(now);
        let mut packets = match self.sender.timer_tick(seconds_from_ns(now)) {
            Ok(packets) => packets,
            Err(error) => {
                self.recorder.fail(error);
                Vec::new()
            }
        };
        if self.active {
            self.refill_sender();
            match self.sender.poll_transmit(seconds_from_ns(now)) {
                Ok(new_packets) => packets.extend(new_packets),
                Err(error) => self.recorder.fail(error),
            }
        }
        self.emit_data(packets).await;
        self.schedule_timer(context);
    }

    async fn receive_data(&mut self, packet: Packet, now: u64) {
        let outcome = match self.receiver.receive_segment(&packet, seconds_from_ns(now)) {
            Ok(outcome) => outcome,
            Err(error) => {
                self.recorder.fail(error);
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
                    self.recorder.fail(error);
                    break;
                }
            }
        }
        self.emit_ack(acknowledgments).await;
    }

    fn refill_sender(&mut self) {
        let writable = self.sender.writable_bytes();
        if writable == 0 {
            return;
        }
        if let Err(error) = self.sender.admit_application_write(writable) {
            self.recorder.fail(error);
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

    fn schedule_timer(&self, context: &Context<Self>) {
        self.mailbox_tracker.enqueue(self.config.mailbox);
        if let Err(error) = context.schedule_event(
            Duration::from_nanos(self.config.timer_interval_ns),
            &Self::TIMER_SID,
            (),
        ) {
            self.mailbox_tracker.dequeue(self.config.mailbox);
            self.recorder.fail(error);
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
            self.recorder.fail(error);
        }
        self.into()
    }
}
