use std::time::Duration;

use days::flows::tcp_socket::TcpSocketSender;
use nexosim::model::{
    BuildContext, Context, InitializedModel, Model, ModelRegistry, ProtoModel, SchedulableId,
};
use nexosim::ports::Output;

use crate::metrics::{MailboxTracker, Recorder, TrackedPacket};
use crate::transport::{FLOW_HOP_1, SocketPairConfig, emit_packets, now_ns, seconds_from_ns};

pub(crate) struct SourceEndpoint {
    sender: TcpSocketSender,
    stream_bytes: usize,
    application_cursor: usize,
    timer_interval_ns: u64,
    pub(crate) data_output: Output<TrackedPacket>,
    recorder: Recorder,
    mailbox_tracker: MailboxTracker,
}

impl SourceEndpoint {
    const START_SID: SchedulableId<Self, ()> = SchedulableId::__from_decorated(0);
    const TIMER_SID: SchedulableId<Self, ()> = SchedulableId::__from_decorated(1);
    pub(crate) const MAILBOX: &'static str = "source";

    pub(crate) fn new(
        socket: SocketPairConfig,
        stream_bytes: usize,
        timer_interval_ns: u64,
        recorder: Recorder,
        mailbox_tracker: MailboxTracker,
    ) -> Result<Self, days::flows::tcp_socket::TcpSocketError> {
        Ok(Self {
            sender: TcpSocketSender::new_reno(FLOW_HOP_1, 0, socket.socket)?,
            stream_bytes,
            application_cursor: 0,
            timer_interval_ns,
            data_output: Output::default(),
            recorder,
            mailbox_tracker,
        })
    }

    pub(crate) async fn acknowledgment(&mut self, tracked: TrackedPacket, context: &Context<Self>) {
        let acknowledgment = tracked.arrive(Self::MAILBOX);
        let now = now_ns(context);
        self.recorder.record(
            now,
            "source",
            "ack_arrival",
            acknowledgment.flow_id,
            acknowledgment
                .ack
                .map_or(acknowledgment.packet_id, |ack| ack.sequence_num),
            acknowledgment.size,
            acknowledgment.ack.map_or(0, |ack| ack.advertised_window),
        );
        match self
            .sender
            .receive_ack(&acknowledgment, seconds_from_ns(now))
        {
            Ok(packets) => {
                self.emit(packets, now).await;
                self.fill_socket(now).await;
            }
            Err(error) => self.recorder.fail(error),
        }
    }

    async fn start(&mut self, _: (), context: &Context<Self>) {
        self.mailbox_tracker.dequeue(Self::MAILBOX);
        let now = now_ns(context);
        self.fill_socket(now).await;
        self.schedule_timer(context);
    }

    async fn timer(&mut self, _: (), context: &Context<Self>) {
        self.mailbox_tracker.dequeue(Self::MAILBOX);
        let now = now_ns(context);
        match self.sender.timer_tick(seconds_from_ns(now)) {
            Ok(packets) => self.emit(packets, now).await,
            Err(error) => self.recorder.fail(error),
        }
        self.fill_socket(now).await;
        if self.application_cursor < self.stream_bytes || self.sender.send_buffered_bytes() > 0 {
            self.schedule_timer(context);
        }
    }

    async fn fill_socket(&mut self, now: u64) {
        let remaining = self.stream_bytes.saturating_sub(self.application_cursor);
        if remaining > 0 {
            match self.sender.admit_application_write(remaining) {
                Ok(admission) => {
                    self.application_cursor = self
                        .application_cursor
                        .saturating_add(admission.accepted_bytes);
                    if admission.accepted_bytes > 0 {
                        self.recorder.record(
                            now,
                            "source",
                            "application_write",
                            FLOW_HOP_1,
                            self.application_cursor - admission.accepted_bytes,
                            admission.accepted_bytes,
                            self.application_cursor,
                        );
                    }
                    if admission.blocked_bytes > 0 {
                        self.recorder.record(
                            now,
                            "source",
                            "writer_blocked",
                            FLOW_HOP_1,
                            self.application_cursor,
                            admission.blocked_bytes,
                            self.application_cursor,
                        );
                    }
                }
                Err(error) => self.recorder.fail(error),
            }
        }
        match self.sender.poll_transmit(seconds_from_ns(now)) {
            Ok(packets) => self.emit(packets, now).await,
            Err(error) => self.recorder.fail(error),
        }
    }

    async fn emit(&mut self, packets: Vec<days::flows::packet::Packet>, now: u64) {
        for packet in &packets {
            self.recorder.record(
                now,
                "source",
                "segment_emit",
                packet.flow_id,
                packet.packet_id,
                packet.size,
                self.sender.send_buffered_bytes(),
            );
        }
        emit_packets(
            packets,
            &mut self.data_output,
            "hop1_forward",
            &self.mailbox_tracker,
        )
        .await;
    }

    fn schedule_timer(&self, context: &Context<Self>) {
        self.mailbox_tracker.enqueue(Self::MAILBOX);
        if let Err(error) = context.schedule_event(
            Duration::from_nanos(self.timer_interval_ns),
            &Self::TIMER_SID,
            (),
        ) {
            self.mailbox_tracker.dequeue(Self::MAILBOX);
            self.recorder.fail(error);
        }
    }
}

impl Model for SourceEndpoint {
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
        self.mailbox_tracker.enqueue(Self::MAILBOX);
        if let Err(error) = context.schedule_event(Duration::from_nanos(1), &Self::START_SID, ()) {
            self.mailbox_tracker.dequeue(Self::MAILBOX);
            self.recorder.fail(error);
        }
        self.into()
    }
}
