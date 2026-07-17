use std::collections::VecDeque;
use std::time::Duration;

use days::flows::packet::Packet;
use days::flows::tcp_socket::{DeliveredBytes, TcpSocketReceiver, TcpSocketSender};
use nexosim::model::{
    BuildContext, Context, InitializedModel, Model, ModelRegistry, ProtoModel, SchedulableId,
};
use nexosim::ports::Output;

use crate::metrics::{MailboxTracker, Recorder, TrackedPacket};
use crate::overlay::{FrameAssembler, FramedStream};
use crate::transport::{
    FLOW_HOP_1, FLOW_HOP_2, SocketPairConfig, emit_packets, now_ns, seconds_from_ns,
};

#[derive(Clone, Copy, Debug)]
struct PendingFrame {
    frame_id: usize,
    remaining_bytes: usize,
}

pub(crate) struct RelayEndpoint {
    upstream_receiver: TcpSocketReceiver,
    downstream_sender: TcpSocketSender,
    stream: FramedStream,
    assembler: FrameAssembler,
    application_buffer_capacity: usize,
    application_buffer_occupied: usize,
    pending_frames: VecDeque<PendingFrame>,
    downstream_stream_cursor: usize,
    timer_interval_ns: u64,
    pub(crate) upstream_ack_output: Output<TrackedPacket>,
    pub(crate) downstream_data_output: Output<TrackedPacket>,
    recorder: Recorder,
    mailbox_tracker: MailboxTracker,
}

impl RelayEndpoint {
    const START_SID: SchedulableId<Self, ()> = SchedulableId::__from_decorated(0);
    const TIMER_SID: SchedulableId<Self, ()> = SchedulableId::__from_decorated(1);
    pub(crate) const MAILBOX: &'static str = "relay";

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        upstream_socket: SocketPairConfig,
        downstream_socket: SocketPairConfig,
        stream: FramedStream,
        maximum_frame_payload: usize,
        application_buffer_capacity: usize,
        timer_interval_ns: u64,
        recorder: Recorder,
        mailbox_tracker: MailboxTracker,
    ) -> Result<Self, days::flows::tcp_socket::TcpSocketError> {
        Ok(Self {
            upstream_receiver: TcpSocketReceiver::new(FLOW_HOP_1, 0, upstream_socket.socket)?,
            downstream_sender: TcpSocketSender::new_reno(FLOW_HOP_2, 0, downstream_socket.socket)?,
            stream,
            assembler: FrameAssembler::new(maximum_frame_payload),
            application_buffer_capacity,
            application_buffer_occupied: 0,
            pending_frames: VecDeque::new(),
            downstream_stream_cursor: 0,
            timer_interval_ns,
            upstream_ack_output: Output::default(),
            downstream_data_output: Output::default(),
            recorder,
            mailbox_tracker,
        })
    }

    pub(crate) async fn upstream_segment(
        &mut self,
        tracked: TrackedPacket,
        context: &Context<Self>,
    ) {
        let packet = tracked.arrive(Self::MAILBOX);
        let now = now_ns(context);
        match self
            .upstream_receiver
            .receive_segment(&packet, seconds_from_ns(now))
        {
            Ok(outcome) => {
                self.send_upstream_ack(outcome.acknowledgment, now).await;
                if let Some(delivered) = outcome.delivered {
                    self.process_upstream_delivery(delivered, now).await;
                }
            }
            Err(error) => self.recorder.fail(error),
        }
    }

    pub(crate) async fn downstream_acknowledgment(
        &mut self,
        tracked: TrackedPacket,
        context: &Context<Self>,
    ) {
        let acknowledgment = tracked.arrive(Self::MAILBOX);
        let now = now_ns(context);
        match self
            .downstream_sender
            .receive_ack(&acknowledgment, seconds_from_ns(now))
        {
            Ok(packets) => {
                self.emit_downstream(packets, now).await;
                self.flush_and_recredit(now).await;
            }
            Err(error) => self.recorder.fail(error),
        }
    }

    async fn start(&mut self, _: (), context: &Context<Self>) {
        self.mailbox_tracker.dequeue(Self::MAILBOX);
        let now = now_ns(context);
        match self
            .upstream_receiver
            .grant_read_credit(self.application_buffer_capacity, seconds_from_ns(now))
        {
            Ok(outcome) => {
                self.send_upstream_ack(outcome.acknowledgment, now).await;
                if let Some(delivered) = outcome.delivered {
                    self.process_upstream_delivery(delivered, now).await;
                }
            }
            Err(error) => self.recorder.fail(error),
        }
        self.schedule_timer(context);
    }

    async fn timer(&mut self, _: (), context: &Context<Self>) {
        self.mailbox_tracker.dequeue(Self::MAILBOX);
        let now = now_ns(context);
        match self.downstream_sender.timer_tick(seconds_from_ns(now)) {
            Ok(packets) => self.emit_downstream(packets, now).await,
            Err(error) => self.recorder.fail(error),
        }
        self.flush_and_recredit(now).await;
        if self.downstream_stream_cursor < self.stream.total_bytes()
            || self.downstream_sender.send_buffered_bytes() > 0
        {
            self.schedule_timer(context);
        }
    }

    async fn process_upstream_delivery(&mut self, initial: DeliveredBytes, now: u64) {
        let mut deliveries = VecDeque::from([initial]);
        while let Some(delivered) = deliveries.pop_front() {
            let Some(occupied) = self
                .application_buffer_occupied
                .checked_add(delivered.byte_count)
            else {
                self.recorder
                    .fail("relay application-buffer accounting overflow");
                return;
            };
            if occupied > self.application_buffer_capacity {
                self.recorder.fail(format_args!(
                    "relay application buffer exceeded: {occupied} > {}",
                    self.application_buffer_capacity
                ));
                return;
            }
            self.application_buffer_occupied = occupied;
            self.recorder.record(
                now,
                "relay",
                "socket_read",
                FLOW_HOP_1,
                delivered.stream_offset,
                delivered.byte_count,
                occupied,
            );
            match self.stream_frames(delivered, now) {
                Ok(()) => {}
                Err(error) => {
                    self.recorder.fail(error);
                    return;
                }
            }

            let released = self.flush_downstream(now).await;
            if released == 0 {
                continue;
            }
            match self
                .upstream_receiver
                .grant_read_credit(released, seconds_from_ns(now))
            {
                Ok(outcome) => {
                    self.send_upstream_ack(outcome.acknowledgment, now).await;
                    if let Some(next_delivery) = outcome.delivered {
                        deliveries.push_back(next_delivery);
                    }
                }
                Err(error) => {
                    self.recorder.fail(error);
                    return;
                }
            }
        }
    }

    fn stream_frames(
        &mut self,
        delivered: DeliveredBytes,
        now: u64,
    ) -> Result<(), crate::overlay::frame::FrameError> {
        for frame in
            self.assembler
                .ingest(&self.stream, delivered.stream_offset, delivered.byte_count)?
        {
            self.recorder.record(
                now,
                "relay",
                "frame_assembled",
                FLOW_HOP_1,
                frame.frame_id,
                frame.wire_bytes,
                self.application_buffer_occupied,
            );
            self.pending_frames.push_back(PendingFrame {
                frame_id: frame.frame_id,
                remaining_bytes: frame.wire_bytes,
            });
        }
        Ok(())
    }

    async fn flush_and_recredit(&mut self, now: u64) {
        let released = self.flush_downstream(now).await;
        if released == 0 {
            return;
        }
        match self
            .upstream_receiver
            .grant_read_credit(released, seconds_from_ns(now))
        {
            Ok(outcome) => {
                self.send_upstream_ack(outcome.acknowledgment, now).await;
                if let Some(delivered) = outcome.delivered {
                    self.process_upstream_delivery(delivered, now).await;
                }
            }
            Err(error) => self.recorder.fail(error),
        }
    }

    async fn flush_downstream(&mut self, now: u64) -> usize {
        let mut released = 0_usize;
        while let Some(frame) = self.pending_frames.front_mut() {
            let frame_id = frame.frame_id;
            let requested = frame.remaining_bytes;
            let admission = match self.downstream_sender.admit_application_write(requested) {
                Ok(admission) => admission,
                Err(error) => {
                    self.recorder.fail(error);
                    break;
                }
            };
            if admission.accepted_bytes == 0 {
                break;
            }
            frame.remaining_bytes -= admission.accepted_bytes;
            self.application_buffer_occupied = self
                .application_buffer_occupied
                .saturating_sub(admission.accepted_bytes);
            let stream_offset = self.downstream_stream_cursor;
            self.downstream_stream_cursor = self
                .downstream_stream_cursor
                .saturating_add(admission.accepted_bytes);
            released = released.saturating_add(admission.accepted_bytes);
            self.recorder.record(
                now,
                "relay",
                "downstream_socket_write",
                FLOW_HOP_2,
                stream_offset,
                admission.accepted_bytes,
                frame_id,
            );
            if frame.remaining_bytes == 0 {
                self.pending_frames.pop_front();
            }
        }

        match self.downstream_sender.poll_transmit(seconds_from_ns(now)) {
            Ok(packets) => self.emit_downstream(packets, now).await,
            Err(error) => self.recorder.fail(error),
        }
        debug_assert_eq!(
            self.application_buffer_occupied,
            self.assembler.buffered_bytes()
                + self
                    .pending_frames
                    .iter()
                    .map(|frame| frame.remaining_bytes)
                    .sum::<usize>()
        );
        released
    }

    async fn send_upstream_ack(&mut self, acknowledgment: Packet, now: u64) {
        self.recorder.record(
            now,
            "relay",
            "upstream_ack_emit",
            acknowledgment.flow_id,
            acknowledgment
                .ack
                .map_or(acknowledgment.packet_id, |ack| ack.sequence_num),
            acknowledgment.size,
            acknowledgment.ack.map_or(0, |ack| ack.advertised_window),
        );
        self.upstream_ack_output
            .send(TrackedPacket::enqueue(
                acknowledgment,
                "hop1_reverse",
                &self.mailbox_tracker,
            ))
            .await;
    }

    async fn emit_downstream(&mut self, packets: Vec<Packet>, now: u64) {
        for packet in &packets {
            self.recorder.record(
                now,
                "relay",
                "downstream_segment_emit",
                packet.flow_id,
                packet.packet_id,
                packet.size,
                self.downstream_sender.send_buffered_bytes(),
            );
        }
        emit_packets(
            packets,
            &mut self.downstream_data_output,
            "hop2_forward",
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

impl Model for RelayEndpoint {
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
