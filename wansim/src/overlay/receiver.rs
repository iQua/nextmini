use std::time::Duration;

use days::flows::packet::Packet;
use days::flows::tcp_socket::{DeliveredBytes, TcpSocketReceiver};
use nexosim::model::{
    BuildContext, Context, InitializedModel, Model, ModelRegistry, ProtoModel, SchedulableId,
};
use nexosim::ports::Output;

use crate::metrics::{MailboxTracker, Recorder, TrackedPacket};
use crate::overlay::{FrameAssembler, FramedStream};
use crate::transport::{FLOW_HOP_2, SocketPairConfig, now_ns, seconds_from_ns};

pub(crate) struct ReceiverEndpoint {
    receiver: TcpSocketReceiver,
    stream: FramedStream,
    assembler: FrameAssembler,
    completed_frames: usize,
    resume_at_ns: u64,
    pub(crate) ack_output: Output<TrackedPacket>,
    recorder: Recorder,
    mailbox_tracker: MailboxTracker,
}

impl ReceiverEndpoint {
    const RESUME_SID: SchedulableId<Self, ()> = SchedulableId::__from_decorated(0);
    pub(crate) const MAILBOX: &'static str = "receiver";

    pub(crate) fn new(
        socket: SocketPairConfig,
        stream: FramedStream,
        maximum_frame_payload: usize,
        resume_at_ns: u64,
        recorder: Recorder,
        mailbox_tracker: MailboxTracker,
    ) -> Result<Self, days::flows::tcp_socket::TcpSocketError> {
        Ok(Self {
            receiver: TcpSocketReceiver::new(FLOW_HOP_2, 0, socket.socket)?,
            stream,
            assembler: FrameAssembler::new(maximum_frame_payload),
            completed_frames: 0,
            resume_at_ns,
            ack_output: Output::default(),
            recorder,
            mailbox_tracker,
        })
    }

    pub(crate) async fn segment(&mut self, tracked: TrackedPacket, context: &Context<Self>) {
        let packet = tracked.arrive(Self::MAILBOX);
        let now = now_ns(context);
        match self.receiver.receive_segment(&packet, seconds_from_ns(now)) {
            Ok(outcome) => {
                self.send_ack(outcome.acknowledgment, now).await;
                if let Some(delivered) = outcome.delivered {
                    self.consume(delivered, now);
                }
            }
            Err(error) => self.recorder.fail(error),
        }
    }

    async fn resume(&mut self, _: (), context: &Context<Self>) {
        self.mailbox_tracker.dequeue(Self::MAILBOX);
        let now = now_ns(context);
        self.recorder.record(
            now,
            "receiver",
            "application_resume",
            FLOW_HOP_2,
            self.receiver.application_read_sequence(),
            0,
            self.receiver.receive_buffered_bytes(),
        );
        match self
            .receiver
            .grant_read_credit(self.stream.total_bytes(), seconds_from_ns(now))
        {
            Ok(outcome) => {
                self.send_ack(outcome.acknowledgment, now).await;
                if let Some(delivered) = outcome.delivered {
                    self.consume(delivered, now);
                }
            }
            Err(error) => self.recorder.fail(error),
        }
    }

    fn consume(&mut self, delivered: DeliveredBytes, now: u64) {
        self.recorder.record(
            now,
            "receiver",
            "application_read",
            FLOW_HOP_2,
            delivered.stream_offset,
            delivered.byte_count,
            self.receiver.application_read_sequence(),
        );
        match self
            .assembler
            .ingest(&self.stream, delivered.stream_offset, delivered.byte_count)
        {
            Ok(frames) => {
                for frame in frames {
                    self.completed_frames += 1;
                    self.recorder.record(
                        now,
                        "receiver",
                        "frame_delivered",
                        FLOW_HOP_2,
                        frame.frame_id,
                        frame.wire_bytes,
                        self.completed_frames,
                    );
                }
                if self.completed_frames == self.stream.frame_count() {
                    self.recorder.record(
                        now,
                        "receiver",
                        "stream_complete",
                        FLOW_HOP_2,
                        self.receiver.application_read_sequence(),
                        self.stream.total_bytes(),
                        self.completed_frames,
                    );
                }
            }
            Err(error) => self.recorder.fail(error),
        }
    }

    async fn send_ack(&mut self, acknowledgment: Packet, now: u64) {
        self.recorder.record(
            now,
            "receiver",
            "ack_emit",
            acknowledgment.flow_id,
            acknowledgment
                .ack
                .map_or(acknowledgment.packet_id, |ack| ack.sequence_num),
            acknowledgment.size,
            acknowledgment.ack.map_or(0, |ack| ack.advertised_window),
        );
        self.ack_output
            .send(TrackedPacket::enqueue(
                acknowledgment,
                "hop2_reverse",
                &self.mailbox_tracker,
            ))
            .await;
    }
}

impl Model for ReceiverEndpoint {
    type Env = ();

    fn register_schedulables(
        context: &mut BuildContext<impl ProtoModel<Model = Self>>,
    ) -> ModelRegistry {
        let mut registry = ModelRegistry::default();
        registry.add(context.register_schedulable(Self::resume));
        registry
    }

    async fn init(self, context: &Context<Self>, _: &mut Self::Env) -> InitializedModel<Self> {
        self.mailbox_tracker.enqueue(Self::MAILBOX);
        if let Err(error) = context.schedule_event(
            Duration::from_nanos(self.resume_at_ns),
            &Self::RESUME_SID,
            (),
        ) {
            self.mailbox_tracker.dequeue(Self::MAILBOX);
            self.recorder.fail(error);
        }
        self.into()
    }
}
