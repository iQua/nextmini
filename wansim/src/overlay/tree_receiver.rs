use std::collections::VecDeque;
use std::time::Duration;

use days::flows::packet::Packet;
use days::flows::tcp_socket::{DeliveredBytes, TcpSocketReceiver};
use nexosim::model::{
    BuildContext, Context, InitializedModel, Model, ModelRegistry, ProtoModel, SchedulableId,
};
use nexosim::ports::Output;
use thiserror::Error;

use crate::determinism::DECISION_DELTA_NS;
use crate::metrics::{MailboxTracker, OwnershipLedger, Recorder, TrackedPacket};
use crate::overlay::{DofBucket, FrameAssembler, FramedStream};
use crate::scenario::ReceiverTiming;
use crate::transport::{SocketPairConfig, now_ns, seconds_from_ns};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RuntimeFrameKind {
    Data,
    // W0b emits fixed source data only; the unit gate constructs this variant to pin the lane
    // split that W1 control frames will enter.
    #[cfg_attr(not(test), expect(dead_code))]
    Control,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct RuntimeFrame {
    frame_id: usize,
    wire_bytes: usize,
    frame_end_sequence: usize,
    transport_acked_through: usize,
    kind: RuntimeFrameKind,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum InboxAdmission {
    DataQueued,
    DataDropped,
    ControlQueued,
    ControlBackpressured,
}

#[derive(Debug)]
struct HybridInboxes {
    data: VecDeque<RuntimeFrame>,
    control: VecDeque<RuntimeFrame>,
    data_capacity: usize,
    control_capacity: usize,
}

impl HybridInboxes {
    fn new(data_capacity: usize, control_capacity: usize) -> Self {
        Self {
            data: VecDeque::new(),
            control: VecDeque::new(),
            data_capacity,
            control_capacity,
        }
    }

    fn admit(&mut self, frame: RuntimeFrame) -> InboxAdmission {
        match frame.kind {
            RuntimeFrameKind::Data if self.data.len() >= self.data_capacity => {
                InboxAdmission::DataDropped
            }
            RuntimeFrameKind::Data => {
                self.data.push_back(frame);
                InboxAdmission::DataQueued
            }
            RuntimeFrameKind::Control if self.control.len() >= self.control_capacity => {
                InboxAdmission::ControlBackpressured
            }
            RuntimeFrameKind::Control => {
                self.control.push_back(frame);
                InboxAdmission::ControlQueued
            }
        }
    }

    fn data_bytes(&self) -> usize {
        self.data.iter().map(|frame| frame.wire_bytes).sum()
    }
}

pub(crate) struct TreeReceiverEndpoint {
    component: &'static str,
    mailbox: &'static str,
    reverse_link_mailbox: &'static str,
    receive_owner: &'static str,
    runtime_owner: &'static str,
    data_inbox_owner: &'static str,
    service_owner: &'static str,
    flow_id: usize,
    receiver: TcpSocketReceiver,
    stream: FramedStream,
    assembler: FrameAssembler,
    frame_wire_bytes: usize,
    runtime_commands: VecDeque<RuntimeFrame>,
    runtime_command_capacity: usize,
    runtime_busy: bool,
    inboxes: HybridInboxes,
    service_start_scheduled: bool,
    in_service: Option<RuntimeFrame>,
    timing: ReceiverTiming,
    runtime_command_service_ns: u64,
    decoder_sink_service_ns: u64,
    dof: DofBucket,
    completed: bool,
    pub(crate) ack_output: Output<TrackedPacket>,
    recorder: Recorder,
    mailbox_tracker: MailboxTracker,
    ownership: OwnershipLedger,
}

impl TreeReceiverEndpoint {
    const TRANSPORT_RESUME_SID: SchedulableId<Self, ()> = SchedulableId::__from_decorated(0);
    const RUNTIME_SERVICE_SID: SchedulableId<Self, ()> = SchedulableId::__from_decorated(1);
    const DECODER_START_SID: SchedulableId<Self, ()> = SchedulableId::__from_decorated(2);
    const DECODER_FINISH_SID: SchedulableId<Self, ()> = SchedulableId::__from_decorated(3);

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        component: &'static str,
        mailbox: &'static str,
        reverse_link_mailbox: &'static str,
        receive_owner: &'static str,
        runtime_owner: &'static str,
        data_inbox_owner: &'static str,
        service_owner: &'static str,
        flow_id: usize,
        socket: SocketPairConfig,
        stream: FramedStream,
        maximum_frame_payload: usize,
        source_symbols: usize,
        runtime_command_capacity: usize,
        data_inbox_capacity: usize,
        control_inbox_capacity: usize,
        timing: ReceiverTiming,
        runtime_command_service_ns: u64,
        decoder_sink_service_ns: u64,
        recorder: Recorder,
        mailbox_tracker: MailboxTracker,
        ownership: OwnershipLedger,
    ) -> Result<Self, TreeReceiverBuildError> {
        let frame_wire_bytes = stream.frame_wire_bytes();
        for capacity in [
            runtime_command_capacity,
            data_inbox_capacity,
            control_inbox_capacity,
        ] {
            let _ = capacity
                .checked_mul(frame_wire_bytes)
                .ok_or(TreeReceiverBuildError::GeometryOverflow)?;
        }
        for owner in [
            receive_owner,
            runtime_owner,
            data_inbox_owner,
            service_owner,
        ] {
            ownership.set(owner, 0);
        }
        Ok(Self {
            component,
            mailbox,
            reverse_link_mailbox,
            receive_owner,
            runtime_owner,
            data_inbox_owner,
            service_owner,
            flow_id,
            receiver: TcpSocketReceiver::new(flow_id, 0, socket.socket)?,
            stream,
            assembler: FrameAssembler::new(maximum_frame_payload),
            frame_wire_bytes,
            runtime_commands: VecDeque::new(),
            runtime_command_capacity,
            runtime_busy: false,
            inboxes: HybridInboxes::new(data_inbox_capacity, control_inbox_capacity),
            service_start_scheduled: false,
            in_service: None,
            timing,
            runtime_command_service_ns,
            decoder_sink_service_ns,
            dof: DofBucket::new(source_symbols),
            completed: false,
            ack_output: Output::default(),
            recorder,
            mailbox_tracker,
            ownership,
        })
    }

    pub(crate) async fn segment(&mut self, tracked: TrackedPacket, context: &Context<Self>) {
        let packet = tracked.arrive(self.mailbox);
        let now = now_ns(context);
        match self.receiver.receive_segment(&packet, seconds_from_ns(now)) {
            Ok(outcome) => {
                self.update_receive_owner();
                self.send_ack(outcome.acknowledgment, now).await;
                if let Some(delivered) = outcome.delivered {
                    self.ingest_transport_delivery(delivered, now, context);
                }
            }
            Err(error) => self.recorder.fail(error),
        }
    }

    async fn transport_resume(&mut self, _: (), context: &Context<Self>) {
        self.mailbox_tracker.dequeue(self.mailbox);
        let now = now_ns(context);
        let credit = match self
            .runtime_command_capacity
            .checked_mul(self.frame_wire_bytes)
        {
            Some(credit) => credit,
            None => {
                self.recorder.fail("receiver runtime-credit overflow");
                return;
            }
        };
        self.recorder.record(
            now,
            self.component,
            "transport_read_resume",
            self.flow_id,
            self.receiver.application_read_sequence(),
            credit,
            self.receiver.receive_buffered_bytes(),
        );
        self.grant_transport_credit(credit, now, context).await;
    }

    async fn runtime_service(&mut self, _: (), context: &Context<Self>) {
        self.mailbox_tracker.dequeue(self.mailbox);
        let now = now_ns(context);
        self.runtime_busy = false;
        let Some(frame) = self.runtime_commands.pop_front() else {
            self.recorder
                .fail("runtime service fired without a command");
            return;
        };
        self.update_runtime_owner();
        self.recorder.record(
            now,
            self.component,
            "runtime_command_dispatch",
            self.flow_id,
            frame.frame_id,
            frame.wire_bytes,
            self.runtime_commands.len(),
        );
        self.dispatch_runtime_frame(frame, now, context);
        self.grant_transport_credit(frame.wire_bytes, now, context)
            .await;
        self.schedule_runtime_if_needed(context);
    }

    async fn decoder_start(&mut self, _: (), context: &Context<Self>) {
        self.mailbox_tracker.dequeue(self.mailbox);
        self.service_start_scheduled = false;
        let now = now_ns(context);
        if self.in_service.is_some() {
            self.recorder.fail("decoder service double-started");
            return;
        }
        let Some(frame) = self.inboxes.data.pop_front() else {
            return;
        };
        self.update_data_inbox_owner();
        self.in_service = Some(frame);
        self.ownership.set(self.service_owner, frame.wire_bytes);
        self.recorder.record(
            now,
            self.component,
            "decoder_sink_service_start",
            self.flow_id,
            frame.frame_id,
            frame.wire_bytes,
            self.inboxes.data.len(),
        );
        self.mailbox_tracker.enqueue(self.mailbox);
        if let Err(error) = context.schedule_event(
            Duration::from_nanos(self.decoder_sink_service_ns),
            &Self::DECODER_FINISH_SID,
            (),
        ) {
            self.mailbox_tracker.dequeue(self.mailbox);
            self.recorder.fail(error);
        }
    }

    async fn decoder_finish(&mut self, _: (), context: &Context<Self>) {
        self.mailbox_tracker.dequeue(self.mailbox);
        let now = now_ns(context);
        let Some(frame) = self.in_service.take() else {
            self.recorder
                .fail("decoder service finished without an in-service frame");
            return;
        };
        self.ownership.set(self.service_owner, 0);
        let observation = self.dof.observe(frame.frame_id);
        self.recorder.record(
            now,
            self.component,
            "frame_delivered",
            self.flow_id,
            frame.frame_id,
            frame.wire_bytes,
            observation.rank,
        );
        self.recorder.record(
            now,
            self.component,
            if observation.innovative {
                "dof_innovative"
            } else {
                "dof_duplicate"
            },
            self.flow_id,
            frame.frame_id,
            frame.wire_bytes,
            observation.rank,
        );
        if observation.complete && !self.completed {
            self.completed = true;
            self.recorder.record(
                now,
                self.component,
                "stream_complete",
                self.flow_id,
                self.receiver.application_read_sequence(),
                self.stream.total_bytes(),
                observation.rank,
            );
        }
        self.schedule_decoder_if_needed(now, context);
    }

    async fn grant_transport_credit(
        &mut self,
        byte_count: usize,
        now: u64,
        context: &Context<Self>,
    ) {
        match self
            .receiver
            .grant_read_credit(byte_count, seconds_from_ns(now))
        {
            Ok(outcome) => {
                self.update_receive_owner();
                self.send_ack(outcome.acknowledgment, now).await;
                if let Some(delivered) = outcome.delivered {
                    self.ingest_transport_delivery(delivered, now, context);
                }
            }
            Err(error) => self.recorder.fail(error),
        }
    }

    fn ingest_transport_delivery(
        &mut self,
        delivered: DeliveredBytes,
        now: u64,
        context: &Context<Self>,
    ) {
        self.recorder.record(
            now,
            self.component,
            "transport_application_read",
            self.flow_id,
            delivered.stream_offset,
            delivered.byte_count,
            self.receiver.application_read_sequence(),
        );
        let frames =
            match self
                .assembler
                .ingest(&self.stream, delivered.stream_offset, delivered.byte_count)
            {
                Ok(frames) => frames,
                Err(error) => {
                    self.recorder.fail(error);
                    return;
                }
            };
        for frame in frames {
            if self.runtime_commands.len() >= self.runtime_command_capacity {
                self.recorder
                    .fail("shared runtime command mailbox exceeded modeled capacity");
                return;
            }
            let Some(frame_end_sequence) = frame
                .frame_id
                .checked_add(1)
                .and_then(|ordinal| ordinal.checked_mul(frame.wire_bytes))
            else {
                self.recorder.fail("receiver frame sequence overflow");
                return;
            };
            let transport_acked_through = self.receiver.next_sequence_expected();
            if transport_acked_through < frame_end_sequence {
                self.recorder.fail(format_args!(
                    "{} runtime delivery preceded TCP acknowledgment: {} < {}",
                    self.component, transport_acked_through, frame_end_sequence
                ));
                return;
            }
            self.recorder.record(
                now,
                self.component,
                "transport_frame_delivered",
                self.flow_id,
                frame.frame_id,
                frame.wire_bytes,
                transport_acked_through,
            );
            self.runtime_commands.push_back(RuntimeFrame {
                frame_id: frame.frame_id,
                wire_bytes: frame.wire_bytes,
                frame_end_sequence,
                transport_acked_through,
                kind: RuntimeFrameKind::Data,
            });
            self.update_runtime_owner();
            self.recorder.record(
                now,
                self.component,
                "runtime_command_enqueue",
                self.flow_id,
                frame.frame_id,
                frame.wire_bytes,
                self.runtime_commands.len(),
            );
        }
        self.schedule_runtime_if_needed(context);
    }

    fn dispatch_runtime_frame(&mut self, frame: RuntimeFrame, now: u64, context: &Context<Self>) {
        if frame.transport_acked_through < frame.frame_end_sequence {
            self.recorder
                .fail("runtime dispatch observed an unacknowledged transport frame");
            return;
        }
        match self.inboxes.admit(frame) {
            InboxAdmission::DataQueued => {
                self.update_data_inbox_owner();
                self.recorder.record(
                    now,
                    self.component,
                    "data_inbox_enqueue",
                    self.flow_id,
                    frame.frame_id,
                    frame.wire_bytes,
                    self.inboxes.data.len(),
                );
                self.schedule_decoder_if_needed(now, context);
            }
            InboxAdmission::DataDropped => {
                self.recorder.record(
                    now,
                    self.component,
                    "data_inbox_drop_after_tcp_ack",
                    self.flow_id,
                    frame.frame_id,
                    frame.wire_bytes,
                    frame.transport_acked_through,
                );
            }
            InboxAdmission::ControlQueued => {
                self.recorder.record(
                    now,
                    self.component,
                    "control_inbox_enqueue",
                    self.flow_id,
                    frame.frame_id,
                    frame.wire_bytes,
                    self.inboxes.control.len(),
                );
            }
            InboxAdmission::ControlBackpressured => {
                self.recorder.record(
                    now,
                    self.component,
                    "control_inbox_backpressure",
                    self.flow_id,
                    frame.frame_id,
                    frame.wire_bytes,
                    self.inboxes.control.len(),
                );
            }
        }
    }

    fn schedule_runtime_if_needed(&mut self, context: &Context<Self>) {
        if self.runtime_busy || self.runtime_commands.is_empty() {
            return;
        }
        self.runtime_busy = true;
        self.mailbox_tracker.enqueue(self.mailbox);
        if let Err(error) = context.schedule_event(
            Duration::from_nanos(self.runtime_command_service_ns),
            &Self::RUNTIME_SERVICE_SID,
            (),
        ) {
            self.runtime_busy = false;
            self.mailbox_tracker.dequeue(self.mailbox);
            self.recorder.fail(error);
        }
    }

    fn schedule_decoder_if_needed(&mut self, now: u64, context: &Context<Self>) {
        if self.service_start_scheduled || self.in_service.is_some() || self.inboxes.data.is_empty()
        {
            return;
        }
        self.service_start_scheduled = true;
        let until_start = self.timing.service_start_at_ns.saturating_sub(now);
        let delay = until_start.max(DECISION_DELTA_NS);
        self.mailbox_tracker.enqueue(self.mailbox);
        if let Err(error) =
            context.schedule_event(Duration::from_nanos(delay), &Self::DECODER_START_SID, ())
        {
            self.service_start_scheduled = false;
            self.mailbox_tracker.dequeue(self.mailbox);
            self.recorder.fail(error);
        }
    }

    async fn send_ack(&mut self, acknowledgment: Packet, now: u64) {
        self.recorder.record(
            now,
            self.component,
            "tcp_ack_emit",
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
                self.reverse_link_mailbox,
                &self.mailbox_tracker,
            ))
            .await;
    }

    fn update_receive_owner(&self) {
        self.ownership
            .set(self.receive_owner, self.receiver.receive_buffered_bytes());
    }

    fn update_runtime_owner(&self) {
        let bytes = self
            .runtime_commands
            .iter()
            .map(|frame| frame.wire_bytes)
            .sum();
        self.ownership.set(self.runtime_owner, bytes);
    }

    fn update_data_inbox_owner(&self) {
        self.ownership
            .set(self.data_inbox_owner, self.inboxes.data_bytes());
    }
}

impl Model for TreeReceiverEndpoint {
    type Env = ();

    fn register_schedulables(
        context: &mut BuildContext<impl ProtoModel<Model = Self>>,
    ) -> ModelRegistry {
        let mut registry = ModelRegistry::default();
        registry.add(context.register_schedulable(Self::transport_resume));
        registry.add(context.register_schedulable(Self::runtime_service));
        registry.add(context.register_schedulable(Self::decoder_start));
        registry.add(context.register_schedulable(Self::decoder_finish));
        registry
    }

    async fn init(self, context: &Context<Self>, _: &mut Self::Env) -> InitializedModel<Self> {
        self.mailbox_tracker.enqueue(self.mailbox);
        if let Err(error) = context.schedule_event(
            Duration::from_nanos(self.timing.transport_resume_at_ns),
            &Self::TRANSPORT_RESUME_SID,
            (),
        ) {
            self.mailbox_tracker.dequeue(self.mailbox);
            self.recorder.fail(error);
        }
        self.into()
    }
}

#[derive(Debug, Error)]
pub(crate) enum TreeReceiverBuildError {
    #[error(transparent)]
    Tcp(#[from] days::flows::tcp_socket::TcpSocketError),
    #[error("receiver runtime-command geometry overflows usize")]
    GeometryOverflow,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn frame(kind: RuntimeFrameKind, frame_id: usize) -> RuntimeFrame {
        RuntimeFrame {
            frame_id,
            wire_bytes: 512,
            frame_end_sequence: (frame_id + 1) * 512,
            transport_acked_through: (frame_id + 1) * 512,
            kind,
        }
    }

    #[test]
    fn full_data_lane_drops_data_without_consuming_control_capacity() {
        let mut inboxes = HybridInboxes::new(1, 1);
        assert_eq!(
            inboxes.admit(frame(RuntimeFrameKind::Data, 0)),
            InboxAdmission::DataQueued
        );
        assert_eq!(
            inboxes.admit(frame(RuntimeFrameKind::Data, 1)),
            InboxAdmission::DataDropped
        );
        assert_eq!(
            inboxes.admit(frame(RuntimeFrameKind::Control, 2)),
            InboxAdmission::ControlQueued
        );
    }
}
