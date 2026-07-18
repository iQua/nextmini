use std::collections::VecDeque;
use std::time::Duration;

use days::flows::packet::Packet;
use days::flows::tcp_socket::{DeliveredBytes, TcpSocketReceiver, TcpSocketSender};
use nexosim::model::{
    BuildContext, Context, InitializedModel, Model, ModelRegistry, ProtoModel, SchedulableId,
};
use nexosim::ports::Output;

use crate::metrics::{MailboxTracker, OwnershipLedger, Recorder, TrackedPacket};
use crate::overlay::{FrameAssembler, FramedStream};
use crate::scenario::FanoutAdmission;
use crate::transport::{SocketPairConfig, emit_packets, now_ns, seconds_from_ns};

#[derive(Clone, Copy, Debug)]
pub(crate) struct RelayChildSpec {
    pub(crate) endpoint_component: &'static str,
    pub(crate) flow_id: usize,
    pub(crate) forward_link_mailbox: &'static str,
    pub(crate) queue_owner: &'static str,
    pub(crate) send_owner: &'static str,
    pub(crate) downstream_receive_owner: &'static str,
}

#[derive(Clone, Debug)]
struct PendingFanoutFrame {
    frame_id: usize,
    wire_bytes: usize,
    admitted: Vec<bool>,
}

#[derive(Clone, Copy, Debug)]
struct ChildQueueFrame {
    frame_id: usize,
    remaining_bytes: usize,
}

#[derive(Clone, Copy, Debug, Default)]
struct DeferredFrames {
    next_frame_id: usize,
    end_frame_id: usize,
    wire_bytes: usize,
}

impl DeferredFrames {
    fn is_empty(self) -> bool {
        self.next_frame_id == self.end_frame_id
    }

    fn len(self) -> usize {
        self.end_frame_id.saturating_sub(self.next_frame_id)
    }

    fn push(
        &mut self,
        frame: ChildQueueFrame,
        capacity_frames: usize,
    ) -> Result<usize, &'static str> {
        if self.len() >= capacity_frames {
            return Err("isolated-credit frame debt exceeded its hard session bound");
        }
        if self.is_empty() {
            self.next_frame_id = frame.frame_id;
            self.end_frame_id = frame
                .frame_id
                .checked_add(1)
                .ok_or("isolated-credit frame-id overflow")?;
            self.wire_bytes = frame.remaining_bytes;
            return Ok(1);
        }
        if frame.frame_id != self.end_frame_id {
            return Err("isolated-credit frame debt is not contiguous");
        }
        if frame.remaining_bytes != self.wire_bytes {
            return Err("isolated-credit frame debt changed wire geometry");
        }
        self.end_frame_id = self
            .end_frame_id
            .checked_add(1)
            .ok_or("isolated-credit frame-id overflow")?;
        Ok(self.len())
    }

    fn front(self) -> Option<ChildQueueFrame> {
        (!self.is_empty()).then_some(ChildQueueFrame {
            frame_id: self.next_frame_id,
            remaining_bytes: self.wire_bytes,
        })
    }

    fn pop_front(&mut self) -> Option<ChildQueueFrame> {
        let frame = self.front()?;
        self.next_frame_id = self.next_frame_id.saturating_add(1);
        Some(frame)
    }
}

struct RelayChild {
    spec: RelayChildSpec,
    sender: TcpSocketSender,
    queue: VecDeque<ChildQueueFrame>,
    deferred: DeferredFrames,
    deferred_capacity_frames: usize,
    queue_capacity: usize,
    queue_occupied: usize,
    stream_cursor: usize,
}

pub(crate) struct FanoutRelayEndpoint {
    component: &'static str,
    mailbox: &'static str,
    upstream_flow_id: usize,
    upstream_reverse_link_mailbox: &'static str,
    upstream_receive_owner: &'static str,
    application_owner: &'static str,
    upstream_receiver: TcpSocketReceiver,
    children: Vec<RelayChild>,
    stream: FramedStream,
    assembler: FrameAssembler,
    application_buffer_capacity: usize,
    application_buffer_occupied: usize,
    pending_frames: VecDeque<PendingFanoutFrame>,
    admission: FanoutAdmission,
    timer_interval_ns: u64,
    start_delay_ns: u64,
    pub(crate) upstream_ack_output: Output<TrackedPacket>,
    pub(crate) child_data_outputs: [Output<TrackedPacket>; 4],
    recorder: Recorder,
    mailbox_tracker: MailboxTracker,
    ownership: OwnershipLedger,
}

impl FanoutRelayEndpoint {
    const START_SID: SchedulableId<Self, ()> = SchedulableId::__from_decorated(0);
    const TIMER_SID: SchedulableId<Self, ()> = SchedulableId::__from_decorated(1);

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        component: &'static str,
        mailbox: &'static str,
        upstream_flow_id: usize,
        upstream_reverse_link_mailbox: &'static str,
        upstream_receive_owner: &'static str,
        application_owner: &'static str,
        upstream_socket: SocketPairConfig,
        child_socket: SocketPairConfig,
        child_specs: Vec<RelayChildSpec>,
        stream: FramedStream,
        maximum_frame_payload: usize,
        application_buffer_capacity: usize,
        child_queue_capacity: usize,
        admission: FanoutAdmission,
        timer_interval_ns: u64,
        recorder: Recorder,
        mailbox_tracker: MailboxTracker,
        ownership: OwnershipLedger,
    ) -> Result<Self, days::flows::tcp_socket::TcpSocketError> {
        assert!((1..=4).contains(&child_specs.len()));
        ownership.set(upstream_receive_owner, 0);
        ownership.set(application_owner, 0);
        for spec in &child_specs {
            ownership.set(spec.queue_owner, 0);
            ownership.set(spec.send_owner, 0);
        }
        for (index, spec) in child_specs.iter().enumerate() {
            recorder.record(
                0,
                component,
                "fanout_child_configured",
                spec.flow_id,
                index,
                0,
                index,
            );
            recorder.record(
                0,
                spec.endpoint_component,
                "fanout_parent_configured",
                spec.flow_id,
                index,
                0,
                index,
            );
        }
        let deferred_capacity_frames = stream.frame_count();
        let children = child_specs
            .into_iter()
            .map(|spec| {
                Ok(RelayChild {
                    spec,
                    sender: child_socket.sender(spec.flow_id, 0)?,
                    queue: VecDeque::new(),
                    deferred: DeferredFrames::default(),
                    deferred_capacity_frames,
                    queue_capacity: child_queue_capacity,
                    queue_occupied: 0,
                    stream_cursor: 0,
                })
            })
            .collect::<Result<Vec<_>, days::flows::tcp_socket::TcpSocketError>>()?;
        Ok(Self {
            component,
            mailbox,
            upstream_flow_id,
            upstream_reverse_link_mailbox,
            upstream_receive_owner,
            application_owner,
            upstream_receiver: TcpSocketReceiver::new(upstream_flow_id, 0, upstream_socket.socket)?,
            children,
            stream,
            assembler: FrameAssembler::new(maximum_frame_payload),
            application_buffer_capacity,
            application_buffer_occupied: 0,
            pending_frames: VecDeque::new(),
            admission,
            timer_interval_ns,
            start_delay_ns: 1,
            upstream_ack_output: Output::default(),
            child_data_outputs: std::array::from_fn(|_| Output::default()),
            recorder,
            mailbox_tracker,
            ownership,
        })
    }

    pub(crate) fn set_start_delay_ns(&mut self, start_delay_ns: u64) {
        self.start_delay_ns = start_delay_ns;
    }

    pub(crate) async fn upstream_segment(
        &mut self,
        tracked: TrackedPacket,
        context: &Context<Self>,
    ) {
        self.recorder
            .count_event_class("dispatch_relay_upstream_segment");
        let packet = tracked.arrive(self.mailbox);
        let now = now_ns(context);
        match self
            .upstream_receiver
            .receive_segment(&packet, seconds_from_ns(now))
        {
            Ok(outcome) => {
                self.update_upstream_receive_owner();
                self.send_upstream_ack(outcome.acknowledgment, now).await;
                self.drive(now, outcome.delivered).await;
            }
            Err(error) => self.recorder.fail(error),
        }
    }

    pub(crate) async fn child0_acknowledgment(
        &mut self,
        tracked: TrackedPacket,
        context: &Context<Self>,
    ) {
        self.child_acknowledgment(0, tracked, context).await;
    }

    pub(crate) async fn child1_acknowledgment(
        &mut self,
        tracked: TrackedPacket,
        context: &Context<Self>,
    ) {
        self.child_acknowledgment(1, tracked, context).await;
    }

    pub(crate) async fn child2_acknowledgment(
        &mut self,
        tracked: TrackedPacket,
        context: &Context<Self>,
    ) {
        self.child_acknowledgment(2, tracked, context).await;
    }

    pub(crate) async fn child3_acknowledgment(
        &mut self,
        tracked: TrackedPacket,
        context: &Context<Self>,
    ) {
        self.child_acknowledgment(3, tracked, context).await;
    }

    async fn child_acknowledgment(
        &mut self,
        child_index: usize,
        tracked: TrackedPacket,
        context: &Context<Self>,
    ) {
        self.recorder.count_event_class("dispatch_relay_child_ack");
        let acknowledgment = tracked.arrive(self.mailbox);
        let now = now_ns(context);
        self.recorder.record(
            now,
            self.component,
            "child_ack_arrival",
            acknowledgment.flow_id,
            acknowledgment
                .ack
                .map_or(acknowledgment.packet_id, |ack| ack.sequence_num),
            acknowledgment.size,
            acknowledgment.ack.map_or(0, |ack| ack.advertised_window),
        );
        let packets = match self.children[child_index]
            .sender
            .receive_ack(&acknowledgment, seconds_from_ns(now))
        {
            Ok(packets) => packets,
            Err(error) => {
                self.recorder.fail(error);
                return;
            }
        };
        self.update_child_send_owner(child_index);
        self.emit_child(child_index, packets, now).await;
        self.drive(now, None).await;
    }

    async fn start(&mut self, _: (), context: &Context<Self>) {
        self.recorder.count_event_class("dispatch_relay_start");
        self.mailbox_tracker.dequeue(self.mailbox);
        let now = now_ns(context);
        match self
            .upstream_receiver
            .grant_read_credit(self.application_buffer_capacity, seconds_from_ns(now))
        {
            Ok(outcome) => {
                self.update_upstream_receive_owner();
                self.send_upstream_ack(outcome.acknowledgment, now).await;
                self.drive(now, outcome.delivered).await;
            }
            Err(error) => self.recorder.fail(error),
        }
        self.schedule_timer(context);
    }

    async fn timer(&mut self, _: (), context: &Context<Self>) {
        self.recorder.count_event_class("dispatch_relay_timer");
        self.mailbox_tracker.dequeue(self.mailbox);
        let now = now_ns(context);
        for child_index in 0..self.children.len() {
            let packets = match self.children[child_index]
                .sender
                .timer_tick(seconds_from_ns(now))
            {
                Ok(packets) => packets,
                Err(error) => {
                    self.recorder.fail(error);
                    continue;
                }
            };
            self.emit_child(child_index, packets, now).await;
        }
        self.drive(now, None).await;
        if self.is_active() {
            self.schedule_timer(context);
        }
    }

    async fn drive(&mut self, now: u64, initial_delivery: Option<DeliveredBytes>) {
        let mut deliveries = VecDeque::new();
        if let Some(delivered) = initial_delivery {
            deliveries.push_back(delivered);
        }
        loop {
            while let Some(delivered) = deliveries.pop_front() {
                if !self.process_upstream_delivery(delivered, now) {
                    return;
                }
            }
            let released = self.progress_children(now).await;
            if released == 0 {
                break;
            }
            match self
                .upstream_receiver
                .grant_read_credit(released, seconds_from_ns(now))
            {
                Ok(outcome) => {
                    self.update_upstream_receive_owner();
                    self.send_upstream_ack(outcome.acknowledgment, now).await;
                    if let Some(delivered) = outcome.delivered {
                        deliveries.push_back(delivered);
                    }
                }
                Err(error) => {
                    self.recorder.fail(error);
                    return;
                }
            }
        }
    }

    fn process_upstream_delivery(&mut self, delivered: DeliveredBytes, now: u64) -> bool {
        let Some(occupied) = self
            .application_buffer_occupied
            .checked_add(delivered.byte_count)
        else {
            self.recorder
                .fail("relay application-buffer accounting overflow");
            return false;
        };
        if occupied > self.application_buffer_capacity {
            self.recorder.fail(format_args!(
                "{} application buffer exceeded: {occupied} > {}",
                self.component, self.application_buffer_capacity
            ));
            return false;
        }
        self.application_buffer_occupied = occupied;
        self.update_application_owner();
        self.recorder.record(
            now,
            self.component,
            "upstream_socket_read",
            self.upstream_flow_id,
            delivered.stream_offset,
            delivered.byte_count,
            occupied,
        );
        let frames =
            match self
                .assembler
                .ingest(&self.stream, delivered.stream_offset, delivered.byte_count)
            {
                Ok(frames) => frames,
                Err(error) => {
                    self.recorder.fail(error);
                    return false;
                }
            };
        for frame in frames {
            self.recorder.record(
                now,
                self.component,
                "frame_assembled",
                self.upstream_flow_id,
                frame.frame_id,
                frame.wire_bytes,
                self.application_buffer_occupied,
            );
            self.pending_frames.push_back(PendingFanoutFrame {
                frame_id: frame.frame_id,
                wire_bytes: frame.wire_bytes,
                admitted: vec![false; self.children.len()],
            });
        }
        true
    }

    async fn progress_children(&mut self, now: u64) -> usize {
        let mut released_total = 0_usize;
        loop {
            let mut progressed = false;
            for child_index in 0..self.children.len() {
                progressed |= self.drain_child(child_index, now).await;
            }
            if self.admission == FanoutAdmission::IsolatedCredit {
                for child_index in 0..self.children.len() {
                    progressed |= self.replay_deferred(child_index, now);
                }
            }
            let (released, admitted) = match self.admission {
                FanoutAdmission::Sequential => self.admit_sequential(now),
                FanoutAdmission::Concurrent => self.admit_concurrent(now),
                FanoutAdmission::IsolatedCredit => self.admit_isolated_credit(now),
            };
            released_total = released_total.saturating_add(released);
            progressed |= admitted > 0;
            for child_index in 0..self.children.len() {
                progressed |= self.drain_child(child_index, now).await;
            }
            if !progressed {
                break;
            }
        }
        released_total
    }

    fn admit_sequential(&mut self, now: u64) -> (usize, usize) {
        let mut released = 0_usize;
        let mut admitted = 0_usize;
        while let Some(frame) = self.pending_frames.front().cloned() {
            let Some(child_index) = frame.admitted.iter().position(|value| !value) else {
                self.pending_frames.pop_front();
                self.application_buffer_occupied = self
                    .application_buffer_occupied
                    .saturating_sub(frame.wire_bytes);
                released = released.saturating_add(frame.wire_bytes);
                self.update_application_owner();
                continue;
            };
            if !self.try_admit_frame(0, child_index, frame, now) {
                break;
            }
            admitted = admitted.saturating_add(1);
        }
        (released, admitted)
    }

    fn admit_concurrent(&mut self, now: u64) -> (usize, usize) {
        let mut admitted = 0_usize;
        for child_index in 0..self.children.len() {
            for frame_index in 0..self.pending_frames.len() {
                let frame = self.pending_frames[frame_index].clone();
                if frame.admitted[child_index] {
                    continue;
                }
                if !self.try_admit_frame(frame_index, child_index, frame, now) {
                    break;
                }
                admitted = admitted.saturating_add(1);
            }
        }
        let mut released = 0_usize;
        while self
            .pending_frames
            .front()
            .is_some_and(|frame| frame.admitted.iter().all(|value| *value))
        {
            let Some(frame) = self.pending_frames.pop_front() else {
                break;
            };
            self.application_buffer_occupied = self
                .application_buffer_occupied
                .saturating_sub(frame.wire_bytes);
            released = released.saturating_add(frame.wire_bytes);
            self.update_application_owner();
        }
        (released, admitted)
    }

    fn admit_isolated_credit(&mut self, now: u64) -> (usize, usize) {
        let mut admitted = 0_usize;
        for frame_index in 0..self.pending_frames.len() {
            for child_index in 0..self.children.len() {
                if self.pending_frames[frame_index].admitted[child_index] {
                    continue;
                }
                let frame = self.pending_frames[frame_index].clone();
                if self.children[child_index].deferred.is_empty()
                    && self.try_admit_frame(frame_index, child_index, frame.clone(), now)
                {
                    admitted = admitted.saturating_add(1);
                } else {
                    let child = &mut self.children[child_index];
                    let deferred = ChildQueueFrame {
                        frame_id: frame.frame_id,
                        remaining_bytes: frame.wire_bytes,
                    };
                    let depth = match child
                        .deferred
                        .push(deferred, child.deferred_capacity_frames)
                    {
                        Ok(depth) => depth,
                        Err(error) => {
                            self.recorder.fail(error);
                            0
                        }
                    };
                    self.pending_frames[frame_index].admitted[child_index] = true;
                    self.recorder.record(
                        now,
                        self.component,
                        "isolated_credit_deferred",
                        child.spec.flow_id,
                        frame.frame_id,
                        frame.wire_bytes,
                        depth,
                    );
                    admitted = admitted.saturating_add(1);
                }
            }
        }
        let mut released = 0_usize;
        while self
            .pending_frames
            .front()
            .is_some_and(|frame| frame.admitted.iter().all(|value| *value))
        {
            let Some(frame) = self.pending_frames.pop_front() else {
                break;
            };
            self.application_buffer_occupied = self
                .application_buffer_occupied
                .saturating_sub(frame.wire_bytes);
            released = released.saturating_add(frame.wire_bytes);
            self.update_application_owner();
        }
        (released, admitted)
    }

    fn replay_deferred(&mut self, child_index: usize, now: u64) -> bool {
        let child = &mut self.children[child_index];
        let Some(frame) = child.deferred.front() else {
            return false;
        };
        let can_admit = child
            .queue_occupied
            .checked_add(frame.remaining_bytes)
            .is_some_and(|occupied| occupied <= child.queue_capacity);
        if !can_admit {
            return false;
        }
        let Some(frame) = child.deferred.pop_front() else {
            return false;
        };
        child.queue.push_back(frame);
        child.queue_occupied += frame.remaining_bytes;
        self.ownership
            .set(child.spec.queue_owner, child.queue_occupied);
        self.recorder.record(
            now,
            self.component,
            "isolated_credit_replay",
            child.spec.flow_id,
            frame.frame_id,
            frame.remaining_bytes,
            child.deferred.len(),
        );
        true
    }

    fn try_admit_frame(
        &mut self,
        frame_index: usize,
        child_index: usize,
        frame: PendingFanoutFrame,
        now: u64,
    ) -> bool {
        let child = &mut self.children[child_index];
        let can_admit = child
            .queue_occupied
            .checked_add(frame.wire_bytes)
            .is_some_and(|occupied| occupied <= child.queue_capacity);
        if !can_admit {
            let downstream_owned = child
                .queue_occupied
                .saturating_add(child.sender.send_buffered_bytes())
                .saturating_add(self.ownership.get(child.spec.downstream_receive_owner));
            self.recorder.record(
                now,
                self.component,
                "child_admission_blocked",
                child.spec.flow_id,
                frame.frame_id,
                frame.wire_bytes,
                downstream_owned,
            );
            return false;
        }
        child.queue.push_back(ChildQueueFrame {
            frame_id: frame.frame_id,
            remaining_bytes: frame.wire_bytes,
        });
        child.queue_occupied += frame.wire_bytes;
        self.pending_frames[frame_index].admitted[child_index] = true;
        self.ownership
            .set(child.spec.queue_owner, child.queue_occupied);
        self.recorder.record(
            now,
            self.component,
            "child_queue_admit",
            child.spec.flow_id,
            frame.frame_id,
            frame.wire_bytes,
            child_index,
        );
        true
    }

    async fn drain_child(&mut self, child_index: usize, now: u64) -> bool {
        let mut moved = 0_usize;
        let packets = {
            let child = &mut self.children[child_index];
            while let Some(frame) = child.queue.front_mut() {
                let frame_id = frame.frame_id;
                let admission = match child.sender.admit_application_write(frame.remaining_bytes) {
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
                child.queue_occupied = child
                    .queue_occupied
                    .saturating_sub(admission.accepted_bytes);
                let stream_offset = child.stream_cursor;
                child.stream_cursor = child.stream_cursor.saturating_add(admission.accepted_bytes);
                moved = moved.saturating_add(admission.accepted_bytes);
                self.recorder.record(
                    now,
                    self.component,
                    "child_socket_write",
                    child.spec.flow_id,
                    stream_offset,
                    admission.accepted_bytes,
                    frame_id,
                );
                if frame.remaining_bytes == 0 {
                    child.queue.pop_front();
                }
            }
            match child.sender.poll_transmit(seconds_from_ns(now)) {
                Ok(packets) => packets,
                Err(error) => {
                    self.recorder.fail(error);
                    Vec::new()
                }
            }
        };
        self.update_child_queue_owner(child_index);
        self.update_child_send_owner(child_index);
        self.emit_child(child_index, packets, now).await;
        moved > 0
    }

    async fn emit_child(&mut self, child_index: usize, packets: Vec<Packet>, now: u64) {
        let spec = self.children[child_index].spec;
        let buffered = self.children[child_index].sender.send_buffered_bytes();
        for packet in &packets {
            self.recorder.record(
                now,
                self.component,
                "child_segment_emit",
                packet.flow_id,
                packet.packet_id,
                packet.size,
                buffered,
            );
        }
        emit_packets(
            packets,
            &mut self.child_data_outputs[child_index],
            spec.forward_link_mailbox,
            &self.mailbox_tracker,
        )
        .await;
    }

    async fn send_upstream_ack(&mut self, acknowledgment: Packet, now: u64) {
        self.recorder.record(
            now,
            self.component,
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
                self.upstream_reverse_link_mailbox,
                &self.mailbox_tracker,
            ))
            .await;
    }

    fn update_upstream_receive_owner(&self) {
        self.ownership.set(
            self.upstream_receive_owner,
            self.upstream_receiver.receive_buffered_bytes(),
        );
    }

    fn update_application_owner(&self) {
        self.ownership
            .set(self.application_owner, self.application_buffer_occupied);
    }

    fn update_child_queue_owner(&self, child_index: usize) {
        let child = &self.children[child_index];
        self.ownership
            .set(child.spec.queue_owner, child.queue_occupied);
    }

    fn update_child_send_owner(&self, child_index: usize) {
        let child = &self.children[child_index];
        self.ownership
            .set(child.spec.send_owner, child.sender.send_buffered_bytes());
    }

    fn is_active(&self) -> bool {
        self.upstream_receiver.application_read_sequence() < self.stream.total_bytes()
            || self.application_buffer_occupied > 0
            || self.children.iter().any(|child| {
                child.queue_occupied > 0
                    || !child.deferred.is_empty()
                    || child.sender.send_buffered_bytes() > 0
            })
    }

    fn schedule_timer(&self, context: &Context<Self>) {
        self.mailbox_tracker.enqueue(self.mailbox);
        if let Err(error) = context.schedule_event(
            Duration::from_nanos(self.timer_interval_ns),
            &Self::TIMER_SID,
            (),
        ) {
            self.mailbox_tracker.dequeue(self.mailbox);
            self.recorder.fail(error);
        }
    }
}

impl Model for FanoutRelayEndpoint {
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
        self.mailbox_tracker.enqueue(self.mailbox);
        if let Err(error) = context.schedule_event(
            Duration::from_nanos(self.start_delay_ns),
            &Self::START_SID,
            (),
        ) {
            self.mailbox_tracker.dequeue(self.mailbox);
            self.recorder.fail(error);
        }
        self.into()
    }
}

#[cfg(test)]
mod tests {
    use super::{ChildQueueFrame, DeferredFrames};

    #[test]
    fn isolated_credit_debt_is_a_bounded_contiguous_run() {
        let mut debt = DeferredFrames::default();
        assert_eq!(
            debt.push(
                ChildQueueFrame {
                    frame_id: 7,
                    remaining_bytes: 512,
                },
                2,
            ),
            Ok(1)
        );
        assert_eq!(
            debt.push(
                ChildQueueFrame {
                    frame_id: 8,
                    remaining_bytes: 512,
                },
                2,
            ),
            Ok(2)
        );
        assert!(
            debt.push(
                ChildQueueFrame {
                    frame_id: 9,
                    remaining_bytes: 512,
                },
                2,
            )
            .is_err()
        );
        assert_eq!(debt.pop_front().map(|frame| frame.frame_id), Some(7));
        assert_eq!(debt.pop_front().map(|frame| frame.frame_id), Some(8));
        assert!(debt.is_empty());
    }

    #[test]
    fn isolated_credit_debt_rejects_gaps_and_geometry_changes() {
        let mut gap = DeferredFrames::default();
        gap.push(
            ChildQueueFrame {
                frame_id: 3,
                remaining_bytes: 512,
            },
            4,
        )
        .expect("first debt");
        assert!(
            gap.push(
                ChildQueueFrame {
                    frame_id: 5,
                    remaining_bytes: 512,
                },
                4,
            )
            .is_err()
        );

        let mut geometry = DeferredFrames::default();
        geometry
            .push(
                ChildQueueFrame {
                    frame_id: 3,
                    remaining_bytes: 512,
                },
                4,
            )
            .expect("first debt");
        assert!(
            geometry
                .push(
                    ChildQueueFrame {
                        frame_id: 4,
                        remaining_bytes: 256,
                    },
                    4,
                )
                .is_err()
        );
    }
}
