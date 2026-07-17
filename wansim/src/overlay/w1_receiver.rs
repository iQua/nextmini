use std::collections::VecDeque;
use std::time::Duration;

use days::flows::packet::Packet;
use days::flows::tcp_socket::{DeliveredBytes, TcpSocketReceiver};
use nexosim::model::{
    BuildContext, Context, InitializedModel, Model, ModelRegistry, ProtoModel, SchedulableId,
};
use nexosim::ports::Output;
use thiserror::Error;

use crate::metrics::{MailboxTracker, Recorder, TrackedPacket};
use crate::protocol::{
    CarouselConfigError, CarouselReceiver, CarouselTiming, ControlFrame, ProtocolKind,
    RoundsReceiver, StripeReceiver,
};
use crate::scenario::ReceiverAdmissionPolicy;
use crate::transport::{SocketPairConfig, emit_packets, now_ns, seconds_from_ns};

use super::{ControlRx, ControlStream, ControlTx, FrameAssembler, FramedStream};

const TREE_COUNT: usize = 2;

#[derive(Clone, Debug)]
pub(crate) enum W1ReceiverProtocol {
    Carousel(CarouselReceiver),
    Rounds(RoundsReceiver),
    Striped(StripeReceiver),
    Inactive,
}

impl W1ReceiverProtocol {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        kind: ProtocolKind,
        peer_id: u64,
        source_symbols: usize,
        quotas: Vec<usize>,
        ready_at_ns: u64,
        timing: CarouselTiming,
        active: bool,
    ) -> Result<Self, CarouselConfigError> {
        Self::new_with_ack_units(
            kind,
            peer_id,
            source_symbols,
            quotas,
            ready_at_ns,
            timing,
            active,
            1,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new_with_ack_units(
        kind: ProtocolKind,
        peer_id: u64,
        source_symbols: usize,
        quotas: Vec<usize>,
        ready_at_ns: u64,
        timing: CarouselTiming,
        active: bool,
        ack_progress_units: u64,
    ) -> Result<Self, CarouselConfigError> {
        if !active {
            return Ok(Self::Inactive);
        }
        Ok(match kind {
            ProtocolKind::PooledCarousel => {
                Self::Carousel(CarouselReceiver::new_with_total_blocks(
                    peer_id,
                    source_symbols,
                    ready_at_ns,
                    timing,
                    ack_progress_units,
                )?)
            }
            ProtocolKind::PooledRounds => Self::Rounds(RoundsReceiver::new(source_symbols)),
            ProtocolKind::EqualSplitStriping
            | ProtocolKind::RateProportionalStriping
            | ProtocolKind::PerStripeFec => Self::Striped(StripeReceiver::new(quotas)),
        })
    }

    fn observe_data(
        &mut self,
        tree: usize,
        local_symbol: usize,
        now_ns: u64,
    ) -> Option<ControlFrame> {
        let pooled_symbol = (local_symbol as u64)
            .saturating_mul(TREE_COUNT as u64)
            .saturating_add(tree as u64);
        match self {
            Self::Carousel(receiver) => {
                receiver.observe_symbol(pooled_symbol, now_ns);
                None
            }
            Self::Rounds(receiver) => {
                receiver.observe_symbol(pooled_symbol, now_ns);
                None
            }
            Self::Striped(receiver) => receiver.observe_symbol(tree, local_symbol, now_ns),
            Self::Inactive => None,
        }
    }

    fn on_control(&mut self, frame: &ControlFrame, now_ns: u64) -> Option<ControlFrame> {
        match (self, frame) {
            (Self::Carousel(receiver), _) => receiver.on_control(frame, now_ns),
            (Self::Rounds(receiver), ControlFrame::SourceDone { round_id }) => {
                Some(receiver.on_source_done(*round_id))
            }
            _ => None,
        }
    }

    fn poll(&mut self, now_ns: u64) -> Option<ControlFrame> {
        match self {
            Self::Carousel(receiver) => receiver.poll(now_ns),
            Self::Rounds(_) | Self::Striped(_) | Self::Inactive => None,
        }
    }

    fn local_completion_ns(&self) -> Option<u64> {
        match self {
            Self::Carousel(receiver) => receiver.local_completion_ns(),
            Self::Rounds(receiver) => receiver.local_completion_ns(),
            Self::Striped(receiver) => receiver.local_completion_ns(),
            Self::Inactive => None,
        }
    }
}

struct DataIngress {
    flow_id: usize,
    reverse_link_mailbox: &'static str,
    receiver: TcpSocketReceiver,
    stream: FramedStream,
    assembler: FrameAssembler,
}

struct ReceiverControl {
    reverse_link_mailbox: &'static str,
    downlink: ControlRx,
    uplink: ControlTx,
}

#[derive(Clone, Debug)]
enum RuntimeCommand {
    Data {
        tree: usize,
        frame_id: usize,
        wire_bytes: usize,
        transport_acked_through: usize,
    },
    Control {
        frame: ControlFrame,
        wire_bytes: usize,
    },
}

impl RuntimeCommand {
    fn wire_bytes(&self) -> usize {
        match self {
            Self::Data { wire_bytes, .. } | Self::Control { wire_bytes, .. } => *wire_bytes,
        }
    }

    fn is_data(&self) -> bool {
        matches!(self, Self::Data { .. })
    }
}

#[derive(Clone, Copy, Debug)]
struct DataServiceItem {
    tree: usize,
    frame_id: usize,
    wire_bytes: usize,
}

pub(crate) struct W1ReceiverEndpoint {
    component: &'static str,
    mailbox: &'static str,
    data_ingress: [DataIngress; TREE_COUNT],
    data_frame_wire_bytes: usize,
    runtime_commands: VecDeque<RuntimeCommand>,
    runtime_command_capacity: usize,
    initial_credit_per_tree_frames: usize,
    runtime_busy: bool,
    admission_policy: ReceiverAdmissionPolicy,
    data_inbox: VecDeque<DataServiceItem>,
    data_inbox_capacity: usize,
    decoder_busy: bool,
    in_service: Option<DataServiceItem>,
    runtime_service_ns: u64,
    decoder_sink_service_ns: u64,
    protocol: W1ReceiverProtocol,
    completion_recorded: bool,
    control: Option<ReceiverControl>,
    timer_interval_ns: u64,
    pub(crate) data_ack_outputs: [Output<TrackedPacket>; TREE_COUNT],
    pub(crate) control_reverse_output: Output<TrackedPacket>,
    recorder: Recorder,
    mailbox_tracker: MailboxTracker,
}

impl W1ReceiverEndpoint {
    const START_SID: SchedulableId<Self, ()> = SchedulableId::__from_decorated(0);
    const TIMER_SID: SchedulableId<Self, ()> = SchedulableId::__from_decorated(1);
    const RUNTIME_SID: SchedulableId<Self, ()> = SchedulableId::__from_decorated(2);
    const DECODER_FINISH_SID: SchedulableId<Self, ()> = SchedulableId::__from_decorated(3);

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        component: &'static str,
        mailbox: &'static str,
        data_flow_ids: [usize; TREE_COUNT],
        data_reverse_mailboxes: [&'static str; TREE_COUNT],
        data_socket: SocketPairConfig,
        streams: [FramedStream; TREE_COUNT],
        maximum_frame_payload: usize,
        runtime_command_capacity: usize,
        data_inbox_capacity: usize,
        admission_policy: ReceiverAdmissionPolicy,
        initial_credit_per_tree_frames: usize,
        runtime_service_ns: u64,
        decoder_sink_service_ns: u64,
        protocol: W1ReceiverProtocol,
        control_geometry: Option<ReceiverControlGeometry>,
        control_socket: SocketPairConfig,
        timer_interval_ns: u64,
        recorder: Recorder,
        mailbox_tracker: MailboxTracker,
    ) -> Result<Self, W1ReceiverBuildError> {
        let data_frame_wire_bytes = streams[0].frame_wire_bytes();
        if streams[1].frame_wire_bytes() != data_frame_wire_bytes {
            return Err(W1ReceiverBuildError::Geometry);
        }
        let [stream0, stream1] = streams;
        let data_ingress = [
            DataIngress {
                flow_id: data_flow_ids[0],
                reverse_link_mailbox: data_reverse_mailboxes[0],
                receiver: TcpSocketReceiver::new(data_flow_ids[0], 0, data_socket.socket)?,
                stream: stream0,
                assembler: FrameAssembler::new(maximum_frame_payload),
            },
            DataIngress {
                flow_id: data_flow_ids[1],
                reverse_link_mailbox: data_reverse_mailboxes[1],
                receiver: TcpSocketReceiver::new(data_flow_ids[1], 0, data_socket.socket)?,
                stream: stream1,
                assembler: FrameAssembler::new(maximum_frame_payload),
            },
        ];
        let control = control_geometry
            .map(
                |geometry| -> Result<ReceiverControl, days::flows::tcp_socket::TcpSocketError> {
                    Ok(ReceiverControl {
                        reverse_link_mailbox: geometry.reverse_link_mailbox,
                        downlink: ControlRx::new(
                            geometry.downlink_flow_id,
                            control_socket,
                            geometry.downlink_stream,
                        )?,
                        uplink: ControlTx::new(
                            geometry.uplink_flow_id,
                            control_socket,
                            geometry.uplink_stream,
                        )?,
                    })
                },
            )
            .transpose()?;
        Ok(Self {
            component,
            mailbox,
            data_ingress,
            data_frame_wire_bytes,
            runtime_commands: VecDeque::new(),
            runtime_command_capacity,
            initial_credit_per_tree_frames,
            runtime_busy: false,
            admission_policy,
            data_inbox: VecDeque::new(),
            data_inbox_capacity,
            decoder_busy: false,
            in_service: None,
            runtime_service_ns,
            decoder_sink_service_ns,
            protocol,
            completion_recorded: false,
            control,
            timer_interval_ns,
            data_ack_outputs: std::array::from_fn(|_| Output::default()),
            control_reverse_output: Output::default(),
            recorder,
            mailbox_tracker,
        })
    }

    pub(crate) async fn data0_segment(&mut self, tracked: TrackedPacket, context: &Context<Self>) {
        self.data_segment(0, tracked, context).await;
    }

    pub(crate) async fn data1_segment(&mut self, tracked: TrackedPacket, context: &Context<Self>) {
        self.data_segment(1, tracked, context).await;
    }

    pub(crate) async fn control_packet(&mut self, tracked: TrackedPacket, context: &Context<Self>) {
        let packet = tracked.arrive(self.mailbox);
        let now = now_ns(context);
        let Some(control) = self.control.as_mut() else {
            self.recorder.fail("inactive receiver got a control packet");
            return;
        };
        if packet.flow_id == control.uplink.flow_id() && packet.ack.is_some() {
            match control.uplink.receive_ack(&packet, now) {
                Ok(packets) => self.emit_control_reverse(packets).await,
                Err(error) => self.recorder.fail(error),
            }
        } else if packet.flow_id == control.downlink.flow_id() && packet.ack.is_none() {
            match control.downlink.receive(&packet, now) {
                Ok((acknowledgments, frames)) => {
                    for frame in frames {
                        self.enqueue_runtime(RuntimeCommand::Control {
                            wire_bytes: frame.payload_bytes() + 4,
                            frame,
                        });
                    }
                    self.emit_control_reverse(acknowledgments).await;
                    self.schedule_runtime(context);
                }
                Err(error) => self.recorder.fail(error),
            }
        } else {
            self.recorder
                .fail("receiver control packet has wrong direction or flow");
        }
        self.drive_control(now).await;
    }

    async fn start(&mut self, _: (), context: &Context<Self>) {
        self.mailbox_tracker.dequeue(self.mailbox);
        let now = now_ns(context);
        let credit = match self
            .initial_credit_per_tree_frames
            .checked_mul(self.data_frame_wire_bytes)
        {
            Some(credit) => credit,
            None => {
                self.recorder.fail("receiver runtime credit overflow");
                return;
            }
        };
        for tree in 0..TREE_COUNT {
            self.grant_data_credit(tree, credit, now, context).await;
        }
        if let Some(control) = self.control.as_mut() {
            match control.downlink.start(now) {
                Ok(packets) => self.emit_control_reverse(packets).await,
                Err(error) => self.recorder.fail(error),
            }
        }
        self.drive_control(now).await;
        self.schedule_timer(context);
    }

    async fn timer(&mut self, _: (), context: &Context<Self>) {
        self.mailbox_tracker.dequeue(self.mailbox);
        let now = now_ns(context);
        if let Some(frame) = self.protocol.poll(now) {
            self.queue_control(frame);
        }
        if let Some(control) = self.control.as_mut() {
            match control.uplink.timer(now) {
                Ok(packets) => self.emit_control_reverse(packets).await,
                Err(error) => self.recorder.fail(error),
            }
        }
        self.drive_control(now).await;
        self.schedule_timer(context);
    }

    async fn runtime_service(&mut self, _: (), context: &Context<Self>) {
        self.mailbox_tracker.dequeue(self.mailbox);
        self.runtime_busy = false;
        let now = now_ns(context);
        let Some(command_index) = self.runnable_runtime_command(now) else {
            return;
        };
        let Some(command) = self.runtime_commands.remove(command_index) else {
            self.recorder.fail("runtime service fired with no command");
            return;
        };
        self.recorder.record(
            now,
            self.component,
            "runtime_command_dispatch",
            0,
            0,
            command.wire_bytes(),
            self.runtime_commands.len(),
        );
        match command {
            RuntimeCommand::Data {
                tree,
                frame_id,
                wire_bytes,
                transport_acked_through,
            } => {
                let mut accepted = false;
                if self.data_inbox.len() >= self.data_inbox_capacity {
                    if self.admission_policy == ReceiverAdmissionPolicy::HybridDrop {
                        self.recorder.record(
                            now,
                            self.component,
                            "data_inbox_drop_after_tcp_ack",
                            self.data_ingress[tree].flow_id,
                            frame_id,
                            wire_bytes,
                            transport_acked_through,
                        );
                    } else {
                        self.recorder
                            .fail("blocking data command dispatched into a full inbox");
                    }
                } else {
                    accepted = true;
                    self.data_inbox.push_back(DataServiceItem {
                        tree,
                        frame_id,
                        wire_bytes,
                    });
                    self.recorder.record(
                        now,
                        self.component,
                        "data_inbox_enqueue",
                        self.data_ingress[tree].flow_id,
                        frame_id,
                        wire_bytes,
                        self.data_inbox.len(),
                    );
                    self.schedule_decoder(context);
                }
                if accepted || self.admission_policy == ReceiverAdmissionPolicy::HybridDrop {
                    self.grant_data_credit(tree, wire_bytes, now, context).await;
                }
            }
            RuntimeCommand::Control { frame, wire_bytes } => {
                let (event, value) = match &frame {
                    ControlFrame::SourceDone { round_id } => {
                        ("source_done_runtime_dispatch", *round_id as usize)
                    }
                    ControlFrame::AckProbe { .. } => ("ack_probe_runtime_dispatch", 0),
                    ControlFrame::SessionComplete => ("session_complete_runtime_dispatch", 0),
                    _ => ("control_lane_dispatch", frame.payload_bytes()),
                };
                self.recorder
                    .record(now, self.component, event, 0, 0, wire_bytes, value);
                if let Some(response) = self.protocol.on_control(&frame, now) {
                    if let ControlFrame::Need { deficit, .. } = &response {
                        self.recorder.record(
                            now,
                            self.component,
                            "round_need_generated",
                            0,
                            0,
                            response.payload_bytes() + 4,
                            *deficit,
                        );
                    }
                    self.queue_control(response);
                }
                self.drive_control(now).await;
            }
        }
        self.schedule_runtime(context);
    }

    async fn decoder_finish(&mut self, _: (), context: &Context<Self>) {
        self.mailbox_tracker.dequeue(self.mailbox);
        self.decoder_busy = false;
        let now = now_ns(context);
        let Some(item) = self.in_service.take() else {
            self.recorder.fail("decoder finished without an item");
            return;
        };
        self.recorder.record(
            now,
            self.component,
            "decoder_sink_complete",
            self.data_ingress[item.tree].flow_id,
            item.frame_id,
            item.wire_bytes,
            item.tree,
        );
        if let Some(response) = self.protocol.observe_data(item.tree, item.frame_id, now) {
            self.queue_control(response);
        }
        if !self.completion_recorded
            && let Some(completion_ns) = self.protocol.local_completion_ns()
        {
            self.completion_recorded = true;
            self.recorder.record(
                completion_ns,
                self.component,
                "protocol_local_complete",
                self.data_ingress[item.tree].flow_id,
                item.frame_id,
                item.wire_bytes,
                item.tree,
            );
        }
        self.drive_control(now).await;
        self.schedule_decoder(context);
        self.schedule_runtime(context);
    }

    async fn data_segment(&mut self, tree: usize, tracked: TrackedPacket, context: &Context<Self>) {
        let packet = tracked.arrive(self.mailbox);
        let now = now_ns(context);
        match self.data_ingress[tree]
            .receiver
            .receive_segment(&packet, seconds_from_ns(now))
        {
            Ok(outcome) => {
                self.emit_data_ack(tree, vec![outcome.acknowledgment]).await;
                if let Some(delivered) = outcome.delivered {
                    self.ingest_data_delivery(tree, delivered, context);
                }
            }
            Err(error) => self.recorder.fail(error),
        }
    }

    async fn grant_data_credit(
        &mut self,
        tree: usize,
        byte_count: usize,
        now: u64,
        context: &Context<Self>,
    ) {
        match self.data_ingress[tree]
            .receiver
            .grant_read_credit(byte_count, seconds_from_ns(now))
        {
            Ok(outcome) => {
                self.emit_data_ack(tree, vec![outcome.acknowledgment]).await;
                if let Some(delivered) = outcome.delivered {
                    self.ingest_data_delivery(tree, delivered, context);
                }
            }
            Err(error) => self.recorder.fail(error),
        }
    }

    fn ingest_data_delivery(
        &mut self,
        tree: usize,
        delivered: DeliveredBytes,
        context: &Context<Self>,
    ) {
        let now = now_ns(context);
        let frames = match self.data_ingress[tree].assembler.ingest(
            &self.data_ingress[tree].stream,
            delivered.stream_offset,
            delivered.byte_count,
        ) {
            Ok(frames) => frames,
            Err(error) => {
                self.recorder.fail(error);
                return;
            }
        };
        for frame in frames {
            let transport_acked_through = self.data_ingress[tree].receiver.next_sequence_expected();
            self.recorder.record(
                now,
                self.component,
                "runtime_command_enqueue_data",
                self.data_ingress[tree].flow_id,
                frame.frame_id,
                frame.wire_bytes,
                self.runtime_commands.len() + 1,
            );
            self.enqueue_runtime(RuntimeCommand::Data {
                tree,
                frame_id: frame.frame_id,
                wire_bytes: frame.wire_bytes,
                transport_acked_through,
            });
        }
        self.schedule_runtime(context);
    }

    fn enqueue_runtime(&mut self, command: RuntimeCommand) {
        if self.runtime_commands.len() >= self.runtime_command_capacity {
            self.recorder
                .fail("shared runtime command mailbox exceeded modeled capacity");
            return;
        }
        self.runtime_commands.push_back(command);
    }

    fn schedule_runtime(&mut self, context: &Context<Self>) {
        if self.runtime_busy || self.runtime_commands.is_empty() {
            return;
        }
        self.runtime_busy = true;
        self.mailbox_tracker.enqueue(self.mailbox);
        if let Err(error) = context.schedule_event(
            Duration::from_nanos(self.runtime_service_ns),
            &Self::RUNTIME_SID,
            (),
        ) {
            self.runtime_busy = false;
            self.mailbox_tracker.dequeue(self.mailbox);
            self.recorder.fail(error);
        }
    }

    fn runnable_runtime_command(&mut self, now: u64) -> Option<usize> {
        let front = self.runtime_commands.front()?;
        if !front.is_data() || self.data_inbox.len() < self.data_inbox_capacity {
            return Some(0);
        }
        let (event, control_index) = match self.admission_policy {
            ReceiverAdmissionPolicy::HybridDrop => return Some(0),
            ReceiverAdmissionPolicy::NaiveBlocking => ("data_inbox_blocking_wait", None),
            ReceiverAdmissionPolicy::IsolatedCredit => (
                "isolated_credit_wait",
                self.runtime_commands
                    .iter()
                    .position(|command| !command.is_data()),
            ),
        };
        let RuntimeCommand::Data {
            tree,
            frame_id,
            wire_bytes,
            transport_acked_through,
        } = front
        else {
            unreachable!("front command was checked as data")
        };
        self.recorder.record(
            now,
            self.component,
            event,
            self.data_ingress[*tree].flow_id,
            *frame_id,
            *wire_bytes,
            *transport_acked_through,
        );
        control_index
    }

    fn schedule_decoder(&mut self, context: &Context<Self>) {
        if self.decoder_busy || self.data_inbox.is_empty() {
            return;
        }
        self.decoder_busy = true;
        self.in_service = self.data_inbox.pop_front();
        self.mailbox_tracker.enqueue(self.mailbox);
        if let Err(error) = context.schedule_event(
            Duration::from_nanos(self.decoder_sink_service_ns),
            &Self::DECODER_FINISH_SID,
            (),
        ) {
            self.decoder_busy = false;
            self.in_service = None;
            self.mailbox_tracker.dequeue(self.mailbox);
            self.recorder.fail(error);
        }
    }

    fn queue_control(&mut self, frame: ControlFrame) {
        if let Some(control) = self.control.as_mut() {
            control.uplink.queue(frame);
        }
    }

    async fn drive_control(&mut self, now: u64) {
        let Some(control) = self.control.as_mut() else {
            return;
        };
        match control.uplink.drive(now) {
            Ok((packets, submitted)) => {
                for submitted in submitted {
                    let frame = submitted.frame;
                    let wire_bytes = submitted.wire_bytes;
                    self.recorder.record(
                        now,
                        self.component,
                        "protocol_control_submitted",
                        control.uplink.flow_id(),
                        0,
                        wire_bytes,
                        frame.payload_bytes(),
                    );
                }
                self.emit_control_reverse(packets).await;
            }
            Err(error) => self.recorder.fail(error),
        }
    }

    async fn emit_data_ack(&mut self, tree: usize, packets: Vec<Packet>) {
        emit_packets(
            packets,
            &mut self.data_ack_outputs[tree],
            self.data_ingress[tree].reverse_link_mailbox,
            &self.mailbox_tracker,
        )
        .await;
    }

    async fn emit_control_reverse(&mut self, packets: Vec<Packet>) {
        let Some(control) = self.control.as_ref() else {
            return;
        };
        emit_packets(
            packets,
            &mut self.control_reverse_output,
            control.reverse_link_mailbox,
            &self.mailbox_tracker,
        )
        .await;
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

impl Model for W1ReceiverEndpoint {
    type Env = ();

    fn register_schedulables(
        context: &mut BuildContext<impl ProtoModel<Model = Self>>,
    ) -> ModelRegistry {
        let mut registry = ModelRegistry::default();
        registry.add(context.register_schedulable(Self::start));
        registry.add(context.register_schedulable(Self::timer));
        registry.add(context.register_schedulable(Self::runtime_service));
        registry.add(context.register_schedulable(Self::decoder_finish));
        registry
    }

    async fn init(self, context: &Context<Self>, _: &mut Self::Env) -> InitializedModel<Self> {
        self.mailbox_tracker.enqueue(self.mailbox);
        if let Err(error) = context.schedule_event(Duration::from_nanos(1), &Self::START_SID, ()) {
            self.mailbox_tracker.dequeue(self.mailbox);
            self.recorder.fail(error);
        }
        self.into()
    }
}

pub(crate) struct ReceiverControlGeometry {
    pub(crate) downlink_flow_id: usize,
    pub(crate) uplink_flow_id: usize,
    pub(crate) reverse_link_mailbox: &'static str,
    pub(crate) downlink_stream: ControlStream,
    pub(crate) uplink_stream: ControlStream,
}

#[derive(Debug, Error)]
pub(crate) enum W1ReceiverBuildError {
    #[error(transparent)]
    Tcp(#[from] days::flows::tcp_socket::TcpSocketError),
    #[error("W1 receiver data-stream geometry differs between trees")]
    Geometry,
}
