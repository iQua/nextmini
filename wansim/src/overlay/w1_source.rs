use std::collections::BTreeSet;
use std::time::Duration;

use days::flows::packet::Packet;
use days::flows::tcp_socket::TcpSocketSender;
use nexosim::model::{
    BuildContext, Context, InitializedModel, Model, ModelRegistry, ProtoModel, SchedulableId,
};
use nexosim::ports::Output;
use thiserror::Error;

use crate::determinism::DECISION_DELTA_NS;
use crate::metrics::{MailboxTracker, Recorder, TrackedPacket};
use crate::protocol::{
    CarouselConfigError, CarouselSender, CarouselSenderState, CarouselTiming, ControlFrame,
    ProtocolKind, RoundsSender, RoundsSenderState, StripeSender, StripeSenderMode,
};
use crate::transport::{SocketPairConfig, emit_packets, now_ns, seconds_from_ns};

use super::{ControlRx, ControlStream, ControlTx};

const TREE_COUNT: usize = 2;
const MAX_PEERS: usize = 8;

#[derive(Clone, Debug)]
pub(crate) enum W1SourceProtocol {
    Carousel(CarouselSender),
    Rounds(RoundsSender),
    Striped(StripeSender),
}

impl W1SourceProtocol {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        kind: ProtocolKind,
        source_symbols: usize,
        quotas: Vec<usize>,
        peers: &[u64],
        ready_at_ns: u64,
        timing: CarouselTiming,
    ) -> Result<Self, W1SourceBuildError> {
        Ok(match kind {
            ProtocolKind::PooledCarousel => Self::Carousel(CarouselSender::new(
                1,
                peers.iter().copied(),
                ready_at_ns,
                timing,
            )?),
            ProtocolKind::PooledRounds => {
                Self::Rounds(RoundsSender::new(source_symbols, peers.iter().copied()))
            }
            ProtocolKind::EqualSplitStriping | ProtocolKind::RateProportionalStriping => {
                Self::Striped(StripeSender::new(
                    StripeSenderMode::Finite,
                    quotas,
                    peers.iter().copied(),
                ))
            }
            ProtocolKind::PerStripeFec => Self::Striped(StripeSender::new(
                StripeSenderMode::ContinuousFec,
                quotas,
                peers.iter().copied(),
            )),
        })
    }

    fn next_pooled_emission(&mut self) -> bool {
        match self {
            Self::Carousel(sender) => sender.next_data_emission().is_some(),
            Self::Rounds(sender) => sender.next_data_emission().is_some(),
            Self::Striped(_) => false,
        }
    }

    fn next_striped_emission(&mut self, writable_trees: &BTreeSet<usize>) -> Option<usize> {
        match self {
            Self::Striped(sender) => sender.next_emission(writable_trees).map(|(tree, _)| tree),
            Self::Carousel(_) | Self::Rounds(_) => None,
        }
    }

    fn poll_controls(&mut self, now_ns: u64, recorder: &Recorder) -> Vec<(u64, ControlFrame)> {
        match self {
            Self::Carousel(sender) => match sender.poll(now_ns) {
                Ok(controls) => controls
                    .into_iter()
                    .map(|control| (control.peer_id, control.frame))
                    .collect(),
                Err(error) => {
                    recorder.fail(error);
                    Vec::new()
                }
            },
            Self::Rounds(sender) => sender.poll_controls(),
            Self::Striped(_) => Vec::new(),
        }
    }

    fn on_control(&mut self, peer_id: u64, frame: &ControlFrame, now_ns: u64, recorder: &Recorder) {
        match (self, frame) {
            (Self::Carousel(sender), ControlFrame::BlockAck(ack)) => {
                if let Err(error) = sender.on_block_ack(peer_id, ack, now_ns) {
                    recorder.fail(error);
                }
            }
            (Self::Rounds(sender), ControlFrame::Need { round_id, deficit }) => {
                sender.on_need(peer_id, *round_id, *deficit);
            }
            (Self::Striped(sender), ControlFrame::StripeAck { stripe_id }) => {
                sender.on_ack(peer_id, *stripe_id);
            }
            _ => {}
        }
    }

    fn is_pooled(&self) -> bool {
        matches!(self, Self::Carousel(_) | Self::Rounds(_))
    }

    fn finished(&self) -> bool {
        match self {
            Self::Carousel(sender) => sender.state() == CarouselSenderState::Finished,
            Self::Rounds(sender) => sender.state() == RoundsSenderState::Finished,
            Self::Striped(sender) => sender.all_complete(),
        }
    }
}

struct SourceControlPeer {
    peer_id: u64,
    downlink: ControlTx,
    uplink: ControlRx,
}

pub(crate) struct W1SourceEndpoint {
    component: &'static str,
    mailbox: &'static str,
    data_senders: [TcpSocketSender; TREE_COUNT],
    data_flow_ids: [usize; TREE_COUNT],
    data_forward_mailboxes: [&'static str; TREE_COUNT],
    data_frame_wire_bytes: usize,
    maximum_frames_per_tree: usize,
    emitted_per_tree: [usize; TREE_COUNT],
    total_emissions: usize,
    completion_recorded: bool,
    next_pool_tree: usize,
    protocol: W1SourceProtocol,
    controls: Vec<SourceControlPeer>,
    control_forward_mailboxes: Vec<&'static str>,
    timer_interval_ns: u64,
    drive_scheduled: bool,
    pub(crate) data_outputs: [Output<TrackedPacket>; TREE_COUNT],
    pub(crate) control_forward_outputs: [Output<TrackedPacket>; MAX_PEERS],
    recorder: Recorder,
    mailbox_tracker: MailboxTracker,
}

impl W1SourceEndpoint {
    const START_SID: SchedulableId<Self, ()> = SchedulableId::__from_decorated(0);
    const TIMER_SID: SchedulableId<Self, ()> = SchedulableId::__from_decorated(1);
    const DRIVE_SID: SchedulableId<Self, ()> = SchedulableId::__from_decorated(2);

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        component: &'static str,
        mailbox: &'static str,
        data_flow_ids: [usize; TREE_COUNT],
        data_forward_mailboxes: [&'static str; TREE_COUNT],
        data_socket: SocketPairConfig,
        data_frame_wire_bytes: usize,
        maximum_frames_per_tree: usize,
        protocol: W1SourceProtocol,
        peer_ids: &[u64],
        control_flow_ids: &[(usize, usize)],
        control_forward_mailboxes: Vec<&'static str>,
        control_socket: SocketPairConfig,
        downlink_streams: &[ControlStream],
        uplink_streams: &[ControlStream],
        timer_interval_ns: u64,
        recorder: Recorder,
        mailbox_tracker: MailboxTracker,
    ) -> Result<Self, W1SourceBuildError> {
        if peer_ids.len() > MAX_PEERS
            || peer_ids.len() != control_flow_ids.len()
            || peer_ids.len() != control_forward_mailboxes.len()
            || peer_ids.len() != downlink_streams.len()
            || peer_ids.len() != uplink_streams.len()
        {
            return Err(W1SourceBuildError::PeerGeometry);
        }
        let data_senders = [
            TcpSocketSender::new_reno(data_flow_ids[0], 0, data_socket.socket)?,
            TcpSocketSender::new_reno(data_flow_ids[1], 0, data_socket.socket)?,
        ];
        let controls = peer_ids
            .iter()
            .copied()
            .zip(control_flow_ids.iter().copied())
            .zip(downlink_streams.iter().cloned())
            .zip(uplink_streams.iter().cloned())
            .map(
                |(((peer_id, (downlink_flow, uplink_flow)), downlink_stream), uplink_stream)| {
                    Ok(SourceControlPeer {
                        peer_id,
                        downlink: ControlTx::new(downlink_flow, control_socket, downlink_stream)?,
                        uplink: ControlRx::new(uplink_flow, control_socket, uplink_stream)?,
                    })
                },
            )
            .collect::<Result<Vec<_>, days::flows::tcp_socket::TcpSocketError>>()?;
        Ok(Self {
            component,
            mailbox,
            data_senders,
            data_flow_ids,
            data_forward_mailboxes,
            data_frame_wire_bytes,
            maximum_frames_per_tree,
            emitted_per_tree: [0; TREE_COUNT],
            total_emissions: 0,
            completion_recorded: false,
            next_pool_tree: 0,
            protocol,
            controls,
            control_forward_mailboxes,
            timer_interval_ns,
            drive_scheduled: false,
            data_outputs: std::array::from_fn(|_| Output::default()),
            control_forward_outputs: std::array::from_fn(|_| Output::default()),
            recorder,
            mailbox_tracker,
        })
    }

    pub(crate) async fn data0_ack(&mut self, tracked: TrackedPacket, context: &Context<Self>) {
        self.data_ack(0, tracked, context).await;
    }

    pub(crate) async fn data1_ack(&mut self, tracked: TrackedPacket, context: &Context<Self>) {
        self.data_ack(1, tracked, context).await;
    }

    pub(crate) async fn control_peer0_packet(
        &mut self,
        tracked: TrackedPacket,
        context: &Context<Self>,
    ) {
        self.control_packet(0, tracked, context).await;
    }

    pub(crate) async fn control_peer1_packet(
        &mut self,
        tracked: TrackedPacket,
        context: &Context<Self>,
    ) {
        self.control_packet(1, tracked, context).await;
    }

    pub(crate) async fn control_peer2_packet(
        &mut self,
        tracked: TrackedPacket,
        context: &Context<Self>,
    ) {
        self.control_packet(2, tracked, context).await;
    }

    pub(crate) async fn control_peer3_packet(
        &mut self,
        tracked: TrackedPacket,
        context: &Context<Self>,
    ) {
        self.control_packet(3, tracked, context).await;
    }

    pub(crate) async fn control_peer4_packet(
        &mut self,
        tracked: TrackedPacket,
        context: &Context<Self>,
    ) {
        self.control_packet(4, tracked, context).await;
    }

    pub(crate) async fn control_peer5_packet(
        &mut self,
        tracked: TrackedPacket,
        context: &Context<Self>,
    ) {
        self.control_packet(5, tracked, context).await;
    }

    pub(crate) async fn control_peer6_packet(
        &mut self,
        tracked: TrackedPacket,
        context: &Context<Self>,
    ) {
        self.control_packet(6, tracked, context).await;
    }

    pub(crate) async fn control_peer7_packet(
        &mut self,
        tracked: TrackedPacket,
        context: &Context<Self>,
    ) {
        self.control_packet(7, tracked, context).await;
    }

    async fn start(&mut self, _: (), context: &Context<Self>) {
        self.mailbox_tracker.dequeue(self.mailbox);
        let now = now_ns(context);
        for peer_index in 0..self.controls.len() {
            match self.controls[peer_index].uplink.start(now) {
                Ok(packets) => self.emit_control_forward(peer_index, packets).await,
                Err(error) => self.recorder.fail(error),
            }
        }
        self.request_drive(context);
        self.schedule_timer(context);
    }

    async fn timer(&mut self, _: (), context: &Context<Self>) {
        self.mailbox_tracker.dequeue(self.mailbox);
        let now = now_ns(context);
        for tree in 0..TREE_COUNT {
            match self.data_senders[tree].timer_tick(seconds_from_ns(now)) {
                Ok(packets) => self.emit_data(tree, packets).await,
                Err(error) => self.recorder.fail(error),
            }
        }
        for peer_index in 0..self.controls.len() {
            match self.controls[peer_index].downlink.timer(now) {
                Ok(packets) => self.emit_control_forward(peer_index, packets).await,
                Err(error) => self.recorder.fail(error),
            }
        }
        self.request_drive(context);
        self.schedule_timer(context);
    }

    async fn data_ack(&mut self, tree: usize, tracked: TrackedPacket, context: &Context<Self>) {
        let packet = tracked.arrive(self.mailbox);
        let now = now_ns(context);
        match self.data_senders[tree].receive_ack(&packet, seconds_from_ns(now)) {
            Ok(packets) => self.emit_data(tree, packets).await,
            Err(error) => self.recorder.fail(error),
        }
        self.request_drive(context);
    }

    async fn control_packet(
        &mut self,
        peer_index: usize,
        tracked: TrackedPacket,
        context: &Context<Self>,
    ) {
        let packet = tracked.arrive(self.mailbox);
        let now = now_ns(context);
        let Some(peer) = self.controls.get_mut(peer_index) else {
            self.recorder
                .fail("control packet targeted inactive peer slot");
            return;
        };
        if packet.flow_id == peer.downlink.flow_id() && packet.ack.is_some() {
            match peer.downlink.receive_ack(&packet, now) {
                Ok(packets) => self.emit_control_forward(peer_index, packets).await,
                Err(error) => self.recorder.fail(error),
            }
        } else if packet.flow_id == peer.uplink.flow_id() && packet.ack.is_none() {
            match peer.uplink.receive(&packet, now) {
                Ok((acknowledgments, frames)) => {
                    for frame in frames {
                        let (event, value) = match &frame {
                            ControlFrame::BlockAck(ack) => {
                                ("block_ack_received", ack.completed_watermark as usize)
                            }
                            ControlFrame::Need { deficit, .. } => {
                                ("round_deficit_received", *deficit)
                            }
                            ControlFrame::StripeAck { stripe_id } => {
                                ("stripe_ack_received", *stripe_id)
                            }
                            _ => ("protocol_control_received", 0),
                        };
                        self.recorder.record(
                            now,
                            self.component,
                            event,
                            packet.flow_id,
                            peer_index,
                            frame.payload_bytes() + 4,
                            value,
                        );
                        self.protocol
                            .on_control(peer.peer_id, &frame, now, &self.recorder);
                    }
                    self.emit_control_forward(peer_index, acknowledgments).await;
                }
                Err(error) => self.recorder.fail(error),
            }
        } else {
            self.recorder
                .fail("source control packet has wrong direction or flow");
        }
        self.request_drive(context);
    }

    async fn drive_decision(&mut self, _: (), context: &Context<Self>) {
        self.mailbox_tracker.dequeue(self.mailbox);
        self.drive_scheduled = false;
        self.drive(now_ns(context)).await;
    }

    async fn drive(&mut self, now: u64) {
        let controls = self.protocol.poll_controls(now, &self.recorder);
        for (peer_id, frame) in controls {
            let Some(peer) = self
                .controls
                .iter_mut()
                .find(|peer| peer.peer_id == peer_id)
            else {
                self.recorder
                    .fail("protocol emitted control for unknown peer");
                continue;
            };
            peer.downlink.queue(frame);
        }
        self.drive_control(now).await;
        self.drive_data(now).await;

        let controls = self.protocol.poll_controls(now, &self.recorder);
        for (peer_id, frame) in controls {
            let Some(peer) = self
                .controls
                .iter_mut()
                .find(|peer| peer.peer_id == peer_id)
            else {
                self.recorder
                    .fail("protocol emitted control for unknown peer");
                continue;
            };
            peer.downlink.queue(frame);
        }
        self.drive_control(now).await;
        if !self.completion_recorded && self.protocol.finished() {
            self.completion_recorded = true;
            self.recorder.record(
                now,
                self.component,
                "protocol_sender_complete",
                0,
                0,
                0,
                self.total_emissions,
            );
        }
    }

    async fn drive_data(&mut self, now: u64) {
        loop {
            let writable: BTreeSet<_> = (0..TREE_COUNT)
                .filter(|tree| {
                    self.data_senders[*tree].writable_bytes() >= self.data_frame_wire_bytes
                        && self.emitted_per_tree[*tree] < self.maximum_frames_per_tree
                })
                .collect();
            if writable.is_empty() {
                break;
            }
            let tree = if self.protocol.is_pooled() {
                let selected = (0..TREE_COUNT)
                    .map(|offset| (self.next_pool_tree + offset) % TREE_COUNT)
                    .find(|tree| writable.contains(tree));
                let Some(tree) = selected else {
                    break;
                };
                if !self.protocol.next_pooled_emission() {
                    break;
                }
                self.next_pool_tree = (tree + 1) % TREE_COUNT;
                tree
            } else {
                let Some(tree) = self.protocol.next_striped_emission(&writable) else {
                    break;
                };
                tree
            };

            let admission =
                match self.data_senders[tree].admit_application_write(self.data_frame_wire_bytes) {
                    Ok(admission) => admission,
                    Err(error) => {
                        self.recorder.fail(error);
                        break;
                    }
                };
            if admission.accepted_bytes != self.data_frame_wire_bytes
                || admission.blocked_bytes != 0
            {
                self.recorder.fail("data frame admission was not atomic");
                break;
            }
            let local_frame = self.emitted_per_tree[tree];
            self.emitted_per_tree[tree] += 1;
            self.total_emissions += 1;
            self.recorder.record(
                now,
                self.component,
                "data_frame_emitted",
                self.data_flow_ids[tree],
                self.total_emissions - 1,
                self.data_frame_wire_bytes,
                local_frame,
            );
            match self.data_senders[tree].poll_transmit(seconds_from_ns(now)) {
                Ok(packets) => self.emit_data(tree, packets).await,
                Err(error) => {
                    self.recorder.fail(error);
                    break;
                }
            }
        }
    }

    async fn drive_control(&mut self, now: u64) {
        for peer_index in 0..self.controls.len() {
            match self.controls[peer_index].downlink.drive(now) {
                Ok((packets, submitted)) => {
                    for submitted in submitted {
                        let frame = submitted.frame;
                        let wire_bytes = submitted.wire_bytes;
                        let event = match &frame {
                            ControlFrame::AckProbe { .. } => "ack_probe_submitted",
                            ControlFrame::SessionComplete => "session_complete_submitted",
                            ControlFrame::SourceDone { .. } => "source_done_submitted",
                            _ => "protocol_control_submitted",
                        };
                        self.recorder.record(
                            now,
                            self.component,
                            event,
                            self.controls[peer_index].downlink.flow_id(),
                            peer_index,
                            wire_bytes,
                            frame.payload_bytes(),
                        );
                    }
                    self.emit_control_forward(peer_index, packets).await;
                }
                Err(error) => self.recorder.fail(error),
            }
        }
    }

    async fn emit_data(&mut self, tree: usize, packets: Vec<Packet>) {
        emit_packets(
            packets,
            &mut self.data_outputs[tree],
            self.data_forward_mailboxes[tree],
            &self.mailbox_tracker,
        )
        .await;
    }

    async fn emit_control_forward(&mut self, peer_index: usize, packets: Vec<Packet>) {
        emit_packets(
            packets,
            &mut self.control_forward_outputs[peer_index],
            self.control_forward_mailboxes[peer_index],
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

    fn request_drive(&mut self, context: &Context<Self>) {
        if self.drive_scheduled {
            return;
        }
        self.drive_scheduled = true;
        self.mailbox_tracker.enqueue(self.mailbox);
        if let Err(error) = context.schedule_event(
            Duration::from_nanos(DECISION_DELTA_NS),
            &Self::DRIVE_SID,
            (),
        ) {
            self.drive_scheduled = false;
            self.mailbox_tracker.dequeue(self.mailbox);
            self.recorder.fail(error);
        }
    }
}

impl Model for W1SourceEndpoint {
    type Env = ();

    fn register_schedulables(
        context: &mut BuildContext<impl ProtoModel<Model = Self>>,
    ) -> ModelRegistry {
        let mut registry = ModelRegistry::default();
        registry.add(context.register_schedulable(Self::start));
        registry.add(context.register_schedulable(Self::timer));
        registry.add(context.register_schedulable(Self::drive_decision));
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

#[derive(Debug, Error)]
pub(crate) enum W1SourceBuildError {
    #[error(transparent)]
    Tcp(#[from] days::flows::tcp_socket::TcpSocketError),
    #[error(transparent)]
    Carousel(#[from] CarouselConfigError),
    #[error("W1 source peer/control geometry is inconsistent")]
    PeerGeometry,
}
