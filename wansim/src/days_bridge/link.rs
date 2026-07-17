use std::collections::{BTreeSet, VecDeque};
use std::time::Duration;

use days::flows::packet::Packet;
use nexosim::model::{BuildContext, Context, Model, ModelRegistry, ProtoModel, SchedulableId};
use nexosim::ports::Output;

use crate::metrics::{MailboxTracker, Recorder, TrackedPacket};
use crate::transport::{now_ns, seconds_from_ns};

#[derive(Clone, Debug)]
pub(crate) struct PhysicalLinkConfig {
    pub(crate) component: &'static str,
    pub(crate) mailbox: &'static str,
    pub(crate) downstream_mailbox: &'static str,
    pub(crate) rate_bps: u64,
    pub(crate) propagation_ns: u64,
    pub(crate) queue_bytes: usize,
    pub(crate) drop_attempts: BTreeSet<u64>,
}

pub(crate) struct PhysicalLink {
    config: PhysicalLinkConfig,
    waiting: VecDeque<Packet>,
    in_service: Option<Packet>,
    propagating: VecDeque<Packet>,
    occupied_bytes: usize,
    arrival_attempt: u64,
    pub(crate) output: Output<TrackedPacket>,
    recorder: Recorder,
    mailbox_tracker: MailboxTracker,
}

impl PhysicalLink {
    const FINISH_SERIALIZATION_SID: SchedulableId<Self, ()> = SchedulableId::__from_decorated(0);
    const FINISH_PROPAGATION_SID: SchedulableId<Self, ()> = SchedulableId::__from_decorated(1);

    pub(crate) fn new(
        config: PhysicalLinkConfig,
        recorder: Recorder,
        mailbox_tracker: MailboxTracker,
    ) -> Self {
        Self {
            config,
            waiting: VecDeque::new(),
            in_service: None,
            propagating: VecDeque::new(),
            occupied_bytes: 0,
            arrival_attempt: 0,
            output: Output::default(),
            recorder,
            mailbox_tracker,
        }
    }

    pub(crate) async fn receive(&mut self, tracked: TrackedPacket, context: &Context<Self>) {
        let packet = tracked.arrive(self.config.mailbox);
        let now = now_ns(context);
        let attempt = self.arrival_attempt;
        self.arrival_attempt = self.arrival_attempt.saturating_add(1);
        if self.config.drop_attempts.contains(&attempt) {
            self.recorder.record(
                now,
                self.config.component,
                "segment_drop",
                packet.flow_id,
                packet.packet_id,
                packet.size,
                attempt as usize,
            );
            return;
        }
        if self
            .occupied_bytes
            .checked_add(packet.size)
            .is_none_or(|occupied| occupied > self.config.queue_bytes)
        {
            self.recorder.record(
                now,
                self.config.component,
                "queue_drop",
                packet.flow_id,
                packet.packet_id,
                packet.size,
                self.occupied_bytes,
            );
            return;
        }
        self.occupied_bytes += packet.size;
        self.recorder.record(
            now,
            self.config.component,
            "queue_admit",
            packet.flow_id,
            packet.packet_id,
            packet.size,
            self.occupied_bytes,
        );
        self.waiting.push_back(packet);
        if self.in_service.is_none() {
            self.start_next(context);
        }
    }

    fn start_next(&mut self, context: &Context<Self>) {
        let Some(packet) = self.waiting.pop_front() else {
            return;
        };
        let serialization_ns = match serialization_ns(packet.size, self.config.rate_bps) {
            Some(duration) => duration,
            None => {
                self.recorder.fail("link serialization duration overflow");
                return;
            }
        };
        self.recorder.record(
            now_ns(context),
            self.config.component,
            "serialization_start",
            packet.flow_id,
            packet.packet_id,
            packet.size,
            serialization_ns as usize,
        );
        self.in_service = Some(packet);
        self.mailbox_tracker.enqueue(self.config.mailbox);
        if let Err(error) = context.schedule_event(
            Duration::from_nanos(serialization_ns),
            &Self::FINISH_SERIALIZATION_SID,
            (),
        ) {
            self.mailbox_tracker.dequeue(self.config.mailbox);
            self.recorder.fail(error);
        }
    }

    async fn finish_serialization(&mut self, _: (), context: &Context<Self>) {
        self.mailbox_tracker.dequeue(self.config.mailbox);
        let Some(mut packet) = self.in_service.take() else {
            self.recorder
                .fail("serialization completed without a packet");
            return;
        };
        let now = now_ns(context);
        self.occupied_bytes = self.occupied_bytes.saturating_sub(packet.size);
        self.recorder.record(
            now,
            self.config.component,
            "serialization_end",
            packet.flow_id,
            packet.packet_id,
            packet.size,
            self.occupied_bytes,
        );
        let arrival_ns = now.saturating_add(self.config.propagation_ns);
        packet.departure_update(seconds_from_ns(arrival_ns));
        self.propagating.push_back(packet);
        self.mailbox_tracker.enqueue(self.config.mailbox);
        if let Err(error) = context.schedule_event(
            Duration::from_nanos(self.config.propagation_ns),
            &Self::FINISH_PROPAGATION_SID,
            (),
        ) {
            self.mailbox_tracker.dequeue(self.config.mailbox);
            self.recorder.fail(error);
        }
        self.start_next(context);
    }

    async fn finish_propagation(&mut self, _: (), context: &Context<Self>) {
        self.mailbox_tracker.dequeue(self.config.mailbox);
        let Some(packet) = self.propagating.pop_front() else {
            self.recorder.fail("propagation completed without a packet");
            return;
        };
        self.recorder.record(
            now_ns(context),
            self.config.component,
            "arrival",
            packet.flow_id,
            packet.packet_id,
            packet.size,
            0,
        );
        self.output
            .send(TrackedPacket::enqueue(
                packet,
                self.config.downstream_mailbox,
                &self.mailbox_tracker,
            ))
            .await;
    }
}

impl Model for PhysicalLink {
    type Env = ();

    fn register_schedulables(
        context: &mut BuildContext<impl ProtoModel<Model = Self>>,
    ) -> ModelRegistry {
        let mut registry = ModelRegistry::default();
        registry.add(context.register_schedulable(Self::finish_serialization));
        registry.add(context.register_schedulable(Self::finish_propagation));
        registry
    }
}

fn serialization_ns(wire_bytes: usize, rate_bps: u64) -> Option<u64> {
    let bits_nanoseconds = (wire_bytes as u128).checked_mul(8_000_000_000)?;
    let rate = rate_bps as u128;
    let rounded_up = bits_nanoseconds.checked_add(rate.checked_sub(1)?)? / rate;
    u64::try_from(rounded_up).ok()
}
