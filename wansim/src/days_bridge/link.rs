use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::time::Duration;

use days::flows::packet::Packet;
use nexosim::model::{BuildContext, Context, Model, ModelRegistry, ProtoModel, SchedulableId};
use nexosim::ports::Output;

use crate::determinism::DECISION_DELTA_NS;
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
    pending_arrivals: BTreeMap<u64, Vec<Packet>>,
    pending_serialization_finish: Option<u64>,
    scheduled_resolutions: BTreeSet<u64>,
    occupied_bytes: usize,
    arrival_attempt: u64,
    pub(crate) output: Output<TrackedPacket>,
    recorder: Recorder,
    mailbox_tracker: MailboxTracker,
}

impl PhysicalLink {
    const SERIALIZATION_DEADLINE_SID: SchedulableId<Self, ()> = SchedulableId::__from_decorated(0);
    const FINISH_PROPAGATION_SID: SchedulableId<Self, ()> = SchedulableId::__from_decorated(1);
    const RESOLVE_TIMESTAMP_SID: SchedulableId<Self, u64> = SchedulableId::__from_decorated(2);

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
            pending_arrivals: BTreeMap::new(),
            pending_serialization_finish: None,
            scheduled_resolutions: BTreeSet::new(),
            occupied_bytes: 0,
            arrival_attempt: 0,
            output: Output::default(),
            recorder,
            mailbox_tracker,
        }
    }

    pub(crate) async fn receive(&mut self, tracked: TrackedPacket, context: &Context<Self>) {
        let packet = tracked.arrive(self.config.mailbox);
        let timestamp = now_ns(context);
        self.pending_arrivals
            .entry(timestamp)
            .or_default()
            .push(packet);
        self.schedule_resolution(timestamp, context);
    }

    fn process_arrival(&mut self, packet: Packet, timestamp: u64) {
        let attempt = self.arrival_attempt;
        self.arrival_attempt = self.arrival_attempt.saturating_add(1);
        if self.config.drop_attempts.contains(&attempt) {
            self.recorder.record(
                timestamp,
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
                timestamp,
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
            timestamp,
            self.config.component,
            "queue_admit",
            packet.flow_id,
            packet.packet_id,
            packet.size,
            self.occupied_bytes,
        );
        self.waiting.push_back(packet);
    }

    fn start_next(&mut self, modeled_now: u64, context: &Context<Self>) {
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
            modeled_now,
            self.config.component,
            "serialization_start",
            packet.flow_id,
            packet.packet_id,
            packet.size,
            serialization_ns as usize,
        );
        self.in_service = Some(packet);
        self.mailbox_tracker.enqueue(self.config.mailbox);
        let scheduling_delay_ns = serialization_ns.saturating_sub(DECISION_DELTA_NS);
        if let Err(error) = context.schedule_event(
            Duration::from_nanos(scheduling_delay_ns),
            &Self::SERIALIZATION_DEADLINE_SID,
            (),
        ) {
            self.mailbox_tracker.dequeue(self.config.mailbox);
            self.recorder.fail(error);
        }
    }

    async fn serialization_deadline(&mut self, _: (), context: &Context<Self>) {
        self.mailbox_tracker.dequeue(self.config.mailbox);
        let timestamp = now_ns(context);
        self.pending_serialization_finish = Some(timestamp);
        self.schedule_resolution(timestamp, context);
    }

    async fn resolve_timestamp(&mut self, timestamp: u64, context: &Context<Self>) {
        self.mailbox_tracker.dequeue(self.config.mailbox);
        self.scheduled_resolutions.remove(&timestamp);

        if self.pending_serialization_finish == Some(timestamp) {
            self.pending_serialization_finish = None;
            self.finish_serialization(timestamp, context);
        }

        if let Some(mut arrivals) = self.pending_arrivals.remove(&timestamp) {
            arrivals.sort_by_key(|packet| {
                (
                    packet.flow_id,
                    packet.packet_id,
                    packet.ack.is_some(),
                    packet.size,
                )
            });
            for packet in arrivals {
                self.process_arrival(packet, timestamp);
            }
        }

        if self.in_service.is_none() {
            self.start_next(timestamp, context);
        }
    }

    fn finish_serialization(&mut self, timestamp: u64, context: &Context<Self>) {
        let Some(mut packet) = self.in_service.take() else {
            self.recorder
                .fail("serialization completed without a packet");
            return;
        };
        self.occupied_bytes = self.occupied_bytes.saturating_sub(packet.size);
        self.recorder.record(
            timestamp,
            self.config.component,
            "serialization_end",
            packet.flow_id,
            packet.packet_id,
            packet.size,
            self.occupied_bytes,
        );
        let arrival_ns = timestamp.saturating_add(self.config.propagation_ns);
        packet.departure_update(seconds_from_ns(arrival_ns));
        self.propagating.push_back(packet);
        self.mailbox_tracker.enqueue(self.config.mailbox);
        let scheduling_delay_ns = self.config.propagation_ns.saturating_sub(DECISION_DELTA_NS);
        if let Err(error) = context.schedule_event(
            Duration::from_nanos(scheduling_delay_ns),
            &Self::FINISH_PROPAGATION_SID,
            (),
        ) {
            self.mailbox_tracker.dequeue(self.config.mailbox);
            self.recorder.fail(error);
        }
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

    fn schedule_resolution(&mut self, timestamp: u64, context: &Context<Self>) {
        if !self.scheduled_resolutions.insert(timestamp) {
            return;
        }
        self.mailbox_tracker.enqueue(self.config.mailbox);
        if let Err(error) = context.schedule_event(
            Duration::from_nanos(DECISION_DELTA_NS),
            &Self::RESOLVE_TIMESTAMP_SID,
            timestamp,
        ) {
            self.mailbox_tracker.dequeue(self.config.mailbox);
            self.scheduled_resolutions.remove(&timestamp);
            self.recorder.fail(error);
        }
    }
}

impl Model for PhysicalLink {
    type Env = ();

    fn register_schedulables(
        context: &mut BuildContext<impl ProtoModel<Model = Self>>,
    ) -> ModelRegistry {
        let mut registry = ModelRegistry::default();
        registry.add(context.register_schedulable(Self::serialization_deadline));
        registry.add(context.register_schedulable(Self::finish_propagation));
        registry.add(context.register_schedulable(Self::resolve_timestamp));
        registry
    }
}

fn serialization_ns(wire_bytes: usize, rate_bps: u64) -> Option<u64> {
    let bits_nanoseconds = (wire_bytes as u128).checked_mul(8_000_000_000)?;
    let rate = rate_bps as u128;
    let rounded_up = bits_nanoseconds.checked_add(rate.checked_sub(1)?)? / rate;
    u64::try_from(rounded_up).ok()
}
