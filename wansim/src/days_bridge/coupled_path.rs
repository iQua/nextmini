use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::time::Duration;

use days::flows::packet::Packet;
use nexosim::model::{BuildContext, Context, Model, ModelRegistry, ProtoModel, SchedulableId};
use nexosim::ports::Output;

use crate::determinism::DECISION_DELTA_NS;
use crate::metrics::{MailboxTracker, Recorder, TrackedPacket};
use crate::transport::{now_ns, seconds_from_ns};

const STAGE_COUNT: usize = 4;
const SERVERS_PER_STAGE: usize = 3;
const SHARED_SERVER: usize = 0;

#[derive(Clone, Debug)]
pub(crate) struct CoupledPathConfig {
    pub(crate) component: &'static str,
    pub(crate) mailbox: &'static str,
    pub(crate) aggregate_rate_bps: u64,
    pub(crate) propagation_ns: u64,
    pub(crate) aggregate_queue_bytes: usize,
    pub(crate) overlap_percent: u8,
    pub(crate) flow_lanes: BTreeMap<usize, usize>,
    pub(crate) downstream_mailboxes: BTreeMap<usize, &'static str>,
}

#[derive(
    Clone, Copy, Debug, serde::Deserialize, serde::Serialize, PartialEq, Eq, PartialOrd, Ord,
)]
struct ServerKey {
    stage: usize,
    server: usize,
    generation: u64,
}

#[derive(Clone, Debug)]
struct StageArrival {
    stage: usize,
    packet: Packet,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
struct PropagationCompletion {
    next_stage: usize,
    packet: Packet,
}

#[derive(Default)]
struct Server {
    waiting: VecDeque<Packet>,
    in_service: Option<Packet>,
    occupied_bytes: usize,
    generation: u64,
}

pub(crate) struct CoupledPath {
    config: CoupledPathConfig,
    shared_stages: [bool; STAGE_COUNT],
    servers: [[Server; SERVERS_PER_STAGE]; STAGE_COUNT],
    pending_arrivals: BTreeMap<u64, Vec<StageArrival>>,
    pending_finishes: BTreeMap<u64, Vec<ServerKey>>,
    scheduled_resolutions: BTreeSet<u64>,
    pub(crate) output: Output<TrackedPacket>,
    recorder: Recorder,
    mailbox_tracker: MailboxTracker,
}

impl CoupledPath {
    const SERVICE_DEADLINE_SID: SchedulableId<Self, ServerKey> = SchedulableId::__from_decorated(0);
    const PROPAGATION_SID: SchedulableId<Self, PropagationCompletion> =
        SchedulableId::__from_decorated(1);
    const RESOLVE_TIMESTAMP_SID: SchedulableId<Self, u64> = SchedulableId::__from_decorated(2);

    pub(crate) fn new(
        config: CoupledPathConfig,
        recorder: Recorder,
        mailbox_tracker: MailboxTracker,
    ) -> Result<Self, &'static str> {
        if config.aggregate_rate_bps < 2
            || config.propagation_ns < STAGE_COUNT as u64
            || config.aggregate_queue_bytes < 2
        {
            return Err("coupled-path rate, propagation, or queue geometry is too small");
        }
        if !matches!(config.overlap_percent, 0 | 25 | 50 | 100) {
            return Err("coupled-path overlap must be 0, 25, 50, or 100 percent");
        }
        if config.flow_lanes.is_empty()
            || config
                .flow_lanes
                .iter()
                .any(|(flow, lane)| *lane > 1 || !config.downstream_mailboxes.contains_key(flow))
        {
            return Err("coupled-path flow routes must be complete two-lane mappings");
        }
        let shared_stages = match config.overlap_percent {
            0 => [false, false, false, false],
            25 => [false, true, false, false],
            50 => [false, true, false, true],
            100 => [true, true, true, true],
            _ => unreachable!("validated overlap"),
        };
        Ok(Self {
            config,
            shared_stages,
            servers: std::array::from_fn(|_| std::array::from_fn(|_| Server::default())),
            pending_arrivals: BTreeMap::new(),
            pending_finishes: BTreeMap::new(),
            scheduled_resolutions: BTreeSet::new(),
            output: Output::default(),
            recorder,
            mailbox_tracker,
        })
    }

    pub(crate) async fn receive(&mut self, tracked: TrackedPacket, context: &Context<Self>) {
        let packet = tracked.arrive(self.config.mailbox);
        let timestamp = now_ns(context);
        self.pending_arrivals
            .entry(timestamp)
            .or_default()
            .push(StageArrival { stage: 0, packet });
        self.schedule_resolution(timestamp, context);
    }

    async fn service_deadline(&mut self, key: ServerKey, context: &Context<Self>) {
        self.mailbox_tracker.dequeue(self.config.mailbox);
        let timestamp = now_ns(context);
        self.pending_finishes
            .entry(timestamp)
            .or_default()
            .push(key);
        self.schedule_resolution(timestamp, context);
    }

    async fn propagation_complete(
        &mut self,
        completion: PropagationCompletion,
        context: &Context<Self>,
    ) {
        self.mailbox_tracker.dequeue(self.config.mailbox);
        let timestamp = now_ns(context);
        if completion.next_stage == STAGE_COUNT {
            self.emit(completion.packet, timestamp).await;
        } else {
            self.pending_arrivals
                .entry(timestamp)
                .or_default()
                .push(StageArrival {
                    stage: completion.next_stage,
                    packet: completion.packet,
                });
            self.schedule_resolution(timestamp, context);
        }
    }

    async fn resolve_timestamp(&mut self, timestamp: u64, context: &Context<Self>) {
        self.mailbox_tracker.dequeue(self.config.mailbox);
        self.scheduled_resolutions.remove(&timestamp);

        if let Some(mut finishes) = self.pending_finishes.remove(&timestamp) {
            finishes.sort_unstable();
            for key in finishes {
                self.finish_service(key, timestamp, context);
            }
        }
        if let Some(mut arrivals) = self.pending_arrivals.remove(&timestamp) {
            arrivals.sort_by_key(|arrival| {
                (
                    arrival.stage,
                    arrival.packet.flow_id,
                    arrival.packet.packet_id,
                    arrival.packet.ack.is_some(),
                    arrival.packet.size,
                )
            });
            for arrival in arrivals {
                self.admit(arrival, timestamp);
            }
        }
        for stage in 0..STAGE_COUNT {
            for server in 0..SERVERS_PER_STAGE {
                self.start_next(stage, server, timestamp, context);
            }
        }
    }

    fn admit(&mut self, arrival: StageArrival, timestamp: u64) {
        let Some(&lane) = self.config.flow_lanes.get(&arrival.packet.flow_id) else {
            self.recorder.fail(format_args!(
                "{} received unmapped flow {}",
                self.config.component, arrival.packet.flow_id
            ));
            return;
        };
        let server_index = if self.shared_stages[arrival.stage] {
            SHARED_SERVER
        } else {
            lane + 1
        };
        let capacity = self.server_queue_capacity(arrival.stage);
        let server = &mut self.servers[arrival.stage][server_index];
        if server
            .occupied_bytes
            .checked_add(arrival.packet.size)
            .is_none_or(|occupied| occupied > capacity)
        {
            self.recorder.record(
                timestamp,
                self.config.component,
                "queue_drop",
                arrival.packet.flow_id,
                arrival.packet.packet_id,
                arrival.packet.size,
                server.occupied_bytes,
            );
            return;
        }
        server.occupied_bytes += arrival.packet.size;
        self.recorder.record(
            timestamp,
            self.config.component,
            "coupled_queue_admit",
            arrival.packet.flow_id,
            arrival.packet.packet_id,
            arrival.packet.size,
            arrival.stage * SERVERS_PER_STAGE + server_index,
        );
        server.waiting.push_back(arrival.packet);
    }

    fn start_next(
        &mut self,
        stage: usize,
        server_index: usize,
        timestamp: u64,
        context: &Context<Self>,
    ) {
        if self.servers[stage][server_index].in_service.is_some() {
            return;
        }
        let Some(packet) = self.servers[stage][server_index].waiting.pop_front() else {
            return;
        };
        let rate = if server_index == SHARED_SERVER {
            self.config.aggregate_rate_bps
        } else {
            self.config.aggregate_rate_bps / 2
        };
        let Some(duration_ns) = serialization_ns(packet.size, rate) else {
            self.recorder.fail("coupled-path serialization overflow");
            return;
        };
        let server = &mut self.servers[stage][server_index];
        server.generation = server.generation.saturating_add(1);
        let key = ServerKey {
            stage,
            server: server_index,
            generation: server.generation,
        };
        self.recorder.record(
            timestamp,
            self.config.component,
            "coupled_serialization_start",
            packet.flow_id,
            packet.packet_id,
            packet.size,
            stage * SERVERS_PER_STAGE + server_index,
        );
        server.in_service = Some(packet);
        self.mailbox_tracker.enqueue(self.config.mailbox);
        if let Err(error) = context.schedule_event(
            Duration::from_nanos(duration_ns.saturating_sub(DECISION_DELTA_NS)),
            &Self::SERVICE_DEADLINE_SID,
            key,
        ) {
            self.mailbox_tracker.dequeue(self.config.mailbox);
            self.recorder.fail(error);
        }
    }

    fn finish_service(&mut self, key: ServerKey, timestamp: u64, context: &Context<Self>) {
        let server = &mut self.servers[key.stage][key.server];
        if server.generation != key.generation {
            return;
        }
        let Some(mut packet) = server.in_service.take() else {
            self.recorder
                .fail("coupled-path service completed without a packet");
            return;
        };
        server.occupied_bytes = server.occupied_bytes.saturating_sub(packet.size);
        self.recorder.record(
            timestamp,
            self.config.component,
            "coupled_serialization_end",
            packet.flow_id,
            packet.packet_id,
            packet.size,
            key.stage * SERVERS_PER_STAGE + key.server,
        );
        let propagation_ns = self.config.propagation_ns / STAGE_COUNT as u64;
        let arrival_ns = timestamp.saturating_add(propagation_ns);
        packet.departure_update(seconds_from_ns(arrival_ns));
        self.mailbox_tracker.enqueue(self.config.mailbox);
        if let Err(error) = context.schedule_event(
            Duration::from_nanos(propagation_ns.saturating_sub(DECISION_DELTA_NS)),
            &Self::PROPAGATION_SID,
            PropagationCompletion {
                next_stage: key.stage + 1,
                packet,
            },
        ) {
            self.mailbox_tracker.dequeue(self.config.mailbox);
            self.recorder.fail(error);
        }
    }

    async fn emit(&mut self, packet: Packet, timestamp: u64) {
        let Some(&downstream) = self.config.downstream_mailboxes.get(&packet.flow_id) else {
            self.recorder.fail(format_args!(
                "{} has no downstream for flow {}",
                self.config.component, packet.flow_id
            ));
            return;
        };
        self.recorder.record(
            timestamp,
            self.config.component,
            "coupled_path_exit",
            packet.flow_id,
            packet.packet_id,
            packet.size,
            usize::from(packet.ack.is_some()),
        );
        self.output
            .send(TrackedPacket::enqueue(
                packet,
                downstream,
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

    fn server_queue_capacity(&self, stage: usize) -> usize {
        if self.shared_stages[stage] {
            self.config.aggregate_queue_bytes
        } else {
            self.config.aggregate_queue_bytes / 2
        }
    }
}

impl Model for CoupledPath {
    type Env = ();

    fn register_schedulables(
        context: &mut BuildContext<impl ProtoModel<Model = Self>>,
    ) -> ModelRegistry {
        let mut registry = ModelRegistry::default();
        registry.add(context.register_schedulable(Self::service_deadline));
        registry.add(context.register_schedulable(Self::propagation_complete));
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn overlap_masks_have_exact_quarter_cardinality() {
        let config = |overlap_percent| CoupledPathConfig {
            component: "test_path",
            mailbox: "test_path",
            aggregate_rate_bps: 160_000_000,
            propagation_ns: 1_000_000,
            aggregate_queue_bytes: 131_072,
            overlap_percent,
            flow_lanes: BTreeMap::from([(1, 0), (2, 1)]),
            downstream_mailboxes: BTreeMap::from([(1, "left"), (2, "right")]),
        };
        for (percent, count) in [(0, 0), (25, 1), (50, 2), (100, 4)] {
            let path = CoupledPath::new(
                config(percent),
                Recorder::new("test", 0),
                MailboxTracker::default(),
            )
            .expect("valid path");
            assert_eq!(
                path.shared_stages.iter().filter(|shared| **shared).count(),
                count
            );
        }
    }
}
