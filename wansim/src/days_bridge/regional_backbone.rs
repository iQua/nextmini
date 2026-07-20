use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::time::Duration;

use days::flows::packet::Packet;
use nexosim::model::{
    BuildContext, Context, InitializedModel, Model, ModelRegistry, ProtoModel, SchedulableId,
};
use nexosim::ports::Output;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::determinism::{CounterPrf, DECISION_DELTA_NS};
use crate::metrics::{EgressLedger, MailboxTracker, Recorder, TrackedPacket};
use crate::transport::{now_ns, seconds_from_ns};

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct BackboneResourceConfig {
    pub(crate) name: String,
    pub(crate) rate_bps: u64,
    pub(crate) queue_bytes: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct BackboneRouteHop {
    pub(crate) resource: usize,
    pub(crate) propagation_ns: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct BackboneRouteConfig {
    pub(crate) hops: Vec<BackboneRouteHop>,
    pub(crate) downstream_mailbox: &'static str,
    pub(crate) egress_nano_usd_per_gb: u64,
}

#[derive(Clone, Debug)]
pub(crate) struct RegionalBackboneConfig {
    pub(crate) component: &'static str,
    pub(crate) mailbox: &'static str,
    pub(crate) resources: Vec<BackboneResourceConfig>,
    /// Key is `(flow_id, is_tcp_ack)`.
    pub(crate) routes: BTreeMap<(usize, bool), BackboneRouteConfig>,
    pub(crate) background_flow_ids: BTreeSet<usize>,
    pub(crate) tree_probe_flow_ids: BTreeSet<usize>,
    pub(crate) egress_ledger: EgressLedger,
    pub(crate) jitter_enabled: bool,
    pub(crate) jitter_max_ppm: u32,
    pub(crate) jitter_epoch_ns: u64,
    pub(crate) sample_interval_ns: u64,
    pub(crate) simulation_end_ns: u64,
    pub(crate) prf: CounterPrf,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub(crate) enum RegionalBackboneError {
    #[error("regional backbone requires resources, routes, and nonzero timing")]
    Empty,
    #[error("regional backbone resource {0} has invalid geometry")]
    Resource(usize),
    #[error("regional backbone route {flow_id}/{is_ack} is empty or out of range")]
    Route { flow_id: usize, is_ack: bool },
    #[error("regional backbone route lacks its TCP reverse direction for flow {0}")]
    MissingReverse(usize),
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq, PartialOrd, Ord)]
struct ServiceKey {
    resource: usize,
    generation: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct RoutedPacket {
    packet: Packet,
    route_key: (usize, bool),
    position: usize,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct PropagationCompletion {
    routed: RoutedPacket,
}

#[derive(Default)]
struct ResourceState {
    waiting: VecDeque<RoutedPacket>,
    in_service: Option<RoutedPacket>,
    occupied_bytes: usize,
    high_water_bytes: usize,
    generation: u64,
    sampled_wire_bytes: usize,
    sampled_background_bytes: usize,
    propagation_frontier_ns: u64,
}

pub(crate) struct RegionalBackbone {
    config: RegionalBackboneConfig,
    resources: Vec<ResourceState>,
    pending_arrivals: BTreeMap<u64, Vec<RoutedPacket>>,
    pending_finishes: BTreeMap<u64, Vec<ServiceKey>>,
    scheduled_resolutions: BTreeSet<u64>,
    tree_probe_bytes: BTreeMap<usize, usize>,
    sample_index: usize,
    pub(crate) output: Output<TrackedPacket>,
    recorder: Recorder,
    mailbox_tracker: MailboxTracker,
}

impl RegionalBackbone {
    const SERVICE_SID: SchedulableId<Self, ServiceKey> = SchedulableId::__from_decorated(0);
    const PROPAGATION_SID: SchedulableId<Self, PropagationCompletion> =
        SchedulableId::__from_decorated(1);
    const RESOLVE_SID: SchedulableId<Self, u64> = SchedulableId::__from_decorated(2);
    const SAMPLE_SID: SchedulableId<Self, ()> = SchedulableId::__from_decorated(3);

    pub(crate) fn new(
        config: RegionalBackboneConfig,
        recorder: Recorder,
        mailbox_tracker: MailboxTracker,
    ) -> Result<Self, RegionalBackboneError> {
        if config.resources.is_empty()
            || config.routes.is_empty()
            || config.sample_interval_ns == 0
            || config.simulation_end_ns == 0
            || config.jitter_epoch_ns == 0
            || config.jitter_max_ppm > 1_000_000
        {
            return Err(RegionalBackboneError::Empty);
        }
        for (index, resource) in config.resources.iter().enumerate() {
            if resource.name.is_empty() || resource.rate_bps == 0 || resource.queue_bytes == 0 {
                return Err(RegionalBackboneError::Resource(index));
            }
        }
        for (&(flow_id, is_ack), route) in &config.routes {
            if route.hops.is_empty()
                || route
                    .hops
                    .iter()
                    .any(|hop| hop.resource >= config.resources.len() || hop.propagation_ns == 0)
            {
                return Err(RegionalBackboneError::Route { flow_id, is_ack });
            }
            if !config.routes.contains_key(&(flow_id, !is_ack)) {
                return Err(RegionalBackboneError::MissingReverse(flow_id));
            }
        }
        let resource_count = config.resources.len();
        Ok(Self {
            config,
            resources: (0..resource_count)
                .map(|_| ResourceState::default())
                .collect(),
            pending_arrivals: BTreeMap::new(),
            pending_finishes: BTreeMap::new(),
            scheduled_resolutions: BTreeSet::new(),
            tree_probe_bytes: BTreeMap::new(),
            sample_index: 0,
            output: Output::default(),
            recorder,
            mailbox_tracker,
        })
    }

    pub(crate) async fn receive(&mut self, tracked: TrackedPacket, context: &Context<Self>) {
        self.recorder.count_event_class("dispatch_backbone_receive");
        let packet = tracked.arrive(self.config.mailbox);
        let route_key = (packet.flow_id, packet.ack.is_some());
        if !self.config.routes.contains_key(&route_key) {
            self.recorder.fail(format_args!(
                "regional backbone received unmapped route {}/{}",
                route_key.0, route_key.1
            ));
            return;
        }
        let timestamp = now_ns(context);
        self.pending_arrivals
            .entry(timestamp)
            .or_default()
            .push(RoutedPacket {
                packet,
                route_key,
                position: 0,
            });
        self.schedule_resolution(timestamp, context);
    }

    async fn service_deadline(&mut self, key: ServiceKey, context: &Context<Self>) {
        self.recorder
            .count_event_class("dispatch_backbone_service_deadline");
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
        self.recorder
            .count_event_class("dispatch_backbone_propagation_complete");
        self.mailbox_tracker.dequeue(self.config.mailbox);
        let timestamp = now_ns(context);
        let route_len = self.config.routes[&completion.routed.route_key].hops.len();
        if completion.routed.position >= route_len {
            self.emit(completion.routed, timestamp).await;
        } else {
            self.pending_arrivals
                .entry(timestamp)
                .or_default()
                .push(completion.routed);
            self.schedule_resolution(timestamp, context);
        }
    }

    async fn resolve_timestamp(&mut self, timestamp: u64, context: &Context<Self>) {
        self.recorder
            .count_event_class("dispatch_backbone_resolve_timestamp");
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
                    self.route_resource(arrival),
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
        for resource in 0..self.resources.len() {
            self.start_next(resource, timestamp, context);
        }
    }

    async fn sample(&mut self, _: (), context: &Context<Self>) {
        self.recorder.count_event_class("dispatch_backbone_sample");
        self.mailbox_tracker.dequeue(self.config.mailbox);
        let now = now_ns(context);
        for (resource, state) in self.resources.iter_mut().enumerate() {
            self.recorder.record(
                now,
                self.config.component,
                "wr_resource_sample",
                resource,
                self.sample_index,
                state.sampled_wire_bytes,
                state.sampled_background_bytes,
            );
            state.sampled_wire_bytes = 0;
            state.sampled_background_bytes = 0;
        }
        for &flow_id in &self.config.tree_probe_flow_ids {
            self.recorder.record(
                now,
                self.config.component,
                "wr_tree_rate_sample",
                flow_id,
                self.sample_index,
                self.tree_probe_bytes.remove(&flow_id).unwrap_or(0),
                0,
            );
        }
        self.sample_index = self.sample_index.saturating_add(1);
        if now < self.config.simulation_end_ns {
            self.schedule_sample(context);
        }
    }

    fn route_resource(&self, routed: &RoutedPacket) -> usize {
        self.config.routes[&routed.route_key].hops[routed.position].resource
    }

    fn admit(&mut self, routed: RoutedPacket, timestamp: u64) {
        let resource_index = self.route_resource(&routed);
        let resource = &mut self.resources[resource_index];
        if resource
            .occupied_bytes
            .checked_add(routed.packet.size)
            .is_none_or(|occupied| occupied > self.config.resources[resource_index].queue_bytes)
        {
            self.recorder.record(
                timestamp,
                self.config.component,
                "queue_drop",
                routed.packet.flow_id,
                routed.packet.packet_id,
                routed.packet.size,
                resource_index,
            );
            return;
        }
        resource.occupied_bytes += routed.packet.size;
        if resource.occupied_bytes > resource.high_water_bytes {
            resource.high_water_bytes = resource.occupied_bytes;
            self.recorder.record(
                timestamp,
                self.config.component,
                "wr_resource_queue_high_water",
                resource_index,
                0,
                resource.high_water_bytes,
                self.config.resources[resource_index].queue_bytes,
            );
        }
        resource.waiting.push_back(routed);
    }

    fn start_next(&mut self, resource_index: usize, _timestamp: u64, context: &Context<Self>) {
        if self.resources[resource_index].in_service.is_some() {
            return;
        }
        let Some(routed) = self.resources[resource_index].waiting.pop_front() else {
            return;
        };
        let Some(duration_ns) = serialization_ns(
            routed.packet.size,
            self.config.resources[resource_index].rate_bps,
        ) else {
            self.recorder
                .fail("regional backbone serialization overflow");
            return;
        };
        let resource = &mut self.resources[resource_index];
        resource.generation = resource.generation.saturating_add(1);
        let key = ServiceKey {
            resource: resource_index,
            generation: resource.generation,
        };
        resource.in_service = Some(routed);
        self.recorder
            .count_event_class("backbone_resource_service_started");
        self.mailbox_tracker.enqueue(self.config.mailbox);
        if let Err(error) = context.schedule_event(
            Duration::from_nanos(duration_ns.saturating_sub(DECISION_DELTA_NS)),
            &Self::SERVICE_SID,
            key,
        ) {
            self.mailbox_tracker.dequeue(self.config.mailbox);
            self.recorder.fail(error);
        }
    }

    fn finish_service(&mut self, key: ServiceKey, timestamp: u64, context: &Context<Self>) {
        self.recorder
            .count_event_class("backbone_resource_service_finished");
        let resource = &mut self.resources[key.resource];
        if resource.generation != key.generation {
            return;
        }
        let Some(mut routed) = resource.in_service.take() else {
            self.recorder
                .fail("regional backbone service completed without a packet");
            return;
        };
        resource.occupied_bytes = resource.occupied_bytes.saturating_sub(routed.packet.size);
        resource.sampled_wire_bytes = resource
            .sampled_wire_bytes
            .saturating_add(routed.packet.size);
        if self
            .config
            .background_flow_ids
            .contains(&routed.packet.flow_id)
        {
            resource.sampled_background_bytes = resource
                .sampled_background_bytes
                .saturating_add(routed.packet.size);
        }
        let route = &self.config.routes[&routed.route_key];
        if routed.position == 0
            && !self
                .config
                .background_flow_ids
                .contains(&routed.packet.flow_id)
            && self
                .config
                .egress_ledger
                .charge(routed.packet.size, route.egress_nano_usd_per_gb)
                .is_err()
        {
            self.recorder.fail("foreground egress ledger overflow");
        }
        let hop = &route.hops[routed.position];
        debug_assert_eq!(hop.resource, key.resource);
        let propagation_ns =
            self.jittered_propagation_ns(hop.propagation_ns, key.resource, timestamp);
        routed.position = routed.position.saturating_add(1);
        let arrival_ns = self.propagation_arrival_ns(key.resource, timestamp, propagation_ns);
        let actual_propagation_ns = arrival_ns.saturating_sub(timestamp);
        routed.packet.departure_update(seconds_from_ns(arrival_ns));
        self.mailbox_tracker.enqueue(self.config.mailbox);
        if let Err(error) = context.schedule_event(
            Duration::from_nanos(actual_propagation_ns.saturating_sub(DECISION_DELTA_NS)),
            &Self::PROPAGATION_SID,
            PropagationCompletion { routed },
        ) {
            self.mailbox_tracker.dequeue(self.config.mailbox);
            self.recorder.fail(error);
        }
    }

    fn jittered_propagation_ns(&self, base: u64, resource: usize, departure_ns: u64) -> u64 {
        if !self.config.jitter_enabled {
            return base;
        }
        let width = u64::from(self.config.jitter_max_ppm);
        let epoch = departure_ns / self.config.jitter_epoch_ns;
        let draw = self
            .config
            .prf
            .draw_u64("wr-propagation-jitter", resource as u64, epoch, 0);
        let signed_ppm =
            i128::from(draw % (width.saturating_mul(2).saturating_add(1))) - i128::from(width);
        let adjustment = i128::from(base).saturating_mul(signed_ppm) / 1_000_000;
        u64::try_from((i128::from(base) + adjustment).max(1)).unwrap_or(u64::MAX)
    }

    fn propagation_arrival_ns(
        &mut self,
        resource: usize,
        departure_ns: u64,
        propagation_ns: u64,
    ) -> u64 {
        let nominal = departure_ns.saturating_add(propagation_ns);
        let state = &mut self.resources[resource];
        let arrival = nominal.max(state.propagation_frontier_ns.saturating_add(1));
        state.propagation_frontier_ns = arrival;
        arrival
    }

    async fn emit(&mut self, routed: RoutedPacket, timestamp: u64) {
        let flow_id = routed.packet.flow_id;
        if self.config.tree_probe_flow_ids.contains(&flow_id) && routed.packet.ack.is_none() {
            let bytes = self.tree_probe_bytes.entry(flow_id).or_default();
            *bytes = bytes.saturating_add(routed.packet.size);
        }
        let downstream = self.config.routes[&routed.route_key].downstream_mailbox;
        self.output
            .send(TrackedPacket::enqueue(
                routed.packet,
                downstream,
                &self.mailbox_tracker,
            ))
            .await;
        self.recorder.record(
            timestamp,
            self.config.component,
            "wr_backbone_exit",
            flow_id,
            0,
            0,
            usize::from(routed.route_key.1),
        );
    }

    fn schedule_resolution(&mut self, timestamp: u64, context: &Context<Self>) {
        if !self.scheduled_resolutions.insert(timestamp) {
            return;
        }
        self.mailbox_tracker.enqueue(self.config.mailbox);
        if let Err(error) = context.schedule_event(
            Duration::from_nanos(DECISION_DELTA_NS),
            &Self::RESOLVE_SID,
            timestamp,
        ) {
            self.mailbox_tracker.dequeue(self.config.mailbox);
            self.scheduled_resolutions.remove(&timestamp);
            self.recorder.fail(error);
        }
    }

    fn schedule_sample(&self, context: &Context<Self>) {
        self.mailbox_tracker.enqueue(self.config.mailbox);
        if let Err(error) = context.schedule_event(
            Duration::from_nanos(self.config.sample_interval_ns),
            &Self::SAMPLE_SID,
            (),
        ) {
            self.mailbox_tracker.dequeue(self.config.mailbox);
            self.recorder.fail(error);
        }
    }
}

impl Model for RegionalBackbone {
    type Env = ();

    fn register_schedulables(
        context: &mut BuildContext<impl ProtoModel<Model = Self>>,
    ) -> ModelRegistry {
        let mut registry = ModelRegistry::default();
        registry.add(context.register_schedulable(Self::service_deadline));
        registry.add(context.register_schedulable(Self::propagation_complete));
        registry.add(context.register_schedulable(Self::resolve_timestamp));
        registry.add(context.register_schedulable(Self::sample));
        registry
    }

    async fn init(self, context: &Context<Self>, _: &mut Self::Env) -> InitializedModel<Self> {
        self.schedule_sample(context);
        self.into()
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

    fn valid_config() -> RegionalBackboneConfig {
        RegionalBackboneConfig {
            component: "wr_backbone",
            mailbox: "wr_backbone",
            resources: vec![BackboneResourceConfig {
                name: "nic-east".to_owned(),
                rate_bps: 1_000_000_000,
                queue_bytes: 65_536,
            }],
            routes: BTreeMap::from([
                (
                    (1, false),
                    BackboneRouteConfig {
                        hops: vec![BackboneRouteHop {
                            resource: 0,
                            propagation_ns: 1_000_000,
                        }],
                        downstream_mailbox: "receiver",
                        egress_nano_usd_per_gb: 10_000_000,
                    },
                ),
                (
                    (1, true),
                    BackboneRouteConfig {
                        hops: vec![BackboneRouteHop {
                            resource: 0,
                            propagation_ns: 1_000_000,
                        }],
                        downstream_mailbox: "sender",
                        egress_nano_usd_per_gb: 10_000_000,
                    },
                ),
            ]),
            background_flow_ids: BTreeSet::new(),
            tree_probe_flow_ids: BTreeSet::from([1]),
            egress_ledger: EgressLedger::default(),
            jitter_enabled: true,
            jitter_max_ppm: 50_000,
            jitter_epoch_ns: 100_000_000,
            sample_interval_ns: 5_000_000,
            simulation_end_ns: 1_000_000_000,
            prf: CounterPrf::new(7, "test"),
        }
    }

    #[test]
    fn rejects_a_flow_without_its_tcp_reverse_route() {
        let mut config = valid_config();
        config.routes.remove(&(1, true));
        let error =
            RegionalBackbone::new(config, Recorder::new("test", 0), MailboxTracker::default())
                .err()
                .expect("invalid route");
        assert_eq!(error, RegionalBackboneError::MissingReverse(1));
    }

    #[test]
    fn jitter_is_seeded_epoch_varying_bounded_and_optional() {
        let recorder = Recorder::new("test", 0);
        let tracker = MailboxTracker::default();
        let backbone = RegionalBackbone::new(valid_config(), recorder.clone(), tracker.clone())
            .expect("valid backbone");
        let first = backbone.jittered_propagation_ns(1_000_000, 0, 0);
        let second = backbone.jittered_propagation_ns(1_000_000, 0, 99_999_999);
        assert_eq!(first, second);
        let later = backbone.jittered_propagation_ns(1_000_000, 0, 100_000_000);
        assert_ne!(first, later, "the pinned seed must vary across epochs");
        assert!((950_000..=1_050_000).contains(&first));
        assert!((950_000..=1_050_000).contains(&later));
        let mut disabled = valid_config();
        disabled.jitter_enabled = false;
        let backbone = RegionalBackbone::new(disabled, recorder, tracker).expect("valid backbone");
        assert_eq!(backbone.jittered_propagation_ns(1_000_000, 0, 0), 1_000_000);
    }

    #[test]
    fn propagation_frontier_prevents_jitter_epoch_reordering() {
        let mut backbone = RegionalBackbone::new(
            valid_config(),
            Recorder::new("test", 0),
            MailboxTracker::default(),
        )
        .expect("valid backbone");
        backbone.resources[0].propagation_frontier_ns = 200_000_000;
        let arrival = backbone.propagation_arrival_ns(0, 100_000_000, 1_000_000);
        assert_eq!(arrival, 200_000_001);
    }
}
