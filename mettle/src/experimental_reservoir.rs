//! Simulation-only reservoir prototype, an extension **BEYOND the METTLE paper**.
//!
//! The paper specifies the METTLE graph and discusses feedback/rate adaptation. It does not
//! specify puncturing a finite graph into an initial wire set and a delayed reserve. This module is
//! deliberately `doc(hidden)`, has no wire or manifest representation, and exists only to make the
//! Stage 3.0/3.1 experiment deterministic and reviewable before any integration is considered.
//!
//! A terminal bin position `b` is protected from reservation iff there is a source `s` in
//! `0..source_count` for which `b == floor((1 + c_interior) * s)`, the graph's TLE position. This is
//! a positional definition: the bin stays protected when other sources also contribute non-TLE
//! edges to the same equation. Every other position below the finite terminal departure end is
//! eligible, including a position whose equation has no edges.

use std::collections::{BTreeSet, BinaryHeap};

use crate::{MettleParams, OverheadRatio};

pub mod simulation;

/// Version of the Stage 3 reserve-position pseudorandom function.
pub const RESERVE_PRF_VERSION: u16 = 1;

const INTERIOR_OVERHEAD_DENOMINATOR: u32 = 1_000_000;
const RESERVE_PRF_DOMAIN_LOW: u64 = 0x5245_5345_5256_4531;
const RESERVE_PRF_DOMAIN_HIGH: u64 = 0x4245_594F_4E44_5031;

/// Checked rational used for finite Stage 3 experiment rates, including zero.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReservoirRate {
    numerator: u32,
    denominator: u32,
}

impl ReservoirRate {
    pub fn new(numerator: u32, denominator: u32) -> Result<Self, ReservoirError> {
        if denominator == 0 {
            return Err(ReservoirError::ZeroDenominator);
        }
        let divisor = gcd(numerator, denominator);
        Ok(Self {
            numerator: numerator / divisor,
            denominator: denominator / divisor,
        })
    }

    #[must_use]
    pub const fn numerator(self) -> u32 {
        self.numerator
    }

    #[must_use]
    pub const fn denominator(self) -> u32 {
        self.denominator
    }

    #[must_use]
    pub fn as_f64(self) -> f64 {
        f64::from(self.numerator) / f64::from(self.denominator)
    }

    fn checked_add(self, rhs: Self) -> Result<Self, ReservoirError> {
        let numerator = u128::from(self.numerator)
            .checked_mul(u128::from(rhs.denominator))
            .and_then(|lhs| {
                u128::from(rhs.numerator)
                    .checked_mul(u128::from(self.denominator))
                    .and_then(|rhs| lhs.checked_add(rhs))
            })
            .ok_or(ReservoirError::ArithmeticOverflow)?;
        let denominator = u128::from(self.denominator)
            .checked_mul(u128::from(rhs.denominator))
            .ok_or(ReservoirError::ArithmeticOverflow)?;
        let divisor = gcd_u128(numerator, denominator);
        Self::new(
            u32::try_from(numerator / divisor).map_err(|_| ReservoirError::ArithmeticOverflow)?,
            u32::try_from(denominator / divisor).map_err(|_| ReservoirError::ArithmeticOverflow)?,
        )
    }

    fn finite_symbol_count(self, source_count: u64) -> Result<u128, ReservoirError> {
        let expanded_numerator = u128::from(self.denominator)
            .checked_add(u128::from(self.numerator))
            .ok_or(ReservoirError::ArithmeticOverflow)?;
        u128::from(source_count)
            .checked_mul(expanded_numerator)
            .map(|scaled| scaled.div_ceil(u128::from(self.denominator)))
            .ok_or(ReservoirError::ArithmeticOverflow)
    }
}

/// Exact finite geometry for the simulation-only punctured graph.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FiniteReservoirGeometry {
    source_count: u64,
    wire_rate: ReservoirRate,
    reserve_rate: ReservoirRate,
    total_rate: ReservoirRate,
    params: MettleParams,
    interior_overhead: ReservoirRate,
    wire_bin_count: u128,
    terminal_bin_count: u128,
    reserve_cardinality: usize,
    eligible_bin_count: u128,
}

impl FiniteReservoirGeometry {
    /// Solve the METTLE interior `c` against the actual finite total count, including the
    /// compressed tail, then puncture exactly `terminal_count - wire_count` positions.
    pub fn solve(
        source_count: u64,
        wire_rate: ReservoirRate,
        reserve_rate: ReservoirRate,
    ) -> Result<Self, ReservoirError> {
        if source_count == 0 {
            return Err(ReservoirError::EmptySourcePrefix);
        }
        let total_rate = wire_rate.checked_add(reserve_rate)?;
        let wire_bin_count = wire_rate.finite_symbol_count(source_count)?;
        let target_terminal_bin_count = total_rate.finite_symbol_count(source_count)?;
        let minimum_terminal_bin_count =
            MettleParams::new(OverheadRatio::ZERO).terminal_departure_end_exclusive(source_count);
        if minimum_terminal_bin_count > target_terminal_bin_count {
            return Err(ReservoirError::TargetBelowTailFloor {
                target_terminal_bin_count,
                minimum_terminal_bin_count,
            });
        }

        let high_numerator = u32::try_from(
            u128::from(total_rate.numerator)
                .checked_mul(u128::from(INTERIOR_OVERHEAD_DENOMINATOR))
                .ok_or(ReservoirError::ArithmeticOverflow)?
                .div_ceil(u128::from(total_rate.denominator)),
        )
        .map_err(|_| ReservoirError::ArithmeticOverflow)?;
        let terminal_count_for = |numerator| {
            params_for_interior_numerator(numerator).terminal_departure_end_exclusive(source_count)
        };

        let mut low = 0u32;
        let mut high = high_numerator;
        while low < high {
            let middle = low + (high - low) / 2;
            if terminal_count_for(middle) < target_terminal_bin_count {
                low = middle + 1;
            } else {
                high = middle;
            }
        }

        let terminal_bin_count = terminal_count_for(low);
        if terminal_bin_count != target_terminal_bin_count {
            let lower_terminal_bin_count = low
                .checked_sub(1)
                .map_or(minimum_terminal_bin_count, terminal_count_for);
            return Err(ReservoirError::TargetNotRepresentable {
                target_terminal_bin_count,
                lower_terminal_bin_count,
                upper_terminal_bin_count: terminal_bin_count,
            });
        }

        let reserve_cardinality = terminal_bin_count
            .checked_sub(wire_bin_count)
            .ok_or(ReservoirError::ArithmeticOverflow)?;
        let eligible_bin_count = terminal_bin_count
            .checked_sub(u128::from(source_count))
            .ok_or(ReservoirError::ArithmeticOverflow)?;
        if reserve_cardinality > eligible_bin_count {
            return Err(ReservoirError::ReserveExceedsEligiblePositions {
                reserve_cardinality,
                eligible_bin_count,
            });
        }

        Ok(Self {
            source_count,
            wire_rate,
            reserve_rate,
            total_rate,
            params: params_for_interior_numerator(low),
            interior_overhead: ReservoirRate::new(low, INTERIOR_OVERHEAD_DENOMINATOR)?,
            wire_bin_count,
            terminal_bin_count,
            reserve_cardinality: usize::try_from(reserve_cardinality)
                .map_err(|_| ReservoirError::ArithmeticOverflow)?,
            eligible_bin_count,
        })
    }

    #[must_use]
    pub const fn source_count(self) -> u64 {
        self.source_count
    }

    #[must_use]
    pub const fn wire_rate(self) -> ReservoirRate {
        self.wire_rate
    }

    #[must_use]
    pub const fn reserve_rate(self) -> ReservoirRate {
        self.reserve_rate
    }

    #[must_use]
    pub const fn total_rate(self) -> ReservoirRate {
        self.total_rate
    }

    #[must_use]
    pub const fn params(self) -> MettleParams {
        self.params
    }

    #[must_use]
    pub const fn interior_overhead(self) -> ReservoirRate {
        self.interior_overhead
    }

    #[must_use]
    pub const fn wire_bin_count(self) -> u128 {
        self.wire_bin_count
    }

    #[must_use]
    pub const fn terminal_bin_count(self) -> u128 {
        self.terminal_bin_count
    }

    #[must_use]
    pub const fn reserve_cardinality(self) -> usize {
        self.reserve_cardinality
    }

    #[must_use]
    pub const fn eligible_bin_count(self) -> u128 {
        self.eligible_bin_count
    }
}

/// Exact deterministic reserve set for one finite graph.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReserveSet {
    emission_order: Vec<u128>,
    ids_ascending: Vec<u128>,
}

impl ReserveSet {
    /// Select exactly the geometry's reserve cardinality by keeping the lowest keyed PRF scores.
    /// Equal scores are ordered by bin id, so the result is total and stable.
    pub fn select(
        geometry: FiniteReservoirGeometry,
        reserve_seed: u64,
    ) -> Result<Self, ReservoirError> {
        let cardinality = geometry.reserve_cardinality;
        let mut selected = BinaryHeap::<(u64, u128)>::new();
        selected
            .try_reserve_exact(cardinality)
            .map_err(|_| ReservoirError::AllocationFailed)?;

        let mut next_tle_source = 0u64;
        for bin_id in 0..geometry.terminal_bin_count {
            let is_tle_position = next_tle_source < geometry.source_count
                && geometry.params.tle_bin_id(next_tle_source) == bin_id;
            if is_tle_position {
                next_tle_source += 1;
                continue;
            }

            let candidate = (reserve_prf_score(reserve_seed, bin_id), bin_id);
            if selected.len() < cardinality {
                selected.push(candidate);
            } else if selected.peek().is_some_and(|largest| candidate < *largest) {
                selected.pop();
                selected.push(candidate);
            }
        }
        if next_tle_source != geometry.source_count || selected.len() != cardinality {
            return Err(ReservoirError::GeometryInvariantViolated);
        }

        let mut selected = selected.into_vec();
        selected.sort_unstable();
        let emission_order = selected
            .into_iter()
            .map(|(_, bin_id)| bin_id)
            .collect::<Vec<_>>();
        let mut ids_ascending = emission_order.clone();
        ids_ascending.sort_unstable();

        Ok(Self {
            emission_order,
            ids_ascending,
        })
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.emission_order.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.emission_order.is_empty()
    }

    #[must_use]
    pub fn contains(&self, bin_id: u128) -> bool {
        self.ids_ascending.binary_search(&bin_id).is_ok()
    }

    #[must_use]
    pub fn emission_order(&self) -> &[u128] {
        &self.emission_order
    }

    #[must_use]
    pub fn ids_ascending(&self) -> &[u128] {
        &self.ids_ascending
    }
}

/// Prototype sender-side freshness tracker shared by all peer reports.
///
/// `claim_fresh` accepts overlapping/reordered candidate lists, but a reserve id can be returned at
/// most once for its first global emission. Loss of an already emitted reserve bin is deliberately
/// outside this type and remains a separately counted Stage 2.4 retransmission.
#[derive(Clone, Debug)]
pub struct FreshReserveEmitter {
    reserve_ids: BTreeSet<u128>,
    emitted_ids: BTreeSet<u128>,
}

impl FreshReserveEmitter {
    #[must_use]
    pub fn new(reserve_set: &ReserveSet) -> Self {
        Self {
            reserve_ids: reserve_set.ids_ascending.iter().copied().collect(),
            emitted_ids: BTreeSet::new(),
        }
    }

    pub fn claim_fresh(
        &mut self,
        candidate_ids: impl IntoIterator<Item = u128>,
        limit: usize,
    ) -> Vec<u128> {
        candidate_ids
            .into_iter()
            .filter(|bin_id| self.reserve_ids.contains(bin_id))
            .filter(|bin_id| self.emitted_ids.insert(*bin_id))
            .take(limit)
            .collect()
    }

    #[must_use]
    pub fn emitted_count(&self) -> usize {
        self.emitted_ids.len()
    }
}

/// Check the independent sender-side payload reservation for this beyond-paper prototype.
pub fn checked_reserve_payload_bytes(
    reserve_cardinality: usize,
    symbol_bytes: usize,
    budget_bytes: usize,
) -> Result<usize, ReservoirError> {
    if symbol_bytes == 0 {
        return Err(ReservoirError::ZeroSymbolBytes);
    }
    let required_bytes = reserve_cardinality
        .checked_mul(symbol_bytes)
        .ok_or(ReservoirError::ArithmeticOverflow)?;
    if required_bytes > budget_bytes {
        return Err(ReservoirError::ReservePayloadBudgetExceeded {
            required_bytes,
            budget_bytes,
        });
    }
    Ok(required_bytes)
}

/// Errors from the simulation-only finite geometry and reserve selector.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReservoirError {
    ZeroDenominator,
    ZeroSymbolBytes,
    EmptySourcePrefix,
    ArithmeticOverflow,
    TargetBelowTailFloor {
        target_terminal_bin_count: u128,
        minimum_terminal_bin_count: u128,
    },
    TargetNotRepresentable {
        target_terminal_bin_count: u128,
        lower_terminal_bin_count: u128,
        upper_terminal_bin_count: u128,
    },
    ReserveExceedsEligiblePositions {
        reserve_cardinality: u128,
        eligible_bin_count: u128,
    },
    ReservePayloadBudgetExceeded {
        required_bytes: usize,
        budget_bytes: usize,
    },
    AllocationFailed,
    GeometryInvariantViolated,
}

fn params_for_interior_numerator(numerator: u32) -> MettleParams {
    let overhead = if numerator == 0 {
        OverheadRatio::ZERO
    } else {
        OverheadRatio::new(numerator, INTERIOR_OVERHEAD_DENOMINATOR)
            .expect("non-zero fixed-denominator interior overhead is valid")
    };
    MettleParams::new(overhead)
}

fn reserve_prf_score(seed: u64, bin_id: u128) -> u64 {
    let low = bin_id as u64;
    let high = (bin_id >> u64::BITS) as u64;
    let low_hash = splitmix64(seed ^ RESERVE_PRF_DOMAIN_LOW ^ low);
    splitmix64(low_hash ^ RESERVE_PRF_DOMAIN_HIGH ^ high)
}

fn splitmix64(mut value: u64) -> u64 {
    value = value.wrapping_add(0x9E37_79B9_7F4A_7C15);
    value = (value ^ (value >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    value ^ (value >> 31)
}

const fn gcd(mut lhs: u32, mut rhs: u32) -> u32 {
    while rhs != 0 {
        let remainder = lhs % rhs;
        lhs = rhs;
        rhs = remainder;
    }
    lhs
}

const fn gcd_u128(mut lhs: u128, mut rhs: u128) -> u128 {
    while rhs != 0 {
        let remainder = lhs % rhs;
        lhs = rhs;
        rhs = remainder;
    }
    lhs
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use crate::test_support::edge_bin_ids_with_terminal_source_count;

    use super::{
        FiniteReservoirGeometry, FreshReserveEmitter, RESERVE_PRF_VERSION, ReserveSet,
        ReservoirError, ReservoirRate, checked_reserve_payload_bytes,
    };

    fn rate(numerator: u32, denominator: u32) -> ReservoirRate {
        ReservoirRate::new(numerator, denominator).expect("test rate is valid")
    }

    #[test]
    fn reservoir_prototype_is_explicitly_versioned_beyond_the_paper() {
        assert_eq!(RESERVE_PRF_VERSION, 1);
    }

    #[test]
    fn finite_counts_include_tail_rounding_before_reserve_cardinality() {
        let geometry = FiniteReservoirGeometry::solve(8192, rate(8, 100), rate(1, 1000))
            .expect("8.1% finite geometry is representable");

        assert_eq!(geometry.wire_bin_count(), 8848);
        assert_eq!(geometry.terminal_bin_count(), 8856);
        assert_eq!(geometry.reserve_cardinality(), 8);
        assert_eq!(geometry.eligible_bin_count(), 664);
        assert_ne!(geometry.reserve_cardinality(), 9);
    }

    #[test]
    fn finite_solver_matches_the_corrected_stage_2_6_count() {
        let geometry = FiniteReservoirGeometry::solve(100_000, rate(0, 1), rate(55, 1000))
            .expect("the Stage 2.6 Table-IV count is representable");

        assert_eq!(geometry.terminal_bin_count(), 105_500);
        assert_eq!(geometry.wire_bin_count(), 100_000);
        assert_eq!(geometry.reserve_cardinality(), 5500);
        assert!(geometry.interior_overhead().as_f64() < 0.055);
    }

    #[test]
    fn reserve_selection_has_a_stable_committed_vector() {
        let geometry = FiniteReservoirGeometry::solve(8192, rate(8, 100), rate(1, 1000))
            .expect("8.1% finite geometry is representable");
        let reserve = ReserveSet::select(geometry, 0x5354_4147_4533_0001)
            .expect("reserve selection succeeds");

        assert_eq!(reserve.len(), 8);
        assert_eq!(
            reserve.emission_order(),
            &[8754, 8756, 8759, 924, 7684, 2139, 8553, 8777]
        );
        assert_eq!(
            reserve.ids_ascending(),
            &[924, 2139, 7684, 8553, 8754, 8756, 8759, 8777]
        );
    }

    #[test]
    fn tle_position_remains_protected_when_non_tle_edges_also_touch_it() {
        let source_count = 8192;
        let graph_seed = 0xA11C_E5E1_2026_0716;
        let geometry = FiniteReservoirGeometry::solve(source_count, rate(0, 1), rate(81, 1000))
            .expect("8.1% finite geometry is representable");
        let reserve = ReserveSet::select(geometry, 7).expect("reserve selection succeeds");
        let tle_positions = (0..source_count)
            .map(|source_id| geometry.params().tle_bin_id(source_id))
            .collect::<BTreeSet<_>>();
        let shared_tle_position = (0..source_count)
            .flat_map(|source_id| {
                edge_bin_ids_with_terminal_source_count(
                    geometry.params(),
                    source_id,
                    graph_seed,
                    Some(source_count),
                )[1..]
                    .to_vec()
            })
            .find(|bin_id| tle_positions.contains(bin_id))
            .expect("a non-TLE edge shares some source's TLE position");

        assert_eq!(reserve.len() as u128, geometry.eligible_bin_count());
        assert!(!reserve.contains(shared_tle_position));
    }

    #[test]
    fn reordered_overlapping_peer_reports_claim_each_reserve_id_once() {
        let geometry = FiniteReservoirGeometry::solve(8192, rate(8, 100), rate(1, 1000))
            .expect("8.1% finite geometry is representable");
        let reserve = ReserveSet::select(geometry, 11).expect("reserve selection succeeds");
        let mut emitter = FreshReserveEmitter::new(&reserve);
        let order = reserve.emission_order();

        let peer_b = emitter.claim_fresh(order.iter().rev().copied(), 5);
        let peer_a = emitter.claim_fresh(order.iter().copied(), usize::MAX);
        let peer_c = emitter.claim_fresh(order.iter().cycle().take(order.len() * 2).copied(), 9);
        let emitted = peer_b
            .into_iter()
            .chain(peer_a)
            .chain(peer_c)
            .collect::<BTreeSet<_>>();

        assert_eq!(emitted.len(), reserve.len());
        assert_eq!(emitter.emitted_count(), reserve.len());
    }

    #[test]
    fn finite_geometry_rejects_targets_below_the_tail_floor() {
        assert!(matches!(
            FiniteReservoirGeometry::solve(256, rate(1, 100), rate(1, 100)),
            Err(ReservoirError::TargetBelowTailFloor { .. })
        ));
    }

    #[test]
    fn reserve_payload_budget_is_independent_and_loud_at_the_boundary() {
        assert_eq!(checked_reserve_payload_bytes(8, 1400, 11_200), Ok(11_200));
        assert_eq!(
            checked_reserve_payload_bytes(8, 1400, 11_199),
            Err(ReservoirError::ReservePayloadBudgetExceeded {
                required_bytes: 11_200,
                budget_bytes: 11_199,
            })
        );
        assert_eq!(
            checked_reserve_payload_bytes(8, 0, usize::MAX),
            Err(ReservoirError::ZeroSymbolBytes)
        );
    }
}
