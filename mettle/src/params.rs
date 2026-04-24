#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ParamsError {
    ZeroDenominator,
    ZeroOverhead,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OverheadRatio {
    numerator: u32,
    denominator: u32,
}

impl OverheadRatio {
    pub fn new(numerator: u32, denominator: u32) -> Result<Self, ParamsError> {
        if denominator == 0 {
            return Err(ParamsError::ZeroDenominator);
        }
        if numerator == 0 {
            return Err(ParamsError::ZeroOverhead);
        }
        let divisor = gcd(numerator, denominator);
        Ok(Self {
            numerator: numerator / divisor,
            denominator: denominator / divisor,
        })
    }

    pub const fn numerator(self) -> u32 {
        self.numerator
    }

    pub const fn denominator(self) -> u32 {
        self.denominator
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MettleParams {
    overhead: OverheadRatio,
}

impl MettleParams {
    pub const EDGE_COUNT: usize = 4;
    pub const COUPLING_WINDOW: u64 = 600;
    pub const NON_TLE_PROFILE: [(u32, u32); 3] = [(1, 2), (1, 4), (1, 8)];

    pub const fn new(overhead: OverheadRatio) -> Self {
        Self { overhead }
    }

    pub const fn overhead(self) -> OverheadRatio {
        self.overhead
    }

    fn expansion_numerator(self) -> u128 {
        u128::from(self.overhead.numerator()) + u128::from(self.overhead.denominator())
    }

    pub(crate) fn tle_bin_id(self, source_id: u64) -> u128 {
        (u128::from(source_id) * self.expansion_numerator())
            / u128::from(self.overhead.denominator())
    }

    pub(crate) fn departure_frontier_after_source_count(self, source_count: u64) -> u128 {
        self.tle_bin_id(source_count)
    }

    pub(crate) fn latest_source_id_for_bin(self, bin_id: u128) -> Option<u64> {
        let denominator = u128::from(self.overhead.denominator());
        let expansion_numerator = u128::from(self.overhead.numerator()) + denominator;
        let scaled = bin_id
            .checked_add(1)?
            .checked_mul(denominator)?
            .checked_sub(1)?;
        u64::try_from(scaled / expansion_numerator).ok()
    }

    pub(crate) fn edge_bin_ids_with_terminal_source_count(
        self,
        source_id: u64,
        seed: u64,
        terminal_source_count: Option<u64>,
    ) -> [u128; Self::EDGE_COUNT] {
        let mut edge_bin_ids = [self.tle_bin_id(source_id); Self::EDGE_COUNT];

        for (profile_index, _) in Self::NON_TLE_PROFILE.iter().enumerate() {
            edge_bin_ids[profile_index + 1] = self.non_tle_edge_bin_id_for_profile_index(
                source_id,
                seed,
                profile_index,
                terminal_source_count,
            );
        }

        edge_bin_ids
    }

    #[cfg_attr(not(test), allow(dead_code))]
    fn window_end_exclusive(self, source_id: u64) -> u128 {
        let scaled = (u128::from(source_id) + u128::from(Self::COUPLING_WINDOW))
            * self.expansion_numerator();
        let denominator = u128::from(self.overhead.denominator());

        scaled / denominator + u128::from(!scaled.is_multiple_of(denominator))
    }

    fn tail_compression_factor(
        self,
        source_id: u64,
        terminal_source_count: Option<u64>,
    ) -> Option<(u128, u128)> {
        let terminal_source_count = terminal_source_count?;
        let tail_source_count = terminal_source_count.min(Self::COUPLING_WINDOW);
        let tail_start = terminal_source_count - tail_source_count;
        if source_id < tail_start {
            return None;
        }
        if tail_source_count == 1 {
            return Some((2, 1));
        }
        let factor_denominator = u128::from(tail_source_count - 1);
        let factor_numerator = factor_denominator + u128::from(source_id - tail_start);
        Some((factor_numerator, factor_denominator))
    }

    fn window_width_with_terminal_source_count(
        self,
        source_id: u64,
        terminal_source_count: Option<u64>,
    ) -> u128 {
        let uncompressed_width = self.window_end_exclusive(source_id) - self.tle_bin_id(source_id);
        let Some((factor_numerator, factor_denominator)) =
            self.tail_compression_factor(source_id, terminal_source_count)
        else {
            return uncompressed_width;
        };
        div_ceil(uncompressed_width * factor_denominator, factor_numerator)
    }

    fn window_end_exclusive_with_terminal_source_count(
        self,
        source_id: u64,
        terminal_source_count: Option<u64>,
    ) -> u128 {
        self.tle_bin_id(source_id)
            + self.window_width_with_terminal_source_count(source_id, terminal_source_count)
    }

    pub(crate) fn terminal_departure_end_exclusive(self, terminal_source_count: u64) -> u128 {
        if terminal_source_count == 0 {
            return 0;
        }
        let tail_source_count = terminal_source_count.min(Self::COUPLING_WINDOW);
        let tail_start = terminal_source_count - tail_source_count;
        let mut max_end_exclusive = if tail_start == 0 {
            0
        } else {
            self.window_end_exclusive(tail_start - 1)
        };

        for source_id in tail_start..terminal_source_count {
            max_end_exclusive =
                max_end_exclusive.max(self.window_end_exclusive_with_terminal_source_count(
                    source_id,
                    Some(terminal_source_count),
                ));
        }

        max_end_exclusive
    }

    pub(crate) fn possible_source_id_range_for_bin(
        self,
        bin_id: u128,
        terminal_source_count: Option<u64>,
    ) -> Option<(u64, u64)> {
        let mut latest_source_id = self.latest_source_id_for_bin(bin_id)?;
        if let Some(0) = terminal_source_count {
            return None;
        }
        if let Some(terminal_source_count) = terminal_source_count {
            latest_source_id = latest_source_id.min(terminal_source_count - 1);
        }
        let earliest_candidate = latest_source_id.saturating_sub(Self::COUPLING_WINDOW);
        let mut earliest_source_id = None;
        let mut latest_valid_source_id = None;

        for source_id in earliest_candidate..=latest_source_id {
            if self.source_window_contains_bin(source_id, bin_id, terminal_source_count) {
                earliest_source_id.get_or_insert(source_id);
                latest_valid_source_id = Some(source_id);
            }
        }

        earliest_source_id.zip(latest_valid_source_id)
    }

    #[cfg_attr(not(test), allow(dead_code))]
    fn second_edge_bin_id(self, source_id: u64, seed: u64) -> u128 {
        self.non_tle_edge_bin_id_for_profile_index(source_id, seed, 0, None)
    }

    #[cfg_attr(not(test), allow(dead_code))]
    fn third_edge_bin_id(self, source_id: u64, seed: u64) -> u128 {
        self.non_tle_edge_bin_id_for_profile_index(source_id, seed, 1, None)
    }

    #[cfg_attr(not(test), allow(dead_code))]
    fn fourth_edge_bin_id(self, source_id: u64, seed: u64) -> u128 {
        self.non_tle_edge_bin_id_for_profile_index(source_id, seed, 2, None)
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn edge_bin_ids(self, source_id: u64, seed: u64) -> [u128; Self::EDGE_COUNT] {
        self.edge_bin_ids_with_terminal_source_count(source_id, seed, None)
    }

    #[cfg_attr(not(test), allow(dead_code))]
    fn non_tle_trials(self) -> u128 {
        self.nominal_non_tle_trials()
    }

    fn nominal_non_tle_trials(self) -> u128 {
        (u128::from(Self::COUPLING_WINDOW) * self.expansion_numerator())
            / u128::from(self.overhead.denominator())
    }

    fn non_tle_trials_with_terminal_source_count(
        self,
        source_id: u64,
        terminal_source_count: Option<u64>,
    ) -> u128 {
        let uncompressed_trials = self.nominal_non_tle_trials();
        let Some((factor_numerator, factor_denominator)) =
            self.tail_compression_factor(source_id, terminal_source_count)
        else {
            return uncompressed_trials;
        };
        div_ceil(uncompressed_trials * factor_denominator, factor_numerator)
    }

    fn non_tle_edge_bin_id(
        self,
        source_id: u64,
        seed: u64,
        edge_index: u64,
        denominator: u32,
        terminal_source_count: Option<u64>,
    ) -> u128 {
        let eta = sample_power_of_two_binomial(
            self.non_tle_trials_with_terminal_source_count(source_id, terminal_source_count),
            denominator,
            mix_entropy(seed, source_id, edge_index),
        );
        self.non_tle_edge_bin_id_for_eta(source_id, eta, terminal_source_count)
    }

    fn non_tle_edge_bin_id_for_profile_index(
        self,
        source_id: u64,
        seed: u64,
        profile_index: usize,
        terminal_source_count: Option<u64>,
    ) -> u128 {
        let edge_index = profile_index as u64 + 2;
        let denominator = Self::NON_TLE_PROFILE[profile_index].1;

        self.non_tle_edge_bin_id(
            source_id,
            seed,
            edge_index,
            denominator,
            terminal_source_count,
        )
    }

    fn non_tle_edge_bin_id_for_eta(
        self,
        source_id: u64,
        eta: u128,
        terminal_source_count: Option<u64>,
    ) -> u128 {
        let rightmost_bin = self
            .window_end_exclusive_with_terminal_source_count(source_id, terminal_source_count)
            - 1;
        let nominal_rightmost_bin = self.tle_bin_id(source_id)
            + self.non_tle_trials_with_terminal_source_count(source_id, terminal_source_count);

        nominal_rightmost_bin.saturating_sub(eta).min(rightmost_bin)
    }

    fn source_window_contains_bin(
        self,
        source_id: u64,
        bin_id: u128,
        terminal_source_count: Option<u64>,
    ) -> bool {
        self.tle_bin_id(source_id) <= bin_id
            && bin_id
                < self.window_end_exclusive_with_terminal_source_count(
                    source_id,
                    terminal_source_count,
                )
    }
}

const fn gcd(mut lhs: u32, mut rhs: u32) -> u32 {
    while rhs != 0 {
        let remainder = lhs % rhs;
        lhs = rhs;
        rhs = remainder;
    }
    lhs
}

fn div_ceil(lhs: u128, rhs: u128) -> u128 {
    let quotient = lhs / rhs;
    if lhs.is_multiple_of(rhs) {
        quotient
    } else {
        quotient + 1
    }
}

#[cfg_attr(not(test), allow(dead_code))]
fn mix_entropy(seed: u64, source_id: u64, edge_index: u64) -> u64 {
    seed ^ source_id.rotate_left(21) ^ edge_index.rotate_left(42)
}

#[cfg_attr(not(test), allow(dead_code))]
fn sample_power_of_two_binomial(trials: u128, denominator: u32, mut state: u64) -> u128 {
    debug_assert!(denominator.is_power_of_two());
    let mask = u64::from(denominator - 1);
    let mut successes = 0;
    let mut remaining = trials;

    while remaining != 0 {
        if next_entropy(&mut state) & mask == 0 {
            successes += 1;
        }
        remaining -= 1;
    }

    successes
}

#[cfg_attr(not(test), allow(dead_code))]
fn next_entropy(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut value = *state;
    value = (value ^ (value >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    value ^ (value >> 31)
}

#[cfg(test)]
mod tests {
    use super::{MettleParams, OverheadRatio, ParamsError, div_ceil};

    fn brute_force_possible_source_id_range(
        params: MettleParams,
        bin_id: u128,
        terminal_source_count: u64,
    ) -> Option<(u64, u64)> {
        let touching_sources = (0..terminal_source_count)
            .filter(|&source_id| {
                params.tle_bin_id(source_id) <= bin_id
                    && bin_id
                        < params.window_end_exclusive_with_terminal_source_count(
                            source_id,
                            Some(terminal_source_count),
                        )
            })
            .collect::<Vec<_>>();

        touching_sources
            .first()
            .copied()
            .zip(touching_sources.last().copied())
    }

    #[test]
    fn overhead_rejects_zero_denominator() {
        assert_eq!(OverheadRatio::new(1, 0), Err(ParamsError::ZeroDenominator));
    }

    #[test]
    fn overhead_rejects_zero() {
        assert_eq!(OverheadRatio::new(0, 1), Err(ParamsError::ZeroOverhead));
    }

    #[test]
    fn overhead_keeps_fraction_components() {
        let overhead = OverheadRatio::new(1, 20).expect("valid overhead");
        assert_eq!(overhead.numerator(), 1);
        assert_eq!(overhead.denominator(), 20);
    }

    #[test]
    fn overhead_canonicalizes_equivalent_fractions() {
        assert_eq!(
            OverheadRatio::new(1, 20).expect("valid overhead"),
            OverheadRatio::new(2, 40).expect("valid overhead")
        );
    }

    #[test]
    fn mettle_params_keep_constructor_fields() {
        let overhead = OverheadRatio::new(1, 20).expect("valid overhead");
        let params = MettleParams::new(overhead);
        assert_eq!(params.overhead(), overhead);
    }

    #[test]
    fn mettle_defaults_match_paper_profile() {
        assert_eq!(MettleParams::EDGE_COUNT, 4);
        assert_eq!(MettleParams::COUPLING_WINDOW, 600);
        assert_eq!(MettleParams::NON_TLE_PROFILE, [(1, 2), (1, 4), (1, 8)]);
    }

    #[test]
    fn tle_bin_id_matches_floor_of_one_plus_c_times_source_id() {
        let params = MettleParams::new(OverheadRatio::new(1, 20).expect("valid overhead"));
        assert_eq!(params.tle_bin_id(0), 0);
        assert_eq!(params.tle_bin_id(1), 1);
        assert_eq!(params.tle_bin_id(19), 19);
        assert_eq!(params.tle_bin_id(20), 21);
        assert_eq!(params.tle_bin_id(21), 22);
    }

    #[test]
    fn window_end_bin_matches_ceiling_of_scaled_right_boundary() {
        let params = MettleParams::new(OverheadRatio::new(1, 20).expect("valid overhead"));
        assert_eq!(params.window_end_exclusive(0), 630);
        assert_eq!(params.window_end_exclusive(1), 632);
        assert_eq!(params.window_end_exclusive(7), 638);
    }

    #[test]
    fn half_open_window_width_follows_the_direct_boundary_formula() {
        let params = MettleParams::new(OverheadRatio::new(1, 7).expect("valid overhead"));
        assert_eq!(params.window_end_exclusive(0), 686);
        assert_eq!(params.window_end_exclusive(1), 687);
        assert_eq!(params.window_end_exclusive(0) - params.tle_bin_id(0), 686);
        assert_eq!(params.window_end_exclusive(11) - params.tle_bin_id(11), 687);
    }

    #[test]
    fn second_edge_stays_inside_half_open_window() {
        let params = MettleParams::new(OverheadRatio::new(1, 20).expect("valid overhead"));
        let source_id = 37;
        let edge = params.second_edge_bin_id(source_id, 0x1234_5678_9ABC_DEF0);

        assert!(params.tle_bin_id(source_id) <= edge);
        assert!(edge < params.window_end_exclusive(source_id));
    }

    #[test]
    fn second_edge_is_deterministic_for_same_seed() {
        let params = MettleParams::new(OverheadRatio::new(1, 20).expect("valid overhead"));
        let first = params.second_edge_bin_id(37, 0x1234_5678_9ABC_DEF0);
        let second = params.second_edge_bin_id(37, 0x1234_5678_9ABC_DEF0);

        assert_eq!(first, second);
    }

    #[test]
    fn second_edge_uses_full_half_open_support_at_the_left_boundary() {
        let params = MettleParams::new(OverheadRatio::new(1, 20).expect("valid overhead"));
        assert_eq!(params.non_tle_trials(), 630);
        assert_eq!(params.non_tle_edge_bin_id_for_eta(0, 0, None), 629);
        assert_eq!(
            params.non_tle_edge_bin_id_for_eta(0, params.non_tle_trials(), None),
            0
        );
    }

    #[test]
    fn non_tle_trials_follow_the_paper_constant_width() {
        let params = MettleParams::new(OverheadRatio::new(1, 20).expect("valid overhead"));

        assert_eq!(params.non_tle_trials(), 630);
    }

    #[test]
    fn second_edge_matches_fixed_golden() {
        let params = MettleParams::new(OverheadRatio::new(1, 20).expect("valid overhead"));
        assert_eq!(params.second_edge_bin_id(37, 0x1234_5678_9ABC_DEF0), 363);
    }

    #[test]
    fn third_edge_stays_inside_half_open_window() {
        let params = MettleParams::new(OverheadRatio::new(1, 20).expect("valid overhead"));
        let source_id = 37;
        let edge = params.third_edge_bin_id(source_id, 0x1234_5678_9ABC_DEF0);

        assert!(params.tle_bin_id(source_id) <= edge);
        assert!(edge < params.window_end_exclusive(source_id));
    }

    #[test]
    fn third_edge_is_deterministic_for_same_seed() {
        let params = MettleParams::new(OverheadRatio::new(1, 20).expect("valid overhead"));
        let first = params.third_edge_bin_id(37, 0x1234_5678_9ABC_DEF0);
        let second = params.third_edge_bin_id(37, 0x1234_5678_9ABC_DEF0);

        assert_eq!(first, second);
    }

    #[test]
    fn third_edge_matches_fixed_golden() {
        let params = MettleParams::new(OverheadRatio::new(1, 20).expect("valid overhead"));
        assert_eq!(params.third_edge_bin_id(37, 0x1234_5678_9ABC_DEF0), 509);
    }

    #[test]
    fn fourth_edge_stays_inside_half_open_window() {
        let params = MettleParams::new(OverheadRatio::new(1, 20).expect("valid overhead"));
        let source_id = 37;
        let edge = params.fourth_edge_bin_id(source_id, 0x1234_5678_9ABC_DEF0);

        assert!(params.tle_bin_id(source_id) <= edge);
        assert!(edge < params.window_end_exclusive(source_id));
    }

    #[test]
    fn fourth_edge_handles_the_left_boundary_case() {
        let params = MettleParams::new(OverheadRatio::new(1, 20).expect("valid overhead"));
        let trials = params.non_tle_trials();

        assert_eq!(trials, 630);
        assert_eq!(params.non_tle_edge_bin_id_for_eta(0, 0, None), 629);
        assert_eq!(params.non_tle_edge_bin_id_for_eta(0, trials, None), 0);
    }

    #[test]
    fn fourth_edge_is_deterministic_for_same_seed() {
        let params = MettleParams::new(OverheadRatio::new(1, 20).expect("valid overhead"));
        let first = params.fourth_edge_bin_id(37, 0x1234_5678_9ABC_DEF0);
        let second = params.fourth_edge_bin_id(37, 0x1234_5678_9ABC_DEF0);

        assert_eq!(first, second);
    }

    #[test]
    fn fourth_edge_matches_fixed_golden() {
        let params = MettleParams::new(OverheadRatio::new(1, 20).expect("valid overhead"));
        assert_eq!(params.fourth_edge_bin_id(37, 0x1234_5678_9ABC_DEF0), 590);
    }

    #[test]
    fn tail_compression_leaves_non_tail_sources_unchanged() {
        let params = MettleParams::new(OverheadRatio::new(1, 20).expect("valid overhead"));
        let terminal_source_count = MettleParams::COUPLING_WINDOW + 10;
        let source_id = 9;

        assert_eq!(
            params
                .non_tle_trials_with_terminal_source_count(source_id, Some(terminal_source_count)),
            params.non_tle_trials()
        );
    }

    #[test]
    fn tail_compression_halves_the_last_source_range() {
        let params = MettleParams::new(OverheadRatio::new(1, 20).expect("valid overhead"));
        let terminal_source_count = 10_000;
        let source_id = terminal_source_count - 1;
        let uncompressed_width =
            params.window_end_exclusive(source_id) - params.tle_bin_id(source_id);
        let compressed_width =
            params.window_width_with_terminal_source_count(source_id, Some(terminal_source_count));

        assert_eq!(compressed_width, div_ceil(uncompressed_width, 2));
    }

    #[test]
    fn tail_compression_shrinks_monotonically_across_the_tail() {
        let params = MettleParams::new(OverheadRatio::new(1, 20).expect("valid overhead"));
        let terminal_source_count = 10_000;
        let tail_start = terminal_source_count - MettleParams::COUPLING_WINDOW;
        let first_tail_width =
            params.window_width_with_terminal_source_count(tail_start, Some(terminal_source_count));
        let last_tail_width = params.window_width_with_terminal_source_count(
            terminal_source_count - 1,
            Some(terminal_source_count),
        );

        assert_eq!(
            first_tail_width,
            params.window_end_exclusive(tail_start) - params.tle_bin_id(tail_start)
        );
        assert!(last_tail_width < first_tail_width);
    }

    #[test]
    fn tail_compression_also_applies_when_the_block_is_smaller_than_the_window() {
        let params = MettleParams::new(OverheadRatio::new(1, 20).expect("valid overhead"));
        let terminal_source_count = 127;
        let first_source_width =
            params.window_width_with_terminal_source_count(0, Some(terminal_source_count));
        let last_source_id = terminal_source_count - 1;
        let last_uncompressed_width =
            params.window_end_exclusive(last_source_id) - params.tle_bin_id(last_source_id);
        let last_source_width = params
            .window_width_with_terminal_source_count(last_source_id, Some(terminal_source_count));

        assert_eq!(
            first_source_width,
            params.window_end_exclusive(0) - params.tle_bin_id(0)
        );
        assert_eq!(last_source_width, div_ceil(last_uncompressed_width, 2));
        assert!(last_source_width < first_source_width);
    }

    #[test]
    fn tail_compressed_non_tle_trials_use_the_compressed_nominal_width() {
        let params = MettleParams::new(OverheadRatio::new(1, 20).expect("valid overhead"));
        let terminal_source_count = 127;

        assert_eq!(
            params.non_tle_trials_with_terminal_source_count(0, Some(terminal_source_count)),
            630
        );
        assert_eq!(
            params.non_tle_trials_with_terminal_source_count(
                terminal_source_count - 1,
                Some(terminal_source_count)
            ),
            315
        );
    }

    #[test]
    fn exact_possible_source_range_matches_bruteforce_at_tail_boundary() {
        let params = MettleParams::new(OverheadRatio::new(1, 20).expect("valid overhead"));
        let terminal_source_count = 10_000;
        let tail_start = terminal_source_count - MettleParams::COUPLING_WINDOW;
        let boundary_bin_id = params.tle_bin_id(tail_start) + 17;

        assert_eq!(
            params.possible_source_id_range_for_bin(boundary_bin_id, Some(terminal_source_count)),
            brute_force_possible_source_id_range(params, boundary_bin_id, terminal_source_count)
        );
    }

    #[test]
    fn exact_possible_source_range_matches_bruteforce_inside_tail() {
        let params = MettleParams::new(OverheadRatio::new(1, 20).expect("valid overhead"));
        let terminal_source_count = 10_000;
        let tail_bin_id = params.tle_bin_id(terminal_source_count - 1) + 7;

        assert_eq!(
            params.possible_source_id_range_for_bin(tail_bin_id, Some(terminal_source_count)),
            brute_force_possible_source_id_range(params, tail_bin_id, terminal_source_count)
        );
    }
}
