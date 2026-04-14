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

    pub(crate) fn tle_bin_id(self, source_id: u64) -> u128 {
        let expansion_numerator =
            u128::from(self.overhead.numerator()) + u128::from(self.overhead.denominator());
        (u128::from(source_id) * expansion_numerator) / u128::from(self.overhead.denominator())
    }

    #[cfg_attr(not(test), allow(dead_code))]
    fn window_end_exclusive(self, source_id: u64) -> u128 {
        let expansion_numerator =
            u128::from(self.overhead.numerator()) + u128::from(self.overhead.denominator());
        (u128::from(source_id) + u128::from(Self::COUPLING_WINDOW)) * expansion_numerator
            / u128::from(self.overhead.denominator())
            + u128::from(
                ((u128::from(source_id) + u128::from(Self::COUPLING_WINDOW)) * expansion_numerator)
                    % u128::from(self.overhead.denominator())
                    != 0,
            )
    }

    #[cfg_attr(not(test), allow(dead_code))]
    fn second_edge_bin_id(self, source_id: u64, seed: u64) -> u128 {
        self.non_tle_edge_bin_id(source_id, seed, 2, Self::NON_TLE_PROFILE[0].1)
    }

    #[cfg_attr(not(test), allow(dead_code))]
    fn third_edge_bin_id(self, source_id: u64, seed: u64) -> u128 {
        self.non_tle_edge_bin_id(source_id, seed, 3, Self::NON_TLE_PROFILE[1].1)
    }

    #[cfg_attr(not(test), allow(dead_code))]
    fn fourth_edge_bin_id(self, source_id: u64, seed: u64) -> u128 {
        self.non_tle_edge_bin_id(source_id, seed, 4, Self::NON_TLE_PROFILE[2].1)
    }

    pub(crate) fn edge_bin_ids(self, source_id: u64, seed: u64) -> [u128; Self::EDGE_COUNT] {
        [
            self.tle_bin_id(source_id),
            self.second_edge_bin_id(source_id, seed),
            self.third_edge_bin_id(source_id, seed),
            self.fourth_edge_bin_id(source_id, seed),
        ]
    }

    fn non_tle_trials(self, source_id: u64) -> u128 {
        self.window_end_exclusive(source_id) - self.tle_bin_id(source_id) - 1
    }

    fn non_tle_edge_bin_id(
        self,
        source_id: u64,
        seed: u64,
        edge_index: u64,
        denominator: u32,
    ) -> u128 {
        let eta = sample_power_of_two_binomial(
            self.non_tle_trials(source_id),
            denominator,
            mix_entropy(seed, source_id, edge_index),
        );
        self.non_tle_edge_bin_id_for_eta(source_id, eta)
    }

    fn non_tle_edge_bin_id_for_eta(self, source_id: u64, eta: u128) -> u128 {
        self.window_end_exclusive(source_id) - 1 - eta
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
    use super::{MettleParams, OverheadRatio, ParamsError};

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
        assert_eq!(params.non_tle_trials(0), 629);
        assert_eq!(params.window_end_exclusive(0) - 1, 629);
        assert_eq!(params.window_end_exclusive(0) - 1 - params.non_tle_trials(0), 0);
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
        let trials = params.non_tle_trials(0);

        assert_eq!(trials, 629);
        assert_eq!(params.non_tle_edge_bin_id_for_eta(0, 0), 629);
        assert_eq!(params.non_tle_edge_bin_id_for_eta(0, trials), 0);
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

}
