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

}

const fn gcd(mut lhs: u32, mut rhs: u32) -> u32 {
    while rhs != 0 {
        let remainder = lhs % rhs;
        lhs = rhs;
        rhs = remainder;
    }
    lhs
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
}
