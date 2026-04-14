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
    use super::{OverheadRatio, ParamsError};

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
}
