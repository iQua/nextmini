//! METTLE parameters and deterministic graph placement rules.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};

use super::hash::{derive_u64, uniform_below};

pub const PAPER_EDGE_COUNT: usize = 4;
pub type SourceEdges = [u64; PAPER_EDGE_COUNT];

#[derive(Debug)]
struct BinomialCdf {
    cutoff: Box<[u64]>,
}

impl BinomialCdf {
    fn new(n: u32, denominator: u32) -> Self {
        let p = 1.0 / f64::from(denominator);
        let q = 1.0 - p;

        let mut pmf = vec![0.0_f64; n as usize + 1];
        pmf[0] = q.powi(n as i32);
        for k in 0..n as usize {
            pmf[k + 1] = pmf[k] * ((n as usize - k) as f64 / (k + 1) as f64) * (p / q);
        }

        let mut acc = 0.0_f64;
        let mut cutoff = Vec::with_capacity(pmf.len());
        for prob in pmf {
            acc += prob;
            cutoff.push((acc * u64::MAX as f64) as u64);
        }
        if let Some(last) = cutoff.last_mut() {
            *last = u64::MAX;
        }

        Self {
            cutoff: cutoff.into_boxed_slice(),
        }
    }

    fn sample(&self, u: u64) -> u32 {
        self.cutoff.partition_point(|&x| x < u) as u32
    }
}

#[derive(Debug)]
struct BinomialTables {
    p2: BinomialCdf,
    p4: BinomialCdf,
    p8: BinomialCdf,
}

fn binomial_tables_for(width: u32) -> Arc<BinomialTables> {
    static CACHE: OnceLock<Mutex<HashMap<u32, Arc<BinomialTables>>>> = OnceLock::new();
    let cache = CACHE.get_or_init(|| Mutex::new(HashMap::new()));
    let mut guard = cache
        .lock()
        .expect("binomial table cache lock should not be poisoned");
    guard
        .entry(width)
        .or_insert_with(|| {
            Arc::new(BinomialTables {
                p2: BinomialCdf::new(width, 2),
                p4: BinomialCdf::new(width, 4),
                p8: BinomialCdf::new(width, 8),
            })
        })
        .clone()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LandingProfile {
    BaselineUniform,
    TleUniform,
    PaperDefault,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CodedRate {
    numerator: u32,
    denominator: u32,
}

impl CodedRate {
    pub fn new(numerator: u32, denominator: u32) -> Result<Self, MettleParamError> {
        if denominator == 0 {
            return Err(MettleParamError::ZeroRateDenominator);
        }
        if numerator < denominator {
            return Err(MettleParamError::SubUnitCodedRate);
        }
        Ok(Self {
            numerator,
            denominator,
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
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MettleParams {
    pub source_symbol_bytes: usize,
    pub coupling_window: u32,
    pub coded_rate: CodedRate,
    pub seed: u64,
    pub profile: LandingProfile,
    pub tail_compression: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MettleParamError {
    ZeroSymbolBytes,
    ZeroCouplingWindow,
    ZeroRateDenominator,
    SubUnitCodedRate,
    TooNarrowCodedWindow,
}

impl std::fmt::Display for MettleParamError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let message = match self {
            Self::ZeroSymbolBytes => "source_symbol_bytes must be positive",
            Self::ZeroCouplingWindow => "coupling_window must be positive",
            Self::ZeroRateDenominator => "coded_rate denominator must be positive",
            Self::SubUnitCodedRate => "coded_rate must be at least 1.0",
            Self::TooNarrowCodedWindow => "coded window must be at least four bins wide",
        };
        write!(f, "{message}")
    }
}

impl MettleParams {
    pub fn new(
        source_symbol_bytes: usize,
        coupling_window: u32,
        coded_rate: CodedRate,
        seed: u64,
        profile: LandingProfile,
    ) -> Result<Self, MettleParamError> {
        if source_symbol_bytes == 0 {
            return Err(MettleParamError::ZeroSymbolBytes);
        }
        if coupling_window == 0 {
            return Err(MettleParamError::ZeroCouplingWindow);
        }
        let params = Self {
            source_symbol_bytes,
            coupling_window,
            coded_rate,
            seed,
            profile,
            tail_compression: false,
        };
        if params.window_bins() < PAPER_EDGE_COUNT as u64 {
            return Err(MettleParamError::TooNarrowCodedWindow);
        }
        Ok(params)
    }

    pub fn paper_default(
        source_symbol_bytes: usize,
        coded_rate: CodedRate,
        seed: u64,
    ) -> Result<Self, MettleParamError> {
        Self::new(
            source_symbol_bytes,
            600,
            coded_rate,
            seed,
            LandingProfile::PaperDefault,
        )
    }

    #[must_use]
    pub fn with_profile(mut self, profile: LandingProfile) -> Self {
        self.profile = profile;
        self
    }

    #[must_use]
    pub fn with_tail_compression(mut self, tail_compression: bool) -> Self {
        self.tail_compression = tail_compression;
        self
    }

    #[must_use]
    pub fn base(self, source_id: u64) -> u64 {
        source_id.saturating_mul(u64::from(self.coded_rate.numerator()))
            / u64::from(self.coded_rate.denominator())
    }

    #[must_use]
    pub fn window_bins(self) -> u64 {
        (u64::from(self.coupling_window) * u64::from(self.coded_rate.numerator()))
            .div_ceil(u64::from(self.coded_rate.denominator()))
    }

    #[must_use]
    pub fn right_exclusive(self, source_id: u64) -> u64 {
        self.base(source_id) + self.window_bins()
    }

    #[must_use]
    pub fn source_count_for_bytes(self, total_bytes: usize) -> u64 {
        total_bytes.div_ceil(self.source_symbol_bytes) as u64
    }

    #[must_use]
    pub fn edges_for(self, source_id: u64) -> SourceEdges {
        let base = self.base(source_id);
        let right_exclusive = self.right_exclusive(source_id);
        let width = self.window_bins() as u32;

        let mut edges = [0_u64; PAPER_EDGE_COUNT];
        edges[0] = match self.profile {
            LandingProfile::BaselineUniform => {
                self.sample_uniform_bin(source_id, 0, 0, base, width)
            }
            LandingProfile::TleUniform | LandingProfile::PaperDefault => base,
        };

        for edge_idx in 1..PAPER_EDGE_COUNT {
            let mut salt = 0_u64;
            loop {
                let candidate = match self.profile {
                    LandingProfile::BaselineUniform | LandingProfile::TleUniform => {
                        self.sample_uniform_bin(source_id, edge_idx as u8, salt, base, width)
                    }
                    LandingProfile::PaperDefault => self.sample_paper_default_bin(
                        source_id,
                        edge_idx as u8,
                        salt,
                        right_exclusive,
                        width,
                    ),
                };
                if !edges[..edge_idx].contains(&candidate) {
                    edges[edge_idx] = candidate;
                    break;
                }
                salt = salt.wrapping_add(1);
            }
        }

        edges
    }

    fn sample_uniform_bin(
        self,
        source_id: u64,
        edge_idx: u8,
        salt: u64,
        base: u64,
        width: u32,
    ) -> u64 {
        base + uniform_below(
            self.seed ^ salt.rotate_left(7),
            source_id,
            edge_idx,
            salt,
            u64::from(width),
        )
    }

    fn sample_paper_default_bin(
        self,
        source_id: u64,
        edge_idx: u8,
        salt: u64,
        right_exclusive: u64,
        width: u32,
    ) -> u64 {
        let power = match edge_idx {
            1 => 1,
            2 => 2,
            _ => 3,
        };
        let eta = self.sample_eta_binomial_pow2(source_id, edge_idx, salt, width, power);
        right_exclusive - u64::from(eta)
    }

    fn sample_eta_binomial_pow2(
        self,
        source_id: u64,
        edge_idx: u8,
        salt: u64,
        width: u32,
        power: u8,
    ) -> u32 {
        let sample = derive_u64(
            self.seed ^ 0xC0DE_CAFE ^ salt.rotate_left(17),
            source_id,
            edge_idx,
            u64::from(width) ^ u64::from(power),
        );
        let tables = binomial_tables_for(width);
        let eta = match power {
            1 => tables.p2.sample(sample),
            2 => tables.p4.sample(sample),
            _ => tables.p8.sample(sample),
        };
        eta.clamp(1, width)
    }
}


