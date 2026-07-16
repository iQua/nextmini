//! Deterministic one-process harness for the Stage 2.0 METTLE decoder spike.
//!
//! Run one layout/scenario per process so `getrusage` reports an isolated peak:
//! `cargo run --release -p mettle --example decoder_layout_spike -- dense ordered 65536 1400`.

use std::collections::HashSet;
use std::env;
use std::fmt::{Display, Formatter};
use std::num::NonZeroUsize;
use std::time::{Duration, Instant};

#[cfg(target_os = "linux")]
use std::fs;
#[cfg(not(target_os = "linux"))]
use std::process::Command;

use mettle::test_support::{
    Decoder, DecoderStats, Encoder, edge_bin_ids_with_terminal_source_count,
};
use mettle::{MettleParams, OverheadRatio};

const OVERHEAD_NUMERATOR: u32 = 1;
const OVERHEAD_DENOMINATOR: u32 = 20;
const DEFAULT_SEED: u64 = 0x5EED_2A00_2026_0716;
const LOSS_DENOMINATOR: u64 = 10_000;
const LOSS_NUMERATOR: u64 = 100;
const REORDER_BATCH: usize = 64;
const RSS_PROGRESS_SAMPLES: usize = 16;
const DENSE_PAYLOAD_SAMPLES: usize = 256;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Layout {
    Dense,
    Rolling,
}

impl Layout {
    fn parse(value: &str) -> Result<Self, String> {
        match value {
            "dense" => Ok(Self::Dense),
            "rolling" => Ok(Self::Rolling),
            _ => Err(format!(
                "unknown layout `{value}`; expected dense or rolling"
            )),
        }
    }
}

impl Display for Layout {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Dense => formatter.write_str("dense"),
            Self::Rolling => formatter.write_str("rolling"),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Pattern {
    Construct,
    Ordered,
    LossReorder,
    PrefixStall,
    TerminalJump,
}

impl Pattern {
    fn parse(value: &str) -> Result<Self, String> {
        match value {
            "construct" => Ok(Self::Construct),
            "ordered" => Ok(Self::Ordered),
            "loss-reorder" => Ok(Self::LossReorder),
            "prefix-stall" => Ok(Self::PrefixStall),
            "terminal-jump" => Ok(Self::TerminalJump),
            _ => Err(format!(
                "unknown pattern `{value}`; expected construct, ordered, loss-reorder, prefix-stall, or terminal-jump"
            )),
        }
    }

    const fn uses_encoder(self) -> bool {
        matches!(self, Self::Ordered | Self::LossReorder | Self::PrefixStall)
    }
}

impl Display for Pattern {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Construct => formatter.write_str("construct"),
            Self::Ordered => formatter.write_str("ordered"),
            Self::LossReorder => formatter.write_str("loss-reorder"),
            Self::PrefixStall => formatter.write_str("prefix-stall"),
            Self::TerminalJump => formatter.write_str("terminal-jump"),
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct Config {
    layout: Layout,
    pattern: Pattern,
    source_count: u64,
    symbol_bytes: NonZeroUsize,
}

impl Config {
    fn parse() -> Result<Self, String> {
        let arguments = env::args().skip(1).collect::<Vec<_>>();
        if arguments.len() != 4 {
            return Err(
                "usage: decoder_layout_spike <dense|rolling> <construct|ordered|loss-reorder|prefix-stall|terminal-jump> <source-count> <symbol-bytes>"
                    .to_owned(),
            );
        }
        let layout = Layout::parse(&arguments[0])?;
        let pattern = Pattern::parse(&arguments[1])?;
        let source_count = arguments[2]
            .parse::<u64>()
            .map_err(|error| format!("invalid source count `{}`: {error}", arguments[2]))?;
        if source_count == 0 {
            return Err("source count must be non-zero".to_owned());
        }
        let symbol_bytes = arguments[3]
            .parse::<usize>()
            .map_err(|error| format!("invalid symbol size `{}`: {error}", arguments[3]))?;
        let symbol_bytes = NonZeroUsize::new(symbol_bytes)
            .ok_or_else(|| "symbol size must be non-zero".to_owned())?;

        Ok(Self {
            layout,
            pattern,
            source_count,
            symbol_bytes,
        })
    }
}

struct DecodeMeasurements {
    processed_bins: u64,
    dropped_bins: u64,
    decoded_sources: u64,
    max_push: Duration,
    peak_received_bin_payload_bytes: u128,
    peak_decoded_future_payload_bytes: u128,
    peak_retained_prefix_payload_bytes: u128,
    peak_total_buffered_payload_bytes: u128,
    payload_sample_interval: u64,
}

impl DecodeMeasurements {
    fn new(config: Config) -> Self {
        let payload_sample_interval = match config.layout {
            Layout::Dense => config
                .source_count
                .div_ceil(DENSE_PAYLOAD_SAMPLES as u64)
                .max(1),
            Layout::Rolling => 1,
        };
        Self {
            processed_bins: 0,
            dropped_bins: 0,
            decoded_sources: 0,
            max_push: Duration::ZERO,
            peak_received_bin_payload_bytes: 0,
            peak_decoded_future_payload_bytes: 0,
            peak_retained_prefix_payload_bytes: 0,
            peak_total_buffered_payload_bytes: 0,
            payload_sample_interval,
        }
    }

    fn push(
        &mut self,
        decoder: &mut Decoder,
        layout: Layout,
        symbol_bytes: usize,
        bin_id: u128,
        payload: Vec<u8>,
    ) {
        let started = Instant::now();
        let decoded_now = decoder.push_bin_count(bin_id, payload);
        self.max_push = self.max_push.max(started.elapsed());
        self.processed_bins += 1;
        self.decoded_sources += decoded_now as u64;

        if self.should_sample_payload(layout) {
            self.observe_payloads(decoder.stats(), symbol_bytes);
        }
    }

    fn should_sample_payload(&self, layout: Layout) -> bool {
        match layout {
            Layout::Rolling => true,
            Layout::Dense => self
                .processed_bins
                .is_multiple_of(self.payload_sample_interval),
        }
    }

    fn observe_payloads(&mut self, stats: DecoderStats, symbol_bytes: usize) {
        let symbol_bytes = symbol_bytes as u128;
        let received = stats.received_bins as u128 * symbol_bytes;
        let future = stats.decoded_future_sources as u128 * symbol_bytes;
        let prefix = stats.decoded_prefix_sources as u128 * symbol_bytes;
        self.peak_received_bin_payload_bytes = self.peak_received_bin_payload_bytes.max(received);
        self.peak_decoded_future_payload_bytes = self.peak_decoded_future_payload_bytes.max(future);
        self.peak_retained_prefix_payload_bytes =
            self.peak_retained_prefix_payload_bytes.max(prefix);
        self.peak_total_buffered_payload_bytes = self
            .peak_total_buffered_payload_bytes
            .max(received + future + prefix);
    }

    fn peak_total_buffered_payload_upper_bound(&self, config: Config) -> u128 {
        if config.layout == Layout::Rolling || self.processed_bins == 0 {
            return self.peak_total_buffered_payload_bytes;
        }
        self.peak_total_buffered_payload_bytes
            + u128::from(self.payload_sample_interval.saturating_sub(1))
                * config.symbol_bytes.get() as u128
    }
}

struct RssMeasurements {
    before_construction: Option<u64>,
    after_construction: Option<u64>,
    steady: Option<u64>,
    sampled_peak: Option<u64>,
    after_run: Option<u64>,
    next_progress_sample: usize,
}

impl RssMeasurements {
    fn new() -> Self {
        let before_construction = current_rss_bytes();
        Self {
            before_construction,
            after_construction: None,
            steady: None,
            sampled_peak: before_construction,
            after_run: None,
            next_progress_sample: 1,
        }
    }

    fn record_after_construction(&mut self) {
        self.after_construction = current_rss_bytes();
        self.sampled_peak = max_option(self.sampled_peak, self.after_construction);
    }

    fn sample_progress(&mut self, completed_sources: u64, source_count: u64) {
        while self.next_progress_sample <= RSS_PROGRESS_SAMPLES
            && u128::from(completed_sources) * RSS_PROGRESS_SAMPLES as u128
                >= u128::from(source_count) * self.next_progress_sample as u128
        {
            let sample = current_rss_bytes();
            self.sampled_peak = max_option(self.sampled_peak, sample);
            if (RSS_PROGRESS_SAMPLES / 4..=RSS_PROGRESS_SAMPLES * 3 / 4)
                .contains(&self.next_progress_sample)
            {
                self.steady = max_option(self.steady, sample);
            }
            self.next_progress_sample += 1;
        }
    }

    fn record_after_run(&mut self) {
        self.after_run = current_rss_bytes();
        self.sampled_peak = max_option(self.sampled_peak, self.after_run);
        if self.steady.is_none() {
            self.steady = self.after_run.or(self.after_construction);
        }
    }
}

fn max_option(lhs: Option<u64>, rhs: Option<u64>) -> Option<u64> {
    match (lhs, rhs) {
        (Some(lhs), Some(rhs)) => Some(lhs.max(rhs)),
        (Some(value), None) | (None, Some(value)) => Some(value),
        (None, None) => None,
    }
}

fn main() -> Result<(), String> {
    let config = Config::parse()?;
    let params = MettleParams::new(
        OverheadRatio::new(OVERHEAD_NUMERATOR, OVERHEAD_DENOMINATOR)
            .map_err(|error| format!("invalid fixed overhead: {error:?}"))?,
    );
    let mut rss = RssMeasurements::new();

    let construction_started = Instant::now();
    let mut decoder = match config.layout {
        Layout::Dense => Decoder::new_terminated_dense(
            params,
            config.symbol_bytes,
            DEFAULT_SEED,
            config.source_count,
        ),
        Layout::Rolling => Decoder::new_terminated(
            params,
            config.symbol_bytes,
            DEFAULT_SEED,
            config.source_count,
        ),
    };
    let construction = construction_started.elapsed();
    rss.record_after_construction();

    let initial_stats = decoder.stats();
    match config.layout {
        Layout::Dense => {
            if initial_stats.graph_bins.is_none() {
                return Err("dense decoder did not precompute a graph".to_owned());
            }
        }
        Layout::Rolling => {
            if initial_stats.graph_bins.is_some() {
                return Err("rolling decoder unexpectedly precomputed a graph".to_owned());
            }
        }
    }

    let mut measurements = DecodeMeasurements::new(config);
    measurements.observe_payloads(initial_stats, config.symbol_bytes.get());
    let run_started = Instant::now();
    match config.pattern {
        Pattern::Construct => {}
        Pattern::TerminalJump => {
            run_terminal_jump(config, params, &mut decoder, &mut measurements)?
        }
        pattern if pattern.uses_encoder() => {
            run_encoded_pattern(config, params, &mut decoder, &mut measurements, &mut rss)
        }
        _ => return Err("unsupported spike pattern".to_owned()),
    }
    let run_elapsed = run_started.elapsed();
    measurements.observe_payloads(decoder.stats(), config.symbol_bytes.get());
    rss.record_after_run();
    let process_peak_rss = peak_rss_bytes();

    println!(
        "layout,pattern,source_count,symbol_bytes,overhead_num,overhead_den,construction_ns,rss_before_bytes,rss_after_construction_bytes,steady_rss_bytes,sampled_peak_rss_bytes,process_peak_rss_bytes,rss_after_run_bytes,processed_bins,dropped_bins,decoded_sources,payload_sample_interval_bins,peak_received_bin_payload_bytes,peak_decoded_future_payload_bytes,peak_retained_prefix_payload_bytes,peak_total_buffered_payload_bytes,peak_total_buffered_payload_upper_bound_bytes,max_push_ns,run_ns"
    );
    println!(
        "{},{},{},{},{OVERHEAD_NUMERATOR},{OVERHEAD_DENOMINATOR},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}",
        config.layout,
        config.pattern,
        config.source_count,
        config.symbol_bytes,
        construction.as_nanos(),
        csv_option(rss.before_construction),
        csv_option(rss.after_construction),
        csv_option(rss.steady),
        csv_option(rss.sampled_peak),
        csv_option(process_peak_rss),
        csv_option(rss.after_run),
        measurements.processed_bins,
        measurements.dropped_bins,
        measurements.decoded_sources,
        measurements.payload_sample_interval,
        measurements.peak_received_bin_payload_bytes,
        measurements.peak_decoded_future_payload_bytes,
        measurements.peak_retained_prefix_payload_bytes,
        measurements.peak_total_buffered_payload_bytes,
        measurements.peak_total_buffered_payload_upper_bound(config),
        measurements.max_push.as_nanos(),
        run_elapsed.as_nanos(),
    );

    Ok(())
}

fn run_terminal_jump(
    config: Config,
    params: MettleParams,
    decoder: &mut Decoder,
    measurements: &mut DecodeMeasurements,
) -> Result<(), String> {
    let terminal_source_id = config
        .source_count
        .checked_sub(1)
        .ok_or_else(|| "terminated stream has no sources".to_owned())?;
    let bin_id = edge_bin_ids_with_terminal_source_count(
        params,
        terminal_source_id,
        DEFAULT_SEED,
        Some(config.source_count),
    )
    .into_iter()
    .max()
    .ok_or_else(|| "terminal source has no edge bins".to_owned())?;
    measurements.push(
        decoder,
        config.layout,
        config.symbol_bytes.get(),
        bin_id,
        vec![0; config.symbol_bytes.get()],
    );
    Ok(())
}

fn run_encoded_pattern(
    config: Config,
    params: MettleParams,
    decoder: &mut Decoder,
    measurements: &mut DecodeMeasurements,
    rss: &mut RssMeasurements,
) {
    let mut encoder = Encoder::new_terminated(
        params,
        config.symbol_bytes,
        DEFAULT_SEED,
        config.source_count,
    );
    let mut source = vec![0; config.symbol_bytes.get()];
    let stalled_bins = if config.pattern == Pattern::PrefixStall {
        edge_bin_ids_with_terminal_source_count(params, 0, DEFAULT_SEED, Some(config.source_count))
            .into_iter()
            .collect::<HashSet<_>>()
    } else {
        HashSet::new()
    };
    let mut reorder_batch = Vec::with_capacity(REORDER_BATCH);

    for source_id in 0..config.source_count {
        fill_source_payload(&mut source, source_id);
        let bins = encoder.push_source(&source);
        route_bins(
            config,
            bins,
            &stalled_bins,
            &mut reorder_batch,
            decoder,
            measurements,
        );
        rss.sample_progress(source_id + 1, config.source_count);
    }
    route_bins(
        config,
        encoder.finish(),
        &stalled_bins,
        &mut reorder_batch,
        decoder,
        measurements,
    );
    drain_reorder_batch(config, &mut reorder_batch, decoder, measurements);
}

fn route_bins(
    config: Config,
    bins: Vec<(u128, Vec<u8>)>,
    stalled_bins: &HashSet<u128>,
    reorder_batch: &mut Vec<(u128, Vec<u8>)>,
    decoder: &mut Decoder,
    measurements: &mut DecodeMeasurements,
) {
    for (bin_id, payload) in bins {
        let dropped = stalled_bins.contains(&bin_id)
            || (config.pattern == Pattern::LossReorder && deterministic_loss(bin_id));
        if dropped {
            measurements.dropped_bins += 1;
            continue;
        }

        if config.pattern == Pattern::LossReorder {
            reorder_batch.push((bin_id, payload));
            if reorder_batch.len() == REORDER_BATCH {
                drain_reorder_batch(config, reorder_batch, decoder, measurements);
            }
        } else {
            measurements.push(
                decoder,
                config.layout,
                config.symbol_bytes.get(),
                bin_id,
                payload,
            );
        }
    }
}

fn drain_reorder_batch(
    config: Config,
    reorder_batch: &mut Vec<(u128, Vec<u8>)>,
    decoder: &mut Decoder,
    measurements: &mut DecodeMeasurements,
) {
    while let Some((bin_id, payload)) = reorder_batch.pop() {
        measurements.push(
            decoder,
            config.layout,
            config.symbol_bytes.get(),
            bin_id,
            payload,
        );
    }
}

fn deterministic_loss(bin_id: u128) -> bool {
    let low = bin_id as u64;
    let high = u64::try_from(bin_id >> 64).expect("shifted u128 fits u64");
    let sample = mix64(DEFAULT_SEED ^ low ^ high.rotate_left(23));
    sample % LOSS_DENOMINATOR < LOSS_NUMERATOR
}

fn mix64(mut value: u64) -> u64 {
    value = (value ^ (value >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    value ^ (value >> 31)
}

fn fill_source_payload(payload: &mut [u8], source_id: u64) {
    let mut state = mix64(DEFAULT_SEED ^ source_id);
    for chunk in payload.chunks_mut(std::mem::size_of::<u64>()) {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        let bytes = state.to_le_bytes();
        chunk.copy_from_slice(&bytes[..chunk.len()]);
    }
}

fn csv_option(value: Option<u64>) -> String {
    value.map_or_else(String::new, |value| value.to_string())
}

#[cfg(target_os = "linux")]
fn current_rss_bytes() -> Option<u64> {
    let status = fs::read_to_string("/proc/self/status").ok()?;
    let rss_line = status.lines().find(|line| line.starts_with("VmRSS:"))?;
    let kibibytes = rss_line.split_whitespace().nth(1)?.parse::<u64>().ok()?;
    kibibytes.checked_mul(1024)
}

#[cfg(not(target_os = "linux"))]
fn current_rss_bytes() -> Option<u64> {
    let pid = std::process::id().to_string();
    let output = Command::new("ps")
        .args(["-o", "rss=", "-p", pid.as_str()])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let rss = String::from_utf8(output.stdout)
        .ok()?
        .trim()
        .parse::<u64>()
        .ok()?;
    rss.checked_mul(1024)
}

#[cfg(unix)]
fn peak_rss_bytes() -> Option<u64> {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
    // SAFETY: `usage` points to writable storage for one `rusage`; getrusage
    // initializes it on a zero return code before `assume_init` is called.
    let result = unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) };
    if result != 0 {
        return None;
    }
    // SAFETY: the successful getrusage call initialized the whole value.
    let usage = unsafe { usage.assume_init() };
    let raw = u64::try_from(usage.ru_maxrss).ok()?;
    if cfg!(any(target_os = "macos", target_os = "ios")) {
        Some(raw)
    } else {
        raw.checked_mul(1024)
    }
}

#[cfg(not(unix))]
fn peak_rss_bytes() -> Option<u64> {
    None
}
