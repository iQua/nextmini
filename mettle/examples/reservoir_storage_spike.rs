//! Sender storage comparison for a reservoir extension **BEYOND the METTLE paper**.
//!
//! Run one strategy per process so peak RSS remains attributable:
//! `cargo run --release -p mettle --example reservoir_storage_spike -- retain 65536 1400 2 100 5 100`.
//! Recompute regenerates each requested bin from its real finite candidate source interval and
//! actual four edge choices; it is not the encoder's `O(l)` streaming path.

use std::env;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::mem::size_of;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::time::Instant;

use mettle::experimental_reservoir::{
    FiniteReservoirGeometry, ReserveSet, ReservoirRate, checked_reserve_payload_bytes,
    checked_reserve_strategy_bytes,
};
use mettle::test_support::{
    Encoder, edge_bin_ids_with_terminal_source_count, possible_source_id_range_for_bin,
};

const EXPERIMENT_SCOPE: &str = "BEYOND the METTLE paper";
const RESERVE_PAYLOAD_BUDGET_BYTES: usize = 8 * 1024 * 1024;
const GRAPH_SEED: u64 = 0x5354_4F52_4147_4533;
const RESERVE_SEED: u64 = 0x5052_465F_5354_4733;
const DIGEST_OFFSET: u64 = 0xCBF2_9CE4_8422_2325;
const DIGEST_PRIME: u64 = 0x0000_0100_0000_01B3;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Strategy {
    Retain,
    Recompute,
    Spill,
}

impl Strategy {
    fn parse(value: &str) -> Result<Self, String> {
        match value {
            "retain" => Ok(Self::Retain),
            "recompute" => Ok(Self::Recompute),
            "spill" => Ok(Self::Spill),
            _ => Err(format!(
                "unknown strategy `{value}`; expected retain, recompute, or spill"
            )),
        }
    }

    const fn name(self) -> &'static str {
        match self {
            Self::Retain => "retain",
            Self::Recompute => "recompute",
            Self::Spill => "spill",
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct Config {
    strategy: Strategy,
    source_count: u64,
    symbol_bytes: NonZeroUsize,
    wire_rate: ReservoirRate,
    reserve_rate: ReservoirRate,
}

impl Config {
    fn parse() -> Result<Self, String> {
        let arguments = env::args().skip(1).collect::<Vec<_>>();
        let [
            strategy,
            source_count,
            symbol_bytes,
            wire_num,
            wire_den,
            reserve_num,
            reserve_den,
        ] = arguments.as_slice()
        else {
            return Err(
                "usage: reservoir_storage_spike <retain|recompute|spill> <source-count> <symbol-bytes> <wire-num> <wire-den> <reserve-num> <reserve-den>"
                    .to_owned(),
            );
        };
        let symbol_bytes = parse(symbol_bytes, "symbol bytes")?;
        Ok(Self {
            strategy: Strategy::parse(strategy)?,
            source_count: parse(source_count, "source count")?,
            symbol_bytes: NonZeroUsize::new(symbol_bytes)
                .ok_or_else(|| "symbol bytes must be non-zero".to_owned())?,
            wire_rate: rate(
                parse(wire_num, "wire numerator")?,
                parse(wire_den, "wire denominator")?,
            )?,
            reserve_rate: rate(
                parse(reserve_num, "reserve numerator")?,
                parse(reserve_den, "reserve denominator")?,
            )?,
        })
    }

    fn object_bytes(self) -> Result<usize, String> {
        usize::try_from(self.source_count)
            .map_err(|_| "source count does not fit usize".to_owned())?
            .checked_mul(self.symbol_bytes.get())
            .ok_or_else(|| "object byte count overflowed".to_owned())
    }
}

#[derive(Debug)]
struct StoredPayload {
    bin_id: u128,
    payload: Vec<u8>,
}

#[derive(Clone, Copy, Debug)]
struct SpillEntry {
    bin_id: u128,
    offset: u64,
}

struct SpillStore {
    file: File,
    path: PathBuf,
    entries: Vec<SpillEntry>,
    bytes_written: u64,
}

impl SpillStore {
    fn new(reserve_cardinality: usize) -> Result<Self, String> {
        let path = env::temp_dir().join(format!(
            "nextmini-stage3-reservoir-{}.bin",
            std::process::id()
        ));
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&path)
            .map_err(|error| format!("create spill file {}: {error}", path.display()))?;
        let mut entries = Vec::new();
        entries
            .try_reserve_exact(reserve_cardinality)
            .map_err(|error| format!("reserve spill index: {error}"))?;
        Ok(Self {
            file,
            path,
            entries,
            bytes_written: 0,
        })
    }

    fn push(&mut self, bin_id: u128, payload: &[u8]) -> Result<(), String> {
        let offset = self.bytes_written;
        self.file
            .write_all(payload)
            .map_err(|error| format!("write spill payload: {error}"))?;
        self.bytes_written = self
            .bytes_written
            .checked_add(
                u64::try_from(payload.len())
                    .map_err(|_| "spill payload length does not fit u64".to_owned())?,
            )
            .ok_or_else(|| "spill offset overflowed".to_owned())?;
        self.entries.push(SpillEntry { bin_id, offset });
        Ok(())
    }

    fn read_payload(&mut self, bin_id: u128, payload: &mut [u8]) -> Result<(), String> {
        let entry = self
            .entries
            .binary_search_by_key(&bin_id, |entry| entry.bin_id)
            .ok()
            .and_then(|index| self.entries.get(index).copied())
            .ok_or_else(|| format!("spill index is missing reserve bin {bin_id}"))?;
        self.file
            .seek(SeekFrom::Start(entry.offset))
            .map_err(|error| format!("seek spill payload: {error}"))?;
        self.file
            .read_exact(payload)
            .map_err(|error| format!("read spill payload: {error}"))
    }
}

impl Drop for SpillStore {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

enum StorageState {
    Retain(Vec<StoredPayload>),
    Recompute,
    Spill(SpillStore),
}

impl StorageState {
    fn new(strategy: Strategy, reserve_cardinality: usize) -> Result<Self, String> {
        match strategy {
            Strategy::Retain => {
                let mut payloads = Vec::new();
                payloads
                    .try_reserve_exact(reserve_cardinality)
                    .map_err(|error| format!("reserve retained payload index: {error}"))?;
                Ok(Self::Retain(payloads))
            }
            Strategy::Recompute => Ok(Self::Recompute),
            Strategy::Spill => Ok(Self::Spill(SpillStore::new(reserve_cardinality)?)),
        }
    }

    fn record(&mut self, bin_id: u128, payload: Vec<u8>) -> Result<(), String> {
        match self {
            Self::Retain(payloads) => {
                payloads.push(StoredPayload { bin_id, payload });
                Ok(())
            }
            Self::Recompute => Ok(()),
            Self::Spill(store) => store.push(bin_id, &payload),
        }
    }

    fn index_bytes(&self) -> usize {
        match self {
            Self::Retain(payloads) => payloads.len() * size_of::<StoredPayload>(),
            Self::Recompute => 0,
            Self::Spill(store) => store.entries.len() * size_of::<SpillEntry>(),
        }
    }

    fn spill_bytes(&self) -> u64 {
        match self {
            Self::Spill(store) => store.bytes_written,
            Self::Retain(_) | Self::Recompute => 0,
        }
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct FetchMeasurements {
    digest: u64,
    candidate_sources_checked: u64,
    touching_sources: u64,
}

fn main() -> Result<(), String> {
    let config = Config::parse()?;
    if config.source_count == 0 {
        return Err("source count must be non-zero".to_owned());
    }
    let geometry =
        FiniteReservoirGeometry::solve(config.source_count, config.wire_rate, config.reserve_rate)
            .map_err(|error| format!("finite reservoir geometry failed: {error:?}"))?;
    let reserve = ReserveSet::select(geometry, RESERVE_SEED)
        .map_err(|error| format!("reserve selection failed: {error:?}"))?;
    let reserve_payload_bytes = checked_reserve_payload_bytes(
        reserve.len(),
        config.symbol_bytes.get(),
        RESERVE_PAYLOAD_BUDGET_BYTES,
    )
    .map_err(|error| format!("independent sender reserve budget rejected case: {error:?}"))?;

    let rss_before_bytes = current_rss_bytes();
    let object_started = Instant::now();
    let object = build_object(config)?;
    let object_build_ns = object_started.elapsed().as_nanos();
    let rss_after_object_bytes = current_rss_bytes();

    let mut storage = StorageState::new(config.strategy, reserve.len())?;
    let encode_started = Instant::now();
    encode_and_store(config, geometry, &reserve, &object, &mut storage)?;
    let initial_encode_ns = encode_started.elapsed().as_nanos();
    let rss_after_encode_bytes = current_rss_bytes();
    let strategy_index_bytes = storage.index_bytes();
    let spill_bytes = storage.spill_bytes();

    let fetch_started = Instant::now();
    let fetch = fetch_all(config, geometry, &reserve, &object, &mut storage)?;
    let repair_fetch_ns = fetch_started.elapsed().as_nanos();
    let peak_rss_bytes = peak_rss_bytes();
    let strategy_memory_bytes = checked_reserve_strategy_bytes(
        match config.strategy {
            Strategy::Retain => reserve_payload_bytes,
            Strategy::Recompute | Strategy::Spill => config.symbol_bytes.get(),
        },
        strategy_index_bytes,
        RESERVE_PAYLOAD_BUDGET_BYTES,
    )
    .map_err(|error| format!("independent sender strategy budget rejected case: {error:?}"))?;

    println!(
        "research_scope,strategy,source_count,symbol_bytes,object_bytes,c_wire_num,c_wire_den,c_reserve_num,c_reserve_den,interior_c_num,interior_c_den,wire_bin_count,reserve_cardinality,terminal_bin_count,actual_wire_overhead,actual_total_overhead,reserve_payload_bytes,strategy_index_bytes,strategy_memory_bytes,spill_bytes,sender_reserve_budget_bytes,budget_pass,object_build_ns,initial_encode_ns,repair_fetch_ns,candidate_sources_checked,touching_sources,payload_digest,rss_before_bytes,rss_after_object_bytes,rss_after_encode_bytes,process_peak_rss_bytes"
    );
    println!(
        "{EXPERIMENT_SCOPE},{},{},{},{},{},{},{},{},{},{},{},{},{},{:.9},{:.9},{},{},{},{},{},{},{},{},{},{},{},{:016x},{},{},{},{}",
        config.strategy.name(),
        config.source_count,
        config.symbol_bytes,
        object.len(),
        config.wire_rate.numerator(),
        config.wire_rate.denominator(),
        config.reserve_rate.numerator(),
        config.reserve_rate.denominator(),
        geometry.interior_overhead().numerator(),
        geometry.interior_overhead().denominator(),
        geometry.wire_bin_count(),
        geometry.reserve_cardinality(),
        geometry.terminal_bin_count(),
        geometry.wire_bin_count() as f64 / config.source_count as f64 - 1.0,
        geometry.terminal_bin_count() as f64 / config.source_count as f64 - 1.0,
        reserve_payload_bytes,
        strategy_index_bytes,
        strategy_memory_bytes,
        spill_bytes,
        RESERVE_PAYLOAD_BUDGET_BYTES,
        strategy_memory_bytes <= RESERVE_PAYLOAD_BUDGET_BYTES,
        object_build_ns,
        initial_encode_ns,
        repair_fetch_ns,
        fetch.candidate_sources_checked,
        fetch.touching_sources,
        fetch.digest,
        csv_option(rss_before_bytes),
        csv_option(rss_after_object_bytes),
        csv_option(rss_after_encode_bytes),
        csv_option(peak_rss_bytes),
    );
    Ok(())
}

fn build_object(config: Config) -> Result<Vec<u8>, String> {
    let mut object = Vec::new();
    object
        .try_reserve_exact(config.object_bytes()?)
        .map_err(|error| format!("reserve source object: {error}"))?;
    object.resize(config.object_bytes()?, 0);
    for source_id in 0..config.source_count {
        let start = usize::try_from(source_id)
            .map_err(|_| "source id does not fit usize".to_owned())?
            .checked_mul(config.symbol_bytes.get())
            .ok_or_else(|| "source offset overflowed".to_owned())?;
        let id_bytes = source_id.to_le_bytes();
        let copy_len = id_bytes.len().min(config.symbol_bytes.get());
        object[start..start + copy_len].copy_from_slice(&id_bytes[..copy_len]);
        if config.symbol_bytes.get() > id_bytes.len() {
            object[start + id_bytes.len()] = source_id.wrapping_mul(131) as u8;
        }
    }
    Ok(object)
}

fn encode_and_store(
    config: Config,
    geometry: FiniteReservoirGeometry,
    reserve: &ReserveSet,
    object: &[u8],
    storage: &mut StorageState,
) -> Result<(), String> {
    let mut encoder = Encoder::new_terminated(
        geometry.params(),
        config.symbol_bytes,
        GRAPH_SEED,
        config.source_count,
    );
    let mut observed_bins = 0u128;
    for payload in object.chunks_exact(config.symbol_bytes.get()) {
        for (bin_id, bin_payload) in encoder.push_source(payload) {
            observed_bins += 1;
            if reserve.contains(bin_id) {
                storage.record(bin_id, bin_payload)?;
            }
        }
    }
    for (bin_id, bin_payload) in encoder.finish() {
        observed_bins += 1;
        if reserve.contains(bin_id) {
            storage.record(bin_id, bin_payload)?;
        }
    }
    if observed_bins != geometry.terminal_bin_count() {
        return Err(format!(
            "encoder emitted {observed_bins} bins, expected {}",
            geometry.terminal_bin_count()
        ));
    }
    Ok(())
}

fn fetch_all(
    config: Config,
    geometry: FiniteReservoirGeometry,
    reserve: &ReserveSet,
    object: &[u8],
    storage: &mut StorageState,
) -> Result<FetchMeasurements, String> {
    let mut measurements = FetchMeasurements {
        digest: DIGEST_OFFSET,
        ..FetchMeasurements::default()
    };
    let mut scratch = vec![0; config.symbol_bytes.get()];
    for &bin_id in reserve.emission_order() {
        match storage {
            StorageState::Retain(payloads) => {
                let payload = payloads
                    .binary_search_by_key(&bin_id, |payload| payload.bin_id)
                    .ok()
                    .and_then(|index| payloads.get(index))
                    .ok_or_else(|| format!("retained payload is missing reserve bin {bin_id}"))?;
                hash_payload(&mut measurements.digest, bin_id, &payload.payload);
            }
            StorageState::Recompute => {
                recompute_payload(
                    config,
                    geometry,
                    bin_id,
                    object,
                    &mut scratch,
                    &mut measurements,
                )?;
                hash_payload(&mut measurements.digest, bin_id, &scratch);
            }
            StorageState::Spill(store) => {
                store.read_payload(bin_id, &mut scratch)?;
                hash_payload(&mut measurements.digest, bin_id, &scratch);
            }
        }
    }
    Ok(measurements)
}

fn recompute_payload(
    config: Config,
    geometry: FiniteReservoirGeometry,
    bin_id: u128,
    object: &[u8],
    scratch: &mut [u8],
    measurements: &mut FetchMeasurements,
) -> Result<(), String> {
    scratch.fill(0);
    let Some((first_source, last_source)) =
        possible_source_id_range_for_bin(geometry.params(), bin_id, config.source_count)
    else {
        return Ok(());
    };
    for source_id in first_source..=last_source {
        measurements.candidate_sources_checked = measurements
            .candidate_sources_checked
            .checked_add(1)
            .ok_or_else(|| "candidate source counter overflowed".to_owned())?;
        let edges = edge_bin_ids_with_terminal_source_count(
            geometry.params(),
            source_id,
            GRAPH_SEED,
            Some(config.source_count),
        );
        if !edges.contains(&bin_id) {
            continue;
        }
        measurements.touching_sources = measurements
            .touching_sources
            .checked_add(1)
            .ok_or_else(|| "touching source counter overflowed".to_owned())?;
        let start = usize::try_from(source_id)
            .map_err(|_| "source id does not fit usize".to_owned())?
            .checked_mul(config.symbol_bytes.get())
            .ok_or_else(|| "source offset overflowed".to_owned())?;
        let source = object
            .get(start..start + config.symbol_bytes.get())
            .ok_or_else(|| "source slice is outside the object".to_owned())?;
        for (destination, source) in scratch.iter_mut().zip(source) {
            *destination ^= source;
        }
    }
    Ok(())
}

fn hash_payload(digest: &mut u64, bin_id: u128, payload: &[u8]) {
    for byte in bin_id
        .to_le_bytes()
        .into_iter()
        .chain(payload.iter().copied())
    {
        *digest ^= u64::from(byte);
        *digest = digest.wrapping_mul(DIGEST_PRIME);
    }
}

fn rate(numerator: u32, denominator: u32) -> Result<ReservoirRate, String> {
    ReservoirRate::new(numerator, denominator)
        .map_err(|error| format!("invalid rate {numerator}/{denominator}: {error:?}"))
}

fn parse<T>(value: &str, label: &str) -> Result<T, String>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    value
        .parse()
        .map_err(|error| format!("invalid {label} `{value}`: {error}"))
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
    let output = std::process::Command::new("ps")
        .args(["-o", "rss=", "-p", &std::process::id().to_string()])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    String::from_utf8(output.stdout)
        .ok()?
        .trim()
        .parse::<u64>()
        .ok()?
        .checked_mul(1024)
}

#[cfg(unix)]
fn peak_rss_bytes() -> Option<u64> {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
    // SAFETY: `usage` is writable storage for one `rusage`; a successful `getrusage` call
    // initializes it before `assume_init`.
    let result = unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) };
    if result != 0 {
        return None;
    }
    // SAFETY: success above guarantees the complete `rusage` value was initialized.
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
