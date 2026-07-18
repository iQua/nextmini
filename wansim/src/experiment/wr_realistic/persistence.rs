use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use super::{
    RawTrial, SharingRow, Task, WrPersistentRun, admission_name, artifacts_from_trials,
    build_tasks, cadence_name, run_task, to_csv,
};
use crate::{SCENARIO_SCHEMA_VERSION, SIMULATOR_VERSION};

const CELL_FORMAT_VERSION: u32 = 1;
const CELL_DIRECTORY: &str = "wr-cells";
const MANIFEST_FILE: &str = "manifest.toml";
const STATUS_FILE: &str = "status.csv";
const TRIAL_FILE: &str = "trial.csv";
const SHARING_FILE: &str = "sharing.csv";

#[derive(Debug, Error)]
pub(super) enum PersistenceError {
    #[error("output directory {0} is not empty and has no WR cell manifest")]
    UnclaimedOutput(PathBuf),
    #[error("WR cell output {0} is already initialized; pass --resume to reuse it")]
    ResumeRequired(PathBuf),
    #[error("WR cell manifest mismatch: {0}")]
    ManifestMismatch(String),
    #[error("invalid WR cell shard {path}: {reason}")]
    InvalidShard { path: PathBuf, reason: String },
    #[error("WR cell shard {0} already exists")]
    ExistingShard(PathBuf),
    #[error("WR task {0} is missing after the worker pass")]
    MissingCell(usize),
    #[error("WR persistence worker failed: {0}")]
    Worker(String),
    #[error("WR aggregation failed: {0}")]
    Aggregation(String),
    #[error("WR persistence I/O failed at {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("WR persistence CSV failed at {path}: {source}")]
    Csv {
        path: PathBuf,
        #[source]
        source: csv::Error,
    },
    #[error("WR persistence CSV serialization failed: {0}")]
    CsvSerialize(#[from] csv::Error),
    #[error("WR persistence manifest serialization failed: {0}")]
    ManifestSerialize(#[from] toml::ser::Error),
    #[error("WR persistence manifest parsing failed: {0}")]
    ManifestParse(#[from] toml::de::Error),
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
struct CellManifest {
    cell_format_version: u32,
    scenario_schema_version: u32,
    simulator_version: String,
    seeds: u64,
    total_cells: usize,
}

impl CellManifest {
    fn expected(seeds: u64, total_cells: usize) -> Self {
        Self {
            cell_format_version: CELL_FORMAT_VERSION,
            scenario_schema_version: SCENARIO_SCHEMA_VERSION,
            simulator_version: SIMULATOR_VERSION.to_owned(),
            seeds,
            total_cells,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct CellStatusRow {
    cell_format_version: u32,
    scenario_schema_version: u32,
    simulator_version: String,
    experiment_seeds: u64,
    task_index: usize,
    task_key: String,
    slice: String,
    profile: String,
    placement_index: usize,
    utilization_percent: u8,
    jitter: bool,
    protocol: String,
    source_symbols: usize,
    seed: u64,
    cadence: String,
    admission: String,
    slow_receiver: i8,
    sessions: usize,
    status: String,
    error: String,
}

impl CellStatusRow {
    fn new(
        manifest: &CellManifest,
        task_index: usize,
        task: &Task,
        status: &str,
        error: String,
    ) -> Self {
        Self {
            cell_format_version: manifest.cell_format_version,
            scenario_schema_version: manifest.scenario_schema_version,
            simulator_version: manifest.simulator_version.clone(),
            experiment_seeds: manifest.seeds,
            task_index,
            task_key: task_key(task),
            slice: task.slice.name().to_owned(),
            profile: task.profile.name().to_owned(),
            placement_index: task.placement,
            utilization_percent: task.utilization,
            jitter: task.jitter,
            protocol: task.protocol.name().to_owned(),
            source_symbols: task.k,
            seed: task.seed,
            cadence: cadence_name(task.cadence).to_owned(),
            admission: admission_name(task.admission).to_owned(),
            slow_receiver: task.slow_receiver.map_or(-1, |receiver| {
                i8::try_from(receiver).expect("WR receiver index fits in i8")
            }),
            sessions: task.sessions,
            status: status.to_owned(),
            error,
        }
    }

    fn validate(
        &self,
        manifest: &CellManifest,
        task_index: usize,
        task: &Task,
    ) -> Result<(), String> {
        let expected = Self::new(manifest, task_index, task, &self.status, self.error.clone());
        for (field, actual, wanted) in [
            (
                "cell_format_version",
                self.cell_format_version.to_string(),
                expected.cell_format_version.to_string(),
            ),
            (
                "scenario_schema_version",
                self.scenario_schema_version.to_string(),
                expected.scenario_schema_version.to_string(),
            ),
            (
                "simulator_version",
                self.simulator_version.clone(),
                expected.simulator_version,
            ),
            (
                "experiment_seeds",
                self.experiment_seeds.to_string(),
                expected.experiment_seeds.to_string(),
            ),
            (
                "task_index",
                self.task_index.to_string(),
                expected.task_index.to_string(),
            ),
            ("task_key", self.task_key.clone(), expected.task_key),
            ("slice", self.slice.clone(), expected.slice),
            ("profile", self.profile.clone(), expected.profile),
            (
                "placement_index",
                self.placement_index.to_string(),
                expected.placement_index.to_string(),
            ),
            (
                "utilization_percent",
                self.utilization_percent.to_string(),
                expected.utilization_percent.to_string(),
            ),
            (
                "jitter",
                self.jitter.to_string(),
                expected.jitter.to_string(),
            ),
            ("protocol", self.protocol.clone(), expected.protocol),
            (
                "source_symbols",
                self.source_symbols.to_string(),
                expected.source_symbols.to_string(),
            ),
            ("seed", self.seed.to_string(), expected.seed.to_string()),
            ("cadence", self.cadence.clone(), expected.cadence),
            ("admission", self.admission.clone(), expected.admission),
            (
                "slow_receiver",
                self.slow_receiver.to_string(),
                expected.slow_receiver.to_string(),
            ),
            (
                "sessions",
                self.sessions.to_string(),
                expected.sessions.to_string(),
            ),
        ] {
            if actual != wanted {
                return Err(format!("{field} is {actual:?}, expected {wanted:?}"));
            }
        }
        match self.status.as_str() {
            "success" if self.error.is_empty() => Ok(()),
            "failure" if !self.error.is_empty() => Ok(()),
            "success" => Err("successful cell has a nonempty error".to_owned()),
            "failure" => Err("failed cell has an empty error".to_owned()),
            other => Err(format!("unknown cell status {other:?}")),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct PersistedTrialRow {
    task_index: usize,
    task_key: String,
    profile_name: String,
    placement_id: String,
    barrier_ns: u64,
    sender_ns: u64,
    receiver0_ns: u64,
    receiver1_ns: u64,
    receiver2_ns: u64,
    emissions: usize,
    useful_emissions: usize,
    tree0_emissions: usize,
    tree1_emissions: usize,
    drops: usize,
    blocking_waits: usize,
    positive_round_deficits: usize,
    ack_probes: usize,
    stall_ppm: u64,
    link_drops: usize,
    background_trunk_utilization_ppm: u64,
    a4_trace_correlation_ppm: i64,
    tree0_bytes: u64,
    tree1_bytes: u64,
    maximum_mailbox_high_water: usize,
}

impl PersistedTrialRow {
    fn from_trial(task_index: usize, trial: &RawTrial) -> Self {
        Self {
            task_index,
            task_key: task_key(&trial.task),
            profile_name: trial.profile_name.to_owned(),
            placement_id: trial.placement_id.clone(),
            barrier_ns: trial.barrier_ns,
            sender_ns: trial.sender_ns,
            receiver0_ns: trial.receiver_ns[0],
            receiver1_ns: trial.receiver_ns[1],
            receiver2_ns: trial.receiver_ns[2],
            emissions: trial.emissions,
            useful_emissions: trial.useful_emissions,
            tree0_emissions: trial.per_tree_emissions[0],
            tree1_emissions: trial.per_tree_emissions[1],
            drops: trial.drops,
            blocking_waits: trial.blocking_waits,
            positive_round_deficits: trial.positive_round_deficits,
            ack_probes: trial.ack_probes,
            stall_ppm: trial.stall_ppm,
            link_drops: trial.link_drops,
            background_trunk_utilization_ppm: trial.background_trunk_utilization_ppm,
            a4_trace_correlation_ppm: trial.a4_trace_correlation_ppm,
            tree0_bytes: trial.tree_bytes[0],
            tree1_bytes: trial.tree_bytes[1],
            maximum_mailbox_high_water: trial.maximum_mailbox_high_water,
        }
    }

    fn into_trial(
        self,
        task_index: usize,
        task: Task,
        sharing: Vec<SharingRow>,
    ) -> Result<RawTrial, String> {
        if self.task_index != task_index {
            return Err(format!(
                "trial task_index is {}, expected {task_index}",
                self.task_index
            ));
        }
        let expected_key = task_key(&task);
        if self.task_key != expected_key {
            return Err(format!(
                "trial task_key is {:?}, expected {:?}",
                self.task_key, expected_key
            ));
        }
        if self.profile_name != task.profile.name() {
            return Err(format!(
                "trial profile is {:?}, expected {:?}",
                self.profile_name,
                task.profile.name()
            ));
        }
        let profile_name = task.profile.name();
        Ok(RawTrial {
            task,
            profile_name,
            placement_id: self.placement_id,
            barrier_ns: self.barrier_ns,
            sender_ns: self.sender_ns,
            receiver_ns: [self.receiver0_ns, self.receiver1_ns, self.receiver2_ns],
            emissions: self.emissions,
            useful_emissions: self.useful_emissions,
            per_tree_emissions: [self.tree0_emissions, self.tree1_emissions],
            drops: self.drops,
            blocking_waits: self.blocking_waits,
            positive_round_deficits: self.positive_round_deficits,
            ack_probes: self.ack_probes,
            stall_ppm: self.stall_ppm,
            link_drops: self.link_drops,
            background_trunk_utilization_ppm: self.background_trunk_utilization_ppm,
            a4_trace_correlation_ppm: self.a4_trace_correlation_ppm,
            tree_bytes: [self.tree0_bytes, self.tree1_bytes],
            maximum_mailbox_high_water: self.maximum_mailbox_high_water,
            sharing,
        })
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct PersistedSharingRow {
    profile: String,
    placement: String,
    resource: String,
    total_flow_directions: usize,
    foreground_flow_directions: usize,
    background_flow_directions: usize,
}

impl From<&SharingRow> for PersistedSharingRow {
    fn from(row: &SharingRow) -> Self {
        Self {
            profile: row.profile.to_owned(),
            placement: row.placement.clone(),
            resource: row.resource.clone(),
            total_flow_directions: row.total_flow_directions,
            foreground_flow_directions: row.foreground_flow_directions,
            background_flow_directions: row.background_flow_directions,
        }
    }
}

impl PersistedSharingRow {
    fn into_sharing(self) -> SharingRow {
        SharingRow {
            profile: self.profile,
            placement: self.placement,
            resource: self.resource,
            total_flow_directions: self.total_flow_directions,
            foreground_flow_directions: self.foreground_flow_directions,
            background_flow_directions: self.background_flow_directions,
        }
    }
}

#[derive(Clone, Debug)]
enum PersistedCell {
    Success(RawTrial),
    Failure(CellStatusRow),
}

#[derive(Debug)]
struct LoadedCells {
    successful: Vec<RawTrial>,
    failures: Vec<CellStatusRow>,
    pending: Vec<(usize, Task)>,
    skipped: usize,
}

#[derive(Debug)]
struct CellStore {
    cells: PathBuf,
    manifest: CellManifest,
}

impl CellStore {
    fn open(
        output: &Path,
        seeds: u64,
        total_cells: usize,
        resume: bool,
    ) -> Result<Self, PersistenceError> {
        let expected = CellManifest::expected(seeds, total_cells);
        let cells = output.join(CELL_DIRECTORY);
        let manifest_path = cells.join(MANIFEST_FILE);
        if manifest_path.exists() {
            if !resume {
                return Err(PersistenceError::ResumeRequired(output.to_path_buf()));
            }
            let manifest_text = read_to_string(&manifest_path)?;
            let actual: CellManifest = toml::from_str(&manifest_text)?;
            if actual != expected {
                return Err(PersistenceError::ManifestMismatch(format!(
                    "found {actual:?}, expected {expected:?}"
                )));
            }
            let store = Self {
                cells,
                manifest: actual,
            };
            store.remove_torn_shards()?;
            return Ok(store);
        }

        if output.exists() && directory_has_entries(output)? {
            return Err(PersistenceError::UnclaimedOutput(output.to_path_buf()));
        }
        create_dir_all(output)?;
        create_dir_all(&cells)?;
        let manifest_text = toml::to_string_pretty(&expected)?;
        atomic_write_file(&manifest_path, manifest_text.as_bytes())?;
        Ok(Self {
            cells,
            manifest: expected,
        })
    }

    fn cell_path(&self, task_index: usize) -> PathBuf {
        self.cells.join(format!("{task_index:06}"))
    }

    fn load(
        &self,
        task_index: usize,
        task: &Task,
    ) -> Result<Option<PersistedCell>, PersistenceError> {
        let path = self.cell_path(task_index);
        if !path.exists() {
            return Ok(None);
        }
        let status_path = path.join(STATUS_FILE);
        let status: CellStatusRow = read_one_csv(&status_path)?;
        status
            .validate(&self.manifest, task_index, task)
            .map_err(|reason| PersistenceError::InvalidShard {
                path: path.clone(),
                reason,
            })?;
        match status.status.as_str() {
            "failure" => Ok(Some(PersistedCell::Failure(status))),
            "success" => {
                let trial_path = path.join(TRIAL_FILE);
                let trial: PersistedTrialRow = read_one_csv(&trial_path)?;
                let sharing_path = path.join(SHARING_FILE);
                let sharing = if sharing_path.exists() {
                    read_csv::<PersistedSharingRow>(&sharing_path)?
                        .into_iter()
                        .map(PersistedSharingRow::into_sharing)
                        .collect()
                } else {
                    Vec::new()
                };
                let trial = trial
                    .into_trial(task_index, task.clone(), sharing)
                    .map_err(|reason| PersistenceError::InvalidShard {
                        path: path.clone(),
                        reason,
                    })?;
                Ok(Some(PersistedCell::Success(trial)))
            }
            _ => unreachable!("validated status is success or failure"),
        }
    }

    fn persist_success(
        &self,
        task_index: usize,
        task: &Task,
        trial: &RawTrial,
    ) -> Result<(), PersistenceError> {
        let status = CellStatusRow::new(&self.manifest, task_index, task, "success", String::new());
        let persisted_trial = PersistedTrialRow::from_trial(task_index, trial);
        let sharing = trial
            .sharing
            .iter()
            .map(PersistedSharingRow::from)
            .collect::<Vec<_>>();
        self.persist(task_index, &status, Some(&persisted_trial), &sharing)
    }

    fn persist_failure(
        &self,
        task_index: usize,
        task: &Task,
        error: String,
    ) -> Result<(), PersistenceError> {
        let status = CellStatusRow::new(&self.manifest, task_index, task, "failure", error);
        self.persist(task_index, &status, None, &[])
    }

    fn persist(
        &self,
        task_index: usize,
        status: &CellStatusRow,
        trial: Option<&PersistedTrialRow>,
        sharing: &[PersistedSharingRow],
    ) -> Result<(), PersistenceError> {
        let final_path = self.cell_path(task_index);
        if final_path.exists() {
            return Err(PersistenceError::ExistingShard(final_path));
        }
        let temporary = self.cells.join(format!(
            ".{task_index:06}-{}-{}.tmp",
            std::process::id(),
            TEMPORARY_COUNTER.fetch_add(1, Ordering::Relaxed)
        ));
        create_dir_all(&temporary)?;
        write_durable(&temporary.join(STATUS_FILE), to_csv(&[status])?.as_bytes())?;
        if let Some(trial) = trial {
            write_durable(&temporary.join(TRIAL_FILE), to_csv(&[trial])?.as_bytes())?;
        }
        if !sharing.is_empty() {
            write_durable(&temporary.join(SHARING_FILE), to_csv(sharing)?.as_bytes())?;
        }
        sync_directory(&temporary)?;
        fs::rename(&temporary, &final_path).map_err(|source| PersistenceError::Io {
            path: final_path,
            source,
        })?;
        sync_directory(&self.cells)
    }

    fn remove_torn_shards(&self) -> Result<(), PersistenceError> {
        for entry in read_dir(&self.cells)? {
            let entry = entry.map_err(|source| PersistenceError::Io {
                path: self.cells.clone(),
                source,
            })?;
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if name.starts_with('.') && name.ends_with(".tmp") {
                let path = entry.path();
                if path.is_dir() {
                    fs::remove_dir_all(&path)
                } else {
                    fs::remove_file(&path)
                }
                .map_err(|source| PersistenceError::Io { path, source })?;
            }
        }
        sync_directory(&self.cells)
    }
}

static TEMPORARY_COUNTER: AtomicUsize = AtomicUsize::new(0);

pub(super) fn run(
    seeds: u64,
    workers: usize,
    output: &Path,
    resume: bool,
) -> Result<WrPersistentRun, PersistenceError> {
    let tasks = build_tasks(seeds);
    let store = Arc::new(CellStore::open(output, seeds, tasks.len(), resume)?);
    let initial = load_cells(&store, &tasks)?;
    let skipped_cells = initial.skipped;
    let pending = Arc::new(initial.pending);
    let next = Arc::new(AtomicUsize::new(0));
    let infrastructure_error = Arc::new(Mutex::new(None::<String>));

    thread::scope(|scope| {
        for _ in 0..workers {
            let store = Arc::clone(&store);
            let pending = Arc::clone(&pending);
            let next = Arc::clone(&next);
            let infrastructure_error = Arc::clone(&infrastructure_error);
            scope.spawn(move || {
                loop {
                    if infrastructure_error
                        .lock()
                        .expect("WR persistence error lock")
                        .is_some()
                    {
                        return;
                    }
                    let pending_index = next.fetch_add(1, Ordering::Relaxed);
                    let Some((task_index, task)) = pending.get(pending_index) else {
                        return;
                    };
                    let outcome = match run_task(task.clone()) {
                        Ok(trial) => store.persist_success(*task_index, task, &trial),
                        Err(error) => store.persist_failure(*task_index, task, error.to_string()),
                    };
                    if let Err(error) = outcome {
                        *infrastructure_error
                            .lock()
                            .expect("WR persistence error lock") = Some(error.to_string());
                        return;
                    }
                    eprintln!(
                        "wr_cell_persisted index={} completed={}/{}",
                        task_index,
                        pending_index.saturating_add(1),
                        pending.len()
                    );
                }
            });
        }
    });
    if let Some(error) = infrastructure_error
        .lock()
        .expect("WR persistence error lock")
        .take()
    {
        return Err(PersistenceError::Worker(error));
    }

    let loaded = load_cells(&store, &tasks)?;
    if let Some((task_index, _)) = loaded.pending.first() {
        return Err(PersistenceError::MissingCell(*task_index));
    }
    let failures_csv = to_csv(&loaded.failures)?;
    let artifacts = if loaded.failures.is_empty() {
        Some(
            artifacts_from_trials(&loaded.successful)
                .map_err(|error| PersistenceError::Aggregation(error.to_string()))?,
        )
    } else {
        None
    };
    Ok(WrPersistentRun {
        artifacts,
        failures_csv,
        total_cells: tasks.len(),
        successful_cells: loaded.successful.len(),
        failed_cells: loaded.failures.len(),
        skipped_cells,
    })
}

fn load_cells(store: &CellStore, tasks: &[Task]) -> Result<LoadedCells, PersistenceError> {
    let mut successful = Vec::new();
    let mut failures = Vec::new();
    let mut pending = Vec::new();
    for (task_index, task) in tasks.iter().enumerate() {
        match store.load(task_index, task)? {
            Some(PersistedCell::Success(trial)) => successful.push(trial),
            Some(PersistedCell::Failure(status)) => failures.push(status),
            None => pending.push((task_index, task.clone())),
        }
    }
    let skipped = successful.len().saturating_add(failures.len());
    Ok(LoadedCells {
        successful,
        failures,
        pending,
        skipped,
    })
}

fn task_key(task: &Task) -> String {
    format!(
        "slice={};profile={};placement={};utilization={};jitter={};protocol={};K={};seed={};cadence={};admission={};slow={};sessions={}",
        task.slice.name(),
        task.profile.name(),
        task.placement,
        task.utilization,
        task.jitter,
        task.protocol.name(),
        task.k,
        task.seed,
        cadence_name(task.cadence),
        admission_name(task.admission),
        task.slow_receiver
            .map_or_else(|| "none".to_owned(), |receiver| receiver.to_string()),
        task.sessions,
    )
}

fn directory_has_entries(path: &Path) -> Result<bool, PersistenceError> {
    Ok(read_dir(path)?
        .next()
        .transpose()
        .map_err(|source| PersistenceError::Io {
            path: path.to_path_buf(),
            source,
        })?
        .is_some())
}

fn create_dir_all(path: &Path) -> Result<(), PersistenceError> {
    fs::create_dir_all(path).map_err(|source| PersistenceError::Io {
        path: path.to_path_buf(),
        source,
    })
}

fn read_dir(path: &Path) -> Result<fs::ReadDir, PersistenceError> {
    fs::read_dir(path).map_err(|source| PersistenceError::Io {
        path: path.to_path_buf(),
        source,
    })
}

fn read_to_string(path: &Path) -> Result<String, PersistenceError> {
    fs::read_to_string(path).map_err(|source| PersistenceError::Io {
        path: path.to_path_buf(),
        source,
    })
}

fn read_one_csv<T: DeserializeOwned>(path: &Path) -> Result<T, PersistenceError> {
    let mut rows = read_csv(path)?;
    if rows.len() != 1 {
        return Err(PersistenceError::InvalidShard {
            path: path.to_path_buf(),
            reason: format!("expected exactly one row, found {}", rows.len()),
        });
    }
    Ok(rows.remove(0))
}

fn read_csv<T: DeserializeOwned>(path: &Path) -> Result<Vec<T>, PersistenceError> {
    let mut reader = csv::Reader::from_path(path).map_err(|source| PersistenceError::Csv {
        path: path.to_path_buf(),
        source,
    })?;
    reader
        .deserialize()
        .collect::<Result<Vec<_>, _>>()
        .map_err(|source| PersistenceError::Csv {
            path: path.to_path_buf(),
            source,
        })
}

fn atomic_write_file(path: &Path, contents: &[u8]) -> Result<(), PersistenceError> {
    let parent = path
        .parent()
        .ok_or_else(|| PersistenceError::InvalidShard {
            path: path.to_path_buf(),
            reason: "path has no parent".to_owned(),
        })?;
    let temporary = parent.join(format!(
        ".{}-{}-{}.tmp",
        path.file_name()
            .map_or_else(|| "file".into(), |name| name.to_string_lossy()),
        std::process::id(),
        TEMPORARY_COUNTER.fetch_add(1, Ordering::Relaxed)
    ));
    write_durable(&temporary, contents)?;
    fs::rename(&temporary, path).map_err(|source| PersistenceError::Io {
        path: path.to_path_buf(),
        source,
    })?;
    sync_directory(parent)
}

fn write_durable(path: &Path, contents: &[u8]) -> Result<(), PersistenceError> {
    let mut file = File::create(path).map_err(|source| PersistenceError::Io {
        path: path.to_path_buf(),
        source,
    })?;
    file.write_all(contents)
        .and_then(|()| file.sync_all())
        .map_err(|source| PersistenceError::Io {
            path: path.to_path_buf(),
            source,
        })
}

fn sync_directory(path: &Path) -> Result<(), PersistenceError> {
    File::open(path)
        .and_then(|directory| directory.sync_all())
        .map_err(|source| PersistenceError::Io {
            path: path.to_path_buf(),
            source,
        })
}

#[cfg(test)]
mod tests {
    use std::process::{Command, Stdio};
    use std::time::{Duration, Instant};

    use super::*;

    const CHILD_ENV: &str = "WANSIM_WR_KILL_FIXTURE";
    const ROOT_ENV: &str = "WANSIM_WR_KILL_ROOT";

    #[test]
    fn resume_after_kill_preserves_durable_cells_and_discards_torn_shards() {
        if std::env::var_os(CHILD_ENV).is_some() {
            run_kill_fixture();
            return;
        }

        let root = unique_test_directory();
        let ready = root.with_extension("ready");
        let mut child = Command::new(std::env::current_exe().expect("current test executable"))
            .arg("resume_after_kill_preserves_durable_cells_and_discards_torn_shards")
            .arg("--nocapture")
            .env(CHILD_ENV, "1")
            .env(ROOT_ENV, &root)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn kill fixture");

        let deadline = Instant::now() + Duration::from_secs(10);
        while !ready.exists() && Instant::now() < deadline {
            assert!(
                child.try_wait().expect("poll kill fixture").is_none(),
                "kill fixture exited before publishing its durable cell"
            );
            thread::sleep(Duration::from_millis(10));
        }
        assert!(ready.exists(), "kill fixture did not become ready");
        child.kill().expect("kill fixture after durable write");
        child.wait().expect("reap killed fixture");

        let tasks = build_tasks(16).into_iter().take(2).collect::<Vec<_>>();
        let store = CellStore::open(&root, 16, tasks.len(), true).expect("resume cell store");
        let loaded = load_cells(&store, &tasks).expect("load after kill");
        assert_eq!(loaded.successful.len(), 1);
        assert!(loaded.failures.is_empty());
        assert_eq!(loaded.pending.len(), 1);
        assert_eq!(loaded.pending[0].0, 1);
        assert_eq!(loaded.skipped, 1);
        assert!(
            read_dir(&store.cells)
                .expect("read resumed cell directory")
                .all(|entry| !entry
                    .expect("cell directory entry")
                    .file_name()
                    .to_string_lossy()
                    .ends_with(".tmp"))
        );

        let second = fake_trial(tasks[1].clone(), 2);
        store
            .persist_success(1, &tasks[1], &second)
            .expect("persist resumed cell");
        let reopened = CellStore::open(&root, 16, tasks.len(), true).expect("reopen resumed store");
        let complete = load_cells(&reopened, &tasks).expect("load completed resume");
        assert_eq!(complete.successful.len(), 2);
        assert!(complete.pending.is_empty());
        assert_eq!(complete.skipped, 2);

        fs::remove_dir_all(&root).expect("remove test cell store");
        fs::remove_file(&ready).expect("remove test ready marker");
    }

    #[test]
    fn failed_cell_is_durable_without_replacing_prior_success() {
        let root = unique_test_directory();
        let tasks = build_tasks(16).into_iter().take(2).collect::<Vec<_>>();
        let store = CellStore::open(&root, 16, tasks.len(), false).expect("create cell store");
        store
            .persist_success(0, &tasks[0], &fake_trial(tasks[0].clone(), 1))
            .expect("persist successful cell");
        store
            .persist_failure(1, &tasks[1], "injected cell failure".to_owned())
            .expect("persist failed cell");

        let resumed = CellStore::open(&root, 16, tasks.len(), true).expect("resume cell store");
        let loaded = load_cells(&resumed, &tasks).expect("load terminal cells");
        assert_eq!(loaded.successful.len(), 1);
        assert_eq!(loaded.failures.len(), 1);
        assert_eq!(loaded.failures[0].status, "failure");
        assert_eq!(loaded.failures[0].error, "injected cell failure");
        assert!(loaded.pending.is_empty());
        assert_eq!(loaded.skipped, 2);
        assert!(resumed.cell_path(0).join(TRIAL_FILE).is_file());
        assert!(!resumed.cell_path(1).join(TRIAL_FILE).exists());

        fs::remove_dir_all(root).expect("remove test cell store");
    }

    fn run_kill_fixture() {
        let root = PathBuf::from(std::env::var_os(ROOT_ENV).expect("kill fixture root"));
        let ready = root.with_extension("ready");
        let tasks = build_tasks(16).into_iter().take(2).collect::<Vec<_>>();
        let store = CellStore::open(&root, 16, tasks.len(), false).expect("create fixture store");
        let first = fake_trial(tasks[0].clone(), 1);
        store
            .persist_success(0, &tasks[0], &first)
            .expect("persist fixture cell");
        let torn = store
            .cells
            .join(format!(".000001-{}-killed.tmp", std::process::id()));
        create_dir_all(&torn).expect("create torn fixture shard");
        write_durable(&torn.join(STATUS_FILE), b"partial,status\n")
            .expect("write torn fixture shard");
        write_durable(&ready, b"ready\n").expect("publish fixture readiness");
        thread::sleep(Duration::from_secs(30));
    }

    fn fake_trial(task: Task, ordinal: u64) -> RawTrial {
        RawTrial {
            profile_name: task.profile.name(),
            placement_id: format!("test-placement-{}", task.placement),
            task,
            barrier_ns: ordinal,
            sender_ns: ordinal + 1,
            receiver_ns: [ordinal, ordinal, ordinal],
            emissions: usize::try_from(ordinal).expect("test ordinal fits usize"),
            useful_emissions: usize::try_from(ordinal).expect("test ordinal fits usize"),
            per_tree_emissions: [1, 1],
            drops: 0,
            blocking_waits: 0,
            positive_round_deficits: 0,
            ack_probes: 0,
            stall_ppm: 0,
            link_drops: 0,
            background_trunk_utilization_ppm: 0,
            a4_trace_correlation_ppm: 0,
            tree_bytes: [ordinal, ordinal],
            maximum_mailbox_high_water: 1,
            sharing: Vec::new(),
        }
    }

    fn unique_test_directory() -> PathBuf {
        std::env::temp_dir().join(format!(
            "wansim-wr-resume-{}-{}",
            std::process::id(),
            TEMPORARY_COUNTER.fetch_add(1, Ordering::Relaxed)
        ))
    }
}
