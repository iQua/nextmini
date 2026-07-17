use std::sync::Arc;

use parking_lot::Mutex;
use serde::Serialize;

use crate::{SCENARIO_SCHEMA_VERSION, SIMULATOR_VERSION};

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize)]
pub struct Record {
    pub schema_version: u32,
    pub simulator_version: &'static str,
    pub scenario: String,
    pub seed: u64,
    pub time_ns: u64,
    pub component: &'static str,
    pub event: &'static str,
    pub flow_id: usize,
    pub sequence: usize,
    pub bytes: usize,
    pub value: usize,
}

#[derive(Clone, Debug)]
pub struct Recorder {
    scenario: Arc<str>,
    seed: u64,
    records: Arc<Mutex<Vec<Record>>>,
    failure: Arc<Mutex<Option<String>>>,
}

impl Recorder {
    pub fn new(scenario: impl Into<Arc<str>>, seed: u64) -> Self {
        Self {
            scenario: scenario.into(),
            seed,
            records: Arc::default(),
            failure: Arc::default(),
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn record(
        &self,
        time_ns: u64,
        component: &'static str,
        event: &'static str,
        flow_id: usize,
        sequence: usize,
        bytes: usize,
        value: usize,
    ) {
        self.records.lock().push(Record {
            schema_version: SCENARIO_SCHEMA_VERSION,
            simulator_version: SIMULATOR_VERSION,
            scenario: self.scenario.to_string(),
            seed: self.seed,
            time_ns,
            component,
            event,
            flow_id,
            sequence,
            bytes,
            value,
        });
    }

    pub fn fail(&self, error: impl std::fmt::Display) {
        let mut failure = self.failure.lock();
        if failure.is_none() {
            *failure = Some(error.to_string());
        }
    }

    pub fn failure(&self) -> Option<String> {
        self.failure.lock().clone()
    }

    pub fn records(&self) -> Vec<Record> {
        let mut records = self.records.lock().clone();
        records.sort();
        records
    }

    pub fn to_csv(&self) -> Result<String, csv::Error> {
        let mut writer = csv::WriterBuilder::new()
            .terminator(csv::Terminator::Any(b'\n'))
            .from_writer(Vec::new());
        for record in self.records() {
            writer.serialize(record)?;
        }
        writer.flush()?;
        let bytes = writer
            .into_inner()
            .map_err(|error| csv::Error::from(error.into_error()))?;
        Ok(String::from_utf8_lossy(&bytes).into_owned())
    }
}
