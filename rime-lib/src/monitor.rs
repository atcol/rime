use chrono::{DateTime, Utc};
use iceberg::spec::Snapshot;
use iceberg::{Catalog, TableIdent};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;
use tracing::{debug, info, warn};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotEvent {
    pub table_namespace: Vec<String>,
    pub table_name: String,
    pub snapshot_id: i64,
    pub timestamp_ms: i64,
    pub timestamp_utc: DateTime<Utc>,
    pub summary: HashMap<String, String>,
    pub event_type: SnapshotEventType,
    pub detected_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SnapshotEventType {
    Added,
    Expired,
}

pub struct SnapshotMonitor {
    catalog: Arc<dyn Catalog>,
    tables: Vec<(Vec<String>, String)>, // (namespace, table_name)
    known_snapshots: HashMap<String, HashMap<i64, Arc<Snapshot>>>, // table_key -> snapshot_id -> snapshot
    poll_interval: Duration,
}

impl SnapshotMonitor {
    pub fn new(catalog: Arc<dyn Catalog>, poll_interval: Duration) -> Self {
        Self {
            catalog,
            tables: Vec::new(),
            known_snapshots: HashMap::new(),
            poll_interval,
        }
    }

    pub fn add_table(&mut self, namespace: Vec<String>, table_name: String) {
        let table_key = Self::table_key(&namespace, &table_name);
        self.tables.push((namespace, table_name));
        self.known_snapshots.insert(table_key, HashMap::new());
    }

    pub async fn initialize(&mut self) -> anyhow::Result<()> {
        info!(
            "Initializing snapshot monitor for {} tables",
            self.tables.len()
        );

        for (namespace, table_name) in &self.tables.clone() {
            let table_ident = TableIdent::from_strs(
                namespace
                    .iter()
                    .chain(std::iter::once(table_name))
                    .map(|s| s.as_str()),
            )?;

            match self.catalog.load_table(&table_ident).await {
                Ok(table) => {
                    let table_key = Self::table_key(namespace, table_name);
                    let snapshots: HashMap<_, _> = table
                        .metadata()
                        .snapshots()
                        .map(|s| (s.snapshot_id(), s.clone()))
                        .collect();

                    info!(
                        "Initialized table {}.{} with {} snapshots",
                        namespace.join("."),
                        table_name,
                        snapshots.len()
                    );

                    self.known_snapshots.insert(table_key, snapshots);
                }
                Err(e) => {
                    warn!(
                        "Failed to initialize table {}.{}: {}",
                        namespace.join("."),
                        table_name,
                        e
                    );
                }
            }
        }

        Ok(())
    }

    pub async fn poll_once(&mut self) -> anyhow::Result<Vec<SnapshotEvent>> {
        let mut events = Vec::new();

        for (namespace, table_name) in &self.tables.clone() {
            let table_ident = TableIdent::from_strs(
                namespace
                    .iter()
                    .chain(std::iter::once(table_name))
                    .map(|s| s.as_str()),
            )?;

            match self.catalog.load_table(&table_ident).await {
                Ok(table) => {
                    let current_snapshots: HashMap<_, _> = table
                        .metadata()
                        .snapshots()
                        .map(|s| (s.snapshot_id(), s.clone()))
                        .collect();

                    let table_key = Self::table_key(namespace, table_name);
                    let known = self
                        .known_snapshots
                        .get(&table_key)
                        .cloned()
                        .unwrap_or_default();

                    // Detect new snapshots
                    for (id, snapshot) in &current_snapshots {
                        if !known.contains_key(id) {
                            debug!("New snapshot detected: {}", id);
                            events.push(self.create_snapshot_event(
                                namespace.clone(),
                                table_name.clone(),
                                snapshot,
                                SnapshotEventType::Added,
                            ));
                        }
                    }

                    // Detect expired snapshots
                    for id in known.keys() {
                        if !current_snapshots.contains_key(id) {
                            if let Some(snapshot) = known.get(id) {
                                debug!("Snapshot expired: {}", id);
                                events.push(self.create_snapshot_event(
                                    namespace.clone(),
                                    table_name.clone(),
                                    snapshot,
                                    SnapshotEventType::Expired,
                                ));
                            }
                        }
                    }

                    // Update known snapshots
                    self.known_snapshots.insert(table_key, current_snapshots);
                }
                Err(e) => {
                    warn!(
                        "Failed to poll table {}.{}: {}",
                        namespace.join("."),
                        table_name,
                        e
                    );
                }
            }
        }

        Ok(events)
    }

    pub async fn watch(&mut self) -> anyhow::Result<()> {
        self.initialize().await?;

        info!(
            "Starting snapshot monitoring with {}s interval",
            self.poll_interval.as_secs()
        );

        loop {
            let events = self.poll_once().await?;

            for event in events {
                // Output to stdout as JSON
                println!("{}", serde_json::to_string(&event)?);
            }

            sleep(self.poll_interval).await;
        }
    }

    fn create_snapshot_event(
        &self,
        namespace: Vec<String>,
        table_name: String,
        snapshot: &Snapshot,
        event_type: SnapshotEventType,
    ) -> SnapshotEvent {
        let timestamp_utc = DateTime::from_timestamp_millis(snapshot.timestamp_ms())
            .unwrap_or_else(|| DateTime::from_timestamp(0, 0).unwrap());

        SnapshotEvent {
            table_namespace: namespace,
            table_name,
            snapshot_id: snapshot.snapshot_id(),
            timestamp_ms: snapshot.timestamp_ms(),
            timestamp_utc,
            summary: snapshot.summary().additional_properties.clone(),
            event_type,
            detected_at: Utc::now(),
        }
    }

    fn table_key(namespace: &[String], table_name: &str) -> String {
        format!("{}.{}", namespace.join("."), table_name)
    }
}
