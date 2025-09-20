pub mod analysis;
pub mod catalog;
pub mod config;
pub mod monitor;

pub use analysis::{AnalysisResult, TableAnalyser};
pub use catalog::{CatalogProvider, CatalogType};
pub use config::{CatalogConfig, Config};
pub use monitor::{SnapshotEvent, SnapshotEventType, SnapshotMonitor};

#[derive(Debug, thiserror::Error)]
pub enum RimeError {
    #[error("Catalog error: {0}")]
    Catalog(#[from] iceberg::Error),
    #[error("Configuration error: {0}")]
    Config(String),
    #[error("Analysis error: {0}")]
    Analysis(String),
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}
