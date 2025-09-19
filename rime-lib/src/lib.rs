pub mod catalog;
pub mod config;
pub mod analysis;
pub mod monitor;

pub use catalog::{CatalogProvider, CatalogType};
pub use config::{Config, CatalogConfig};
pub use analysis::{TableAnalyser, AnalysisResult};
pub use monitor::{SnapshotMonitor, SnapshotEvent, SnapshotEventType};

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