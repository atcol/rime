use crate::RimeError;
use iceberg::{Catalog, table::Table};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AnalysisResult {
    pub table_name: String,
    pub namespace: Vec<String>,
    pub metrics: TableMetrics,
    pub maintenance_info: MaintenanceInfo,
    pub performance_info: PerformanceInfo,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableMetrics {
    pub total_files: usize,
    pub total_size_bytes: u64,
    pub total_records: u64,
    pub snapshot_count: usize,
    pub partition_count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MaintenanceInfo {
    pub needs_compaction: bool,
    pub small_files_count: usize,
    pub orphaned_files_estimated: usize,
    pub last_compaction: Option<chrono::DateTime<chrono::Utc>>,
    pub recommendations: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceInfo {
    pub avg_file_size_mb: f64,
    pub scan_efficiency_score: f32,
    pub partition_pruning_effectiveness: f32,
    pub query_patterns: HashMap<String, u32>,
    pub hot_partitions: Vec<String>,
}

pub struct TableAnalyser {
    catalog: Arc<dyn Catalog>,
}

impl TableAnalyser {
    pub fn new(catalog: Arc<dyn Catalog>) -> Self {
        Self { catalog }
    }

    pub async fn analyse_table(
        &self,
        namespace: &[String],
        table_name: &str,
    ) -> Result<AnalysisResult, RimeError> {
        let table_ident = iceberg::TableIdent::new(
            iceberg::NamespaceIdent::from_vec(namespace.to_vec())?,
            table_name.to_string(),
        );
        let table = self.catalog.load_table(&table_ident).await?;

        let metrics = self.collect_table_metrics(&table).await?;
        let maintenance_info = self.analyse_maintenance_needs(&table).await?;
        let performance_info = self.analyse_performance(&table).await?;

        Ok(AnalysisResult {
            table_name: table_name.to_string(),
            namespace: namespace.to_vec(),
            metrics,
            maintenance_info,
            performance_info,
        })
    }

    pub async fn list_namespaces(&self) -> Result<Vec<String>, RimeError> {
        tracing::debug!("Calling catalog.list_namespaces(None)");
        let namespaces = self.catalog.list_namespaces(None).await
            .map_err(|e| {
                tracing::error!("Catalog list_namespaces failed: {:?}", e);
                e
            })?;
        
        tracing::debug!("Raw namespaces from catalog: {:?}", namespaces);
        let result: Vec<String> = namespaces.into_iter()
            .map(|ns| {
                let joined = ns.as_ref().join(".");
                tracing::debug!("Namespace: {:?} -> {}", ns.as_ref(), joined);
                joined
            })
            .collect();
        
        tracing::info!("Converted {} namespaces to strings", result.len());
        Ok(result)
    }

    pub async fn list_tables(&self, namespace: &[String]) -> Result<Vec<String>, RimeError> {
        let namespace_ident = iceberg::NamespaceIdent::from_vec(namespace.to_vec())?;
        let tables = self.catalog.list_tables(&namespace_ident).await?;
        Ok(tables.into_iter().map(|t| t.name().to_string()).collect())
    }

    async fn collect_table_metrics(&self, table: &Table) -> Result<TableMetrics, RimeError> {
        let metadata = table.metadata();
        let current_snapshot = metadata.current_snapshot();

        let (total_files, total_size_bytes, total_records) = if let Some(snapshot) = current_snapshot {
            let summary = snapshot.summary();
            let total_files = summary.additional_properties.get("total-data-files")
                .and_then(|s| s.parse().ok())
                .unwrap_or(0);
            let total_size = summary.additional_properties.get("total-size")
                .and_then(|s| s.parse().ok())
                .unwrap_or(0);
            let total_records = summary.additional_properties.get("total-records")
                .and_then(|s| s.parse().ok())
                .unwrap_or(0);
            (total_files, total_size, total_records)
        } else {
            (0, 0, 0)
        };

        let snapshot_count = metadata.snapshots().len();
        let partition_count = metadata.default_partition_spec().fields().len();

        Ok(TableMetrics {
            total_files,
            total_size_bytes,
            total_records,
            snapshot_count,
            partition_count,
        })
    }

    async fn analyse_maintenance_needs(&self, table: &Table) -> Result<MaintenanceInfo, RimeError> {
        let metadata = table.metadata();
        let mut recommendations = Vec::new();

        let small_files_threshold = 64 * 1024 * 1024; // 64MB
        let mut small_files_count = 0;

        if let Some(snapshot) = metadata.current_snapshot() {
            let summary = snapshot.summary();
            let total_files: usize = summary.additional_properties.get("total-data-files")
                .and_then(|s| s.parse().ok())
                .unwrap_or(0);
            let total_size: u64 = summary.additional_properties.get("total-size")
                .and_then(|s| s.parse().ok())
                .unwrap_or(0);

            if total_files > 0 {
                let avg_file_size = total_size / total_files as u64;
                if avg_file_size < small_files_threshold {
                    small_files_count = total_files / 2; // Estimate
                    recommendations.push("Consider running compaction to merge small files".to_string());
                }
            }

            if total_files > 1000 {
                recommendations.push("High number of files detected, compaction recommended".to_string());
            }
        }

        let needs_compaction = small_files_count > 10 || !recommendations.is_empty();

        if metadata.snapshots().len() > 100 {
            recommendations.push("Consider running expire_snapshots to clean old snapshots".to_string());
        }

        Ok(MaintenanceInfo {
            needs_compaction,
            small_files_count,
            orphaned_files_estimated: small_files_count / 10, // Rough estimate
            last_compaction: None, // Would need to parse snapshot history
            recommendations,
        })
    }

    async fn analyse_performance(&self, table: &Table) -> Result<PerformanceInfo, RimeError> {
        let metadata = table.metadata();
        
        let avg_file_size_mb = if let Some(snapshot) = metadata.current_snapshot() {
            let summary = snapshot.summary();
            let total_files: usize = summary.additional_properties.get("total-data-files")
                .and_then(|s| s.parse().ok())
                .unwrap_or(1);
            let total_size: u64 = summary.additional_properties.get("total-size")
                .and_then(|s| s.parse().ok())
                .unwrap_or(0);
            
            (total_size as f64 / total_files as f64) / (1024.0 * 1024.0)
        } else {
            0.0
        };

        // Calculate scan efficiency based on file size distribution
        let scan_efficiency_score = if avg_file_size_mb > 64.0 {
            0.9
        } else if avg_file_size_mb > 32.0 {
            0.7
        } else if avg_file_size_mb > 16.0 {
            0.5
        } else {
            0.3
        };

        // Estimate partition pruning effectiveness
        let partition_count = metadata.default_partition_spec().fields().len();
        let partition_pruning_effectiveness = if partition_count > 0 {
            0.8 // Assume good partitioning
        } else {
            0.1 // No partitioning
        };

        Ok(PerformanceInfo {
            avg_file_size_mb,
            scan_efficiency_score,
            partition_pruning_effectiveness,
            query_patterns: HashMap::new(), // Would need query history
            hot_partitions: Vec::new(), // Would need access patterns
        })
    }
}
