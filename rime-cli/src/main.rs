use clap::{Parser, Subcommand};
use rime_lib::{Config, CatalogType, TableAnalyser, SnapshotMonitor};
use std::path::PathBuf;
use std::time::Duration;
use tracing::{info, error};

#[derive(Parser)]
#[command(name = "rime")]
#[command(about = "Iceberg table analysis CLI")]
#[command(version = "0.1.0")]
struct Cli {
    #[arg(short, long, help = "Configuration file path")]
    config: Option<PathBuf>,
    
    #[arg(short = 'C', long, help = "Override catalog to use")]
    catalog: Option<String>,
    
    #[arg(short, long, action = clap::ArgAction::Count, help = "Increase verbosity")]
    verbose: u8,
    
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    #[command(about = "Initialize configuration")]
    Init {
        #[arg(long, help = "Force overwrite existing config")]
        force: bool,
    },
    #[command(about = "List available catalogs")]
    Catalogs,
    #[command(about = "List namespaces")]
    Namespaces,
    #[command(about = "List tables in namespace")]
    Tables {
        #[arg(help = "Namespace (e.g., 'db.schema' or 'db')")]
        namespace: String,
    },
    #[command(about = "Analyse table")]
    Analyse {
        #[arg(help = "Namespace (e.g., 'db.schema' or 'db')")]
        namespace: String,
        #[arg(help = "Table name")]
        table: String,
        #[arg(long, help = "Output format", value_enum, default_value = "json")]
        format: OutputFormat,
    },
    #[command(about = "Show table maintenance recommendations")]
    Maintenance {
        #[arg(help = "Namespace (e.g., 'db.schema' or 'db')")]
        namespace: String,
        #[arg(help = "Table name")]
        table: String,
    },
    #[command(about = "Show table performance analysis")]
    Performance {
        #[arg(help = "Namespace (e.g., 'db.schema' or 'db')")]
        namespace: String,
        #[arg(help = "Table name")]
        table: String,
    },
    #[command(about = "Watch for snapshot changes in tables")]
    Watch {
        #[arg(help = "Tables to watch in format 'namespace.table' (can specify multiple)")]
        tables: Vec<String>,
        #[arg(short, long, default_value = "30", help = "Poll interval in seconds")]
        interval: u64,
        #[arg(long, help = "Watch all tables in the specified namespace")]
        namespace: Option<String>,
    },
}

#[derive(clap::ValueEnum, Clone)]
enum OutputFormat {
    Json,
    Yaml,
    Table,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    
    // Initialize tracing
    let level = match cli.verbose {
        0 => tracing::Level::WARN,
        1 => tracing::Level::INFO,
        2 => tracing::Level::DEBUG,
        _ => tracing::Level::TRACE,
    };
    
    tracing_subscriber::fmt()
        .with_max_level(level)
        .init();

    // Load configuration
    let config_path = cli.config.unwrap_or_else(|| {
        dirs::config_dir()
            .unwrap_or_else(|| PathBuf::from("."))
            .join("rime.toml")
    });
    
    match cli.command {
        Commands::Init { force } => {
            init_config(&config_path, force).await?;
        }
        Commands::Catalogs => {
            list_catalogs(&config_path).await?;
        }
        Commands::Namespaces => {
            list_namespaces(&config_path, &cli.catalog).await?;
        }
        Commands::Tables { namespace } => {
            list_tables(&config_path, &cli.catalog, &namespace).await?;
        }
        Commands::Analyse { namespace, table, format } => {
            analyse_table(&config_path, &cli.catalog, &namespace, &table, format).await?;
        }
        Commands::Maintenance { namespace, table } => {
            show_maintenance(&config_path, &cli.catalog, &namespace, &table).await?;
        }
        Commands::Performance { namespace, table } => {
            show_performance(&config_path, &cli.catalog, &namespace, &table).await?;
        }
        Commands::Watch { tables, interval, namespace } => {
            watch_snapshots(&config_path, &cli.catalog, tables, interval, namespace).await?;
        }
    }
    
    Ok(())
}

async fn init_config(config_path: &PathBuf, force: bool) -> anyhow::Result<()> {
    if config_path.exists() && !force {
        anyhow::bail!("Configuration file already exists. Use --force to overwrite.");
    }
    
    let mut config = Config::new();
    
    // Add example configurations
    config.catalogs.insert("glue-example".to_string(), rime_lib::CatalogConfig::Glue {
        region: "us-west-2".to_string(),
        warehouse: "s3://my-bucket/warehouse/".to_string(),
        profile: None,
    });
    
    config.catalogs.insert("rest-example".to_string(), rime_lib::CatalogConfig::Rest {
        uri: "http://localhost:8181".to_string(),
        warehouse: "s3://my-bucket/warehouse/".to_string(),
        credential: None,
        token: None,
    });
    
    config.default_catalog = Some("glue-example".to_string());
    
    let toml_content = config.to_toml_string()?;
    
    if let Some(parent) = config_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    
    std::fs::write(config_path, toml_content)?;
    println!("Configuration initialized at: {}", config_path.display());
    
    Ok(())
}

async fn load_config(config_path: &PathBuf) -> anyhow::Result<Config> {
    if !config_path.exists() {
        anyhow::bail!("Configuration file not found: {}. Run 'rime init' first.", config_path.display());
    }
    
    let content = std::fs::read_to_string(config_path)?;
    Ok(Config::from_toml_str(&content)?)
}

async fn list_catalogs(config_path: &PathBuf) -> anyhow::Result<()> {
    let config = load_config(config_path).await?;
    
    println!("Available catalogs:");
    for (name, catalog_config) in &config.catalogs {
        let catalog_type = match catalog_config {
            rime_lib::CatalogConfig::Glue { .. } => "glue",
            rime_lib::CatalogConfig::Rest { .. } => "rest",
        };
        
        let is_default = config.default_catalog.as_ref() == Some(name);
        let marker = if is_default { " (default)" } else { "" };
        
        println!("  {} [{}]{}", name, catalog_type, marker);
    }
    
    Ok(())
}

async fn get_analyser(config: &Config, catalog_name: &Option<String>) -> anyhow::Result<TableAnalyser> {
    let catalog_config = if let Some(name) = catalog_name {
        config.get_catalog(name)
            .ok_or_else(|| anyhow::anyhow!("Catalog '{}' not found", name))?
    } else {
        config.get_default_catalog()
            .ok_or_else(|| anyhow::anyhow!("No default catalog configured"))?
    };
    
    let catalog_type = match catalog_config {
        rime_lib::CatalogConfig::Glue { .. } => CatalogType::Glue,
        rime_lib::CatalogConfig::Rest { .. } => CatalogType::Rest,
    };
    
    let provider = rime_lib::catalog::create_catalog_provider(&catalog_type);
    let catalog = provider.create_catalog(catalog_config).await?;
    
    Ok(TableAnalyser::new(catalog))
}

fn parse_namespace(namespace: &str) -> Vec<String> {
    namespace.split('.').map(|s| s.to_string()).collect()
}

async fn list_namespaces(
    config_path: &PathBuf,
    catalog_name: &Option<String>,
) -> anyhow::Result<()> {
    let config = load_config(config_path).await?;
    info!("Loaded configuration from: {}", config_path.display());
    
    let catalog_to_use = if let Some(name) = catalog_name {
        info!("Using catalog override: {}", name);
        name.clone()
    } else if let Some(default) = &config.default_catalog {
        info!("Using default catalog: {}", default);
        default.clone()
    } else {
        info!("No catalog specified, using first available");
        config.catalogs.keys().next()
            .ok_or_else(|| anyhow::anyhow!("No catalogs configured"))?
            .clone()
    };
    
    let analyser = get_analyser(&config, catalog_name).await?;
    
    info!("Listing namespaces from catalog: {}", catalog_to_use);
    
    match analyser.list_namespaces().await {
        Ok(namespaces) => {
            info!("Found {} namespace(s)", namespaces.len());
            if namespaces.is_empty() {
                println!("No namespaces found in catalog '{}'", catalog_to_use);
                info!("This might be because:");
                info!("  - The catalog is empty");
                info!("  - Authentication/authorisation issues");
                info!("  - The catalog configuration is incorrect");
                info!("  - The catalog service is unavailable");
            } else {
                println!("Namespaces in catalog '{}':", catalog_to_use);
                for namespace in &namespaces {
                    println!("  {}", namespace);
                    info!("Found namespace: {}", namespace);
                }
            }
        }
        Err(e) => {
            error!("Failed to list namespaces from catalog '{}': {}", catalog_to_use, e);
            error!("Error details: {:?}", e);
            anyhow::bail!("Failed to list namespaces: {}", e);
        }
    }
    
    Ok(())
}

async fn list_tables(
    config_path: &PathBuf,
    catalog_name: &Option<String>,
    namespace: &str,
) -> anyhow::Result<()> {
    let config = load_config(config_path).await?;
    let analyser = get_analyser(&config, catalog_name).await?;
    let namespace_vec = parse_namespace(namespace);
    
    info!("Listing tables in namespace: {}", namespace);
    
    match analyser.list_tables(&namespace_vec).await {
        Ok(tables) => {
            if tables.is_empty() {
                println!("No tables found in namespace: {}", namespace);
            } else {
                println!("Tables in namespace '{}':", namespace);
                for table in tables {
                    println!("  {}", table);
                }
            }
        }
        Err(e) => {
            error!("Failed to list tables: {}", e);
            anyhow::bail!("Failed to list tables: {}", e);
        }
    }
    
    Ok(())
}

async fn analyse_table(
    config_path: &PathBuf,
    catalog_name: &Option<String>,
    namespace: &str,
    table_name: &str,
    format: OutputFormat,
) -> anyhow::Result<()> {
    let config = load_config(config_path).await?;
    let analyser = get_analyser(&config, catalog_name).await?;
    let namespace_vec = parse_namespace(namespace);
    
    info!("Analyzing table: {}.{}", namespace, table_name);
    
    match analyser.analyse_table(&namespace_vec, table_name).await {
        Ok(result) => {
            match format {
                OutputFormat::Json => {
                    println!("{}", serde_json::to_string_pretty(&result)?);
                }
                OutputFormat::Yaml => {
                    println!("{}", serde_yaml::to_string(&result)?);
                }
                OutputFormat::Table => {
                    print_table_analysis(&result);
                }
            }
        }
        Err(e) => {
            error!("Failed to analyse table: {}", e);
            anyhow::bail!("Failed to analyse table: {}", e);
        }
    }
    
    Ok(())
}

fn print_table_analysis(result: &rime_lib::AnalysisResult) {
    println!("Table Analysis: {}.{}", result.namespace.join("."), result.table_name);
    println!("================");
    println!();
    
    println!("Metrics:");
    println!("  Total Files: {}", result.metrics.total_files);
    println!("  Total Size: {:.2} GB", result.metrics.total_size_bytes as f64 / (1024.0 * 1024.0 * 1024.0));
    println!("  Total Records: {}", result.metrics.total_records);
    println!("  Snapshots: {}", result.metrics.snapshot_count);
    println!("  Partitions: {}", result.metrics.partition_count);
    println!();
    
    println!("Maintenance:");
    println!("  Needs Compaction: {}", result.maintenance_info.needs_compaction);
    println!("  Small Files: {}", result.maintenance_info.small_files_count);
    println!("  Estimated Orphaned Files: {}", result.maintenance_info.orphaned_files_estimated);
    
    if !result.maintenance_info.recommendations.is_empty() {
        println!("  Recommendations:");
        for rec in &result.maintenance_info.recommendations {
            println!("    - {}", rec);
        }
    }
    println!();
    
    println!("Performance:");
    println!("  Average File Size: {:.2} MB", result.performance_info.avg_file_size_mb);
    println!("  Scan Efficiency Score: {:.1}", result.performance_info.scan_efficiency_score);
    println!("  Partition Pruning Effectiveness: {:.1}", result.performance_info.partition_pruning_effectiveness);
}

async fn show_maintenance(
    config_path: &PathBuf,
    catalog_name: &Option<String>,
    namespace: &str,
    table_name: &str,
) -> anyhow::Result<()> {
    let config = load_config(config_path).await?;
    let analyser = get_analyser(&config, catalog_name).await?;
    let namespace_vec = parse_namespace(namespace);
    
    match analyser.analyse_table(&namespace_vec, table_name).await {
        Ok(result) => {
            println!("Maintenance Analysis: {}.{}", namespace, table_name);
            println!("===================");
            println!();
            
            let info = &result.maintenance_info;
            println!("Status: {}", if info.needs_compaction { "⚠️  Maintenance Required" } else { "✅ Good" });
            println!("Small Files: {}", info.small_files_count);
            println!("Estimated Orphaned Files: {}", info.orphaned_files_estimated);
            
            if let Some(last_compaction) = &info.last_compaction {
                println!("Last Compaction: {}", last_compaction.format("%Y-%m-%d %H:%M:%S UTC"));
            } else {
                println!("Last Compaction: Unknown");
            }
            
            if !info.recommendations.is_empty() {
                println!();
                println!("Recommendations:");
                for (i, rec) in info.recommendations.iter().enumerate() {
                    println!("  {}. {}", i + 1, rec);
                }
            }
        }
        Err(e) => {
            error!("Failed to analyse table maintenance: {}", e);
            anyhow::bail!("Failed to analyse table maintenance: {}", e);
        }
    }
    
    Ok(())
}

async fn show_performance(
    config_path: &PathBuf,
    catalog_name: &Option<String>,
    namespace: &str,
    table_name: &str,
) -> anyhow::Result<()> {
    let config = load_config(config_path).await?;
    let analyser = get_analyser(&config, catalog_name).await?;
    let namespace_vec = parse_namespace(namespace);
    
    match analyser.analyse_table(&namespace_vec, table_name).await {
        Ok(result) => {
            println!("Performance Analysis: {}.{}", namespace, table_name);
            println!("===================");
            println!();
            
            let perf = &result.performance_info;
            println!("Average File Size: {:.2} MB", perf.avg_file_size_mb);
            println!("Scan Efficiency Score: {:.1}/1.0", perf.scan_efficiency_score);
            println!("Partition Pruning Effectiveness: {:.1}/1.0", perf.partition_pruning_effectiveness);
            
            if !perf.hot_partitions.is_empty() {
                println!();
                println!("Hot Partitions:");
                for partition in &perf.hot_partitions {
                    println!("  - {}", partition);
                }
            }
            
            if !perf.query_patterns.is_empty() {
                println!();
                println!("Query Patterns:");
                for (pattern, count) in &perf.query_patterns {
                    println!("  {}: {} times", pattern, count);
                }
            }
        }
        Err(e) => {
            error!("Failed to analyse table performance: {}", e);
            anyhow::bail!("Failed to analyse table performance: {}", e);
        }
    }

    Ok(())
}

async fn watch_snapshots(
    config_path: &PathBuf,
    catalog_name: &Option<String>,
    tables: Vec<String>,
    interval: u64,
    namespace_filter: Option<String>,
) -> anyhow::Result<()> {
    let config = load_config(config_path).await?;

    let catalog_config = if let Some(name) = catalog_name {
        config.get_catalog(name)
            .ok_or_else(|| anyhow::anyhow!("Catalog '{}' not found", name))?
    } else {
        config.get_default_catalog()
            .ok_or_else(|| anyhow::anyhow!("No default catalog configured"))?
    };

    let catalog_type = match catalog_config {
        rime_lib::CatalogConfig::Glue { .. } => CatalogType::Glue,
        rime_lib::CatalogConfig::Rest { .. } => CatalogType::Rest,
    };

    let provider = rime_lib::catalog::create_catalog_provider(&catalog_type);
    let catalog = provider.create_catalog(catalog_config).await?;

    let mut monitor = SnapshotMonitor::new(catalog, Duration::from_secs(interval));

    // Add tables to monitor
    if let Some(namespace) = namespace_filter {
        // Watch all tables in namespace
        let analyser = get_analyser(&config, catalog_name).await?;
        let namespace_vec = parse_namespace(&namespace);

        info!("Discovering tables in namespace: {}", namespace);
        let table_list = analyser.list_tables(&namespace_vec).await?;

        for table_name in table_list {
            info!("Adding table to monitor: {}.{}", namespace, table_name);
            monitor.add_table(namespace_vec.clone(), table_name);
        }
    } else {
        // Watch specific tables
        for table_spec in tables {
            if let Some((namespace, table_name)) = parse_table_spec(&table_spec) {
                info!("Adding table to monitor: {}.{}", namespace.join("."), table_name);
                monitor.add_table(namespace, table_name);
            } else {
                error!("Invalid table specification: {}. Use format 'namespace.table'", table_spec);
                anyhow::bail!("Invalid table specification: {}", table_spec);
            }
        }
    }

    info!("Starting snapshot monitoring...");

    // Start monitoring (this will run indefinitely)
    monitor.watch().await?;

    Ok(())
}

fn parse_table_spec(table_spec: &str) -> Option<(Vec<String>, String)> {
    let parts: Vec<&str> = table_spec.split('.').collect();
    if parts.len() < 2 {
        return None;
    }

    let table_name = parts.last()?.to_string();
    let namespace = parts[..parts.len()-1].iter().map(|s| s.to_string()).collect();

    Some((namespace, table_name))
}