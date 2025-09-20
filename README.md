# Rime - Iceberg Table Analysis Tool

Rime is a Rust-based library and CLI tool for analyzing Apache Iceberg tables, providing maintenance recommendations and performance insights.

## Features

- **Multi-catalog support**: AWS Glue and REST catalogs
- **Table analysis**: Comprehensive metrics, maintenance needs, and performance insights
- **Snapshot monitoring**: Real-time monitoring of Iceberg snapshot changes
- **TOML configuration**: Easy catalog configuration and switching
- **CLI interface**: User-friendly command-line interface with multiple output formats

## Installation

### From Source
```bash
cargo install --path rime-cli
```

### Pre-built Binaries
Download the latest binary for your platform from the [GitHub Releases](https://github.com/atcol/rime/releases) page:

- **Linux (x86_64)**: `rime-linux-x86_64`
- **macOS (Intel)**: `rime-macos-x86_64`
- **macOS (Apple Silicon)**: `rime-macos-aarch64`
- **Windows**: `rime-windows-x86_64.exe`

### Development Builds
Development builds are available as artifacts from the [GitHub Actions](https://github.com/atcol/rime/actions) page for the latest commits.

## Quick Start

1. Initialize configuration:
```bash
rime init
```

2. List available catalogs:
```bash
rime catalogs
```

3. List namespaces:
```bash
rime namespaces
```

4. List tables in a namespace:
```bash
rime tables my_database.my_schema
```

5. Analyse a table:
```bash
rime analyse my_database.my_schema my_table
```

6. Get maintenance recommendations:
```bash
rime maintenance my_database.my_schema my_table
```

7. View performance analysis:
```bash
rime performance my_database.my_schema my_table
```

8. Monitor snapshot changes in real-time:
```bash
# Watch specific tables
rime watch my_database.my_schema.my_table

# Watch all tables in a namespace
rime watch --namespace my_database.my_schema

# Watch with custom polling interval
rime watch my_database.my_schema.my_table --interval 60
```

## Configuration

Configuration is stored in `~/.config/rime.toml` (or `./rime.toml`):

```toml
default_catalog = "my_glue_catalog"

[catalogs.my_glue_catalog]
type = "glue"
region = "us-west-2"
warehouse = "s3://my-bucket/warehouse/"
profile = "default"  # optional

[catalogs.my_rest_catalog]
type = "rest"
uri = "http://localhost:8181"
warehouse = "s3://my-bucket/warehouse/"
credential = "username:password"  # optional
token = "bearer_token"  # optional
```

## CLI Usage

### Global Options
- `-c, --config <PATH>`: Specify configuration file path
- `-C, --catalog <NAME>`: Override catalog to use
- `-v`: Increase verbosity (can be used multiple times)

### Commands

#### Initialize Configuration
```bash
rime init [--force]
```

#### List Catalogs
```bash
rime catalogs
```

#### List Namespaces
```bash
rime namespaces
```

#### List Tables
```bash
rime tables <namespace>
```

#### Analyse Table
```bash
rime analyse <namespace> <table> [--format json|yaml|table]
```

#### Maintenance Analysis
```bash
rime maintenance <namespace> <table>
```

#### Performance Analysis
```bash
rime performance <namespace> <table>
```

#### Watch Snapshot Changes
```bash
# Watch specific tables (streaming JSON to stdout)
rime watch <table1> [table2...] [--interval <seconds>]

# Watch all tables in a namespace
rime watch --namespace <namespace> [--interval <seconds>]
```

**Examples:**
```bash
# Watch a single table with default 30-second interval
rime watch my_db.my_table

# Watch multiple tables with 60-second interval
rime watch my_db.table1 my_db.table2 --interval 60

# Watch all tables in a namespace
rime watch --namespace my_db

# Stream events to file or pipe to other tools
rime watch my_db.my_table > snapshot_events.jsonl
rime watch my_db.my_table | jq 'select(.event_type == "Added")'
```

**Output Format:** Each snapshot change event is output as JSON:
```json
{
  "table_namespace": ["my_db"],
  "table_name": "my_table",
  "snapshot_id": 5897493584092098644,
  "timestamp_ms": 1674829871000,
  "timestamp_utc": "2023-01-27T13:04:31Z",
  "summary": {
    "added-data-files": "1",
    "added-records": "1000",
    "total-records": "5000"
  },
  "event_type": "Added",
  "detected_at": "2023-01-27T13:05:01.234Z"
}
```

## Library Usage

```rust
use rime_lib::{Config, TableAnalyser, catalog::create_catalog_provider, CatalogType};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Load configuration
    let config = Config::from_toml_str(&std::fs::read_to_string("rime.toml")?)?;
    let catalog_config = config.get_default_catalog().unwrap();
    
    // Create catalog
    let catalog_type = match catalog_config {
        rime_lib::CatalogConfig::Glue { .. } => CatalogType::Glue,
        rime_lib::CatalogConfig::Rest { .. } => CatalogType::Rest,
    };
    let provider = create_catalog_provider(&catalog_type);
    let catalog = provider.create_catalog(catalog_config).await?;
    
    // Analyse table
    let analyser = TableAnalyser::new(catalog);
    let result = analyser.analyse_table(&["my_database".to_string()], "my_table").await?;
    
    println!("Analysis: {:#?}", result);
    Ok(())
}
```

## Analysis Features

### Table Metrics
- Total files and size
- Record count
- Snapshot count
- Partition count

### Maintenance Analysis
- Small files detection
- Compaction recommendations
- Orphaned files estimation
- Snapshot cleanup suggestions

### Performance Analysis
- Average file size
- Scan efficiency scoring
- Partition pruning effectiveness
- Query pattern analysis (when available)

### Snapshot Monitoring
- Real-time change detection
- JSON event streaming
- Configurable polling intervals
- Support for multiple tables and namespaces
- Event types: snapshot creation and expiration

## Development

```bash
# Build the workspace
cargo build

# Run tests
cargo test

# Run the CLI
cargo run --bin rime -- --help

# Build release
cargo build --release
```

## Architecture

- **rime-lib**: Core library with Iceberg integration and analysis logic
- **rime-cli**: Command-line interface built with Clap and Tokio

The library provides a pluggable catalog architecture supporting both AWS Glue and REST catalogs, with extensible analysis modules for different types of table insights.
