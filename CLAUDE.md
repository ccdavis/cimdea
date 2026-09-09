# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Cimdea is a Rust library for processing IPUMS-like microdata (demographic survey/census data) with aggregation, extraction, and tabulation capabilities. The project provides both a library (`cimdea`) and command-line tools:
- `abacus` - Creates cross-tabulations from IPUMS datasets
- `dataversion` - Extracts version metadata from IPUMS data files (parquet or fixed-width)
- `check-server-status` - Checks IPUMS data deployment status across internal, demo, and live servers via SSH

## Development Commands

### Building and Testing
```bash
# Build the project
cargo build

# Run all tests
cargo test

# Run tests with output
cargo test -- --nocapture

# Run specific test
cargo test test_name

# Build documentation
cargo doc --no-deps

# Run benchmarks
cargo bench
```

### Running the CLI Tool
```bash
# Build and run the abacus CLI
cargo run --bin abacus -- --help

# Simple tabulation example
cargo run --bin abacus -- tab usa us2015b MARST

# Complex request from JSON file
cargo run --bin abacus -- request path/to/request.json

# With output formatting
cargo run --bin abacus -- -f json tab usa us2015b MARST -o output.json
```

### Running the dataversion CLI
```bash
# Extract version from parquet dataset (directory or single file)
cargo run --bin dataversion -- /path/to/parquet/us2015b

# Extract version from partitioned parquet dataset
cargo run --bin dataversion -- /path/to/parquet/us1900j

# Extract version from fixed-width data file
cargo run --bin dataversion -- /path/to/us2015b_usa.dat.gz

# Output as JSON
cargo run --bin dataversion -- --format json /path/to/data
```

### Running the check-server-status CLI
```bash
# Check internal server (requires SSH access)
cargo run --bin check-server-status -- --internal

# Check demo server
cargo run --bin check-server-status -- --demo

# Check live servers (connects to each product's production server)
cargo run --bin check-server-status -- --live

# Check all environments
cargo run --bin check-server-status -- --internal --demo --live

# Check specific products only
cargo run --bin check-server-status -- --internal -p usa,cps,ipumsi

# Plain text output (no colors) to file
cargo run --bin check-server-status -- --internal --plain -o status.txt

# With custom server config
cargo run --bin check-server-status -- --internal --config my-servers.toml
```

## Architecture Overview

### Core Data Flow
1. **Request Creation**: User defines tabulation via `SimpleRequest` or `AbacusRequest`
2. **Context Loading**: `Context` loads metadata and establishes data connections
3. **Query Generation**: `query_gen` module converts requests to SQL queries
4. **Data Processing**: DuckDB executes queries on Parquet/fixed-width data
5. **Output Formatting**: Results formatted as JSON, CSV, HTML, or text tables

### Key Architectural Components

**Data Access Layer**:
- `conventions::Context` - Entry point for metadata and data access
- `ipums_data_model` - Models record types, hierarchies, and weights
- `fixed_width` - Parser for legacy IPUMS fixed-width format
- `layout` - Handles IPUMS layout file parsing

**Request Processing**:
- `request` - Defines `DataRequest` trait and implementations (`SimpleRequest`, `AbacusRequest`)
- `input_schema_tabulation` - Handles complex request features (category bins, subpopulations)
- `query_gen` - Converts requests to platform-specific SQL queries

**Data Processing**:
- `tabulate` - Main execution engine using DuckDB
- `extract` - Data extraction functionality (complementary to tabulation)

**Server Status Checking**:
- `deployment` - Models server environments (Internal, Demo, Live), product configurations, and deployment targets with path generation
- `remote` - SSH connection pool with ControlMaster multiplexing for efficient remote command execution
- `server_status` - Status checking logic, dataset comparison, and timestamp grouping for deployment verification

**IPUMS Conventions**:
- Hierarchical record types (Household → Person relationships)
- Standardized variable names across time periods
- Weight variables for statistical accuracy
- Directory structure: `data_root/parquet/` and `data_root/layouts/`
- File naming: `{dataset}_{collection}.{record_type}.{format}`
- Parquet datasets can be single `.parquet` files or partitioned directories (where `*.parquet` is a directory containing multiple parquet files)
- System variables (record type '#' in layouts) store version metadata in fixed-width files

### Data Dependencies
- **Test Data**: Located in `tests/data_root/` with sample layouts and Parquet files
- **Layouts**: `*.layout.txt` files describe variable schemas and data types
- **Parquet Files**: Actual microdata stored in `parquet/{dataset}/` directories
- **Request Examples**: JSON request samples in `tests/requests/`

### External Dependencies
- **DuckDB**: Primary query engine with bundled SQLite-compatible embedded database
- **Parquet**: Apache Parquet format for columnar data storage
- **Clap**: Command-line argument parsing for the `abacus` binary

## Module Documentation

### Core Modules
- **`conventions`** - Core metadata and context management. The `Context` struct is the central entry point for loading metadata and establishing data connections
- **`request`** - Defines the `DataRequest` trait with two main implementations:
  - `SimpleRequest` - Basic tabulations from product, dataset, and variable names
  - `AbacusRequest` - Complex requests supporting category bins, subpopulations, and general/detailed variable selections
- **`query_gen`** - SQL query generation with support for conditions (`Condition`) and comparison operations (`CompareOperation`)
- **`tabulate`** - Execution engine that returns `Tabulation` and `Table` structs with formatting options (JSON, CSV, HTML, text)
- **`ipums_data_model`** - Core data structures for IPUMS record types, hierarchies, and weight definitions
- **`ipums_metadata_model`** - Models for IPUMS variables, datasets, and data types (IpumsVariable, IpumsDataset, IpumsDataType)
- **`input_schema_tabulation`** - Complex request parsing for category bins, general/detailed selections, and case selections
- **`layout`** - Parses IPUMS layout files defining variable schemas, data types, and column widths
- **`fixed_width`** - Parser for legacy IPUMS fixed-width format with conversion to modern formats
- **`mderror`** - Custom error types (`MdError`) for metadata, parsing, and execution failures
- **`data_version`** - Extracts version metadata from data files. Supports both parquet (reads key-value metadata) and fixed-width (reads system variables with record type '#' from first line)
- **`parquet_metadata`** - Reads and parses IPUMS metadata embedded in parquet files (variables, samples, version info)

### Server Status Modules
- **`deployment`** - Server deployment configuration for IPUMS data products:
  - `Environment` enum - Internal, Demo, Live server environments
  - `DataFormat` enum - FixedWidth, Parquet, Derived data formats
  - `ProductDeployment` - Configuration for each of 12 IPUMS products (domain, live server, formats, third-party flag)
  - `DeploymentTarget` - Combines environment + product with path generation methods
  - `DeploymentRegistry` - Product registry with hardcoded defaults and optional TOML/JSON config override
- **`remote`** - SSH remote execution with ControlMaster connection pooling:
  - `SshConnectionPool` - Manages SSH connections with socket multiplexing for connection reuse
  - `RemoteError` - Error types for connection failures, skipped connections, command failures
  - Methods for remote operations: `exec()`, `path_exists()`, `list_files()`, `get_timestamps()`, `list_content_dirs()`
- **`server_status`** - Status checking logic for deployment verification:
  - `FormatStatus` - Status of a data format (Present, Missing, NotConfigured, Unknown)
  - `ProductStatus` - Complete status for a product including parquet, fixed-width, derived, and comparison
  - `DatasetComparison` - Comparison result between FW and parquet datasets (Match, Mismatch, Skipped)
  - `ServerStatusChecker` - Main checker using SSH connection pool
  - `format_timestamp_groups()` - Groups timestamps into 12-hour windows for display

### Key Types and Traits
- **`DataRequest` trait** - Core interface for all tabulation requests
- **`Context` struct** - Manages metadata loading and data source connections
- **`MicroDataCollection`** - Stores collection metadata including record hierarchies and weights
- **`TableFormat` enum** - Output format options (Json, Csv, Html, Text)
- **`OutputColumn` enum** - Column types in tabulation results
- **`DataVersion` struct** - Holds extracted version metadata with dynamic key-value storage
- **`DeploymentRegistry` struct** - Registry of product deployment configurations with config override support
- **`SshConnectionPool` struct** - SSH connection manager with ControlMaster multiplexing
- **`ServerStatusChecker` struct** - Checks deployment status using SSH connection pool

## Testing Strategy

Tests are integration-focused, using real IPUMS data samples in `tests/data_root/`. The test suite covers:
- CLI functionality (`test_abacus_cli.rs`)
- Core tabulation logic (`test_tabulate.rs`)
- Serialization (`test_serialize_tabulation.rs`)

Run tests from project root since they rely on relative paths to test data.

## Error Handling

The codebase uses a custom error type `MdError` (metadata error) defined in `mderror.rs`. Errors are categorized and include context about metadata loading, parsing, and query execution failures.