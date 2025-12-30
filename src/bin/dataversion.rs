//! A command-line utility to extract version information from IPUMS data files.
//!
//! This tool reads version metadata from both Parquet and fixed-width IPUMS data files
//! and outputs it in either JSON or human-readable text format.
//!
//! # Usage
//!
//! ```bash
//! # For parquet data (directory containing .parquet files)
//! dataversion /pkg/ipums/usa/output_data/current/parquet/us2015b
//!
//! # For fixed-width data (.dat.gz file)
//! dataversion /pkg/ipums/usa/output_data/current/us2015b_usa.dat.gz
//!
//! # Output as JSON (default is text)
//! dataversion --format json /path/to/data
//! ```

use cimdea::data_version::{extract_version, DataVersion};
use clap::{Parser, ValueEnum};
use std::process;

#[derive(Parser, Debug)]
#[command(
    name = "dataversion",
    version,
    about = "Extract version information from IPUMS data files",
    long_about = "Extract version information from IPUMS data files.\n\n\
                  Supports both Parquet and fixed-width (.dat.gz) formats.\n\
                  Version information includes release numbers, commit hashes,\n\
                  branch names, and other build metadata."
)]
struct Args {
    /// Path to the data file or directory.
    ///
    /// For Parquet: path to a directory containing .parquet files
    /// (e.g., /pkg/ipums/usa/output_data/current/parquet/us2015b)
    ///
    /// For fixed-width: path to a .dat.gz file
    /// (e.g., /pkg/ipums/usa/output_data/current/us2015b_usa.dat.gz)
    #[arg(value_name = "PATH")]
    path: String,

    /// Output format
    #[arg(short, long, value_enum, default_value = "text")]
    format: OutputFormat,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum OutputFormat {
    /// Human-readable text output
    Text,
    /// Machine-readable JSON output
    Json,
}

fn main() {
    let args = Args::parse();

    match extract_version(&args.path) {
        Ok(version) => {
            output_version(&version, args.format);
        }
        Err(e) => {
            eprintln!("Error: {}", e);
            process::exit(1);
        }
    }
}

fn output_version(version: &DataVersion, format: OutputFormat) {
    match format {
        OutputFormat::Text => {
            println!("{}", version.to_text());
        }
        OutputFormat::Json => match version.to_json() {
            Ok(json) => println!("{}", json),
            Err(e) => {
                eprintln!("Error serializing to JSON: {}", e);
                process::exit(1);
            }
        },
    }
}
