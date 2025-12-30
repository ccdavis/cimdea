//! Module for extracting version information from IPUMS data files.
//!
//! This module provides functionality to extract version metadata from both
//! Parquet and fixed-width IPUMS data files. Version information includes
//! any metadata stored in the file that isn't variable or sample data.

use crate::layout::DatasetLayout;
use crate::mderror::{metadata_error, MdError};
use flate2::read::GzDecoder;
use parquet::file::reader::{FileReader, SerializedFileReader};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;

/// System variable names used for version information in IPUMS data files.
/// These variables have record type '#' in layout files.
const SYSTEM_RECORD_TYPE: &str = "#";

/// Keys in parquet metadata that should be excluded from version info
/// (they contain data definitions, not version information)
const EXCLUDED_METADATA_KEYS: &[&str] = &["variables", "samples", "datasets"];

/// Version information extracted from an IPUMS data file.
///
/// Uses a dynamic map to store all version-related metadata,
/// allowing for new fields to be added without code changes.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DataVersion {
    /// The source file path
    pub source_path: String,
    /// The data format (parquet or fixed-width)
    pub format: DataFormat,
    /// Number of variables in the file (if available)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub variable_count: Option<usize>,
    /// Dynamic version metadata - all key-value pairs from the file
    #[serde(flatten)]
    pub metadata: BTreeMap<String, String>,
}

/// The format of the data file.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum DataFormat {
    #[default]
    Parquet,
    FixedWidth,
}

impl std::fmt::Display for DataFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DataFormat::Parquet => write!(f, "parquet"),
            DataFormat::FixedWidth => write!(f, "fixed-width"),
        }
    }
}

impl DataVersion {
    /// Create a new empty DataVersion for a given path and format.
    pub fn new(source_path: &str, format: DataFormat) -> Self {
        Self {
            source_path: source_path.to_string(),
            format,
            variable_count: None,
            metadata: BTreeMap::new(),
        }
    }

    /// Check if any version information was found.
    pub fn has_version_info(&self) -> bool {
        !self.metadata.is_empty() || self.variable_count.is_some()
    }

    /// Output as JSON string.
    pub fn to_json(&self) -> Result<String, MdError> {
        serde_json::to_string_pretty(self)
            .map_err(|e| metadata_error!("Failed to serialize version info to JSON: {}", e))
    }

    /// Output as human-readable text.
    pub fn to_text(&self) -> String {
        let mut lines = Vec::new();
        lines.push(format!("Source: {}", self.source_path));
        lines.push(format!("Format: {}", self.format));

        if let Some(count) = self.variable_count {
            lines.push(format!("Variables: {}", count));
        }

        // Output all metadata in sorted order (BTreeMap is already sorted)
        for (key, value) in &self.metadata {
            lines.push(format!("{}: {}", key, value));
        }

        if !self.has_version_info() {
            lines.push("No version information found".to_string());
        }

        lines.join("\n")
    }
}

/// Extract version information from a data file path.
///
/// Automatically detects whether the path points to a parquet file/directory
/// or a fixed-width data file based on the path structure.
///
/// # Arguments
/// * `path` - Path to the data file or directory
///
/// # Returns
/// A `DataVersion` struct with extracted version information, or an error.
///
/// # Examples
/// ```
/// use cimdea::data_version::extract_version;
///
/// // For parquet (directory containing .parquet files)
/// // let version = extract_version("/path/to/parquet/us2015b").unwrap();
///
/// // For fixed-width (.dat.gz file)
/// // let version = extract_version("/path/to/us2015b_usa.dat.gz").unwrap();
/// ```
pub fn extract_version(path: &str) -> Result<DataVersion, MdError> {
    let path_obj = Path::new(path);

    // Determine file type based on path
    if is_fixed_width_path(path_obj) {
        extract_version_from_fixed_width(path)
    } else if is_parquet_path(path_obj) {
        extract_version_from_parquet(path)
    } else {
        Err(metadata_error!(
            "Cannot determine data format for path '{}'. \
             Expected a .parquet file, a directory containing .parquet files, \
             or a .dat.gz fixed-width file.",
            path
        ))
    }
}

/// Check if a path appears to be a parquet file or directory.
fn is_parquet_path(path: &Path) -> bool {
    // Check if it's a .parquet file
    if let Some(ext) = path.extension() {
        if ext == "parquet" {
            return true;
        }
    }

    if path.is_file() {
        return false;
    }

    // Check if parent directory is named "parquet" - by convention this means
    // the child directory contains parquet files (e.g., /path/to/parquet/us1900j)
    if let Some(parent) = path.parent() {
        if let Some(parent_name) = parent.file_name() {
            if parent_name == "parquet" {
                return true;
            }
        }
    }

    // Check if it's a directory containing .parquet files
    if path.is_dir() {
        if let Ok(entries) = std::fs::read_dir(path) {
            for entry in entries.flatten() {
                if let Some(ext) = entry.path().extension() {
                    if ext == "parquet" {
                        return true;
                    }
                }
            }
        }
    }

    false
}

/// Check if a path appears to be a fixed-width data file.
fn is_fixed_width_path(path: &Path) -> bool {
    let path_str = path.to_string_lossy();
    // Fixed-width files typically end in .dat.gz or .dat
    path_str.ends_with(".dat.gz") || path_str.ends_with(".dat")
}

/// Extract version information from a parquet file or directory.
///
/// Reads all key-value metadata from the parquet file. For "variables",
/// stores the count. Ignores "samples" and "datasets". Everything else
/// is treated as version information.
pub fn extract_version_from_parquet(path: &str) -> Result<DataVersion, MdError> {
    let path_obj = Path::new(path);
    let mut version = DataVersion::new(path, DataFormat::Parquet);

    // Find a parquet file to read metadata from
    let parquet_file = find_parquet_file(path_obj)?;

    // Open and read the parquet file metadata
    let file = File::open(&parquet_file).map_err(|e| {
        metadata_error!(
            "Failed to open parquet file at {}: {}",
            parquet_file.display(),
            e
        )
    })?;

    let reader = SerializedFileReader::new(file).map_err(|e| {
        metadata_error!(
            "Failed to create parquet reader for {}: {}",
            parquet_file.display(),
            e
        )
    })?;

    // Extract all key-value metadata
    if let Some(kv_metadata) = reader.metadata().file_metadata().key_value_metadata() {
        for kv in kv_metadata {
            let key = kv.key.as_str();

            // Handle special cases
            if key == "variables" {
                // Count the number of variables
                if let Some(ref value) = kv.value {
                    version.variable_count = count_json_entries(value);
                }
            } else if EXCLUDED_METADATA_KEYS.contains(&key) {
                // Skip samples/datasets
                continue;
            } else if let Some(ref value) = kv.value {
                // Everything else is version info
                if !value.is_empty() {
                    version.metadata.insert(key.to_string(), value.clone());
                }
            }
        }
    }

    Ok(version)
}

/// Count the number of entries in a JSON string (array length or object key count).
fn count_json_entries(json_str: &str) -> Option<usize> {
    // Try to parse as a JSON object (HashMap)
    if let Ok(map) = serde_json::from_str::<serde_json::Map<String, serde_json::Value>>(json_str) {
        return Some(map.len());
    }

    // Try to parse as a JSON array
    if let Ok(arr) = serde_json::from_str::<Vec<serde_json::Value>>(json_str) {
        return Some(arr.len());
    }

    None
}

/// Find a parquet file in a path (which may be a file or directory).
/// Handles both single parquet files and partitioned parquet datasets
/// (where .parquet is a directory containing the actual parquet files).
fn find_parquet_file(path: &Path) -> Result<std::path::PathBuf, MdError> {
    if path.is_file() {
        return Ok(path.to_path_buf());
    }

    if path.is_dir() {
        // Look for .parquet entries (files or directories)
        let mut parquet_entries: Vec<_> = std::fs::read_dir(path)
            .map_err(|e| metadata_error!("Cannot read directory '{}': {}", path.display(), e))?
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().is_some_and(|ext| ext == "parquet"))
            .collect();

        if parquet_entries.is_empty() {
            return Err(metadata_error!(
                "No parquet files found in directory '{}'",
                path.display()
            ));
        }

        // Sort to get consistent results, prefer H record type
        parquet_entries.sort_by(|a, b| {
            let a_is_h = a.path().to_string_lossy().contains(".H.");
            let b_is_h = b.path().to_string_lossy().contains(".H.");
            b_is_h.cmp(&a_is_h).then_with(|| a.path().cmp(&b.path()))
        });

        let selected = parquet_entries[0].path();

        // If the selected entry is a file, return it directly
        if selected.is_file() {
            return Ok(selected);
        }

        // If it's a directory (partitioned parquet), find an actual file inside
        if selected.is_dir() {
            return find_parquet_file_in_partition(&selected);
        }
    }

    Err(metadata_error!(
        "Path '{}' is neither a file nor a directory",
        path.display()
    ))
}

/// Find an actual parquet file inside a partitioned parquet directory.
fn find_parquet_file_in_partition(partition_dir: &Path) -> Result<std::path::PathBuf, MdError> {
    let mut parquet_files: Vec<_> = std::fs::read_dir(partition_dir)
        .map_err(|e| {
            metadata_error!(
                "Cannot read partition directory '{}': {}",
                partition_dir.display(),
                e
            )
        })?
        .filter_map(|e| e.ok())
        .filter(|e| {
            let p = e.path();
            p.is_file() && p.extension().is_some_and(|ext| ext == "parquet")
        })
        .collect();

    if parquet_files.is_empty() {
        return Err(metadata_error!(
            "No parquet files found in partition directory '{}'",
            partition_dir.display()
        ));
    }

    // Sort for consistent results
    parquet_files.sort_by_key(|a| a.path());

    Ok(parquet_files[0].path())
}

/// Extract version information from a fixed-width data file.
///
/// This reads the layout file to find ALL system variables (record type '#'),
/// then reads the first line of the compressed data file to extract their values.
pub fn extract_version_from_fixed_width(data_path: &str) -> Result<DataVersion, MdError> {
    let mut version = DataVersion::new(data_path, DataFormat::FixedWidth);

    // Find the layout file for this data file
    let layout_path = crate::fixed_width::layout_file_for(data_path)?;
    let layout_path = Path::new(&layout_path);

    // Load the layout using the existing DatasetLayout parser
    // (it already includes system variables with record type '#')
    let layout = DatasetLayout::try_from_layout_file(layout_path)?;

    // Get system variables (record type '#')
    let system_record_layout = match layout.for_rectype(SYSTEM_RECORD_TYPE) {
        Some(rl) => rl,
        None => {
            // No system variables in layout, return empty version
            return Ok(version);
        }
    };

    let system_vars = system_record_layout.vars();

    if system_vars.is_empty() {
        return Ok(version);
    }

    // Read the first line of data
    let first_line = read_first_line(data_path)?;

    // Extract ALL system variable values from the first line
    for var in system_vars {
        // Layout start positions are 1-based
        let start = var.start.saturating_sub(1);
        let end = start + var.width;

        if end <= first_line.len() {
            let value_bytes = &first_line[start..end];
            // Convert to string and trim whitespace
            let value = String::from_utf8_lossy(value_bytes).trim().to_string();

            if !value.is_empty() {
                version.metadata.insert(var.name.clone(), value);
            }
        }
    }

    Ok(version)
}

/// Read the first line of a data file (handles .gz compression).
fn read_first_line(data_path: &str) -> Result<Vec<u8>, MdError> {
    let path = Path::new(data_path);
    let file = File::open(path)
        .map_err(|e| metadata_error!("Cannot open data file '{}': {}", data_path, e))?;

    let first_line: Vec<u8> = if data_path.ends_with(".gz") {
        let decoder = GzDecoder::new(file);
        let mut reader = BufReader::new(decoder);
        let mut line = Vec::new();
        reader
            .read_until(b'\n', &mut line)
            .map_err(|e| metadata_error!("Error reading gzipped file '{}': {}", data_path, e))?;
        // Remove trailing newline if present
        if line.last() == Some(&b'\n') {
            line.pop();
        }
        if line.last() == Some(&b'\r') {
            line.pop();
        }
        line
    } else {
        let mut reader = BufReader::new(file);
        let mut line = Vec::new();
        reader
            .read_until(b'\n', &mut line)
            .map_err(|e| metadata_error!("Error reading file '{}': {}", data_path, e))?;
        if line.last() == Some(&b'\n') {
            line.pop();
        }
        if line.last() == Some(&b'\r') {
            line.pop();
        }
        line
    };

    Ok(first_line)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_data_version_to_json() {
        let mut version = DataVersion::new("/test/path", DataFormat::Parquet);
        version
            .metadata
            .insert("release_number".to_string(), "1.0.0".to_string());
        version
            .metadata
            .insert("commit_hash".to_string(), "abc123".to_string());

        let json = version.to_json().expect("Should serialize to JSON");
        assert!(json.contains("release_number"));
        assert!(json.contains("1.0.0"));
        assert!(json.contains("commit_hash"));
        assert!(json.contains("abc123"));
    }

    #[test]
    fn test_data_version_to_text() {
        let mut version = DataVersion::new("/test/path", DataFormat::FixedWidth);
        version
            .metadata
            .insert("CORE_VERS_RELEASE_NUMBER".to_string(), "2.0.0".to_string());
        version
            .metadata
            .insert("CORE_VERS_BRANCH_NAME".to_string(), "main".to_string());

        let text = version.to_text();
        assert!(text.contains("Source: /test/path"));
        assert!(text.contains("Format: fixed-width"));
        assert!(text.contains("CORE_VERS_RELEASE_NUMBER: 2.0.0"));
        assert!(text.contains("CORE_VERS_BRANCH_NAME: main"));
    }

    #[test]
    fn test_is_parquet_path() {
        assert!(is_parquet_path(Path::new("test.parquet")));
        assert!(is_parquet_path(Path::new(
            "tests/data_root/parquet/us2015b"
        )));
        assert!(!is_parquet_path(Path::new("test.dat.gz")));

        // Test convention: parent directory named "parquet" implies parquet dataset
        assert!(is_parquet_path(Path::new(
            "/home/user/data/parquet/us1900j"
        )));
        assert!(is_parquet_path(Path::new(
            "/pkg/ipums/usa/output_data/parquet/us2015b"
        )));
    }

    #[test]
    fn test_is_fixed_width_path() {
        assert!(is_fixed_width_path(Path::new("us2015b_usa.dat.gz")));
        assert!(is_fixed_width_path(Path::new("/path/to/us2015b_usa.dat")));
        assert!(!is_fixed_width_path(Path::new("test.parquet")));
    }

    #[test]
    fn test_extract_version_from_parquet() {
        let result = extract_version_from_parquet("tests/data_root/parquet/us2015b");
        // Should succeed even if test files don't have version metadata
        assert!(result.is_ok());
        let version = result.unwrap();
        assert_eq!(version.format, DataFormat::Parquet);
        assert!(version.source_path.contains("us2015b"));
    }

    #[test]
    fn test_extract_version_from_fixed_width() {
        let data_path = "tests/data_root/us2015b_usa.dat.gz";
        let version =
            extract_version_from_fixed_width(data_path).expect("Should load fixed-width version");

        assert_eq!(version.format, DataFormat::FixedWidth);
        assert!(
            version.metadata.contains_key("CORE_VERS_RELEASE_NUMBER"),
            "Should extract CORE_VERS_RELEASE_NUMBER"
        );

        let layout_path =
            crate::fixed_width::layout_file_for(data_path).expect("Should locate layout file");
        let layout = DatasetLayout::try_from_layout_file(Path::new(&layout_path))
            .expect("Should load layout");
        let system_layout = layout
            .for_rectype(SYSTEM_RECORD_TYPE)
            .expect("Should have system record layout");
        let line = read_first_line(data_path).expect("Should read first line");

        let release_var = system_layout
            .vars()
            .iter()
            .find(|var| var.name == "CORE_VERS_RELEASE_NUMBER")
            .expect("Should find CORE_VERS_RELEASE_NUMBER");
        let start = release_var.start.saturating_sub(1);
        let end = start + release_var.width;
        let expected = String::from_utf8_lossy(&line[start..end])
            .trim()
            .to_string();

        assert_eq!(
            version
                .metadata
                .get("CORE_VERS_RELEASE_NUMBER")
                .expect("Should extract release number"),
            &expected
        );
    }

    #[test]
    fn test_data_version_has_version_info() {
        let empty = DataVersion::new("/test", DataFormat::Parquet);
        assert!(!empty.has_version_info());

        let mut with_metadata = DataVersion::new("/test", DataFormat::Parquet);
        with_metadata
            .metadata
            .insert("version".to_string(), "1.0".to_string());
        assert!(with_metadata.has_version_info());

        let mut with_count = DataVersion::new("/test", DataFormat::Parquet);
        with_count.variable_count = Some(100);
        assert!(with_count.has_version_info());
    }

    #[test]
    fn test_count_json_entries() {
        // Test object counting
        let obj_json = r#"{"a": 1, "b": 2, "c": 3}"#;
        assert_eq!(count_json_entries(obj_json), Some(3));

        // Test array counting
        let arr_json = r#"[1, 2, 3, 4, 5]"#;
        assert_eq!(count_json_entries(arr_json), Some(5));

        // Test invalid JSON
        assert_eq!(count_json_entries("not json"), None);
    }
}
