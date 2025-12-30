//! Server status checking logic for IPUMS data deployments.
//!
//! This module provides types and functions for checking the status of data
//! deployed on IPUMS servers, comparing datasets across formats, and grouping
//! file timestamps for display.

use crate::deployment::{DataFormat, DeploymentTarget};
use crate::remote::SshConnectionPool;
use std::collections::HashSet;
use std::path::Path;

/// Information about a single dataset
#[derive(Debug, Clone)]
pub struct DatasetInfo {
    pub name: String,
    pub timestamp: Option<i64>,
}

/// Status of a specific data format on a server
#[derive(Debug, Clone)]
pub enum FormatStatus {
    /// Format found with datasets
    Present {
        datasets: Vec<DatasetInfo>,
        date_summary: String,
    },
    /// Format expected but not found
    Missing,
    /// Format not configured for this product
    NotConfigured,
    /// Could not check (connection error, etc.)
    Unknown(String),
}

impl FormatStatus {
    /// Get the count of datasets if present
    pub fn dataset_count(&self) -> Option<usize> {
        match self {
            FormatStatus::Present { datasets, .. } => Some(datasets.len()),
            _ => None,
        }
    }

    /// Get the list of dataset names if present
    pub fn dataset_names(&self) -> Vec<String> {
        match self {
            FormatStatus::Present { datasets, .. } => datasets.iter().map(|d| d.name.clone()).collect(),
            _ => Vec::new(),
        }
    }

    /// Check if status indicates data is present
    pub fn is_present(&self) -> bool {
        matches!(self, FormatStatus::Present { .. })
    }

    /// Check if status indicates data is missing
    pub fn is_missing(&self) -> bool {
        matches!(self, FormatStatus::Missing)
    }

    /// Check if status indicates an error
    pub fn is_error(&self) -> bool {
        matches!(self, FormatStatus::Unknown(_))
    }
}

/// Comparison result between two format's datasets
#[derive(Debug, Clone)]
pub enum DatasetComparison {
    /// Datasets match exactly
    Match,
    /// Cannot compare (one or both missing)
    Skipped,
    /// Datasets differ
    Mismatch {
        fw_only: Vec<String>,
        parquet_only: Vec<String>,
    },
}

impl DatasetComparison {
    /// Check if the comparison found a match
    pub fn is_match(&self) -> bool {
        matches!(self, DatasetComparison::Match)
    }

    /// Check if the comparison found a mismatch
    pub fn is_mismatch(&self) -> bool {
        matches!(self, DatasetComparison::Mismatch { .. })
    }
}

/// Complete status for a product on a server
#[derive(Debug, Clone)]
pub struct ProductStatus {
    pub product_name: String,
    pub base_path: String,
    pub path_exists: bool,
    pub parquet: FormatStatus,
    pub fixed_width: FormatStatus,
    pub derived: FormatStatus,
    pub comparison: Option<DatasetComparison>,
}

/// A group of timestamps within a time window
#[derive(Debug, Clone)]
pub struct TimestampGroup {
    pub start_time: i64,
    pub count: usize,
}

/// Group timestamps into 12-hour windows and format for display
///
/// Returns a formatted string like:
/// - `[Dec 15]` for a single group
/// - `[Dec 15: 50, Nov 1: 2]` for multiple groups
pub fn format_timestamp_groups(timestamps: &[i64]) -> String {
    if timestamps.is_empty() {
        return String::new();
    }

    let mut sorted: Vec<i64> = timestamps.to_vec();
    sorted.sort();

    const WINDOW_SECONDS: i64 = 43200; // 12 hours
    let mut groups: Vec<TimestampGroup> = Vec::new();

    for ts in sorted {
        match groups.last_mut() {
            Some(group) if ts - group.start_time <= WINDOW_SECONDS => {
                group.count += 1;
            }
            _ => {
                groups.push(TimestampGroup {
                    start_time: ts,
                    count: 1,
                });
            }
        }
    }

    // Get current year for comparison
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);

    // Simple year extraction (approximate, but good enough for display)
    let current_year = 1970 + (now / 31536000); // seconds per year

    if groups.len() == 1 {
        let date_str = format_timestamp(groups[0].start_time, current_year);
        format!("[{}]", date_str)
    } else {
        let parts: Vec<String> = groups
            .iter()
            .map(|g| {
                let date_str = format_timestamp(g.start_time, current_year);
                format!("{}: {}", date_str, g.count)
            })
            .collect();
        format!("[{}]", parts.join(", "))
    }
}

/// Format a Unix timestamp as a human-readable date
fn format_timestamp(ts: i64, current_year: i64) -> String {
    // Convert epoch to date components manually
    // This is a simplified implementation - for production use chrono
    let days_since_epoch = ts / 86400;
    let year = 1970 + (days_since_epoch / 365); // Approximate

    let months = [
        "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
    ];

    // Approximate month and day (this is simplified)
    let day_of_year = days_since_epoch % 365;
    let month_days = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
    let mut month = 0;
    let mut remaining = day_of_year as i32;
    for (i, &days) in month_days.iter().enumerate() {
        if remaining < days {
            month = i;
            break;
        }
        remaining -= days;
    }
    let day = remaining + 1;

    if year == current_year {
        format!("{} {:02}", months[month], day)
    } else {
        format!("{} {:02} {}", months[month], day, year)
    }
}

/// Compare two lists of dataset names
pub fn compare_datasets(fw_datasets: &[String], parquet_datasets: &[String]) -> DatasetComparison {
    if fw_datasets.is_empty() || parquet_datasets.is_empty() {
        return DatasetComparison::Skipped;
    }

    let fw_set: HashSet<_> = fw_datasets.iter().collect();
    let parquet_set: HashSet<_> = parquet_datasets.iter().collect();

    let fw_only: Vec<String> = fw_set
        .difference(&parquet_set)
        .map(|s| (*s).clone())
        .collect();
    let parquet_only: Vec<String> = parquet_set
        .difference(&fw_set)
        .map(|s| (*s).clone())
        .collect();

    if fw_only.is_empty() && parquet_only.is_empty() {
        DatasetComparison::Match
    } else {
        DatasetComparison::Mismatch {
            fw_only,
            parquet_only,
        }
    }
}

/// Extract dataset name from a fixed-width filename
///
/// Given a path like `/path/to/us2015b_usa.dat.gz`, extracts `us2015b`
fn extract_fw_dataset_name(path: &str, suffix: &str) -> Option<String> {
    Path::new(path)
        .file_name()
        .and_then(|f| f.to_str())
        .and_then(|name| {
            // Remove .dat.gz extension
            name.strip_suffix(".dat.gz")
                .or_else(|| name.strip_suffix(".dat"))
        })
        .and_then(|name| {
            // Remove the product suffix
            name.strip_suffix(suffix)
        })
        .map(String::from)
}

/// Main checker struct that uses the connection pool
pub struct ServerStatusChecker<'a> {
    pool: &'a SshConnectionPool,
}

impl<'a> ServerStatusChecker<'a> {
    pub fn new(pool: &'a SshConnectionPool) -> Self {
        Self { pool }
    }

    /// Check status for a single deployment target
    pub fn check_target(&self, target: &DeploymentTarget) -> ProductStatus {
        let path_exists = self
            .pool
            .dir_exists(&target.server, &target.current_path())
            .unwrap_or(false);

        if !path_exists {
            return ProductStatus {
                product_name: target.product.name.clone(),
                base_path: target.base_path.clone(),
                path_exists: false,
                parquet: FormatStatus::Unknown("Path not found".to_string()),
                fixed_width: FormatStatus::Unknown("Path not found".to_string()),
                derived: FormatStatus::Unknown("Path not found".to_string()),
                comparison: None,
            };
        }

        let parquet = self.check_parquet(target);
        let fixed_width = self.check_fixed_width(target);
        let derived = self.check_derived(target);

        // Compare FW and Parquet if both are configured
        let comparison = if target.product.expects_format(DataFormat::FixedWidth)
            && target.product.expects_format(DataFormat::Parquet)
        {
            Some(compare_datasets(
                &fixed_width.dataset_names(),
                &parquet.dataset_names(),
            ))
        } else {
            None
        };

        ProductStatus {
            product_name: target.product.name.clone(),
            base_path: target.base_path.clone(),
            path_exists: true,
            parquet,
            fixed_width,
            derived,
            comparison,
        }
    }

    fn check_parquet(&self, target: &DeploymentTarget) -> FormatStatus {
        if !target.product.expects_format(DataFormat::Parquet) {
            return FormatStatus::NotConfigured;
        }

        let parquet_path = target.parquet_path();

        // First check if the parquet directory exists
        match self.pool.dir_exists(&target.server, &parquet_path) {
            Ok(false) => return FormatStatus::Missing,
            Err(e) => return FormatStatus::Unknown(e.to_string()),
            Ok(true) => {}
        }

        match self.pool.list_content_dirs(&target.server, &parquet_path) {
            Ok(dirs) if !dirs.is_empty() => {
                let timestamps = self
                    .pool
                    .get_timestamps(&target.server, &format!("{}/*", parquet_path))
                    .unwrap_or_default();

                let datasets: Vec<DatasetInfo> = dirs
                    .into_iter()
                    .map(|name| DatasetInfo {
                        name,
                        timestamp: None,
                    })
                    .collect();

                FormatStatus::Present {
                    datasets,
                    date_summary: format_timestamp_groups(&timestamps),
                }
            }
            Ok(_) => FormatStatus::Missing,
            Err(e) => FormatStatus::Unknown(e.to_string()),
        }
    }

    fn check_fixed_width(&self, target: &DeploymentTarget) -> FormatStatus {
        if !target.product.expects_format(DataFormat::FixedWidth) {
            return FormatStatus::NotConfigured;
        }

        let pattern = target.fw_pattern();

        match self.pool.list_files(&target.server, &pattern) {
            Ok(files) if !files.is_empty() => {
                let timestamps = self
                    .pool
                    .get_timestamps(&target.server, &pattern)
                    .unwrap_or_default();

                let suffix = target.product.fw_suffix();
                let datasets: Vec<DatasetInfo> = files
                    .into_iter()
                    .filter_map(|path| {
                        extract_fw_dataset_name(&path, &suffix).map(|name| DatasetInfo {
                            name,
                            timestamp: None,
                        })
                    })
                    .collect();

                FormatStatus::Present {
                    datasets,
                    date_summary: format_timestamp_groups(&timestamps),
                }
            }
            Ok(_) => FormatStatus::Missing,
            Err(e) => FormatStatus::Unknown(e.to_string()),
        }
    }

    fn check_derived(&self, target: &DeploymentTarget) -> FormatStatus {
        if !target.product.expects_format(DataFormat::Derived) {
            return FormatStatus::NotConfigured;
        }

        let derived_path = target.derived_path();

        // First check if the derived directory exists
        match self.pool.dir_exists(&target.server, &derived_path) {
            Ok(false) => return FormatStatus::Missing,
            Err(e) => return FormatStatus::Unknown(e.to_string()),
            Ok(true) => {}
        }

        match self.pool.list_content_dirs(&target.server, &derived_path) {
            Ok(dirs) if !dirs.is_empty() => {
                let timestamps = self
                    .pool
                    .get_timestamps(&target.server, &format!("{}/*", derived_path))
                    .unwrap_or_default();

                let datasets: Vec<DatasetInfo> = dirs
                    .into_iter()
                    .map(|name| DatasetInfo {
                        name,
                        timestamp: None,
                    })
                    .collect();

                FormatStatus::Present {
                    datasets,
                    date_summary: format_timestamp_groups(&timestamps),
                }
            }
            Ok(_) => FormatStatus::Missing,
            Err(e) => FormatStatus::Unknown(e.to_string()),
        }
    }
}

/// Result summary counters
#[derive(Debug, Default, Clone)]
pub struct StatusSummary {
    pub ok: usize,
    pub warnings: usize,
    pub missing: usize,
    pub errors: usize,
    pub skipped: usize,
}

impl StatusSummary {
    pub fn new() -> Self {
        Self::default()
    }

    /// Update summary based on a format status
    pub fn add_format_status(&mut self, status: &FormatStatus) {
        match status {
            FormatStatus::Present { .. } => self.ok += 1,
            FormatStatus::Missing => self.missing += 1,
            FormatStatus::NotConfigured => {} // Don't count
            FormatStatus::Unknown(_) => self.errors += 1,
        }
    }

    /// Update summary based on a dataset comparison
    pub fn add_comparison(&mut self, comparison: &DatasetComparison) {
        match comparison {
            DatasetComparison::Match => {} // Already counted via format status
            DatasetComparison::Mismatch { .. } => self.warnings += 1,
            DatasetComparison::Skipped => {} // Don't count
        }
    }

    /// Add a skipped product
    pub fn add_skipped(&mut self) {
        self.skipped += 1;
    }

    /// Get total issues (warnings + missing + errors)
    pub fn total_issues(&self) -> usize {
        self.warnings + self.missing + self.errors
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compare_datasets_match() {
        let fw = vec!["us2015b".to_string(), "us2016a".to_string()];
        let parquet = vec!["us2016a".to_string(), "us2015b".to_string()];

        let result = compare_datasets(&fw, &parquet);
        assert!(matches!(result, DatasetComparison::Match));
    }

    #[test]
    fn test_compare_datasets_mismatch() {
        let fw = vec!["us2015b".to_string(), "us2016a".to_string()];
        let parquet = vec!["us2016a".to_string(), "us2017a".to_string()];

        let result = compare_datasets(&fw, &parquet);
        match result {
            DatasetComparison::Mismatch {
                fw_only,
                parquet_only,
            } => {
                assert_eq!(fw_only, vec!["us2015b".to_string()]);
                assert_eq!(parquet_only, vec!["us2017a".to_string()]);
            }
            _ => panic!("Expected mismatch"),
        }
    }

    #[test]
    fn test_compare_datasets_empty() {
        let fw: Vec<String> = vec![];
        let parquet = vec!["us2015b".to_string()];

        let result = compare_datasets(&fw, &parquet);
        assert!(matches!(result, DatasetComparison::Skipped));
    }

    #[test]
    fn test_extract_fw_dataset_name() {
        let path = "/web/internal.cps.ipums.org/share/data/current/cps2015_03_cps.dat.gz";
        let result = extract_fw_dataset_name(path, "_cps");
        assert_eq!(result, Some("cps2015_03".to_string()));

        let path2 = "/path/to/us2015b_health.dat.gz";
        let result2 = extract_fw_dataset_name(path2, "_health");
        assert_eq!(result2, Some("us2015b".to_string()));
    }

    #[test]
    fn test_timestamp_grouping_single() {
        let timestamps = vec![1734220800, 1734220900, 1734221000]; // All within 12 hours
        let result = format_timestamp_groups(&timestamps);
        assert!(result.starts_with('['));
        assert!(result.ends_with(']'));
        assert!(!result.contains(':'));
    }

    #[test]
    fn test_timestamp_grouping_multiple() {
        let timestamps = vec![
            1734220800, // Dec 15, 2024 (approx)
            1731628800, // Nov 15, 2024 (approx)
        ];
        let result = format_timestamp_groups(&timestamps);
        assert!(result.contains(": 1"));
    }

    #[test]
    fn test_timestamp_grouping_empty() {
        let timestamps: Vec<i64> = vec![];
        let result = format_timestamp_groups(&timestamps);
        assert!(result.is_empty());
    }

    #[test]
    fn test_format_status_helpers() {
        let present = FormatStatus::Present {
            datasets: vec![DatasetInfo {
                name: "test".to_string(),
                timestamp: None,
            }],
            date_summary: "[Dec 15]".to_string(),
        };
        assert!(present.is_present());
        assert!(!present.is_missing());
        assert_eq!(present.dataset_count(), Some(1));

        let missing = FormatStatus::Missing;
        assert!(!missing.is_present());
        assert!(missing.is_missing());
        assert_eq!(missing.dataset_count(), None);

        let unknown = FormatStatus::Unknown("error".to_string());
        assert!(unknown.is_error());
    }

    #[test]
    fn test_status_summary() {
        let mut summary = StatusSummary::new();

        summary.add_format_status(&FormatStatus::Present {
            datasets: vec![],
            date_summary: String::new(),
        });
        summary.add_format_status(&FormatStatus::Missing);
        summary.add_format_status(&FormatStatus::Unknown("err".to_string()));
        summary.add_format_status(&FormatStatus::NotConfigured);

        assert_eq!(summary.ok, 1);
        assert_eq!(summary.missing, 1);
        assert_eq!(summary.errors, 1);
        assert_eq!(summary.total_issues(), 2);
    }
}
