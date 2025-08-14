//! Module for loading IPUMS metadata from Parquet files.
//!
//! This module provides functionality to extract metadata embedded in Parquet files'
//! key-value metadata. IPUMS Parquet files can contain JSON-encoded metadata about
//! variables, samples, and data structure.

use crate::ipums_metadata_model::{
    IpumsCategory, IpumsDataType, IpumsDataset, IpumsValue, IpumsVariable, UniversalCategoryType,
};
use crate::mderror::{metadata_error, MdError};
use parquet::file::reader::{FileReader, SerializedFileReader};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::File;
use std::path::Path;

/// Variable metadata as stored in Parquet key-value metadata
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct ParquetVariableMetadata {
    pub label: String,
    #[serde(default, deserialize_with = "deserialize_categories")]
    pub categories: HashMap<String, String>,
    #[serde(default)]
    pub data_type: String,
    #[serde(default)]
    pub column_start: Option<usize>,
    #[serde(default)]
    pub column_width: Option<usize>,
    #[serde(default)]
    pub general_width: Option<usize>,
    #[serde(default)]
    pub record_type: Option<String>,
    #[serde(default)]
    pub is_allocated: bool,
    #[serde(default)]
    pub is_internal: bool,
    #[serde(default)]
    pub is_restricted: bool,
    #[serde(default)]
    pub is_source_variable: bool,
    #[serde(default)]
    pub has_editing_rules: bool,
    #[serde(default)]
    pub has_no_input: bool,
    #[serde(default)]
    pub has_source_variables_as_input: bool,
    #[serde(default)]
    pub hide_status: i32,
    #[serde(default)]
    pub monetary: String,
    #[serde(default)]
    pub quality_flag: String,
    #[serde(default)]
    pub recoding_type: i32,
    #[serde(default)]
    pub restrictions_apply: bool,
    #[serde(default)]
    pub sort_order: i32,
    #[serde(default)]
    pub source_for: String,
    #[serde(default)]
    pub source_variables: Vec<String>,
    #[serde(default)]
    pub tabulation_type: i32,
}

fn deserialize_categories<'de, D>(deserializer: D) -> Result<HashMap<String, String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let opt: Option<HashMap<String, String>> = Option::deserialize(deserializer)?;
    Ok(opt.unwrap_or_default())
}

/// Raw metadata extracted from Parquet files
#[derive(Debug, Default)]
pub struct RawParquetMetadata {
    pub variables: String,
    pub samples: String,
    pub version: String,
}

/// Main struct for extracting metadata from Parquet files
pub struct ParquetMetadataReader;

impl ParquetMetadataReader {
    /// Convert a HashMap of category codes to labels into a Vec of IpumsCategory objects
    fn convert_categories(
        categories_map: &HashMap<String, String>,
        data_type: &str,
    ) -> Vec<IpumsCategory> {
        let mut categories = Vec::new();
        
        for (code_str, label) in categories_map {
            // Parse the code value based on the variable's data type
            let value = match data_type.to_lowercase().as_str() {
                "integer" | "fixed" => {
                    // Try to parse as integer
                    if let Ok(code) = code_str.parse::<i64>() {
                        IpumsValue::Integer(code)
                    } else {
                        // If it fails, store as string
                        IpumsValue::String {
                            utf8: true,
                            value: code_str.as_bytes().to_vec(),
                        }
                    }
                }
                "double" | "float" => IpumsValue::Float(code_str.clone()),
                _ => IpumsValue::String {
                    utf8: true,
                    value: code_str.as_bytes().to_vec(),
                },
            };
            
            // Determine the category meaning based on common IPUMS conventions
            let meaning = Self::determine_category_meaning(code_str, label);
            
            categories.push(IpumsCategory::new(label, meaning, value));
        }
        
        // Sort categories by their code for consistent ordering
        categories.sort_by(|a, b| {
            match (&a.value, &b.value) {
                (IpumsValue::Integer(a_val), IpumsValue::Integer(b_val)) => a_val.cmp(b_val),
                _ => std::cmp::Ordering::Equal,
            }
        });
        
        categories
    }
    
    /// Determine the UniversalCategoryType based on code and label patterns
    fn determine_category_meaning(code: &str, label: &str) -> UniversalCategoryType {
        let label_lower = label.to_lowercase();
        
        // Check for common patterns in IPUMS data
        if label_lower.contains("n/a") || label_lower.contains("not applicable") {
            UniversalCategoryType::NotApplicable
        } else if label_lower.contains("missing") || label_lower.contains("unknown") 
                || label_lower.contains("illegible") || code == "999" || code == "9999" 
                || code == "99999" || code == "998" || code == "9998" {
            UniversalCategoryType::Missing
        } else if label_lower.contains("not in universe") || label_lower.contains("niu") {
            UniversalCategoryType::NotInUniverse
        } else if label_lower.contains("topcode") || label_lower.contains("top code") {
            UniversalCategoryType::TopCode
        } else if label_lower.contains("bottomcode") || label_lower.contains("bottom code") {
            UniversalCategoryType::BottomCode
        } else {
            UniversalCategoryType::Value
        }
    }

    /// Extract raw IPUMS metadata from a parquet file's key-value metadata
    pub fn extract_raw_metadata(file_path: &Path) -> Result<RawParquetMetadata, MdError> {
        let file = File::open(file_path).map_err(|e| {
            metadata_error!(
                "Failed to open parquet file at {}: {}",
                file_path.display(),
                e
            )
        })?;

        let reader = SerializedFileReader::new(file).map_err(|e| {
            metadata_error!(
                "Failed to create parquet reader for {}: {}",
                file_path.display(),
                e
            )
        })?;

        let mut metadata = RawParquetMetadata::default();

        if let Some(kv_metadata) = reader.metadata().file_metadata().key_value_metadata() {
            for kv in kv_metadata {
                match kv.key.as_str() {
                    "variables" => {
                        if let Some(ref value) = kv.value {
                            metadata.variables = value.clone();
                        }
                    }
                    "samples" => {
                        if let Some(ref value) = kv.value {
                            metadata.samples = value.clone();
                        }
                    }
                    "version" => {
                        if let Some(ref value) = kv.value {
                            metadata.version = value.clone();
                        }
                    }
                    _ => {}
                }
            }

            if metadata.variables.is_empty() && metadata.samples.is_empty() {
                Err(metadata_error!(
                    "No IPUMS metadata found in parquet file at {}",
                    file_path.display()
                ))
            } else {
                Ok(metadata)
            }
        } else {
            Err(metadata_error!(
                "No key-value metadata found in parquet file at {}",
                file_path.display()
            ))
        }
    }

    /// Parse variable metadata from JSON string
    pub fn parse_variable_metadata(
        json_str: &str,
        record_type: &str,
    ) -> Result<Vec<IpumsVariable>, MdError> {
        let variables_map: HashMap<String, serde_json::Value> =
            serde_json::from_str(json_str).map_err(|e| {
                metadata_error!("Failed to parse variables JSON: {}", e)
            })?;

        let mut variables = Vec::new();
        let mut failed_vars = Vec::new();

        for (var_name, var_value) in variables_map {
            match serde_json::from_value::<ParquetVariableMetadata>(var_value.clone()) {
                Ok(metadata) => {
                    // Convert categories if present and not empty
                    let categories = if !metadata.categories.is_empty() {
                        Some(Self::convert_categories(&metadata.categories, &metadata.data_type))
                    } else {
                        None
                    };

                    let ipums_var = IpumsVariable {
                        name: var_name.clone(),
                        data_type: Some(IpumsDataType::from(metadata.data_type.as_str())),
                        label: Some(metadata.label),
                        record_type: metadata
                            .record_type
                            .unwrap_or_else(|| record_type.to_string()),
                        categories,
                        formatting: metadata
                            .column_start
                            .and_then(|start| metadata.column_width.map(|width| (start, width))),
                        general_width: metadata.general_width.or(metadata.column_width),
                        description: None, // Could be populated from additional metadata if available
                        category_bins: None,
                        id: 0, // Will be assigned when added to MetadataEntities
                    };
                    variables.push(ipums_var);
                }
                Err(e) => {
                    failed_vars.push(var_name.clone());
                    eprintln!(
                        "Warning: Failed to deserialize metadata for variable '{}': {}",
                        var_name, e
                    );
                }
            }
        }

        if !failed_vars.is_empty() {
            eprintln!(
                "Warning: Failed to parse {} variables: {:?}",
                failed_vars.len(),
                failed_vars
            );
        }

        if variables.is_empty() {
            Err(metadata_error!(
                "No valid variables could be parsed from metadata"
            ))
        } else {
            Ok(variables)
        }
    }

    /// Parse dataset/sample metadata from JSON string
    pub fn parse_samples_metadata(json_str: &str) -> Result<Vec<IpumsDataset>, MdError> {
        let samples_map: HashMap<String, serde_json::Value> =
            serde_json::from_str(json_str).map_err(|e| {
                metadata_error!("Failed to parse samples JSON: {}", e)
            })?;

        let mut datasets = Vec::new();

        for (sample_name, sample_value) in samples_map {
            // Extract what we can from the JSON value
            let label = sample_value
                .get("label")
                .and_then(|v| v.as_str())
                .map(String::from);

            let year = sample_value
                .get("year")
                .and_then(|v| v.as_u64())
                .map(|v| v as usize);

            let month = sample_value
                .get("month")
                .and_then(|v| v.as_u64())
                .map(|v| v as usize);

            let sampling_density = sample_value
                .get("density")
                .and_then(|v| v.as_f64())
                .or_else(|| {
                    sample_value
                        .get("sampling_density")
                        .and_then(|v| v.as_f64())
                });

            let dataset = IpumsDataset {
                name: sample_name,
                year,
                month,
                label,
                sampling_density,
                id: 0, // Will be assigned when added to MetadataEntities
            };

            datasets.push(dataset);
        }

        if datasets.is_empty() {
            Err(metadata_error!(
                "No valid datasets could be parsed from metadata"
            ))
        } else {
            Ok(datasets)
        }
    }

    /// Extract schema information from a parquet file
    pub fn get_schema_info(file_path: &Path) -> Result<HashMap<String, (String, bool)>, MdError> {
        let file = File::open(file_path).map_err(|e| {
            metadata_error!(
                "Failed to open parquet file at {}: {}",
                file_path.display(),
                e
            )
        })?;

        let reader = SerializedFileReader::new(file).map_err(|e| {
            metadata_error!(
                "Failed to create parquet reader for {}: {}",
                file_path.display(),
                e
            )
        })?;

        let schema = reader.metadata().file_metadata().schema();
        let mut schema_info = HashMap::new();

        for field in schema.get_fields() {
            let name = field.name().to_string();
            let type_str = format!("{:?}", field.get_physical_type());
            let nullable = field.is_optional();
            schema_info.insert(name, (type_str, nullable));
        }

        Ok(schema_info)
    }

    /// Load all metadata from a parquet file (variables and samples)
    pub fn load_metadata_from_file(
        file_path: &Path,
        record_type: &str,
    ) -> Result<(Vec<IpumsVariable>, Vec<IpumsDataset>), MdError> {
        let raw_metadata = Self::extract_raw_metadata(file_path)?;

        let variables = if !raw_metadata.variables.is_empty() {
            Self::parse_variable_metadata(&raw_metadata.variables, record_type)?
        } else {
            Vec::new()
        };

        let datasets = if !raw_metadata.samples.is_empty() {
            Self::parse_samples_metadata(&raw_metadata.samples)?
        } else {
            Vec::new()
        };

        Ok((variables, datasets))
    }

    /// Check if a parquet file contains IPUMS metadata
    pub fn has_ipums_metadata(file_path: &Path) -> bool {
        if let Ok(file) = File::open(file_path) {
            if let Ok(reader) = SerializedFileReader::new(file) {
                if let Some(kv_metadata) = reader.metadata().file_metadata().key_value_metadata() {
                    return kv_metadata
                        .iter()
                        .any(|kv| kv.key == "variables" || kv.key == "samples");
                }
            }
        }
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_variable_metadata_simple() {
        let json_str = r#"{
            "AGE": {
                "label": "Age",
                "data_type": "integer",
                "categories": {
                    "0": "Less than 1 year",
                    "1": "1 year old"
                }
            },
            "SEX": {
                "label": "Sex",
                "data_type": "integer",
                "categories": {
                    "1": "Male",
                    "2": "Female"
                }
            }
        }"#;

        let variables = ParquetMetadataReader::parse_variable_metadata(json_str, "P").unwrap();
        assert_eq!(variables.len(), 2);

        let age_var = variables.iter().find(|v| v.name == "AGE").unwrap();
        assert_eq!(age_var.label.as_ref().unwrap(), "Age");
        assert_eq!(age_var.record_type, "P");
    }

    #[test]
    fn test_parse_variable_metadata_with_all_fields() {
        let json_str = r#"{
            "AGE": {
                "label": "Age",
                "data_type": "integer",
                "column_start": 58,
                "column_width": 3,
                "general_width": 3,
                "record_type": "P",
                "is_allocated": false,
                "is_internal": false,
                "is_restricted": false,
                "is_source_variable": false,
                "has_editing_rules": false,
                "has_no_input": false,
                "has_source_variables_as_input": true,
                "hide_status": 0,
                "monetary": "",
                "quality_flag": "null",
                "recoding_type": 1,
                "restrictions_apply": false,
                "sort_order": 651,
                "source_for": "null",
                "source_variables": ["US1900J_1000"],
                "tabulation_type": 1,
                "categories": {
                    "0": "Less than 1 year old",
                    "1": "1 year old"
                }
            }
        }"#;

        let variables = ParquetMetadataReader::parse_variable_metadata(json_str, "P").unwrap();
        assert_eq!(variables.len(), 1);

        let age_var = &variables[0];
        assert_eq!(age_var.name, "AGE");
        assert_eq!(age_var.label.as_ref().unwrap(), "Age");
        assert_eq!(age_var.record_type, "P");
        assert_eq!(age_var.general_width, Some(3));
        assert_eq!(age_var.formatting, Some((58, 3)));
    }

    #[test]
    fn test_convert_categories() {
        let mut categories_map = HashMap::new();
        categories_map.insert("0".to_string(), "Less than 1 year old".to_string());
        categories_map.insert("1".to_string(), "1 year old".to_string());
        categories_map.insert("999".to_string(), "Missing".to_string());
        
        let categories = ParquetMetadataReader::convert_categories(&categories_map, "integer");
        
        assert_eq!(categories.len(), 3);
        
        // Check first category (should be sorted by code)
        assert_eq!(categories[0].label(), "Less than 1 year old");
        assert_eq!(categories[0].value, IpumsValue::Integer(0));
        matches!(categories[0].meaning, UniversalCategoryType::Value);
        
        // Check last category (missing value)
        assert_eq!(categories[2].label(), "Missing");
        assert_eq!(categories[2].value, IpumsValue::Integer(999));
        matches!(categories[2].meaning, UniversalCategoryType::Missing);
    }

    #[test]
    fn test_determine_category_meaning() {
        use super::UniversalCategoryType;
        
        // Test Missing patterns
        assert!(matches!(
            ParquetMetadataReader::determine_category_meaning("999", "Missing"),
            UniversalCategoryType::Missing
        ));
        assert!(matches!(
            ParquetMetadataReader::determine_category_meaning("998", "Unknown/illegible"),
            UniversalCategoryType::Missing
        ));
        
        // Test NotApplicable patterns
        assert!(matches!(
            ParquetMetadataReader::determine_category_meaning("99", "N/A or blank"),
            UniversalCategoryType::NotApplicable
        ));
        
        // Test NotInUniverse patterns
        assert!(matches!(
            ParquetMetadataReader::determine_category_meaning("0", "Not in universe"),
            UniversalCategoryType::NotInUniverse
        ));
        assert!(matches!(
            ParquetMetadataReader::determine_category_meaning("0", "NIU"),
            UniversalCategoryType::NotInUniverse
        ));
        
        // Test regular value
        assert!(matches!(
            ParquetMetadataReader::determine_category_meaning("1", "Male"),
            UniversalCategoryType::Value
        ));
    }

    #[test]
    fn test_parse_variable_metadata_with_categories() {
        let json_str = r#"{
            "SEX": {
                "label": "Sex",
                "data_type": "integer",
                "record_type": "P",
                "categories": {
                    "1": "Male",
                    "2": "Female",
                    "9": "Missing"
                }
            }
        }"#;
        
        let variables = ParquetMetadataReader::parse_variable_metadata(json_str, "P").unwrap();
        assert_eq!(variables.len(), 1);
        
        let sex_var = &variables[0];
        assert_eq!(sex_var.name, "SEX");
        assert!(sex_var.categories.is_some());
        
        let categories = sex_var.categories.as_ref().unwrap();
        assert_eq!(categories.len(), 3);
        
        // Verify categories are properly converted
        assert_eq!(categories[0].label(), "Male");
        assert_eq!(categories[0].value, IpumsValue::Integer(1));
        
        assert_eq!(categories[1].label(), "Female");
        assert_eq!(categories[1].value, IpumsValue::Integer(2));
        
        assert_eq!(categories[2].label(), "Missing");
        assert_eq!(categories[2].value, IpumsValue::Integer(9));
        matches!(categories[2].meaning, UniversalCategoryType::Missing);
    }

    #[test]
    fn test_parse_samples_metadata() {
        let json_str = r#"{
            "us2019a": {
                "label": "2019 American Community Survey",
                "year": 2019,
                "sampling_density": 0.01
            },
            "us2020a": {
                "label": "2020 American Community Survey",
                "year": 2020,
                "sampling_density": 0.01
            }
        }"#;

        let datasets = ParquetMetadataReader::parse_samples_metadata(json_str).unwrap();
        assert_eq!(datasets.len(), 2);

        let us2019 = datasets.iter().find(|d| d.name == "us2019a").unwrap();
        assert_eq!(
            us2019.label.as_ref().unwrap(),
            "2019 American Community Survey"
        );
        assert_eq!(us2019.year.unwrap(), 2019);
    }
}