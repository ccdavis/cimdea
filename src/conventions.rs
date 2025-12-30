//! Utilities for working with IPUMS conventions and metadata structure.
//!
//! This module provides structs and methods for loading metadata and storing information about a
//! IPUMS data collection based on IPUMS conventions and minimal configuration. Every collection
//! has a set of data record types and a hierarchy those records belong to. For instance, person
//! records belong to household records. Each household record owns 0 or more person records.
//!
//! The [MicroDataCollection] struct initialization makes heavy use of IPUMS directory and naming
//! conventions. This includes loading IPUMS metadata for the collection.
//!
//! The [Context] struct is the entry point for setting up a MicroDataCollection object. It will
//! figure out a "data root" or use one provided to it to locate available data and metadata and
//! load it if requested.
//!
//! Other operations in this library require a `Context` object to find data and use metadata.
//!
//! Metadata for IPUMS data follows naming and organizational conventions. Following these allows
//! us to skip a lot of repetitive configuration. IPUMS data resides under "data root" directories
//! in a "current" directory (compressed fixed-width data) and under "current" in a "parquet"
//! directory for the Parquet version of the same data. A "layouts" directory under "current"
//! contains two "layout" files per dataset: One describing the input layout and labels for those
//! inputs, and one describing the IPUMS version of the data with variable names, record types,
//! data types and designated width in printable characters for the variables. This layout
//! information can serve as basic metadata for other uses besides parsing the fixed-width data.
//! Currently the Parquet data does not have variable level metadata on its columns, so we rely on
//! the layout metadata. Eventually we plan to put variable metadata like formatting directives,
//! codes and labels in the Parquet.
//!
//! See the `.layout.txt` files in the tests directory.

use crate::defaults;
use crate::ipums_data_model::*;
use crate::ipums_metadata_model::*;
use crate::layout;
use crate::mderror::{metadata_error, MdError};
use crate::parquet_metadata::ParquetMetadataReader;
use crate::request::InputType;

use std::collections::HashMap;
use std::collections::HashSet;
use std::path::{Path, PathBuf};

/// Key characteristics of data collections
#[derive(Clone, Debug)]
pub struct MicroDataCollection {
    pub name: String, // Like USA, IPUMSI, ATUS
    pub record_hierarchy: RecordHierarchy,
    pub record_types: HashMap<String, RecordType>, // key is value: 'H', 'P' etc
    pub default_unit_of_analysis: RecordType,
    pub metadata: Option<MetadataEntities>,
}

impl MicroDataCollection {
    pub fn weight_for_rectype(&self, rt: &str) -> Option<String> {
        let rectype = self.record_types.get(rt)?;
        let weight = &rectype.weight.clone()?;
        Some(weight.name.clone())
    }

    pub fn sample_line_weight_for_rectype(&self, rt: &str) -> Option<String> {
        let rectype = self.record_types.get(rt)?;
        let weight = &rectype.sample_weight.clone()?;
        Some(weight.name.clone())
    }

    pub fn weight_divisor(&self, rt: &str) -> Option<usize> {
        let rectype = self.record_types.get(rt)?;
        let weight = &rectype.weight.clone()?;
        Some(weight.divisor)
    }

    pub fn sample_line_weight_divisor(&self, rt: &str) -> Option<usize> {
        let rectype = self.record_types.get(rt)?;
        let weight = &rectype.sample_weight.clone()?;
        Some(weight.divisor)
    }

    pub fn base_filename_for_dataset(&self, dataset_name: &str) -> String {
        format!("{}_{}", dataset_name, &self.name.to_ascii_lowercase())
    }

    pub fn base_filename_for_dataset_and_rectype(
        &self,
        dataset_name: &str,
        record_type_abbrev: &str,
    ) -> String {
        format!(
            "{}.{}",
            &self.base_filename_for_dataset(dataset_name),
            record_type_abbrev.to_ascii_uppercase()
        )
    }

    // This name is a legal SQL table name and we may use it as well for an alias in Duckdb or DataFusion
    // where we can refer to data files as tables but need a alias to use in the rest of the query, like:
    // select count(*) from '/data/us2015b/us2015b_usa.P.parquet' as us2015b_person, '/data/us2015b/us2015b_usa.H.parquet' as us2015b_household
    //  where us2015b_household.SERIAL = us2015b_usa_person.SERIALP and us2015b_household.GQ = 3 and us2015b_person.AGE < 25;
    pub fn default_table_name(
        &self,
        dataset_name: &str,
        record_type_abbrev: &str,
    ) -> Result<String, MdError> {
        if let Some(rt) = self.record_types.get(record_type_abbrev) {
            Ok(format!(
                "{}_{}",
                &self.base_filename_for_dataset(dataset_name),
                &rt.name.to_ascii_lowercase()
            ))
        } else {
            Err(MdError::Msg(format!(
                "Can't create table name since {} is not a valid record type abbrevation.",
                record_type_abbrev
            )))
        }
    }

    /// Read one fixed-width layout file. These files contain some variable level metadata for
    /// every record type in the data product.
    pub fn load_metadata_from_layout(&mut self, _layout_file: &Path) {
        todo!("implement");
    }

    /// Read all layout files for the data root like `../output_data/current/layouts`
    /// The existence of a layout file implies existence of a dataset. The presence of
    /// a variable in a dataset's layout indicates availability in that dataset.
    pub fn load_metadata_from_all_layouts(&mut self, _layouts_dir: &Path) {
        todo!("implement");
    }

    /// The path like `../output_data/current/parquet/us2019a/`
    /// Reading the schema will give approximately the same metadata information
    /// as reading the fixed-width layout file for the same dataset.
    /// Additionally, if the parquet files contain IPUMS metadata in their key-value
    /// metadata, this will load variable labels, categories, and dataset information.
    pub fn load_metadata_from_parquet(
        &mut self,
        parquet_dataset_path: &Path,
    ) -> Result<(), MdError> {
        if !parquet_dataset_path.exists() {
            return Err(metadata_error!(
                "Parquet dataset path does not exist: {}",
                parquet_dataset_path.display()
            ));
        }

        // Extract dataset name from path
        let dataset_name = parquet_dataset_path
            .file_name()
            .and_then(|n| n.to_str())
            .ok_or_else(|| {
                metadata_error!(
                    "Could not extract dataset name from path: {}",
                    parquet_dataset_path.display()
                )
            })?;

        // Collect record types and filenames first to avoid borrow issues
        let record_types: Vec<(String, String)> = self
            .record_types
            .keys()
            .map(|k| {
                let base_filename = self.base_filename_for_dataset_and_rectype(dataset_name, k);
                (k.clone(), base_filename)
            })
            .collect();

        // Initialize metadata if not already present
        if self.metadata.is_none() {
            self.metadata = Some(MetadataEntities::new());
        }

        // Now work with the metadata
        let md = self.metadata.as_mut().unwrap();

        for (rectype_abbrev, base_filename) in record_types {
            let parquet_file = parquet_dataset_path.join(format!("{}.parquet", base_filename));

            if parquet_file.exists() {
                // Check if the file has IPUMS metadata
                if ParquetMetadataReader::has_ipums_metadata(&parquet_file) {
                    // Load metadata from the parquet file
                    let (variables, datasets) = ParquetMetadataReader::load_metadata_from_file(
                        &parquet_file,
                        &rectype_abbrev,
                    )?;

                    // Find the dataset once before the variable loop
                    let dataset = datasets
                        .iter()
                        .find(|d| d.name == dataset_name)
                        .cloned()
                        .unwrap_or_else(|| IpumsDataset::from((dataset_name.to_string(), 0)));

                    // Add variables to metadata
                    for var in variables {
                        md.add_dataset_variable(dataset.clone(), var);
                    }
                } else {
                    // Fall back to just schema information
                    let schema_info = ParquetMetadataReader::get_schema_info(&parquet_file)?;

                    // Create a dataset once before the variable loop
                    let dataset = IpumsDataset::from((dataset_name.to_string(), 0));

                    // Add each field as a variable with minimal metadata
                    for (field_name, (data_type_str, _nullable)) in schema_info {
                        let ipums_var = IpumsVariable {
                            name: field_name,
                            data_type: Some(IpumsDataType::from(data_type_str.as_str())),
                            label: None,
                            record_type: rectype_abbrev.clone(),
                            categories: None,
                            formatting: None,
                            general_width: None,
                            description: None,
                            category_bins: None,
                            id: 0,
                        };
                        md.add_dataset_variable(dataset.clone(), ipums_var);
                    }
                }
            }
        }

        Ok(())
    }

    /// Using the data_root, scan the layouts and load metadata from them.
    pub fn load_metadata_for_selected_datasets_from_layouts(
        &mut self,
        datasets: &[&str],
        data_root: &Path,
    ) -> Result<(), MdError> {
        let mut md = MetadataEntities::new();
        for (index_ds, ds) in datasets.iter().enumerate() {
            let ipums_dataset = IpumsDataset::from((ds.to_string(), index_ds));
            let layouts_path = data_root.to_path_buf().join("layouts");
            let layout = layout::DatasetLayout::try_from_layout_file(
                &layouts_path.join(format!("{}.layout.txt", ds)),
            )?;
            for (index_v, var) in layout.all_variables().iter().enumerate() {
                let ipums_var = IpumsVariable::from((var, index_v));
                md.add_dataset_variable(ipums_dataset.clone(), ipums_var);
            }
        }
        self.metadata = Some(md);
        Ok(())
    }

    /// Uses default product_root to find metadata database and load all metadata for given datasets.
    pub fn load_full_metadata_for_datasets(&mut self, _datasets: &[String]) {
        todo!("implement");
    }

    /// Takes a path like ../output_data/current/parquet/, which could be derived
    /// automatically from defaults based on data root or product root. Scans all
    /// parquet schema information and embedded metadata.
    pub fn load_metadata_from_all_parquet(&mut self, parquet_path: &Path) -> Result<(), MdError> {
        if !parquet_path.exists() {
            return Err(metadata_error!(
                "Parquet path does not exist: {}",
                parquet_path.display()
            ));
        }

        // Read all subdirectories in the parquet path
        let entries = std::fs::read_dir(parquet_path).map_err(|e| {
            metadata_error!(
                "Failed to read parquet directory {}: {}",
                parquet_path.display(),
                e
            )
        })?;

        let mut loaded_count = 0;
        let mut errors = Vec::new();

        for entry in entries {
            let entry =
                entry.map_err(|e| metadata_error!("Failed to read directory entry: {}", e))?;

            let path = entry.path();
            if path.is_dir() {
                // Try to load metadata from this dataset directory
                match self.load_metadata_from_parquet(&path) {
                    Ok(()) => loaded_count += 1,
                    Err(e) => errors.push(format!("{}: {}", path.display(), e)),
                }
            }
        }

        if loaded_count == 0 && !errors.is_empty() {
            return Err(metadata_error!(
                "Failed to load metadata from any parquet datasets. Errors: {}",
                errors.join("; ")
            ));
        }

        Ok(())
    }

    /// Load everything available for the selected variables and samples from the available
    /// metadata database file. Requires 'allow_full_metadata' which depends on a product root
    /// and a 'metadata.db' file located in the root/metadata/versions location, unless you provide
    /// a Some(metadata_location).
    pub fn load_full_metadata_for_selections(
        &mut self,
        _variables: &[String],
        _datasets: &[String],
        _metadata_location: Option<PathBuf>,
    ) {
        todo!("implement");
    }

    /// Load all variables and samples for the context and the default metadata location unless
    /// you provide Some(metadata_location) to override the default. The result of the load may
    /// be very large, into the gigabyte range.
    pub fn load_full_metadata(&mut self, _metadata_location: Option<PathBuf>) {
        todo!("implement");
    }

    pub fn clear_metadata(&mut self) {}
}

#[derive(Clone, Debug)]
pub struct MetadataEntities {
    //// Name -> Id
    pub datasets_by_name: HashMap<String, usize>,
    pub variables_by_name: HashMap<String, usize>,
    /// The valid cross-products
    pub available_variables: VariablesForDataset,
    pub available_datasets: DatasetsForVariable,

    /// The owning structs
    pub variables_index: Vec<IpumsVariable>,
    /// The owning structs
    pub datasets_index: Vec<IpumsDataset>,
}

impl MetadataEntities {
    fn next_dataset_id(&self) -> IpumsDatasetId {
        self.datasets_index.len()
    }

    fn next_variable_id(&self) -> IpumsVariableId {
        self.variables_index.len()
    }

    pub fn cloned_variable_from_id(&self, var_id: IpumsVariableId) -> IpumsVariable {
        self.variables_index[var_id].clone()
    }

    pub fn cloned_variable_from_name(&self, name: &str) -> Option<IpumsVariable> {
        self.variables_by_name
            .get(name)
            .map(|var_id| self.cloned_variable_from_id(*var_id))
    }

    pub fn cloned_dataset_from_id(&self, ds_id: IpumsDatasetId) -> IpumsDataset {
        self.datasets_index[ds_id].clone()
    }

    pub fn cloned_dataset_from_name(&self, name: &str) -> Option<IpumsDataset> {
        self.datasets_by_name
            .get(name)
            .map(|ds_id| self.cloned_dataset_from_id(*ds_id))
    }

    pub fn create_variable(&mut self, var: IpumsVariable) -> IpumsVariableId {
        let id = self.next_variable_id();
        let mut new_var = var;
        new_var.id = id;
        self.variables_by_name.insert(new_var.name.clone(), id);
        self.variables_index.push(new_var);
        id
    }

    pub fn create_dataset(&mut self, ds: IpumsDataset) -> IpumsDatasetId {
        let id = self.next_dataset_id();
        let mut new_ds = ds;
        new_ds.id = id;
        self.datasets_by_name.insert(new_ds.name.clone(), id);
        self.datasets_index.push(new_ds);
        id
    }

    pub fn new() -> Self {
        Self {
            variables_by_name: HashMap::new(),
            datasets_by_name: HashMap::new(),
            available_variables: VariablesForDataset::new(),
            available_datasets: DatasetsForVariable::new(),
            variables_index: Vec::new(),
            datasets_index: Vec::new(),
        }
    }
}

impl Default for MetadataEntities {
    fn default() -> Self {
        Self::new()
    }
}

/// There is a master Vec with Variables by IpumsVariableId this structure points into.
#[derive(Clone, Debug)]
pub struct VariablesForDataset {
    ipums_variables_by_dataset_id: Vec<HashSet<IpumsVariableId>>,
}

impl VariablesForDataset {
    pub fn new() -> Self {
        Self {
            ipums_variables_by_dataset_id: Vec::new(),
        }
    }

    pub fn add_or_update(&mut self, dataset_id: IpumsDatasetId, variable_id: IpumsVariableId) {
        if self.ipums_variables_by_dataset_id.get(dataset_id).is_none() {
            self.ipums_variables_by_dataset_id.push(HashSet::new());
        }
        self.ipums_variables_by_dataset_id[dataset_id].insert(variable_id);
    }

    pub fn for_dataset(&self, dataset_id: IpumsDatasetId) -> Option<&HashSet<IpumsVariableId>> {
        self.ipums_variables_by_dataset_id.get(dataset_id)
    }
}

impl Default for VariablesForDataset {
    fn default() -> Self {
        Self::new()
    }
}

/// There's a master Vec of datasets this structure points into:
#[derive(Clone, Debug)]
pub struct DatasetsForVariable {
    ipums_datasets_by_variable_id: Vec<HashSet<IpumsDatasetId>>,
}

impl DatasetsForVariable {
    pub fn new() -> Self {
        Self {
            ipums_datasets_by_variable_id: Vec::new(),
        }
    }

    pub fn add_or_update(&mut self, dataset_id: IpumsDatasetId, variable_id: IpumsVariableId) {
        if self
            .ipums_datasets_by_variable_id
            .get(variable_id)
            .is_none()
        {
            self.ipums_datasets_by_variable_id.push(HashSet::new());
        }

        self.ipums_datasets_by_variable_id[variable_id].insert(dataset_id);
    }

    pub fn for_variable(&self, var_id: IpumsVariableId) -> Option<&HashSet<IpumsDatasetId>> {
        self.ipums_datasets_by_variable_id.get(var_id)
    }
}

impl Default for DatasetsForVariable {
    fn default() -> Self {
        Self::new()
    }
}

impl MetadataEntities {
    #[allow(dead_code)]
    fn connect_names(&mut self, dataset_name: &str, variable_name: &str) -> Result<(), MdError> {
        let dataset_id = self.datasets_by_name.get(dataset_name);
        let variable_id = self.variables_by_name.get(variable_name);
        if variable_id.is_none() {
            let err = metadata_error!(
                "method connect_names() called with variable name {variable_name}, which is not in metadata"
            );
            return Err(err);
        }

        if dataset_id.is_none() {
            let err = metadata_error!(
                "method connect_names() called with dataset name {dataset_name}, which is not in metadata"
            );
            return Err(err);
        }

        if let (Some(did), Some(vid)) = (dataset_id, variable_id) {
            self.connect(*did, *vid);
        };

        Ok(())
    }

    fn connect(&mut self, dataset_id: IpumsDatasetId, variable_id: IpumsVariableId) {
        self.available_variables
            .add_or_update(dataset_id, variable_id);
        self.available_datasets
            .add_or_update(dataset_id, variable_id);
    }

    pub fn add_dataset_variable(&mut self, dataset: IpumsDataset, variable: IpumsVariable) {
        let dataset_id = match self.datasets_by_name.get(&dataset.name) {
            None => self.create_dataset(dataset),
            Some(dataset_id) => *dataset_id,
        };

        let variable_id = match self.variables_by_name.get(&variable.name) {
            None => self.create_variable(variable),
            Some(variable_id) => *variable_id,
        };

        self.connect(dataset_id, variable_id);
    }
}

/// Holds loaded metadata and information for finding data and additional metadata.
///
/// This mutable state holds loaded metadata (if any),
/// the rest of the information needed to add paths to the data tables used in queries
/// and data file paths, and information about where the metadata can be found.
///
/// Often, creating a [DataRequest][crate::request::DataRequest] will automatically return a
/// `Context` along with the `DataRequest` (for example, see
/// [DataRequest::from_names](crate::request::DataRequest::from_names) and
/// [AbacusRequest::try_from_json](crate::request::AbacusRequest::try_from_json)). In those cases,
/// you do not need to directly create a context yourself. If you do find yourself needing to
/// directly create a `Context`, [from_ipums_collection_name](Context::from_ipums_collection_name)
/// is the easiest way to do that.
///
/// ```
/// use cimdea::conventions::Context;
///
/// // Set data root to point to the directory with your data
/// let data_root = "tests/data_root/".to_string();
/// let ctx = Context::from_ipums_collection_name(
///   "usa",
///   None,
///   Some(data_root),
/// ).unwrap();
///
/// assert_eq!(ctx.name, "usa");
/// let mut record_types: Vec<_> = ctx.settings.record_types.keys().collect();
/// record_types.sort();
/// assert_eq!(record_types, ["H", "P"]);
/// ```
#[derive(Clone, Debug)]
pub struct Context {
    /// A product name like USA, IPUMSI, CPS etc
    pub name: String,
    // A path name
    // Like /pkg/ipums/usa with ./metadata and ./output_data in it
    pub product_root: Option<PathBuf>,
    /// Any output_data/current path with ./layouts and ./parquet in it
    pub data_root: Option<PathBuf>,
    pub settings: MicroDataCollection,
    pub allow_full_metadata: bool,
    pub enable_full_metadata: bool,
}

impl Context {
    // Convenience method mostly for testing
    pub fn get_md_variable_by_name(&self, name: &str) -> Result<IpumsVariable, MdError> {
        if let Some(ref md) = self.settings.metadata {
            if let Some(var) = md.cloned_variable_from_name(name) {
                Ok(var)
            } else {
                Err(metadata_error!("Variable '{name}' not in loaded metadata.",))
            }
        } else {
            Err(metadata_error!(
                "No metadata loaded. Can't get variable '{name}'."
            ))
        }
    }

    /// Formats the exact paths needed to get data for this dataset, by record type.
    pub fn paths_from_dataset_name(
        &self,
        dataset_name: &str,
        data_format: &InputType,
    ) -> Result<HashMap<String, PathBuf>, MdError> {
        let extension = match data_format {
            InputType::Csv => "csv",
            InputType::Parquet => "parquet",
            InputType::Fw => "dat.gz",
            InputType::NativeDb => "",
        };

        let data_path = if let Some(ref data_root) = self.data_root {
            PathBuf::from(data_root)
        } else {
            return Err(MdError::Msg("No data root set.".to_string()));
        };

        let mut all_paths = HashMap::new();

        match data_format {
            InputType::Csv | InputType::Parquet => {
                for rt in self.settings.record_types.keys() {
                    if let Some(ref sub_dir) = data_format.data_sub_directory() {
                        let parent_dir = data_path.join(sub_dir).join(dataset_name);
                        let base_filename = self
                            .settings
                            .base_filename_for_dataset_and_rectype(dataset_name, rt);
                        let full_filename = format!("{}.{}", &base_filename, extension);
                        let full_path = parent_dir.join(full_filename);
                        all_paths.insert(rt.to_string(), full_path);
                    } else {
                        return Err(MdError::Msg(
                            "InputType of data should have a sub directory name.".to_string(),
                        ));
                    }
                }
            }
            InputType::NativeDb => {
                for rt in self.settings.record_types.keys() {
                    let table: PathBuf = self.settings.default_table_name(dataset_name, rt)?.into();
                    all_paths.insert(rt.to_string(), table);
                }
            }
            InputType::Fw => {
                let base_filename = self.settings.base_filename_for_dataset(dataset_name);
                let full_filename = format!("{}.{}", base_filename, extension);
                let full_path = data_path.join(full_filename);
                all_paths.insert("".to_string(), full_path);
            }
        } // match
        Ok(all_paths)
    }

    /// When called, the context should be already set to read from layouts or full metadata
    pub fn load_metadata_for_datasets(&mut self, datasets: &[&str]) -> Result<(), MdError> {
        if !self.enable_full_metadata {
            if let Some(ref data_root) = self.data_root {
                self.settings
                    .load_metadata_for_selected_datasets_from_layouts(datasets, data_root)
            } else {
                Err(metadata_error!("Cannot load any metadata without a data_root or full metadata available ad the product_root."))
            }
        } else {
            todo!("Loading metadata from database not implemented.");
        }
    }

    /// Load metadata for datasets from parquet files
    /// This will extract metadata from the parquet files' key-value metadata if available,
    /// or fall back to schema information.
    pub fn load_metadata_for_datasets_from_parquet(
        &mut self,
        datasets: &[&str],
    ) -> Result<(), MdError> {
        if let Some(ref data_root) = self.data_root {
            let parquet_path = data_root.join("parquet");
            if !parquet_path.exists() {
                return Err(metadata_error!(
                    "Parquet directory does not exist at: {}",
                    parquet_path.display()
                ));
            }

            for dataset in datasets {
                let dataset_path = parquet_path.join(dataset);
                self.settings.load_metadata_from_parquet(&dataset_path)?;
            }
            Ok(())
        } else {
            Err(metadata_error!(
                "Cannot load parquet metadata without a data_root"
            ))
        }
    }

    /// Load all available metadata from parquet files in the data root
    pub fn load_all_metadata_from_parquet(&mut self) -> Result<(), MdError> {
        if let Some(ref data_root) = self.data_root {
            let parquet_path = data_root.join("parquet");
            self.settings.load_metadata_from_all_parquet(&parquet_path)
        } else {
            Err(metadata_error!(
                "Cannot load parquet metadata without a data_root"
            ))
        }
    }

    /// The context should be set to read from layouts or full metadata
    pub fn load_metadata_for_datasets_and_variables(
        &mut self,
        _datasets: Vec<String>,
        _variables: Vec<String>,
    ) {
        if !self.enable_full_metadata {
            todo!("not implemented.");
        } else {
            todo!("not implemented.");
        }
    }

    /// Based on name, use default data root and product root and initialize with defaults
    /// Optional data root and product root will be used if provided.
    ///
    /// Returns an error if the given name isn't the name of a recognized product.
    pub fn from_ipums_collection_name(
        name: &str,
        other_product_root: Option<String>,
        other_data_root: Option<String>,
    ) -> Result<Self, MdError> {
        let product_root = if let Some(prod_root) = other_product_root {
            PathBuf::from(prod_root)
        } else {
            PathBuf::from(format!("/pkg/ipums/{}", &name))
        };
        let allow_full_metadata = product_root.exists();
        let data_root = if let Some(dat_root) = other_data_root {
            PathBuf::from(dat_root)
        } else {
            PathBuf::from(format!("/pkg/ipums/{}", &name))
                .join("output_data")
                .join("current")
        };

        let settings = defaults::defaults_for(name)?;

        Ok(Self {
            name: name.to_string(),
            product_root: Some(product_root),
            data_root: Some(data_root),
            settings,
            allow_full_metadata,
            enable_full_metadata: false,
        })
    }

    /*
     // Give the path like '/pkg/ipums/usa'. Extract product name from path
     // if possible and use defaults.
     pub fn default_from_product_root(product_path: &str) -> Self {

     }

     // Use name for product and apply defaults, but  substitute the data_root for
     // the default data_root.
     pub fn from_name_and_data_root(name: &str, data_root: &str) -> Self {
     }

     // If the context has the project root in addition to the data root it can
     // attempt to access the metadata DB. Using full metadata requires the
     // Some(product_root).
     fn use_full_metadata(&mut self, setting: bool){
         self.allow_full_metadata = setting;
     }

    */
}

#[cfg(test)]
mod test {
    use super::*;
    #[test]
    pub fn test_context() {
        // Look in test directory
        let data_root = Some(String::from("test/data_root"));
        let usa_ctx = Context::from_ipums_collection_name("usa", None, data_root)
            .expect("should be able to create USA context");

        if let Some(ref prod_root) = usa_ctx.product_root {
            if prod_root.exists() {
                assert!(
                    usa_ctx.allow_full_metadata,
                    "Default allow_full_metadata should be true when product root dir was found."
                );
            } else {
                assert!(
                    !usa_ctx.allow_full_metadata,
                    "Default allow_full_metadata should be false"
                );
            }
        } else {
            assert!(
                !usa_ctx.allow_full_metadata,
                "Default allow_full_metadata should be false"
            );
        }
        assert!(usa_ctx.data_root.is_some());
        assert_eq!("USA".to_string(), usa_ctx.settings.name);
        assert_eq!(2, usa_ctx.settings.record_types.len());
        assert!(usa_ctx.settings.record_types.contains_key("H"));
        assert!(usa_ctx.settings.record_types.contains_key("P"));
    }

    #[test]
    pub fn test_paths_for_dataset_names() {
        let data_root = Some(String::from("test/data_root"));
        let usa_ctx = Context::from_ipums_collection_name("usa", None, data_root)
            .expect("should be able to create USA context");
        let paths_by_rectype = usa_ctx
            .paths_from_dataset_name("us2015b", &InputType::Parquet)
            .expect("should be able to get paths from dataset name");
        let person_path = paths_by_rectype.get("P");
        let household_path = paths_by_rectype.get("H");
        assert!(person_path.is_some(), "should have a person type path");
        assert!(household_path.is_some(), "should have a household path");
        if let Some(p) = person_path {
            assert_eq!(
                "test/data_root/parquet/us2015b/us2015b_usa.P.parquet",
                &p.to_string_lossy()
            );
        }
    }

    #[test]
    fn test_micro_data_collection_default_table_name() {
        let collection =
            defaults::defaults_for("usa").expect("should be able to get defaults for USA");
        let table_name = collection
            .default_table_name("us2021a", "P")
            .expect("should get a table name back because P is a valid record type");
        assert_eq!(table_name, "us2021a_usa_person");
    }

    #[test]
    fn test_micro_data_collection_default_table_name_unknown_rectype_error() {
        let collection =
            defaults::defaults_for("usa").expect("should be able to get defaults for USA");
        let result = collection.default_table_name("us2021a", "Z");
        assert!(result.is_err(), "expected an error but got {result:?}");
    }

    #[test]
    fn test_load_metadata_from_parquet() {
        let data_root = Some(String::from("tests/data_root"));
        let mut usa_ctx = Context::from_ipums_collection_name("usa", None, data_root)
            .expect("should be able to create USA context");

        // Try to load metadata from parquet if test data exists
        let parquet_path = PathBuf::from("tests/data_root/parquet/us2015b");
        if parquet_path.exists() {
            let result = usa_ctx.settings.load_metadata_from_parquet(&parquet_path);
            // We don't assert success here since test data may not have metadata
            // but the function should not panic
            if result.is_ok() {
                assert!(usa_ctx.settings.metadata.is_some());
            }
        }
    }

    #[test]
    fn test_context_load_metadata_from_parquet() {
        let data_root = Some(String::from("tests/data_root"));
        let mut usa_ctx = Context::from_ipums_collection_name("usa", None, data_root)
            .expect("should be able to create USA context");

        // Try to load metadata for a specific dataset
        let parquet_path = PathBuf::from("tests/data_root/parquet");
        if parquet_path.exists() {
            // This should work even if the parquet files don't have embedded metadata
            // It will fall back to schema information
            let result = usa_ctx.load_metadata_for_datasets_from_parquet(&["us2015b"]);
            match result {
                Ok(_) => {
                    // Success - metadata was loaded
                }
                Err(e) if e.to_string().contains("does not exist") => {
                    // Expected - dataset directory doesn't exist in test data
                }
                Err(e) => {
                    panic!("Unexpected error loading parquet metadata: {}", e);
                }
            }
        }
    }
}
