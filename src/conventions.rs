//! This module provides structs and methods for loading metadata and storing information about a IPUMS data
//! collection based on conventions and minimal configuration. Every collection has a set of data record types
//! and a hierarchy those records belong to: For instance, person records belong to household records.
//!
//! The `MicroDataCollection` struct initialization  makes heavy use of IPUMS directory and naming conventions.
//! This includes loading IPUMS metadata for the collection.
//!
//! The `Context` struct is the entry point for setting up a MicroDataCollection object. It will figure out a "data root"
//! or use one provided to it to locate available data and metadata and load it if requested.
//!
//! Other operations in this library will require a context object to find data and use metadata.
//!
//! Metadata for IPUMS data follows naming and organizational conventions. Following these
//! allows us to skip a lot of repetitive configuration. IPUMS data resides under "data root" directories in a "current"
//! directory (compressed fixed-width data) and under "current" in a "parquet" directory for the
//! Parquet version of the same data. A "layouts" directory under "current" contains two "layout" files per dataset: One
//! describing the input layout and labels for those inputs, and one describing the IPUMS version of the data with variable
//! names, record types, data types and designated width in printable characters for the variables. This layout information
//! can serve as basic metadata for other uses besides parsing the fixed-width data. Currently the Parquet data doesn't have
//! variable level metadata on its columns, so we rely on the layout metadata. Eventually we plan to put variable metadata like
//! formatting directives, codes and labels in the Parquet.
//!
//! See the `.layout.txt` files in the test directory.
//!
//!
//!

use crate::defaults;
use crate::ipums_data_model::*;
use crate::ipums_metadata_model::*;
use crate::layout;
use crate::request::InputType;

use std::collections::HashMap;
use std::collections::HashSet;
use std::path::{Path, PathBuf};

/// Key characteristics of collections like all USA Census data, all Time-Use Survey data etc.
pub struct MicroDataCollection {
    pub name: String, // Like USA, IPUMSI, ATUS
    pub record_hierarchy: RecordHierarchy,
    pub record_types: HashMap<String, RecordType>, // key is value: 'H', 'P' etc
    pub metadata: Option<MetadataEntities>,
}

impl MicroDataCollection {
    /// Read one fixed-width layout file. These files contain some variable level metadata for
    /// every record type in the data product.
    fn load_metadata_from_layout(&mut self, layout_file: &Path) {}

    /// Read all layout files for the data root like `../output_data/current/layouts`
    /// The existence of a layout file implies existence of a dataset. The presence of
    /// a variable in a dataset's layout indicates availability in that dataset.
    fn load_metadata_from_all_layouts(&mut self, layouts_dir: &Path) {}

    /// The path like `../output_data/current/parquet/us2019a/`
    /// Reading the schema will give approximately the same metadata information
    /// as reading the fixed-width layout file for the same dataset.
    fn load_metadata_from_parquet(&mut self, parquet_dataset_path: &Path) {}

    /// Using the data_root, scan the layouts and load metadata from them.
    pub fn load_metadata_for_selected_datasets_from_layouts(
        &mut self,
        datasets: &[&str],
        data_root: &Path,
    ) {
        let mut md = MetadataEntities::new();
        for (index_ds, ds) in datasets.iter().enumerate() {
            let ipums_dataset = IpumsDataset::from((ds.to_string(), index_ds));
            let layouts_path = data_root.to_path_buf().join("layouts");
            let layout = layout::DatasetLayout::from_layout_file(
                &layouts_path.join(format!("{}.layout.txt", ds)),
            );
            for (index_v, var) in layout.all_variables().iter().enumerate() {
                let ipums_var = IpumsVariable::from((var, index_v));
                md.add_dataset_variable(ipums_dataset.clone(), ipums_var);
            }
        }
        self.metadata = Some(md);
    }

    /// Uses default product_root to find metadata database and load all metadata for given datasets.
    pub fn load_full_metadata_for_datasets(&mut self, datasets: &[String]) {}

    /// Takes a path like ../output_data/current/parquet/, which could be derived
    /// automatically from defaults based on data root or product root. Scans all
    /// parquet schema information.
    fn load_metadata_from_all_parquet(&mut self, parquet_path: &Path) {}

    /// Load everything available for the selected variables and samples from the available
    /// metadata database file. Requires 'allow_full_metadata' which depends on a product root
    /// and a 'metadata.db' file located in the root/metadata/versions location, unless you provide
    /// a Some(metadata_location).
    pub fn load_full_metadata_for_selections(
        &mut self,
        variables: &[String],
        datasets: &[String],
        metadata_location: Option<PathBuf>,
    ) {
    }

    /// Load all variables and samples for the context and the default metadata location unless
    /// you provide Some(metadata_location) to override the default. The result of the load may
    /// be very large, into the gigabyte range.
    pub fn load_full_metadata(&mut self, metadata_location: Option<PathBuf>) {}

    pub fn clear_metadata(&mut self) {}
}

pub struct MetadataEntities {
    //// Name -> Id
    pub datasets_by_name: HashMap<String, usize>,
    pub variables_by_name: HashMap<String, usize>,
    /// The valid cross-products
    pub available_variables: VariablesForDataset,
    pub available_datasets: DatasetsForVariable,

    //// The owning structs
    pub variables_index: Vec<IpumsVariable>,
    //// The owning structs
    pub datasets_index: Vec<IpumsDataset>,
}

impl MetadataEntities {
    fn next_dataset_id(&self) -> IpumsDatasetId {
        self.datasets_index.len()
    }

    fn next_variable_id(&self) -> IpumsVariableId {
        self.variables_index.len()
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

/// There is a master Vec with Variables by IpumsVariableId this structure points into.
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

//// There's a master Vec of datasets this structure points into:
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

impl MetadataEntities {
    fn connect_names(&mut self, dataset_name: &str, variable_name: &str) {
        let dataset_id = self.datasets_by_name.get(dataset_name);
        let variable_id = self.variables_by_name.get(variable_name);
        if variable_id.is_none() {
            panic!("Internal method connect() should never be called with non-existent metadata names.");
        }

        if dataset_id.is_none() {
            panic!("Internal method connect() should never be called with non-existent metadata names.");
        }

        if let (Some(did), Some(vid)) = (dataset_id, variable_id) {
            self.connect(*did, *vid);
        };
    }

    fn connect(&mut self, dataset_id: IpumsDatasetId, variable_id: IpumsVariableId) {
        self.available_variables
            .add_or_update(dataset_id, variable_id);
        self.available_datasets
            .add_or_update(dataset_id, variable_id);
    }

    pub fn add_dataset_variable(&mut self, dataset: IpumsDataset, variable: IpumsVariable) {
        let dataset_name = &dataset.name.clone();
        let variable_name = &variable.name.clone();

        let dataset_id: IpumsDatasetId = if self.datasets_by_name.contains_key(dataset_name) {
            *self.datasets_by_name.get(dataset_name).unwrap()
        } else {
            self.create_dataset(dataset)
        };

        let variable_id: IpumsVariableId = if self.variables_by_name.contains_key(variable_name) {
            *self.variables_by_name.get(variable_name).unwrap()
        } else {
            self.create_variable(variable)
        };
        self.connect(dataset_id, variable_id);
    }
}
/// This is the mutable state  created and passed around holding the loaded metadata if any
/// and the rest of the information needed to add paths to the data tables used in queries
/// and data file paths, and where the metadata can be found.
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
    /// Formats the exact paths needed to get data for this dataset, by record type.
    pub fn path_from_dataset_name(
        &self,
        dataset_name: &str,
        data_format: InputType,
    ) -> HashMap<String, PathBuf> {
        let base_filename = format!("{}_{}", dataset_name, &self.name);
        let extension = match data_format {
            InputType::Csv => ".csv",
            InputType::Parquet => ".parquet",
            InputType::Fw => ".dat.gz",
        };

        // TODO return errors properly
        let data_path = if let Some(ref data_root) = self.data_root {
            PathBuf::from(data_root)
        } else {
            panic!("No data root set.");
        };

        let mut all_paths = HashMap::new();
        match data_format {
            InputType::Csv | InputType::Parquet => {
                for rt in self.settings.record_types.keys() {
                    let full_filename = format!("{}.{}.{}", base_filename, rt, extension);
                    let full_path = data_path.join(full_filename);
                    all_paths.insert(rt.to_string(), full_path);
                }
            }
            _ => {
                let full_filename = format!("{}.{}", base_filename, extension);
                let full_path = data_path.join(full_filename);
                all_paths.insert("".to_string(), full_path);
            }
        }
        all_paths
    }

    /// When called, the context should be already set to read from layouts or full metadata
    pub fn load_metadata_for_datasets(&mut self, datasets: &[&str]) {
        if !self.enable_full_metadata {
            if let Some(ref data_root) = self.data_root {
                self.settings
                    .load_metadata_for_selected_datasets_from_layouts(datasets, &data_root);
            } else {
                panic!("Cannot load any metadata without a data_root or full metadata available ad the product_root.");
            }
        } else {
            panic!("Loading metadata from database not implemented.");
        }
    }

    /// The context should be set to read from layouts or full metadata
    pub fn load_metadata_for_datasets_and_variables(
        &mut self,
        datasets: Vec<String>,
        variables: Vec<String>,
    ) {
        if !self.enable_full_metadata {
        } else {
        }
    }

    /// Based on name, use default data root and product root and initialize with defaults
    /// Optional data root and product root will be used if provided.
    pub fn from_ipums_collection_name(
        name: &str,
        other_product_root: Option<String>,
        other_data_root: Option<String>,
    ) -> Self {
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
        Self {
            name: name.to_string(),
            product_root: Some(product_root),
            data_root: Some(data_root),
            settings: defaults::defaults_for(name),
            allow_full_metadata,
            enable_full_metadata: false,
        }
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
mod test {
    use super::*;

    #[test]
    pub fn test_context() {
        // Look in test directory
        let data_root = Some(String::from("test/data_root"));
        let mut usa_ctx = Context::from_ipums_collection_name("usa", None, data_root);
        assert!(
            usa_ctx.allow_full_metadata,
            "Default allow_full_metadata should be false"
        );
        assert!(usa_ctx.product_root.is_some());
        assert!(usa_ctx.data_root.is_some());
        assert_eq!("USA".to_string(), usa_ctx.settings.name);
    }
}
