use crate::defaults;
use crate::ipums_data_model::*;
use crate::ipums_metadata_model::*;
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
    pub fn load_metadata_from_layout(&mut self, layout_file: &Path) {}

    /// Read all layout files for the data root like ../output_data/current/layouts
    /// The existence of a layout file implies existence of a dataset. The presence of
    /// a variable in a dataset's layout indicates availability in that dataset.
    pub fn load_metadata_from_all_layouts(&mut self, layouts_dir: &Path) {
        ()
    }

    /// The path like ../output_data/current/parquet/us2019a/
    /// Reading the schema will give approximately the same metadata information
    /// as reading the fixed-width layout file for the same dataset.
    pub fn load_metadata_from_parquet(&mut self, parquet_dataset_path: &Path) {}

    /// Takes a path like ../output_data/current/parquet/, which could be derived
    /// automatically from defaults based on data root or product root. Scans all
    /// parquet schema information.
    pub fn load_metadata_from_all_parquet(&mut self, parquet_path: &Path) {}

    /// Load everything available for the selected variables and samples from the available
    /// metadata database file. Requires 'allow_full_metadata' which depends on a product root
    /// and a 'metadata.db' file located in the root/metadata/versions location, unless you provide
    /// a Some(metadata_location).
    pub fn load_full_metadata_for_selections(
        &mut self,
        variables: &Vec<String>,
        datasets: &Vec<String>,
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
    // Name -> Id
    pub datasets_by_name: Option<HashMap<String, usize>>,
    pub variables_by_name: Option<HashMap<String, usize>>,
    // The valid cross-products
    pub available_variables: VariablesForDataset,
    pub available_datasets: DatasetsForVariable,

    // The owning structs
    pub variables_index: Option<Vec<IpumsVariable>>,
    pub datasets_index: Option<Vec<IpumsDataset>>,
}

// There is a master Vec with Variables by IpumsVariableId this structure points into.
pub struct VariablesForDataset {
    ipums_variables_by_dataset_id: Vec<HashSet<IpumsVariableId>>,
}

impl VariablesForDataset {
    pub fn for_dataset(&self, dataset_id: IpumsDatasetId) -> Option<&HashSet<IpumsVariableId>> {
        self.ipums_variables_by_dataset_id.get(dataset_id)
    }
}

// There's a master Vec of datasets this structures points into:
pub struct DatasetsForVariable {
    ipums_datasets_by_variable_id: Vec<HashSet<IpumsDatasetId>>,
}

impl DatasetsForVariable {
    pub fn for_variable(&self, var_id: IpumsVariableId) -> Option<&HashSet<IpumsDatasetId>> {
        self.ipums_datasets_by_variable_id.get(var_id)
    }
}
// This is the mutable state  created and passed around holding the loaded metadata if any
// and the rest of the information needed to add paths to the data tables used in queries
// and data file paths, and where the metadata can be found.
pub struct Context {
    pub name: String, // A product name like USA, IPUMSI, CPS etc
    // A path name
    // Like /pkg/ipums/usa with ./metadata and ./output_data in it
    pub product_root: Option<PathBuf>,
    // Any output_data/current path with ./layouts and ./parquet in it
    pub data_root: Option<PathBuf>,
    pub settings: MicroDataCollection,
    pub allow_full_metadata: bool,
}

impl Context {
    // Based on name, use default data root and product root and initialize with defaults
    pub fn default_from_name(name: &str) -> Self {
        let product_root: PathBuf = PathBuf::from(format!("/pkg/ipums/{}", &name));
        let allow_full_metadata = product_root.exists();
        Self {
            name: name.to_string(),
            product_root: Some(product_root.clone()),
            data_root: Some(PathBuf::from(format!(
                "{}/output_data/current",
                product_root.to_str().unwrap()
            ))),
            settings: defaults::defaults_for(name),
            allow_full_metadata,
        }
    }

    /*
    // Give the path like '/pkg/ipums/usa'
    pub fn default_from_product_root(product_path: &str) -> Self {

    }

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
