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
    fn load_metadata_from_layout(&mut self, layout_file: &Path) {}

    /// Read all layout files for the data root like ../output_data/current/layouts
    /// The existence of a layout file implies existence of a dataset. The presence of
    /// a variable in a dataset's layout indicates availability in that dataset.
    fn load_metadata_from_all_layouts(&mut self, layouts_dir: &Path) {
        ()
    }

    /// The path like ../output_data/current/parquet/us2019a/
    /// Reading the schema will give approximately the same metadata information
    /// as reading the fixed-width layout file for the same dataset.
    fn load_metadata_from_parquet(&mut self, parquet_dataset_path: &Path) {}

    /// Using the data_root, scan the layouts and load metadata from them.
    pub fn load_metadata_for_datasets(&mut self, datasets: &Vec<String>) {}

    /// Uses default product_root to find metadata database and load all metadata for given datasets.
    pub fn load_full_metadata_for_datasets(&mut self, datasets: &Vec<String>) {}

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
    pub datasets_by_name: HashMap<String, usize>,
    pub variables_by_name: HashMap<String, usize>,
    // The valid cross-products
    pub available_variables: VariablesForDataset,
    pub available_datasets: DatasetsForVariable,

    // The owning structs
    pub variables_index: Vec<IpumsVariable>,
    pub datasets_index: Vec<IpumsDataset>,
}

impl MetadataEntities {
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

// There is a master Vec with Variables by IpumsVariableId this structure points into.
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
        if self.ipums_variables_by_dataset_id.len() - 1 < dataset_id {
            self.ipums_variables_by_dataset_id.push(HashSet::new());
        }
        self.ipums_variables_by_dataset_id[dataset_id].insert(variable_id);
    }

    pub fn for_dataset(&self, dataset_id: IpumsDatasetId) -> Option<&HashSet<IpumsVariableId>> {
        self.ipums_variables_by_dataset_id.get(dataset_id)
    }
}

// There's a master Vec of datasets this structures points into:
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
        if self.ipums_datasets_by_variable_id.len() - 1 < variable_id {
            self.ipums_datasets_by_variable_id.push(HashSet::new());
        }
        self.ipums_datasets_by_variable_id[dataset_id].insert(dataset_id);
    }

    pub fn for_variable(&self, var_id: IpumsVariableId) -> Option<&HashSet<IpumsDatasetId>> {
        self.ipums_datasets_by_variable_id.get(var_id)
    }
}

impl MetadataEntities {
    pub fn add_dataset_variable(&mut self, dataset: IpumsDataset, variable: IpumsVariable) {
        let dataset_name = &dataset.name.clone();
        let variable_name = &variable.name.clone();

        let dataset_id: IpumsDatasetId = if self.datasets_by_name.contains_key(dataset_name) {
            *self.datasets_by_name.get(dataset_name).unwrap()
        } else {
            self.datasets_index.push(dataset);
            let new_id: IpumsDatasetId = self.datasets_index.len();
            self.datasets_by_name.insert(dataset_name.clone(), new_id);
            new_id
        };

        let variable_id: IpumsVariableId = if self.variables_by_name.contains_key(variable_name) {
            *self.variables_by_name.get(variable_name).unwrap()
        } else {
            self.variables_index.push(variable);
            let new_id: IpumsVariableId = self.variables_index.len();
            self.variables_by_name.insert(variable_name.clone(), new_id);
            new_id
        };
        self.available_variables
            .add_or_update(dataset_id, variable_id);
        self.available_datasets
            .add_or_update(dataset_id, variable_id);
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
        let mut usa_ctx = Context::default_from_name("usa");
        assert!(
            usa_ctx.allow_full_metadata,
            "Default allow_full_metadata should be false"
        );
        assert!(usa_ctx.product_root.is_some());
        assert!(usa_ctx.data_root.is_some());
        assert_eq!("USA".to_string(), usa_ctx.settings.name);
    }
}
