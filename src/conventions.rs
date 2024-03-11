use crate::defaults;
use crate::ipums_data_model::*;
use crate::ipums_metadata_model::*;
use std::collections::HashMap;
use std::collections::HashSet;

// Key characteristics of collections like all USA Census data, all Time-Use Survey data etc.
pub struct MicroDataCollection {
    pub name: String, // Like USA, IPUMSI, ATUS
    pub record_hierarchy: RecordHierarchy,
    pub record_types: HashMap<String, RecordType>, // key is value: 'H', 'P' etc
    pub metadata: Option<MetadataEntities>,
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
    pub product_root: Option<String>,
    // Any output_data/current path with ./layouts and ./parquet in it
    pub data_root: Option<String>,
    pub settings: MicroDataCollection,
}

impl Context {
    // Based on name, use default data root and product root and initialize with defaults
    pub fn default_from_name(name: &str) -> Self {
        let product_root = format!("/pkg/ipums/{}", &name);
        Self {
            name: name.to_string(),
            product_root: Some(product_root.clone()),
            data_root: Some(format!("{}/output_data/current", &product_root)),
            settings: defaults::defaults_for(name),
        }
    }

    /*
    // Give the path like '/pkg/ipums/usa'
    pub fn default_from_product_root(product_path: &str) -> Self {

    }

    pub fn from_name_and_data_root(name: &str, data_root: &str) -> Self {
    }
    */
}
