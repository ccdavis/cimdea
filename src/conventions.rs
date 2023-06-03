use std::collections::HashMap;

use crate::ipums_data_model::*;

use crate::ipums_metadata_model::*;

// Key characteristics of collections like all USA Census data, all Time-Use Survey data etc.
pub struct MicroDataCollection {
    pub name: String, // Like USA, IPUMSI, ATUS
    pub record_hierarchy: RecordHierarchy,
    pub record_types: HashMap<String, RecordType>, // key is value: 'H', 'P' etc

    // Optional
    // The HashMap is for lookup by name, the values are indices into
    // the _index vecs. Thos can be accessed directly with the
    // assigned ID of the variable or datasets.
    pub datasets: Option<HashMap<String, usize>>,
    pub variables: Option<HashMap<String, usize>>,
    pub variables_index: Option<Vec<IpumsVariable>>,
    pub datasets_index: Option<Vec<IpumsDataset>>,
}

use lazy_static::*;

lazy_static! {}
