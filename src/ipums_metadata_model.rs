use std::collections::HashMap;
use std::collections::HashSet;

pub type IpumsDatasetId = usize;
pub struct IpumsDataset {
    name: String,
    year: Option<usize>,
    month: Option<usize>,
    label: Option<String>,
    sample: Option<f64>,
    id: IpumsDatasetId, // auto-assigned in order loaded
}

pub type IpumsVariableId = usize;
pub struct IpumsVariable {
    name: String,
    data_type: IpumsDataType,
    label: Option<String>,
    record_type: String, // a value like 'H', 'P'
    categories: Option<Vec<IpumsCategory>>,
    formatting: Option<(usize, usize)>,
    id: IpumsVariableId, // auto-assigned in load order
}

// There is a master Vec with Variables by IpumsVariableId this structure points into.
pub struct VariablesForDataset {
    ipums_variables_by_dataset_id: Vec<HashSet<IpumsVariableId>>,
}

// There's a master Vec of datasets this structures points into:
pub struct DatasetsForVariable {
    ipums_datasets_by_variable_id: Vec<HashSet<IpumsDatasetId>>,
}

pub enum IpumsDataType {
    Integer,
    Float,
    String,
    Fixed(usize),
}

// The Float is a chunk of 8-bit ASCII because it needs to represent a literal
//representation of a float that could be 64, 80 or 128 bits. We aren't expecting
// to do math with it but we do need to precisely preserve the original format.
// The String type is a u8 Vec, not UTF-8 because some old data files use
// fixed-width data formats (normally ISO 8859-1). These "IPUMS values" must match
// exactly values found in data. All other labels and metadata uses UTF-8.
// When data comes from Parquet or other modern formats the String will be UTF-8.
#[derive(Clone, Debug, PartialEq)]
pub enum IpumsValue {
    Integer(i64),
    Float(ascii::AsciiString),
    String { utf8: bool, value: Vec<u8> },
    Fixed { point: usize, base: usize },
}

pub enum UniversalCategoryType {
    NotInUniverse,
    Missing,
    NotApplicable,
    TopCode,
    BottomCode,
}

type IpumsCategoryId = usize;
pub struct IpumsCategory {
    pub label: String,
    pub meaning: UniversalCategoryType,
    pub value: IpumsValue,
    id: IpumsCategoryId,
}
