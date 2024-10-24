//! The "metadata" models serve to assist working with IPUMS data. The entities here match the full IPUMS metadata in terms
//!  of their relationships to one another and their description of the data.
//!
//! However they don't include (1) all fields / pieces
//! of info from the full IPUMS metadata; and (2) do not contain all metadata models -- only those essential for understanding
//! the data files on a technical level. For instance there are no enumeration text or citations metadata modeled here.
//!
//! In addition, these models are intended to support working with data in a "low", "medium" or full metadata environment, hence
//! the numerous fields of Option type. Essential operations must mostly be possible with None values of these fields.
//!
//! ## Low metadata environment
//!
//! We must have a path to an IPUMS data file. The file is conventionally named i.e. `us2019a_usa.data.gz`
//! or `parquet/us2019a/*.parquet`. IPUMS data is stored in multiple parquet datasets, one per record type within a directory
//! named after the IPUMS dataset. The project can be known from context or from the individual parquet files with the`_usa`
//! (for instance) part of the name. For fixed-width compressed data all record types are in a single file; the dataset name is
//! the first part of the file name and the project once again is from context or the `_usa`-like part of the name.
//! Variable names and which variables belong to which dataset are either determined by the parquet schema or the fixed-width
//! layout file which is always in a `layouts/` subdirectory under the directory with the fixed-width data. Layout files look
//! like `layouts/us2019a.layout.txt`.
//!  */
//! This is enough information to know the data types of IPUMS variables, and what variables belong to what dataset and what
//! their record types are.
//!
//! ## Medium Metadata
//!
//! This is achieved either by some access to a metadata database, or extended metadata stored in the Parquet key-value metadata.
//!
//! Metadata can come from a database  for which the schema is known (there is a "raw export" schema, and a fully normalized and
//! cleaned schema that drives the IPUMS websites at IPUMS.) Or metadata in the future may come from Parquet
//! key-value file metadata.
//!
//! The extended key-value metadata is under development currently. At the least, there will be variable labels (short descriptions),
//! metadata version and data versions (for archival / reproducibility purposes). It's also possible variable category (value)
//! labels may be included. These would be used with the understanding that they represent the labels at the time of data creation
//! and can't reflect the latest public revisions to IPUMS metadata. Additionally, relationships to "flag" (data quality) variables,
//! extended weight variables may be included as well. While the metadata may not be suitable for a live documentation or extraction
//! service they can be extremely useful for building simplified tools that require this core metadata.
//!
//! ## Full Metadata
//!
//! Full metadata requires access to the IPUMS metadata and some modeling of all the entities. Full access would allow populating
//! every field in these models and would allow mmodeling much more than this module currently does. Full metadata won't be required
//! for any main operations in this library but would enable access to the most up-to-date versions of documentation-like
//! information such as variable and value labels.
//!
//!
use crate::input_schema_tabulation::CategoryBin;
use crate::layout::LayoutVar;
use std::fmt;

use compressed_string::ComprString;
use interner::global::{GlobalPool, GlobalString};

static STRINGS: GlobalPool<String> = GlobalPool::new();

pub type IpumsDatasetId = usize;
#[derive(Clone, Debug)]
pub struct IpumsDataset {
    pub name: String,
    pub year: Option<usize>,
    pub month: Option<usize>,
    pub label: Option<String>,
    pub sample: Option<f64>,
    /// The 'id' fields in the models are generated when metadata structs get instantiated in order. They are
    /// used for indexing into the metadata storage.
    pub id: IpumsDatasetId, // auto-assigned in order loaded
}

impl From<(String, usize)> for IpumsDataset {
    fn from(value: (String, usize)) -> Self {
        Self {
            name: value.0,
            id: value.1,
            year: None,
            month: None,
            label: None,
            sample: None,
        }
    }
}

pub type IpumsVariableId = usize;
#[derive(Clone, Debug)]
pub struct IpumsVariable {
    pub name: String,
    pub data_type: Option<IpumsDataType>,
    pub label: Option<String>,
    pub record_type: String, // a value like 'H', 'P'
    pub categories: Option<Vec<IpumsCategory>>,
    pub formatting: Option<(usize, usize)>,
    pub general_width: usize,
    pub description: Option<ComprString>,
    pub category_bins: Option<Vec<CategoryBin>>,
    pub id: IpumsVariableId, // auto-assigned in load order
}

impl From<(&LayoutVar, usize)> for IpumsVariable {
    fn from(value: (&LayoutVar, usize)) -> Self {
        Self {
            id: value.1,
            name: value.0.name.clone(),
            record_type: value.0.rectype.clone(),
            data_type: Some(value.0.data_type.clone()),
            label: None,
            categories: None,
            category_bins: None,
            formatting: Some((value.0.start, value.0.width)),
            general_width: value.0.width,
            description: None,
        }
    }
}

#[derive(Clone, Debug)]
pub enum IpumsDataType {
    Integer,
    Float,
    String,
    Fixed(usize),
}

impl fmt::Display for IpumsDataType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            IpumsDataType::Integer => write!(f, "integer"),
            IpumsDataType::Fixed(_) => write!(f, "fixed"),
            IpumsDataType::Float => write!(f, "double"),
            IpumsDataType::String => write!(f, "string"),
        }
    }
}

impl From<&str> for IpumsDataType {
    fn from(value: &str) -> Self {
        match value.to_ascii_lowercase().as_str() {
            "fixed" => Self::Fixed(0),
            "string" => Self::String,
            "double" => Self::Float,
            "float" => Self::Float,
            "integer" => Self::Integer,
            _ => Self::Integer,
        }
    }
}

// The Float is a String because it needs to represent a literal
//representation of a float that could be 64, 80 or 128 bits. We aren't expecting
// to do math with it but we do need to precisely preserve the original format.
// The String type is a u8 Vec, not UTF-8 because some old data files use
// fixed-width data formats (normally ISO 8859-1). These "IPUMS values" must match
// exactly values found in data. All other labels and metadata uses UTF-8.
// When data comes from Parquet or other modern formats the String will be UTF-8.
#[derive(Clone, Debug, PartialEq)]
pub enum IpumsValue {
    Integer(i64),
    Float(String),
    String { utf8: bool, value: Vec<u8> },
    Fixed { point: usize, base: usize },
}
#[derive(Clone, Debug)]
pub enum UniversalCategoryType {
    NotInUniverse,
    Missing,
    NotApplicable,
    TopCode,
    BottomCode,
    Value,
}

type IpumsCategoryId = usize;

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub struct IpumsCategory {
    label_intern: GlobalString,
    pub meaning: UniversalCategoryType,
    pub value: IpumsValue,
    id: IpumsCategoryId,
}

impl IpumsCategory {
    pub fn label(&self) -> &str {
        self.label_intern.as_ref()
    }

    pub fn new(label: &str, meaning: UniversalCategoryType, value: IpumsValue) -> Self {
        let symbol: GlobalString = STRINGS.get(label);
        Self {
            label_intern: symbol,
            meaning,
            value,
            id: 0,
        }
    }
}

mod test {
    #[cfg(test)]
    use super::*;

    #[test]
    pub fn test_category_labels() {
        let cat1 = IpumsCategory::new(
            "first",
            UniversalCategoryType::Value,
            IpumsValue::Integer(1),
        );

        assert_eq!(
            cat1.label(),
            "first",
            "label() method should return a &str with the label."
        );
        let cat2 = IpumsCategory::new(
            "second",
            UniversalCategoryType::Value,
            IpumsValue::Integer(1),
        );

        let label1 = cat1.label();
        let label2 = cat2.label();
        assert_ne!(label1, label2);

        let cat3 = IpumsCategory::new(
            "second",
            UniversalCategoryType::Value,
            IpumsValue::Integer(1),
        );

        assert_eq!(cat2.label(), cat3.label());
        assert_eq!("second", cat3.label());
    }
}
