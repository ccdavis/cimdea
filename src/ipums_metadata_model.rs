use std::collections::HashMap;
use std::collections::HashSet;

/// The "metadata" models serve to assist working with IPUMS data. The entities here match the full IPUMS metadata in terms
///  of their relationships to one another and their description of the data. However they don't include (1) all fields / pieces
/// of info from the full IPUMS metadata; and (2) do not contain all metadata models -- only those essential for understanding
/// the data files on a technical level. For instance there are no enumeration text or citations metadata modeled here.
///
/// In addation, these models are intended to support working with data in a "low", "medium" or full metadata environment, hence
/// the numerous fields of Option type. Essential operations must mostly be possible with None values of these fields.
///
/// ## Low metadata environment
///
/// We must have a path to an IPUMS data file. The file is conventionally named i.e. `us2019a_usa.data.gz`
/// or `parquet/us2019a/*.parquet`. IPUMS data is stored in multiple parquet datasets, one per record type within a directory
/// named after the IPUMS dataset. The project can be known from context or from the individual parquet files with the`_usa`
/// (for instance) part of the name. For fixed-width compressed data all record types are in a single file; the dataset name is
/// the first part of the file name and the project once again is from context or the `_usa`-like part of the name.
/// Variable names and which variables belong to which dataset are either determined by the parquet schema or the fixed-width
/// layout file which is always in a `layouts/` subdirectory under the directory with the fixed-width data. Layout files look
/// like `layouts/us2019a.layout.txt`.
///  */
/// This is enough information to know the data types of IPUMS variables, and what variables belong to what dataset and what
/// their record types are.
///
/// ## Medium Metadata
///
/// This is achieved either by some access to a metadata database, or extended metadata stored in the Parquet key-value metadata.
///
/// Metadata can come from a database  for which the schema is known (there is a "raw export" schema, and a fully normalized and cleaned schema that drives the IPUMS websites at IPUMS.)
///
/// The extended key-value metadata is under development currently. At the least, there will be variable labels (short descriptions),
/// metadata version and data versions (for archival / reproducability purposes). It's also possible variable category (value)
/// labels may be included. These would be used with the understanding that they represent the labels at the time of data creation
/// and can't reflect the latest public revisions to IPUMS metadata. Additionally, relationships to "flag" (data quality) variables,
/// extended weight variables may be included as well. While the metadata may not be suitable for a live documentation or extraction
/// service they can be extremely useful for building simplified tools that require this core metadata.
///
/// ## Full Metadata
///
/// Full metadata requires access to the IPUMS metadata and some modeling of all the entities. Full access would allow populating
/// every field in these models and would allow mmodeling much more than this module currently does. Full metadata won't be required
/// for any main operations in this library but would enable access to the most up-to-date versions of documentation-like
/// information such as variable and value labels.
///
///

pub type IpumsDatasetId = usize;
pub struct IpumsDataset {
    pub name: String,
    pub year: Option<usize>,
    pub month: Option<usize>,
    pub label: Option<String>,
    pub sample: Option<f64>,
    /// The 'id' fields in the models are generated when metadata structs get instantiated in order. They are
    /// used for indexing into the metadata storage.
    id: IpumsDatasetId, // auto-assigned in order loaded
}

pub type IpumsVariableId = usize;
pub struct IpumsVariable {
    pub name: String,
    pub data_type: IpumsDataType,
    pub label: Option<String>,
    pub record_type: String, // a value like 'H', 'P'
    pub categories: Option<Vec<IpumsCategory>>,
    pub formatting: Option<(usize, usize)>,
    id: IpumsVariableId, // auto-assigned in load order
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
