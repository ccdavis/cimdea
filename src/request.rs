use crate::query_gen::Condition;
use crate::ipums_metadata_model::*;



pub trait DataRequest {
    fn extract_query(&self) -> String;
    fn aggregate_query(&self) -> String; 
    fn serialize_to_IPUMS_JSON(&self) -> String;
    fn deserialize_from_ipums_json(json_request: &str) -> Self;
    fn from_simple_request(variable_names:&[&str], dataset_names: &[&str]);
    fn print_codebook(&self) -> String;
    fn print_stata(&self) -> String;
}



pub enum RequestType {
    Tabulation,
    Extract,    
}

pub enum OutputFormat {
    CSV,
    FW,
}

/// In a ComplexRequest, Variables could have attached variables or monetary standardization adjustment factors,
/// datasets could have sub-sample sizes or other attrributes. Here with a SimpleRequest we're requesting either a tabulation from
/// the given sources or an extract of data of same.
/// 
/// When constructing a request or simple request, we may begin with only variable names and dataset names. We must have a minimum
/// set of metadata to build the IpumsVariable and IpumsDataset values out of those names. The IPUMS conventions combined with 
/// data file metadata (Parquet) or IPUMS fixed-width layout files will have enough metadata. If we have access to the IPUMS
/// metadata database the IpumsVariable and IpumsDataset values can be enriched with category labels, variable labels and extra
/// dataset information.
pub struct SimpleRequest {
    pub variables: Vec<IpumsVariable>,
    pub datasets: Vec<IpumsDataset>,
    pub request_type: RequestType,
    pub output_format: OutputFormat,
    pub conditions: Option<Vec<Condition>>,
}
// The new() and some setup stuff is particular to the SimpleRequest or the more complex types of requests.
impl SimpleRequest {
    pub fn from_names(product: &str, datasets: &Vec<String>,variables: &Vec<String>)-> Self {
        // Need the product to retrieve default settings and possibly load metadata
        
        
        // Get variables from metadata

        // get datasets from metadata
        Self {
            datasets: Vec::new(),
            variables: Vec::new(),
            request_type: RequestType::Tabulation,
            output_format: OutputFormat::CSV,
            conditions: None,
        }
    }

}
