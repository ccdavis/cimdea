use crate::query_gen::Condition;


pub trait DataRequest {
    fn extract_query(&self) -> String;
    fn aggregate_query(&self) -> String; 
    fn serialize_to_IPUMS_JSON(&self) -> String;
    fn print_codebook(&self) -> String;
    fn print_stata(&self) -> String;
}

/// In a ComplexRequest Variables could have attached variables or monetary standardization adjustment factors,
/// datasets could have sub-sample sizes or other attrributes. Here with a SimpleRequest we're requesting either a tabulation from
/// the given sources or an extract of data of same.
pub struct SimpleRequest {
    pub variables: Vec<String>,
    pub datasets: Vec<String>,
    pub request_type: RequestType,
    pub output_format: OutputFormat,
    pub conditions: Option<Vec<Condition>>,
}

pub enum RequestType {
    Tabulation,
    Extract,    
}

pub enum OutputFormat {
    CSV,
    FW,
}