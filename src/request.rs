use crate::query_gen::Condition;


// In a ComplexRequest Variables could have attached variables or monetary standardization adjustment factors,
// datasets could have sub-sample sizes or other attrributes. Here we're requesting either a tabulation from
// the given sources or an extract of data of same.
pub struct SimpleRequest {
    pub variables: Vec<String>,
    pub datasets: Vec<String>,
    pub request_type: RequestType,
    pub conditions: Option<Vec<Condition>>,
}

pub enum RequestType {
    Tabulation,
    Extract,    
}