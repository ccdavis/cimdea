use polars::prelude::DataFrameJoinOps;
use sql_builder::{SqlBuilder, prelude::Bind};

// Instead of the DB specific query builders and parameter binding, see if we can do it in a generic way
// TODO For Duck DB we need the table name if it's parquet to looke like ` 'table_name.parquet' ` and it needs to be a valid
// file, or if the parquet is multiple files it would look like ` table_name/*.parquet' `. /
//
// The IPUMS conventions have been applied earlier; the table / filenames have been checked and determined and
// weight variables have been checked. We're assuming inputs here are valid.

// TODO not yet dealing with escaping string values
pub enum CompareOperation {
    Equal,
    Less,
    Greater,
    LessEqual,
    GreaterEqual,
    Between,
    In,
}


pub enum IpumsDataType {
    Integer,
    Float,
    String,
    Fixed(usize)
}

pub struct Condition {
    pub var: String,    
    pub data_type: IpumsDataType,
    pub comparison: CompareOperation,    
    pub compare_to: Vec<String>, // one or more values depending on context
}

impl Condition {
    pub fn new(var: &str, data_type: IpumsDataType, comparison: CompareOperation, compare_to: Vec<String>) -> Self {        
        // TODO check with data type and compare_to for a  valid representation (parse  into i32 for example)
        // If values are string type add appropriate escaping and quotes (possibly)
        Self {
            var: var.to_string(),
            data_type,
            comparison,
            compare_to
        }

    }


}


    

pub fn frequency(table_name: &str, variable_name: &str, weight: Option<String>, divisor: Option<usize>) -> String {
    // frequency field will differ if we are weighting and if there's a divisor
    let freq_field: String = if let Some(w) = weight {
        if let Some(d) = divisor {
            format!("sum({} / {})", &w, d)
        } else {
            format!("sum({} )", &w)
        }
    } else {
        "count(*)".to_string()
    } + " as frequency";

    SqlBuilder::select_from(table_name)
        .field(":var")
        .field(":freq")
        .group_by(":var")
    .sql().unwrap()
        .bind_name(&"var", &variable_name)
        .bind_name(&"freq", &freq_field)
}

// A generalization of frequency()
pub fn cross_tab(tables: &[&str], vars: &[&str], weight: Option<String>, divisor: Option<usize>) -> String {
    "Not implemented".to_string()
}

pub fn cross_tab_subpopulation(tables: &[&str], vars: &[&str], weight: Option<String>, divisor: Option<usize>, subpop: &[Condition]) -> String {
    "Not implemented".to_string()
}
