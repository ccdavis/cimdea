//! CIMDEA == Convenientt IPUMS Microdata Extraction and Aggregation

pub mod conventions;
pub mod defaults;
pub mod fixed_width;
pub mod input_schema_tabulation;
pub mod ipums_data_model;
pub mod ipums_metadata_model;
pub mod layout;
pub mod mderror;
pub mod query_gen;
pub mod request;
pub mod tabulate;

// TODO: I have an idea for how to use this interner library. 
//use interner::global::{GlobalPool, GlobalString};
// static _STRINGS: GlobalPool<String> = GlobalPool::new();
