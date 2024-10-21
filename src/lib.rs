//! CIMDEA == Convenientt IPUMS Microdata Extraction and Aggregation

pub mod conventions;
pub mod defaults;
pub mod input_schema_tabulation;
pub mod ipums_data_model;
pub mod ipums_metadata_model;
pub mod layout;
pub mod mderror;
pub mod query_gen;
pub mod request;
pub mod tabulate;

use interner::global::{GlobalPool, GlobalString};
static STRINGS: GlobalPool<String> = GlobalPool::new();
