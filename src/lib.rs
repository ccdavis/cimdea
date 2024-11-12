//! # CIMDEA: Convenient IPUMS Microdata Extraction and Aggregation
//!
//! ## Computing a Tabulation
//!
//! The simplest way to create a tabulation request is with the aptly named
//! [SimpleRequest](request::SimpleRequest). The [from_names](request::DataRequest::from_names)
//! function supports creating a request from product, dataset, and variable names. `from_names`
//! returns a context with loaded metadata and a `SimpleRequest`.
//!
//! ```
//! use cimdea::request::{DataRequest, SimpleRequest};
//!
//! # let data_root = "tests/data_root/".to_string();
//! // Set data_root to the directory with your input data.
//! // let data_root: String = ...;
//! let (ctx, rq) = SimpleRequest::from_names(
//!     "usa",
//!     &["us2015b"],
//!     &["MARST"],
//!     None,
//!     None,
//!     Some(data_root),
//! ).unwrap();
//! ```
//!
//! Once you have a [Context](conventions::Context) and a type like `SimpleRequest` which
//! implements [DataRequest](request::DataRequest), you can pass them to the
//! [tabulate](tabulate::tabulate) function to compute the tabulation which the `DataRequest`
//! defines.
//!
//! ```
//! # use cimdea::request::{DataRequest, SimpleRequest};
//! use cimdea::tabulate::{self, TableFormat};
//! # let data_root = "tests/data_root/".to_string();
//! # let (ctx, rq) = SimpleRequest::from_names(
//! #     "usa",
//! #     &["us2015b"],
//! #     &["MARST"],
//! #     None,
//! #     None,
//! #     Some(data_root),
//! # ).unwrap();
//! 
//! let tab = tabulate::tabulate(&ctx, rq).unwrap();
//! let json = tab.output(TableFormat::Json).unwrap();
//! ```
//!
//! For more complex requests which need to use features like general versions of
//! variables, subpopulations, or category bins, please see
//! [AbacusRequest](request::AbacusRequest), which also implements `DataRequest`.

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
