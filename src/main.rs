mod conventions;
mod defaults;
mod ipums_data_model;
mod ipums_metadata_model;
mod layout;
mod query_gen;
mod request;
mod tabulate;

use conventions::*;
use query_gen::*;
use request::DataRequest;
use std::collections::HashMap;
use std::sync::Mutex;

fn main() {
    let product = "usa";
    let requested_datasets = ["us2015a","us2016a"];
    let requested_variables = ["RELATE","GQ"];

    // You could set up a context explicitly with a custom data root.
    //let data_root = String::from("test/data_root");
    //let mut ctx =
        //conventions::Context::from_ipums_collection_name(product, None, Some(data_root));

        // Or have the find_by_names construct the default context for the named product and load
        // metadata into that context just for the named metadata on the spot.
        let (context, rq) = request::SimpleRequest::from_names(product, &requested_datasets, &requested_variables, None, None);
        match tabulate::tabulate(&ctx, &rq) {
            Ok(table) => println!("{}", table.output(TableFormat::TextTable)),
            Err(e) => {
                eprintln!("Error trying to tabulate: {}",&e);
            }
        }
}