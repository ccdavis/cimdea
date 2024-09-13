#[allow(unused)]
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
use tabulate::TableFormat;

use clap::Parser;
use std::collections::HashMap;
use std::sync::Mutex;

#[derive(Parser, Debug)]
struct CliRequest {
    pub sample_name: String,
    pub product_name: String,
    pub variable_names: Vec<String>,
}

fn main() {
    let args = CliRequest::parse();

    println!("Parsed args: {:?}", &args);

    // You could set up a context explicitly with a custom data root.
    //let data_root = String::from("test/data_root");
    //let mut ctx =
    //conventions::Context::from_ipums_collection_name(product, None, Some(data_root));

    // Or have the find_by_names construct the default context for the named product and load
    // metadata into that context just for the named metadata on the spot.

    let variable_names: Vec<&str> = args.variable_names.iter().map(|v| &**v).collect();

    let (context, rq) = request::SimpleRequest::from_names(
        &args.product_name,
        &[&args.sample_name],
        &variable_names,
        None,
        None,
        None,
    );
    match tabulate::tabulate(&context, rq) {
        Ok(tables) => {
            for table in tables {
                println!("{}", table.output(TableFormat::TextTable));
            }
        }
        Err(e) => {
            eprintln!("Error trying to tabulate: {}", &e);
        }
    }
}
