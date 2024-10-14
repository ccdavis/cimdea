//! A command line program showing how to use the library. This example program takes variable and sample names and returns a formatted cross-tabulation of the data.

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
use request::AbacusRequest;
use request::DataRequest;
use tabulate::TableFormat;

use clap::Parser;
use std::collections::HashMap;
use std::sync::Mutex;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct CliRequest {
    pub sample_name: String,
    pub product_name: String,
    pub variable_names: Vec<String>,

    #[arg(short, long, default_value = "text")]
    pub format: String,
}

use std::io::{self, BufRead};

fn get_from_stdin() -> String {
    let stdin = io::stdin();
    let mut lines = stdin.lock().lines();
    let data = match lines.collect::<Result<Vec<String>, _>>() {
        Ok(lns) => lns.join("\n"),
        Err(ref e) => {
            eprintln!("Error reading from STDIN: '{}'", e);
            std::process::exit(1);
        }
    };
    data
}

fn main() {
    let args = CliRequest::parse();

    // You could set up a context explicitly with a custom data root.
    //let data_root = String::from("test/data_root");
    //let mut ctx =
    //conventions::Context::from_ipums_collection_name(product, None, Some(data_root));

    // Or have the find_by_names construct the default context for the named product and load
    // metadata into that context just for the named metadata on the spot.

    let table_format = match TableFormat::from_str(&args.format) {
        Ok(tf) => tf,
        Err(e) => {
            eprintln!("{}: '{}'", &e, &args.format);
            std::process::exit(1);
        }
    };

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
                println!("{}", table.output(table_format.clone()));
            }
        }
        Err(e) => {
            eprintln!("Error trying to tabulate: {}", &e);
        }
    }
}
