//! A command line program showing how to use the library. This example program takes variable and sample names and returns a formatted cross-tabulation of the data.
#[allow(unused)]
use std::str::FromStr;

use cimdea::conventions::*;
use cimdea::request::DataRequest;
use cimdea::request::SimpleRequest;
use cimdea::tabulate;
use cimdea::tabulate::TableFormat;
use clap::Parser;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct CliRequest {
    pub sample_name: String,
    pub product_name: String,
    pub variable_names: Vec<String>,

    #[arg(short, long, default_value = "text")]
    pub format: String,
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
    match SimpleRequest::from_names(
        &args.product_name,
        &[&args.sample_name],
        &variable_names,
        None,
        None,
        None,
    ) {
        Ok((context, rq)) => match tabulate::tabulate(&context, &rq) {
            Ok(tables) => {
                for table in tables {
                    println!(
                        "{}",
                        table
                            .output(table_format.clone())
                            .expect("error while writing output")
                    );
                }
            }
            Err(e) => {
                eprintln!("Error trying to tabulate: {}", &e);
            }
        },
        Err(e) => {
            eprintln!("Error trying to tabulate during setup: {}", &e);
        }
    } // match
}
