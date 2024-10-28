use std::io::{self, BufRead};

use cimdea::conventions::Context;
use cimdea::request::{AbacusRequest, DataRequest, SimpleRequest};
use cimdea::tabulate;
use cimdea::tabulate::TableFormat;

use clap::{Args, Parser, Subcommand};

fn get_from_stdin() -> String {
    let stdin = io::stdin();
    let lines = stdin.lock().lines();
    let data = match lines.collect::<Result<Vec<String>, _>>() {
        Ok(lns) => lns.join("\n"),
        Err(ref e) => {
            eprintln!("Error reading from STDIN: '{}'", e);
            std::process::exit(1);
        }
    };
    data
}

fn abacus_request_from_str(rq: &str) -> (Context, AbacusRequest) {
    match AbacusRequest::from_json(rq) {
        Err(e) => {
            eprintln!("Error parsing input JSON: '{}'", &e);
            std::process::exit(1);
        }
        Ok((ctx, ar)) => (ctx, ar),
    }
}

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct CliRequest {
    #[command(subcommand)]
    command: CliCommand,

    /// The path to an output file [default: write to stdout]
    #[arg(short, long, global = true)]
    output: Option<String>,

    /// The output format
    #[arg(short, long, global = true, default_value = "text")]
    format: TableFormat,
}

#[derive(Debug, Subcommand)]
enum CliCommand {
    /// Compute a tabulation of one or more variables from an IPUMS microdata output sample
    Tab(TabArgs),
    /// Given a JSON Abacus request, compute the tabulation it describes
    Request(RequestArgs),
}

#[derive(Args, Debug)]
struct TabArgs {
    /// The name of the product (e.g. usa or ipumsi)
    product: String,
    /// The name of the sample (e.g. us2015b or mx2016h)
    sample: String,
    /// One or more variables to tabulate (e.g. AGE or MARST)
    variables: Vec<String>,
}

#[derive(Args, Debug)]
struct RequestArgs {
    /// The path to the input JSON file [default: read from stdin]
    input_file: Option<String>,
}

fn main() {
    let args = CliRequest::parse();

    let (context, request): (_, Box<dyn DataRequest>) = match args.command {
        CliCommand::Request(request_args) => {
            let input = match request_args.input_file {
                None => get_from_stdin(),
                Some(file) => match std::fs::read_to_string(&file) {
                    Ok(j) => j,
                    Err(e) => {
                        eprintln!("Can't access Abacus request file: '{}'", e);
                        std::process::exit(1);
                    }
                },
            };

            let (context, request) = abacus_request_from_str(&input);
            (context, Box::new(request))
        }
        CliCommand::Tab(tab_args) => {
            let variables: Vec<_> = tab_args.variables.iter().map(|v| v.as_str()).collect();
            match SimpleRequest::from_names(
                &tab_args.product,
                &[&tab_args.sample],
                variables.as_slice(),
                None,
                None,
                None,
            ) {
                Ok((context, request)) => (context, Box::new(request)),
                Err(err) => {
                    eprintln!("Error while setting up tabulation: {err}");
                    std::process::exit(1);
                }
            }
        }
    };

    match tabulate::tabulate(&context, request.as_ref()) {
        Ok(tables) => {
            // Print a JSON array and separate table objects with ',' if more than one in
            // the output.
            println!("[\n");
            for (table_number, table) in tables.iter().enumerate() {
                if table_number > 0 {
                    println!(",");
                }
                println!(
                    "{}",
                    table
                        .output(args.format)
                        .expect("error while writing output")
                );
            }
            println!("\n]\n");
        }
        Err(e) => {
            eprintln!("Error trying to tabulate: {}", &e);
        }
    }
}
