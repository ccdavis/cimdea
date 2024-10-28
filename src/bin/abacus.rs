use std::fs::File;
use std::io::{self, BufRead, Write};

use cimdea::conventions::Context;
use cimdea::request::{AbacusRequest, DataRequest, SimpleRequest};
use cimdea::tabulate::{self, TableFormat};

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

    let result = match args.command {
        CliCommand::Request(request_args) => {
            let input = match request_args.input_file {
                None => get_from_stdin(),
                Some(file) => match std::fs::read_to_string(&file) {
                    Ok(j) => j,
                    Err(err) => {
                        eprintln!("Can't access Abacus request file: {err}");
                        std::process::exit(1);
                    }
                },
            };

            let (context, request) = match AbacusRequest::from_json(&input) {
                Ok(data) => data,
                Err(err) => {
                    eprintln!("Error parsing input JSON: {err}");
                    std::process::exit(1);
                }
            };
            tabulate::tabulate(&context, request)
        }
        CliCommand::Tab(tab_args) => {
            let variables: Vec<_> = tab_args.variables.iter().map(|v| v.as_str()).collect();
            let (context, request) = match SimpleRequest::from_names(
                &tab_args.product,
                &[&tab_args.sample],
                variables.as_slice(),
                None,
                None,
                None,
            ) {
                Ok(data) => data,
                Err(err) => {
                    eprintln!("Error while setting up tabulation: {err}");
                    std::process::exit(1);
                }
            };
            tabulate::tabulate(&context, request)
        }
    };

    let tab = match result {
        Ok(tab) => tab,
        Err(err) => {
            eprintln!("Error trying to tabulate: {err}");
            std::process::exit(1);
        }
    };

    let output = match tab.output(args.format) {
        Ok(output) => output,
        Err(err) => {
            eprintln!("Error while formatting output: {err}");
            std::process::exit(1);
        }
    };

    if let Some(file_name) = args.output {
        let mut file = match File::create(file_name) {
            Ok(file) => file,
            Err(err) => {
                eprintln!("Error while creating output file: {err}");
                std::process::exit(1);
            }
        };

        if let Err(err) = writeln!(file, "{output}") {
            eprintln!("Error while writing output: {err}");
            std::process::exit(1);
        }
    } else {
        println!("{output}");
    }
}
