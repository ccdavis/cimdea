use cimdea::request::AbacusRequest;
use cimdea::tabulate;
use cimdea::conventions::Context;

use std::io::{self, BufRead};
use clap::Parser;

fn get_from_stdin() -> String {
    let stdin = io::stdin();
    let mut lines = stdin.lock().lines();
    let data = match lines.collect::<Result<Vec<String>,_>>() {
        Ok(lns) => lns.join("\n"),
        Err(ref e) => {
            eprintln!("Error reading from STDIN: '{}'",e);
            std::process::exit(1);
        }
    };
    data
}

fn abacus_request_from_str(rq: &str) ->  (Context, AbacusRequest) {
    match AbacusRequest::from_json(rq) {
        Err(e) => {
            eprintln!("Error parsing input JSON: '{}'", &e);
            std::process::exit(1);
        }
        Ok((ctx, ar)) => (ctx,ar),
    }
}

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct CliRequest {
    #[arg(short, long, default_value = "file")]
    pub input: String,

    #[arg(short, long, default_value = "stdout")]
    pub output: String,
}

fn main() {
    let args = CliRequest::parse();
    let input_value = &args.input.to_string();

    let (context, request) = if input_value == "stdin" {
        abacus_request_from_str(&get_from_stdin())
    } else {
        let json = match std::fs::read_to_string(&input_value) {
            Ok(j) => j,
            Err(e) => {
                eprintln!("Can't access Abacus request file: '{}'",e);
                std::process::exit(1);
            }
        };
        abacus_request_from_str(&json)
    };

    let table_format = tabulate::TableFormat::Json;
    match tabulate::tabulate(&context, &request) {
        Ok(tables) => {
            // Print a JSON array and separate table objects with ',' if more than one in
            // the output.
            println!("[\n");
            for (table_number, table) in tables.iter().enumerate() {
                if table_number > 0 {
                    println!(",");
                                    }
                println!("{}", table.output(table_format.clone()));
            }
            println!("\n]\n");
        }
        Err(e) => {
            eprintln!("Error trying to tabulate: {}", &e);
        }
    }

}
