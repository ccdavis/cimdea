//! `ask` — answer an English question about IPUMS microdata by translating it into an Abacus
//! tabulation with an LLM, running it, and printing the table plus an explanation.
//!
//! Example:
//! ```text
//! GEMINI_API_KEY=... ask --product usa --data-root tests/data_root --dataset us2019b \
//!     "How many people are there by marital status?"
//! ```

use std::fs::File;
use std::io::Write;

use cimdea::llm::{GeminiProvider, InteractionsProvider, LlmProvider};
use cimdea::nl_tabulation::{self, NlConfig};
use cimdea::tabulate::TableFormat;

use clap::{Parser, ValueEnum};

#[derive(Clone, Debug, ValueEnum)]
enum ProviderChoice {
    /// Google Gemini via the `generateContent` endpoint.
    Gemini,
    /// Google Gemini via the newer Interactions API (GA "recommended" interface).
    GeminiInteractions,
}

#[derive(Parser, Debug)]
#[command(version, about = "Translate an English request into an Abacus tabulation", long_about = None)]
struct Cli {
    /// The English description of the tabulation you want
    prompt: String,

    /// The IPUMS product/collection
    #[arg(long, default_value = "usa")]
    product: String,

    /// Path to the data root (contains parquet/ and layouts/) [default: inferred from product]
    #[arg(long)]
    data_root: Option<String>,

    /// Dataset(s) to tabulate; repeat the flag for more than one
    #[arg(long = "dataset", required = true)]
    datasets: Vec<String>,

    /// Which LLM provider to use
    #[arg(long, value_enum, default_value = "gemini")]
    provider: ProviderChoice,

    /// Model id override [default: provider's default model]
    #[arg(long)]
    model: Option<String>,

    /// API key [default: read from the provider's environment variable, e.g. GEMINI_API_KEY]
    #[arg(long)]
    api_key: Option<String>,

    /// Use detailed categories for tabulation variables (default: general/simplified categories)
    #[arg(long)]
    detailed: bool,

    /// Output format
    #[arg(short, long, default_value = "text")]
    format: TableFormat,

    /// Also print the generated Abacus request JSON to stderr
    #[arg(long)]
    show_request: bool,

    /// Write output to a file [default: stdout]
    #[arg(short, long)]
    output: Option<String>,
}

fn build_provider(cli: &Cli) -> Result<Box<dyn LlmProvider>, String> {
    // Resolve the model id once: explicit --model, else the provider default.
    let model = || {
        cli.model
            .clone()
            .unwrap_or_else(|| cimdea::llm::DEFAULT_GEMINI_MODEL.to_string())
    };
    match cli.provider {
        ProviderChoice::Gemini => {
            let provider = match &cli.api_key {
                Some(key) => GeminiProvider::new(key.clone(), model()),
                None => GeminiProvider::from_env(cli.model.clone()).map_err(|err| err.to_string())?,
            };
            Ok(Box::new(provider))
        }
        ProviderChoice::GeminiInteractions => {
            let provider = match &cli.api_key {
                Some(key) => InteractionsProvider::new(key.clone(), model()),
                None => {
                    InteractionsProvider::from_env(cli.model.clone()).map_err(|err| err.to_string())?
                }
            };
            Ok(Box::new(provider))
        }
    }
}

fn main() {
    let cli = Cli::parse();

    let provider = match build_provider(&cli) {
        Ok(p) => p,
        Err(err) => {
            eprintln!("Error setting up the LLM provider: {err}");
            std::process::exit(1);
        }
    };

    let cfg = NlConfig {
        product: cli.product.clone(),
        data_root: cli.data_root.clone(),
        datasets: cli.datasets.clone(),
        category_catalog_max: None,
        detailed: cli.detailed,
    };

    let result = match nl_tabulation::run(provider.as_ref(), &cli.prompt, &cfg) {
        Ok(result) => result,
        Err(err) => {
            eprintln!("Error: {err}");
            std::process::exit(1);
        }
    };

    if cli.show_request {
        if let Some(req) = &result.generated_request_json {
            eprintln!("--- generated Abacus request ---\n{req}\n--------------------------------");
        }
    }

    let output = match result.render(cli.format) {
        Ok(output) => output,
        Err(err) => {
            eprintln!("Error formatting output: {err}");
            std::process::exit(1);
        }
    };

    if let Some(file_name) = cli.output {
        match File::create(&file_name).and_then(|mut f| writeln!(f, "{output}")) {
            Ok(()) => {}
            Err(err) => {
                eprintln!("Error writing output file {file_name}: {err}");
                std::process::exit(1);
            }
        }
    } else {
        println!("{output}");
    }
}
