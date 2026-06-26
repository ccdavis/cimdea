//! `ask` — answer an English question about IPUMS microdata by translating it into an Abacus
//! tabulation with an LLM, running it, and printing the table plus an explanation.
//!
//! Environment (key + data root) usually comes from `cimdea.toml`:
//! ```text
//! ask --env dev --dataset us2019b "How many people are there by marital status?"
//! ```
//! Or supply them directly (no config needed):
//! ```text
//! GEMINI_API_KEY=... ask --data-root tests/data_root --dataset us2019b \
//!     "How many people are there by marital status?"
//! ```

use std::fs::File;
use std::io::Write;
use std::path::PathBuf;

use cimdea::app_config::{AppConfig, ResolvedEnvironment};
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

    /// Environment from the config file (e.g. "dev" or "prod") [default: config's default_environment]
    #[arg(long)]
    env: Option<String>,

    /// Path to the TOML config file [default: ./cimdea.toml if present]
    #[arg(long)]
    config: Option<String>,

    /// Path to the data root (contains parquet/ and layouts/) [default: from the chosen environment]
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

    /// API key [default: from the chosen environment's key file, else GEMINI_API_KEY]
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

/// Resolve the environment (key file + data root) from the config file, if one applies.
///
/// A config file is used when `--config` is given or `./cimdea.toml` exists. With no config and no
/// `--env`, returns `None` (the legacy path: `--api-key`/`GEMINI_API_KEY` and `--data-root`).
fn resolve_environment(cli: &Cli) -> Result<Option<ResolvedEnvironment>, String> {
    let config_path = match &cli.config {
        Some(path) => Some(PathBuf::from(path)),
        None => AppConfig::find_default(),
    };

    match config_path {
        Some(path) => {
            let config = AppConfig::load(&path).map_err(|err| err.to_string())?;
            let resolved = config.resolve(cli.env.as_deref()).map_err(|err| err.to_string())?;
            Ok(Some(resolved))
        }
        None if cli.env.is_some() => Err(
            "--env was given but no config file was found (looked for ./cimdea.toml; pass --config <path>)"
                .to_string(),
        ),
        None => Ok(None),
    }
}

/// Build the LLM provider, given the resolved model id and API key (already chosen per precedence).
/// A `None` `api_key` falls back to the provider's environment variable (e.g. `GEMINI_API_KEY`).
fn build_provider(
    choice: &ProviderChoice,
    model: Option<String>,
    api_key: Option<String>,
) -> Result<Box<dyn LlmProvider>, String> {
    let model_id = model
        .clone()
        .unwrap_or_else(|| cimdea::llm::DEFAULT_GEMINI_MODEL.to_string());
    match choice {
        ProviderChoice::Gemini => {
            let provider = match api_key {
                Some(key) => GeminiProvider::new(key, model_id),
                None => GeminiProvider::from_env(model).map_err(|err| err.to_string())?,
            };
            Ok(Box::new(provider))
        }
        ProviderChoice::GeminiInteractions => {
            let provider = match api_key {
                Some(key) => InteractionsProvider::new(key, model_id),
                None => InteractionsProvider::from_env(model).map_err(|err| err.to_string())?,
            };
            Ok(Box::new(provider))
        }
    }
}

fn fail(message: String) -> ! {
    eprintln!("Error: {message}");
    std::process::exit(1);
}

fn main() {
    let cli = Cli::parse();

    // Resolve the dev/prod environment from the config (if any).
    let environment = match resolve_environment(&cli) {
        Ok(env) => env,
        Err(err) => fail(err),
    };
    if let Some(env) = &environment {
        eprintln!("[ask] environment: {} (data root: {})", env.name, env.data_root);
    }

    // API key precedence: explicit --api-key, then the environment's key file, then the provider's
    // environment variable (handled inside build_provider when this is None).
    let api_key: Option<String> = match (&cli.api_key, &environment) {
        (Some(key), _) => Some(key.clone()),
        (None, Some(env)) => match env.read_api_key() {
            Ok(key) => Some(key),
            Err(err) => fail(err.to_string()),
        },
        (None, None) => None,
    };

    let provider = match build_provider(&cli.provider, cli.model.clone(), api_key) {
        Ok(p) => p,
        Err(err) => fail(format!("setting up the LLM provider: {err}")),
    };

    // Data root precedence: explicit --data-root, then the environment's data root.
    let data_root = cli
        .data_root
        .clone()
        .or_else(|| environment.as_ref().map(|env| env.data_root.clone()));

    let cfg = NlConfig {
        product: cli.product.clone(),
        data_root,
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
