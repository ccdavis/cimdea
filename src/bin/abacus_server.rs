//! `abacus-server` — a minimal HTTP API exposing IPUMS metadata search and Abacus tabulation.
//!
//! ```text
//! abacus-server --data-root /path/to/data_root --product usa --port 8080
//! curl localhost:8080/datasets
//! curl 'localhost:8080/variables/search?dataset=us2019b&q=marital'
//! curl -XPOST localhost:8080/tabulate \
//!   -d '{"datasets":["us2019b"],"uoa":"P","request_variables":[{"variable_mnemonic":"MARST"}]}'
//! ```
//!
//! Data root resolution mirrors `abacus-mcp`: `--data-root`, else `--env`/`--config` from
//! `cimdea.toml` (the API key field there is ignored — this server needs no LLM key).

use std::path::PathBuf;

use cimdea::abacus_service::Service;
use cimdea::app_config::AppConfig;
use cimdea::http_api;

use clap::Parser;

#[derive(Parser, Debug)]
#[command(version, about = "HTTP API for Abacus tabulation + IPUMS metadata", long_about = None)]
struct Cli {
    /// IPUMS product/collection
    #[arg(long, default_value = "usa")]
    product: String,

    /// Path to the data root (contains parquet/ and layouts/) [default: from the chosen environment]
    #[arg(long)]
    data_root: Option<String>,

    /// Environment from the config file (e.g. "dev" or "prod") to read the data root from
    #[arg(long)]
    env: Option<String>,

    /// Path to the TOML config file [default: ./cimdea.toml if present]
    #[arg(long)]
    config: Option<String>,

    /// Port to listen on
    #[arg(long, default_value_t = 8080)]
    port: u16,
}

fn resolve_data_root(cli: &Cli) -> Result<Option<String>, String> {
    if cli.data_root.is_some() {
        return Ok(cli.data_root.clone());
    }
    let config_path = match &cli.config {
        Some(path) => Some(PathBuf::from(path)),
        None => AppConfig::find_default(),
    };
    match config_path {
        Some(path) => {
            let config = AppConfig::load(&path).map_err(|e| e.to_string())?;
            let resolved = config.resolve(cli.env.as_deref()).map_err(|e| e.to_string())?;
            Ok(Some(resolved.data_root))
        }
        None if cli.env.is_some() => Err(
            "--env was given but no config file was found (looked for ./cimdea.toml; pass --config)"
                .to_string(),
        ),
        None => Ok(None),
    }
}

fn main() {
    let cli = Cli::parse();
    let data_root = match resolve_data_root(&cli) {
        Ok(dr) => dr,
        Err(err) => {
            eprintln!("Error: {err}");
            std::process::exit(1);
        }
    };

    let service = Service::new(cli.product, data_root);
    if let Err(err) = http_api::serve(service, cli.port) {
        eprintln!("Error: HTTP server failed: {err}");
        std::process::exit(1);
    }
}
