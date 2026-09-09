//! `abacus-mcp` — a Model Context Protocol server (stdio) exposing IPUMS metadata search and Abacus
//! tabulation as tools. Point any MCP-aware AI at it and it can answer tabulation questions like the
//! built-in `ask` feature, but in a free-flowing chat.
//!
//! Register with Claude Code, e.g.:
//! ```text
//! claude mcp add abacus -- /path/to/abacus-mcp --data-root /path/to/data_root --product usa
//! ```
//!
//! Data root resolution: `--data-root`, else `--env`/`--config` reads it from `cimdea.toml` (the API
//! key field there is ignored — this server needs no LLM key).

use std::path::PathBuf;

use cimdea::abacus_service::Service;
use cimdea::app_config::AppConfig;
use cimdea::mcp;

use clap::Parser;

#[derive(Parser, Debug)]
#[command(version, about = "MCP server for Abacus tabulation + IPUMS metadata", long_about = None)]
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

    eprintln!(
        "[abacus-mcp] product '{}', data root {:?}; serving MCP over stdio",
        cli.product, data_root
    );
    let service = Service::new(cli.product, data_root);
    if let Err(err) = mcp::serve_stdio(service) {
        eprintln!("Error: MCP server failed: {err}");
        std::process::exit(1);
    }
}
