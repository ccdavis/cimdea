//! Check IPUMS data deployment status across servers.
//!
//! This tool connects to IPUMS servers via SSH and verifies the presence
//! of parquet, fixed-width, and derived data files.
//!
//! # Usage
//!
//! ```bash
//! # Check internal server
//! check-server-status --internal
//!
//! # Check all three environments
//! check-server-status --internal --demo --live
//!
//! # Check specific products
//! check-server-status --internal -p usa,cps,ipumsi
//!
//! # Plain text output to file
//! check-server-status --internal --plain -o status.txt
//!
//! # With custom config
//! check-server-status --internal --config my-servers.toml
//! ```

use cimdea::deployment::{DeploymentRegistry, Environment, ALL_PRODUCTS};
use cimdea::remote::{RemoteError, SshConnectionPool};
use cimdea::server_status::{
    DatasetComparison, FormatStatus, ProductStatus, ServerStatusChecker, StatusSummary,
};
use clap::Parser;
use std::fs::File;
use std::io::{self, IsTerminal, Write};
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(
    name = "check-server-status",
    version,
    about = "Check IPUMS data deployment status across servers",
    long_about = "Check IPUMS data deployment status (parquet, fixed-width, derived) across \
                  internal, demo, and live server environments.\n\n\
                  At least one of --internal, --demo, or --live is required."
)]
struct Args {
    /// Check internal server directories
    #[arg(long)]
    internal: bool,

    /// Check demo server directories
    #[arg(long)]
    demo: bool,

    /// Check live servers (connects to each product's production server)
    #[arg(long)]
    live: bool,

    /// Plain text output (no colors/symbols)
    #[arg(long)]
    plain: bool,

    /// Specific products to check (comma-separated, default: all)
    #[arg(short, long, value_delimiter = ',')]
    products: Option<Vec<String>>,

    /// Output file path (also writes to stdout)
    #[arg(short, long)]
    output: Option<PathBuf>,

    /// Configuration file override (TOML or JSON)
    #[arg(short, long)]
    config: Option<PathBuf>,
}

/// Terminal output helper with color support
struct OutputFormatter {
    use_colors: bool,
    output_file: Option<File>,
}

impl OutputFormatter {
    fn new(use_colors: bool, output_path: Option<&PathBuf>) -> io::Result<Self> {
        let output_file = output_path.map(File::create).transpose()?;

        Ok(Self {
            use_colors,
            output_file,
        })
    }

    fn write(&mut self, text: &str) {
        // Write to stdout
        println!("{}", text);

        // Write plain text to file
        if let Some(ref mut file) = self.output_file {
            let plain = Self::strip_ansi(text);
            let _ = writeln!(file, "{}", plain);
        }
    }

    fn strip_ansi(text: &str) -> String {
        // Simple ANSI code stripper using manual parsing
        let mut result = String::with_capacity(text.len());
        let mut chars = text.chars().peekable();

        while let Some(c) = chars.next() {
            if c == '\x1b' {
                // Skip escape sequence
                if chars.peek() == Some(&'[') {
                    chars.next(); // consume '['
                    // Skip until we hit a letter
                    while let Some(&nc) = chars.peek() {
                        chars.next();
                        if nc.is_ascii_alphabetic() {
                            break;
                        }
                    }
                }
            } else {
                result.push(c);
            }
        }
        result
    }

    // Color helpers
    fn green(&self, text: &str) -> String {
        if self.use_colors {
            format!("\x1b[32m{}\x1b[0m", text)
        } else {
            text.to_string()
        }
    }

    fn red(&self, text: &str) -> String {
        if self.use_colors {
            format!("\x1b[31m{}\x1b[0m", text)
        } else {
            text.to_string()
        }
    }

    fn yellow(&self, text: &str) -> String {
        if self.use_colors {
            format!("\x1b[33m{}\x1b[0m", text)
        } else {
            text.to_string()
        }
    }

    fn bold(&self, text: &str) -> String {
        if self.use_colors {
            format!("\x1b[1m{}\x1b[0m", text)
        } else {
            text.to_string()
        }
    }

    fn dim(&self, text: &str) -> String {
        if self.use_colors {
            format!("\x1b[2m{}\x1b[0m", text)
        } else {
            text.to_string()
        }
    }

    // Status symbols
    fn ok_symbol(&self) -> String {
        if self.use_colors {
            self.green("OK")
        } else {
            "OK".to_string()
        }
    }

    fn missing_symbol(&self) -> String {
        if self.use_colors {
            self.red("MISSING")
        } else {
            "MISSING".to_string()
        }
    }

    fn warning_symbol(&self) -> String {
        if self.use_colors {
            self.yellow("WARNING")
        } else {
            "WARNING".to_string()
        }
    }

    fn na_symbol(&self) -> String {
        self.dim("-")
    }
}

fn main() {
    let args = Args::parse();

    // Validate that at least one environment is specified
    if !args.internal && !args.demo && !args.live {
        eprintln!("Error: At least one of --internal, --demo, or --live is required");
        eprintln!("Run with --help for usage information");
        std::process::exit(1);
    }

    // Determine if we should use colors (not plain, and stdout is a tty)
    let use_colors = !args.plain && is_terminal();

    let mut formatter = match OutputFormatter::new(use_colors, args.output.as_ref()) {
        Ok(f) => f,
        Err(e) => {
            eprintln!("Error creating output file: {}", e);
            std::process::exit(1);
        }
    };

    // Load deployment configuration
    let registry = match DeploymentRegistry::with_config(args.config.as_deref()) {
        Ok(r) => r,
        Err(e) => {
            eprintln!("Error loading configuration: {}", e);
            std::process::exit(1);
        }
    };

    // Initialize SSH connection pool
    let mut pool = match SshConnectionPool::new() {
        Ok(p) => p,
        Err(e) => {
            eprintln!("Error initializing SSH: {}", e);
            std::process::exit(1);
        }
    };

    // Determine which products to check
    let products: Vec<&str> = match &args.products {
        Some(list) => list.iter().map(|s| s.as_str()).collect(),
        None => ALL_PRODUCTS.to_vec(),
    };

    // Validate product names
    for product in &products {
        if registry.get_product(product).is_none() {
            eprintln!("Error: Unknown product '{}'. Valid products are:", product);
            eprintln!("  {}", ALL_PRODUCTS.join(", "));
            std::process::exit(1);
        }
    }

    let mut summary = StatusSummary::new();

    // Print header
    print_header(&mut formatter);

    // Check internal
    if args.internal {
        check_environment(
            &mut pool,
            &registry,
            Environment::Internal,
            &products,
            &mut formatter,
            &mut summary,
        );
    }

    // Check demo
    if args.demo {
        check_environment(
            &mut pool,
            &registry,
            Environment::Demo,
            &products,
            &mut formatter,
            &mut summary,
        );
    }

    // Check live
    if args.live {
        check_live_environment(
            &mut pool,
            &registry,
            &products,
            &mut formatter,
            &mut summary,
        );
    }

    // Print summary
    print_summary(&mut formatter, &summary);

    // Exit with non-zero if there were issues
    if summary.total_issues() > 0 {
        std::process::exit(1);
    }
}

/// Check if stdout is a terminal
fn is_terminal() -> bool {
    io::stdout().is_terminal()
}

fn print_header(fmt: &mut OutputFormatter) {
    fmt.write("");
    fmt.write(&fmt.bold("=================================================="));
    fmt.write(&fmt.bold("  IPUMS Data Deployment Status Check"));
    fmt.write(&fmt.bold("=================================================="));
    fmt.write("");
}

fn check_environment(
    pool: &mut SshConnectionPool,
    registry: &DeploymentRegistry,
    env: Environment,
    products: &[&str],
    fmt: &mut OutputFormatter,
    summary: &mut StatusSummary,
) {
    let server = &registry.internal_server;

    fmt.write("");
    fmt.write(&format!(
        "Connecting to {} for {} environment...",
        server,
        env.as_str()
    ));
    fmt.write(&fmt.dim(
        "You may be prompted to verify the host key and/or enter your password",
    ));
    fmt.write("");

    // Connect to server
    if let Err(e) = pool.connect(server, false, true) {
        fmt.write(&fmt.red(&format!("Failed to connect to {}: {}", server, e)));
        summary.errors += products.len();
        return;
    }

    fmt.write(&fmt.bold(&format!(
        "{} Environment: {}",
        capitalize(env.as_str()),
        server
    )));
    fmt.write(&fmt.dim("--------------------------------------------------"));

    let checker = ServerStatusChecker::new(pool);

    for product_name in products {
        if let Some(product) = registry.get_product(product_name) {
            let target = registry.target(env, product);
            let status = checker.check_target(&target);
            print_product_status(fmt, &status, summary);
        }
    }
}

fn check_live_environment(
    pool: &mut SshConnectionPool,
    registry: &DeploymentRegistry,
    products: &[&str],
    fmt: &mut OutputFormatter,
    summary: &mut StatusSummary,
) {
    fmt.write("");
    fmt.write(&fmt.bold("Live Environment: (multiple servers)"));
    fmt.write(&fmt.dim("--------------------------------------------------"));

    for product_name in products {
        if let Some(product) = registry.get_product(product_name) {
            // Try to connect to this product's live server
            let is_third_party = product.third_party;

            // Show connection message for each new server
            if !pool.is_connected(&product.live_server) {
                fmt.write("");
                fmt.write(&format!(
                    "Connecting to {} for {}...",
                    product.live_server, product.name
                ));
                if is_third_party {
                    fmt.write(&fmt.yellow("  This is a third-party server"));
                }
            }

            if let Err(e) = pool.connect(&product.live_server, is_third_party, true) {
                fmt.write(&format!(
                    "\n[{}] {}",
                    product.name,
                    fmt.dim(&format!("/web/{}/share/data", product.live_server))
                ));
                match e {
                    RemoteError::ConnectionSkipped => {
                        fmt.write(&format!(
                            "  Status:    {} {}",
                            fmt.na_symbol(),
                            fmt.dim("skipped (third-party server, no access)")
                        ));
                        summary.add_skipped();
                    }
                    _ => {
                        fmt.write(&format!(
                            "  Status:    {} {}",
                            fmt.warning_symbol(),
                            fmt.yellow("connection failed")
                        ));
                        summary.errors += 1;
                    }
                }
                continue;
            }

            let target = registry.target(Environment::Live, product);
            let checker = ServerStatusChecker::new(pool);
            let status = checker.check_target(&target);
            print_product_status(fmt, &status, summary);
        }
    }
}

fn print_product_status(fmt: &mut OutputFormatter, status: &ProductStatus, summary: &mut StatusSummary) {
    fmt.write(&format!(
        "\n[{}] {}",
        status.product_name,
        fmt.dim(&status.base_path)
    ));

    if !status.path_exists {
        fmt.write(&format!(
            "  Status:    {} {}",
            fmt.missing_symbol(),
            fmt.red("Path not found")
        ));
        summary.errors += 1;
        return;
    }

    // Parquet status
    print_format_status(fmt, "Parquet", &status.parquet, summary);

    // Fixed-width status
    print_format_status(fmt, "FW", &status.fixed_width, summary);

    // Derived status
    print_format_status(fmt, "Derived", &status.derived, summary);

    // Comparison status
    if let Some(ref comparison) = status.comparison {
        match comparison {
            DatasetComparison::Match => {
                fmt.write(&format!(
                    "  {:<10} {} FW and Parquet datasets match",
                    "Match:",
                    fmt.ok_symbol()
                ));
            }
            DatasetComparison::Skipped => {
                fmt.write(&format!(
                    "  {:<10} {} skipped (missing data)",
                    "Match:",
                    fmt.na_symbol()
                ));
            }
            DatasetComparison::Mismatch {
                fw_only,
                parquet_only,
            } => {
                let mut detail = String::new();
                if !fw_only.is_empty() {
                    let examples: String = fw_only.iter().take(3).cloned().collect::<Vec<_>>().join(", ");
                    detail.push_str(&format!("FW only ({}): {} ", fw_only.len(), examples));
                    if fw_only.len() > 3 {
                        detail.push_str("... ");
                    }
                }
                if !parquet_only.is_empty() {
                    let examples: String = parquet_only
                        .iter()
                        .take(3)
                        .cloned()
                        .collect::<Vec<_>>()
                        .join(", ");
                    detail.push_str(&format!("Parquet only ({}): {}", parquet_only.len(), examples));
                    if parquet_only.len() > 3 {
                        detail.push_str("...");
                    }
                }
                fmt.write(&format!(
                    "  {:<10} {} {}",
                    "Match:",
                    fmt.warning_symbol(),
                    fmt.yellow(&detail)
                ));
                summary.add_comparison(comparison);
            }
        }
    }
}

fn print_format_status(
    fmt: &mut OutputFormatter,
    label: &str,
    status: &FormatStatus,
    summary: &mut StatusSummary,
) {
    let line = match status {
        FormatStatus::Present {
            datasets,
            date_summary,
        } => {
            format!(
                "  {:<10} {} ({} datasets) {}",
                format!("{}:", label),
                fmt.ok_symbol(),
                datasets.len(),
                date_summary
            )
        }
        FormatStatus::Missing => {
            format!(
                "  {:<10} {} {}",
                format!("{}:", label),
                fmt.missing_symbol(),
                fmt.red("No data found")
            )
        }
        FormatStatus::NotConfigured => {
            format!(
                "  {:<10} {} not configured",
                format!("{}:", label),
                fmt.na_symbol()
            )
        }
        FormatStatus::Unknown(msg) => {
            format!(
                "  {:<10} {} {}",
                format!("{}:", label),
                fmt.warning_symbol(),
                fmt.yellow(msg)
            )
        }
    };
    fmt.write(&line);
    summary.add_format_status(status);
}

fn print_summary(fmt: &mut OutputFormatter, summary: &StatusSummary) {
    fmt.write("");
    fmt.write(&fmt.bold("=================================================="));
    fmt.write(&fmt.bold("  Summary"));
    fmt.write(&fmt.bold("=================================================="));
    fmt.write(&format!("  {}       {}", fmt.green("OK:"), summary.ok));
    if summary.warnings > 0 {
        fmt.write(&format!(
            "  {} {}",
            fmt.yellow("Warnings:"),
            summary.warnings
        ));
    }
    if summary.missing > 0 {
        fmt.write(&format!("  {}  {}", fmt.red("Missing:"), summary.missing));
    }
    if summary.errors > 0 {
        fmt.write(&format!("  {}   {}", fmt.red("Errors:"), summary.errors));
    }
    if summary.skipped > 0 {
        fmt.write(&format!("  {}  {}", fmt.dim("Skipped:"), summary.skipped));
    }
    fmt.write("");
}

fn capitalize(s: &str) -> String {
    let mut chars = s.chars();
    match chars.next() {
        None => String::new(),
        Some(first) => first.to_uppercase().chain(chars).collect(),
    }
}
