//! A Model Context Protocol (MCP) server exposing the Abacus tabulation + metadata tools.
//!
//! Any MCP-aware AI (Claude Code, etc.) can connect and behave like the built-in `ask` feature, but
//! in a free-flowing chat: it discovers datasets and variables with the metadata tools, then submits
//! a tabulation. There is no LLM in this server — the *connecting* model is the intelligence; this
//! server just teaches it (via the `initialize` instructions and tool descriptions) and executes.
//!
//! The transport is stdio with newline-delimited JSON-RPC 2.0. [server::McpServer::handle] is
//! transport-free and unit-testable; [serve_stdio] drives it from stdin/stdout.

pub mod jsonrpc;
pub mod server;

pub use server::{McpServer, PROTOCOL_VERSION};

use std::io::{BufRead, Write};

use crate::abacus_service::Service;

/// Serve the MCP protocol over stdin/stdout until EOF. Diagnostics go to stderr so they never corrupt
/// the JSON-RPC stream on stdout.
pub fn serve_stdio(service: Service) -> std::io::Result<()> {
    let server = McpServer::new(service);
    let stdin = std::io::stdin();
    let mut reader = stdin.lock();
    let stdout = std::io::stdout();
    let mut out = stdout.lock();

    let mut line = String::new();
    loop {
        line.clear();
        if reader.read_line(&mut line)? == 0 {
            break; // EOF
        }
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let response = match serde_json::from_str::<serde_json::Value>(trimmed) {
            Ok(msg) => server.handle(&msg),
            Err(err) => Some(jsonrpc::error(
                serde_json::Value::Null,
                jsonrpc::PARSE_ERROR,
                format!("parse error: {err}"),
            )),
        };
        if let Some(response) = response {
            writeln!(out, "{}", serde_json::to_string(&response)?)?;
            out.flush()?;
        }
    }
    Ok(())
}
