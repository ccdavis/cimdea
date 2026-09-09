//! MCP method dispatch over an [crate::abacus_service::Service].
//!
//! The server is transport-free: [McpServer::handle] takes one parsed JSON-RPC message and returns
//! the response (or `None` for a notification). [super::serve_stdio] wires it to stdin/stdout.

use serde_json::{json, Value};

use crate::abacus_service::{Service, TabRequest, IPUMS_CONVENTIONS};
use crate::mderror::MdError;

use super::jsonrpc;

/// The MCP protocol revision we implement against.
pub const PROTOCOL_VERSION: &str = "2025-06-18";

/// How many search matches to return when the caller doesn't say.
const DEFAULT_SEARCH_MAX: usize = 25;

/// An MCP server exposing the Abacus tabulation + metadata tools.
pub struct McpServer {
    service: Service,
}

impl McpServer {
    pub fn new(service: Service) -> Self {
        Self { service }
    }

    /// Handle one JSON-RPC message. Returns `Some(response)` for a request, `None` for a notification
    /// (no `id`) or a message that warrants no reply.
    pub fn handle(&self, msg: &Value) -> Option<Value> {
        let method = msg.get("method").and_then(|m| m.as_str()).unwrap_or("");
        let has_id = msg.get("id").is_some();
        let id = msg.get("id").cloned().unwrap_or(Value::Null);
        let params = msg.get("params").cloned().unwrap_or(Value::Null);

        match method {
            "initialize" => Some(jsonrpc::success(id, self.initialize_result())),
            "ping" => Some(jsonrpc::success(id, json!({}))),
            "tools/list" => Some(jsonrpc::success(id, json!({ "tools": tool_definitions() }))),
            "tools/call" => Some(self.tools_call(id, &params)),
            // Notifications (e.g. notifications/initialized) and anything else without an id: stay silent.
            _ if !has_id => None,
            other => Some(jsonrpc::error(
                id,
                jsonrpc::METHOD_NOT_FOUND,
                format!("unknown method: {other}"),
            )),
        }
    }

    fn initialize_result(&self) -> Value {
        json!({
            "protocolVersion": PROTOCOL_VERSION,
            "capabilities": { "tools": {} },
            "serverInfo": { "name": "abacus", "version": env!("CARGO_PKG_VERSION") },
            "instructions": instructions(),
        })
    }

    fn tools_call(&self, id: Value, params: &Value) -> Value {
        let name = params.get("name").and_then(|n| n.as_str()).unwrap_or("");
        let args = params.get("arguments").cloned().unwrap_or(json!({}));

        let result = match name {
            "list_datasets" => self.tool_list_datasets(),
            "search_variables" => self.tool_search_variables(&args),
            "get_variables" => self.tool_get_variables(&args),
            "tabulate" => self.tool_tabulate(&args),
            other => {
                return jsonrpc::error(
                    id,
                    jsonrpc::INVALID_PARAMS,
                    format!("unknown tool: {other}"),
                )
            }
        };

        // Tool *execution* errors are reported in the result with isError, not as protocol errors, so
        // the model can read the message and try again.
        match result {
            Ok(text) => jsonrpc::success(id, tool_text(text, false)),
            Err(err) => jsonrpc::success(id, tool_text(err.to_string(), true)),
        }
    }

    // --- tool implementations -------------------------------------------------

    fn tool_list_datasets(&self) -> Result<String, MdError> {
        let datasets = self.service.list_datasets()?;
        let mut out = format!("Datasets available for product '{}':\n", self.service.product);
        for d in &datasets {
            match d.year {
                Some(year) => out.push_str(&format!("- {} (year {})\n", d.name, year)),
                None => out.push_str(&format!("- {}\n", d.name)),
            }
        }
        out.push_str(
            "\nPass one of these as the `dataset` argument to search_variables / get_variables / \
             tabulate.",
        );
        Ok(out)
    }

    fn tool_search_variables(&self, args: &Value) -> Result<String, MdError> {
        let datasets = self.resolve_datasets(args)?;
        let query = args
            .get("query")
            .and_then(|q| q.as_str())
            .unwrap_or("")
            .to_string();
        let max = args
            .get("max")
            .and_then(|m| m.as_u64())
            .map(|m| m as usize)
            .unwrap_or(DEFAULT_SEARCH_MAX);

        let result = self.service.search_variables(&datasets, &query, max)?;
        let mut out = format!(
            "Variables matching \"{}\" in {} ({} shown):\n",
            query,
            datasets.join(", "),
            result.matches.len()
        );
        for m in &result.matches {
            let label = m.label.as_deref().unwrap_or("(no label)");
            let general = if m.has_general { ", has general form" } else { "" };
            out.push_str(&format!(
                "- {} — {} (record type {}, {} value labels{})\n",
                m.name, label, m.record_type, m.n_categories, general
            ));
        }
        if let Some(note) = &result.note {
            out.push_str(&format!("\nNote: {note}\n"));
        }
        Ok(out)
    }

    fn tool_get_variables(&self, args: &Value) -> Result<String, MdError> {
        let datasets = self.resolve_datasets(args)?;
        let names = string_list(args.get("names"));
        if names.is_empty() {
            return Err(MdError::Msg(
                "get_variables needs a `names` array (or comma-separated string) of variable \
                 mnemonics"
                    .to_string(),
            ));
        }
        let docs = self.service.get_variables(&datasets, &names)?;
        if docs.is_empty() {
            return Err(MdError::Msg(format!(
                "none of {} were found in {}",
                names.join(", "),
                datasets.join(", ")
            )));
        }
        let mut out = String::new();
        for doc in &docs {
            let label = doc.label.as_deref().unwrap_or("(no label)");
            out.push_str(&format!(
                "{} — {} (record type {}):\n",
                doc.name, label, doc.record_type
            ));
            if doc.categories.is_empty() {
                out.push_str("    (continuous / no value labels)\n");
            }
            for (code, clabel) in &doc.categories {
                out.push_str(&format!("    {code} = {clabel}\n"));
            }
        }
        Ok(out)
    }

    fn tool_tabulate(&self, args: &Value) -> Result<String, MdError> {
        let datasets = self.resolve_datasets(args)?;
        let detailed = args
            .get("detailed")
            .and_then(|d| d.as_bool())
            .unwrap_or(false);
        // The envelope is the rest of the arguments; unknown keys (datasets, detailed) are ignored.
        let req: TabRequest = serde_json::from_value(args.clone()).map_err(|err| {
            MdError::Msg(format!("could not parse the tabulation request: {err}"))
        })?;
        if req.request_variables.is_empty() {
            return Err(MdError::Msg(
                "tabulate needs at least one entry in `request_variables`".to_string(),
            ));
        }

        let outcome = self.service.tabulate(&req, &datasets, detailed)?;

        let mut out = format!("Tabulation over {}:\n\n", datasets.join(", "));
        out.push_str(&outcome.table_text);
        if !outcome.variable_docs.is_empty() {
            out.push_str("\nVariables:\n");
            for doc in &outcome.variable_docs {
                let label = doc.label.as_deref().unwrap_or("(no label)");
                let marker = if doc.general { " (general categories)" } else { "" };
                out.push_str(&format!(
                    "- {} ({}): {}{}\n",
                    doc.name, doc.record_type, label, marker
                ));
            }
        }
        if !outcome.warnings.is_empty() {
            out.push_str("\nWarnings:\n");
            for w in &outcome.warnings {
                out.push_str(&format!("- {w}\n"));
            }
        }
        out.push_str("\nGenerated Abacus request:\n");
        out.push_str(&outcome.generated_request_json);
        out.push('\n');
        Ok(out)
    }

    /// Resolve the dataset(s) for a tool call from `datasets` (array), `dataset` (string), or — when
    /// neither is given and exactly one dataset exists under the data root — that single dataset.
    fn resolve_datasets(&self, args: &Value) -> Result<Vec<String>, MdError> {
        let mut datasets = string_list(args.get("datasets"));
        if let Some(one) = args.get("dataset").and_then(|d| d.as_str()) {
            if !one.trim().is_empty() {
                datasets.push(one.trim().to_string());
            }
        }
        datasets.retain(|d| !d.trim().is_empty());
        if !datasets.is_empty() {
            return Ok(datasets);
        }
        // Fall back to the sole available dataset, else ask the caller to choose.
        let available = self.service.list_datasets()?;
        match available.as_slice() {
            [only] => Ok(vec![only.name.clone()]),
            many => Err(MdError::Msg(format!(
                "specify a `dataset` — available: {}",
                many.iter()
                    .map(|d| d.name.clone())
                    .collect::<Vec<_>>()
                    .join(", ")
            ))),
        }
    }
}

/// Wrap text as an MCP tool result (`content` array of one text block).
fn tool_text(text: String, is_error: bool) -> Value {
    json!({
        "content": [ { "type": "text", "text": text } ],
        "isError": is_error,
    })
}

/// Read a JSON value as a list of non-empty strings: accepts an array of strings or a single
/// comma-separated string.
fn string_list(value: Option<&Value>) -> Vec<String> {
    match value {
        Some(Value::Array(items)) => items
            .iter()
            .filter_map(|v| v.as_str())
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect(),
        Some(Value::String(s)) => s
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect(),
        _ => Vec::new(),
    }
}

/// The four tool definitions, with JSON-Schema input schemas.
pub fn tool_definitions() -> Value {
    json!([
        {
            "name": "list_datasets",
            "description": "List the IPUMS datasets (samples) available to tabulate, each with its year. Call this first if you don't know which dataset name to use.",
            "inputSchema": { "type": "object", "properties": {}, "additionalProperties": false }
        },
        {
            "name": "search_variables",
            "description": "Find IPUMS variables by a keyword that matches their mnemonic, label, or value-label text (e.g. \"marital\", \"education\", \"income\"). Returns matching variable mnemonics with their labels, record type (P=person, H=household), value-label count, and whether they have a general (simplified) form. Use this to discover which variable answers a question before tabulating. If nothing matches well, the response says so — relay that to the user rather than guessing a variable.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "dataset": { "type": "string", "description": "Dataset name, e.g. \"us2019b\" (from list_datasets)." },
                    "datasets": { "type": "array", "items": { "type": "string" }, "description": "Multiple dataset names (alternative to `dataset`)." },
                    "query": { "type": "string", "description": "Keyword to search for. Empty lists all variables (can be large)." },
                    "max": { "type": "integer", "description": "Max matches to return (default 25)." }
                },
                "additionalProperties": false
            }
        },
        {
            "name": "get_variables",
            "description": "Get the FULL list of integer value codes and labels for one or more variables. Use this before building a subpopulation filter or category bins on a variable whose codes you don't already know (search_variables only summarizes high-cardinality variables).",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "dataset": { "type": "string" },
                    "datasets": { "type": "array", "items": { "type": "string" } },
                    "names": { "type": "array", "items": { "type": "string" }, "description": "Variable mnemonics to expand, e.g. [\"BPL\", \"EDUC\"]." }
                },
                "required": ["names"],
                "additionalProperties": false
            }
        },
        {
            "name": "tabulate",
            "description": "Run a cross-tabulation and return the result table. Provide the request as intent: which variables to tabulate (general or detailed), optional subpopulation filters (AND-ed together), and optional category bins. Product, datasets, and all byte-offset/width mechanics are filled in from metadata. For a \"how many X\" count, put every condition in subpopulation and tabulate one variable pinned to a single value so the result collapses to one number.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "dataset": { "type": "string" },
                    "datasets": { "type": "array", "items": { "type": "string" } },
                    "detailed": { "type": "boolean", "description": "Force detailed categories for every tabulation variable (overrides general)." },
                    "uoa": { "type": "string", "enum": ["P", "H"], "description": "Unit of analysis: P=persons, H=households. Default P." },
                    "request_variables": {
                        "type": "array",
                        "description": "Variables to tabulate.",
                        "items": {
                            "type": "object",
                            "properties": {
                                "variable_mnemonic": { "type": "string" },
                                "general_detailed_selection": { "type": "string", "enum": ["G", ""], "description": "\"G\" for general (simplified) categories — only for variables with a general form; \"\" for detailed." }
                            },
                            "required": ["variable_mnemonic"]
                        }
                    },
                    "subpopulation": {
                        "type": "array",
                        "description": "Filters restricting which records are counted (AND-ed). Use detailed value codes (from get_variables). If you only know a general category code for a variable with a general form, set general_detailed_selection to \"G\" on the filter and use general codes; each general code is expanded to the detailed range it covers.",
                        "items": {
                            "type": "object",
                            "properties": {
                                "variable_mnemonic": { "type": "string" },
                                "general_detailed_selection": { "type": "string", "enum": ["G", ""], "description": "\"G\" when the filter codes are general category codes; \"\" (default) for detailed codes." },
                                "case_selection": { "type": "boolean" },
                                "request_case_selections": {
                                    "type": "array",
                                    "items": {
                                        "type": "object",
                                        "properties": {
                                            "low_code": { "type": ["string", "integer", "null"] },
                                            "high_code": { "type": ["string", "integer", "null"] }
                                        }
                                    }
                                }
                            },
                            "required": ["variable_mnemonic"]
                        }
                    },
                    "category_bins": {
                        "type": "array",
                        "description": "Group a continuous variable into bins. The variable must also appear in request_variables.",
                        "items": {
                            "type": "object",
                            "properties": {
                                "variable": { "type": "string" },
                                "bins": {
                                    "type": "array",
                                    "items": {
                                        "type": "object",
                                        "properties": {
                                            "code": { "type": "integer" },
                                            "value_label": { "type": "string" },
                                            "low": { "type": ["integer", "null"] },
                                            "high": { "type": ["integer", "null"] }
                                        },
                                        "required": ["code", "value_label"]
                                    }
                                }
                            },
                            "required": ["variable", "bins"]
                        }
                    }
                },
                "required": ["request_variables"],
                "additionalProperties": false
            }
        }
    ])
}

/// The server-level instructions returned from `initialize`, teaching a model how to use the tools.
pub fn instructions() -> String {
    format!(
        r#"This server tabulates IPUMS census/survey microdata and serves its variable metadata. You (the connecting model) act as the translator: turn the user's plain-English question into a tabulation by calling these tools.

Workflow:
1. If you don't know the dataset (sample/year), call `list_datasets` and pick the one matching the user's request (e.g. a year they named).
2. Call `search_variables` with a keyword to find the variable(s) that answer the question. Use the returned mnemonics — never invent variable names.
3. If you need exact value codes for a subpopulation filter or category bins on a variable, call `get_variables` to see its full code list.
4. Call `tabulate` with the assembled request. Read back the table to the user with the value labels.
5. If `search_variables` finds nothing suitable, tell the user the concept doesn't appear to be in this data and point them to https://www.ipums.org — do not force an unrelated variable.

Building a tabulate request (intent only — mechanics are filled in for you):
- `uoa`: "P" to count persons, "H" to count households (default "P").
- `request_variables`: the variables to break the table down by. Use `general_detailed_selection` "G" for the general (simplified) categories ONLY when search_variables marked the variable as having a general form and the user didn't ask for detail; otherwise use "".
- `subpopulation`: filters that restrict which records are counted, AND-ed together, using detailed integer value codes (a selection keeps values between low_code and high_code inclusive; set one bound null for an open-ended range). If you only know a general category code, mark that filter `general_detailed_selection` "G" and use general codes — they are expanded to the detailed range they cover.
- `category_bins`: group a continuous variable (e.g. AGE) into ranges; the binned variable must also be in `request_variables`.

Counts vs. breakdowns: when the user asks "how many" of a group (possibly several conditions, e.g. "how many Hispanic women graduated college"), put EVERY condition in `subpopulation` and tabulate exactly ONE variable pinned to a single value, so the result is one number. Break a variable into all its categories only when the user wants the full distribution ("by marital status", "for each").

{IPUMS_CONVENTIONS}"#
    )
}
