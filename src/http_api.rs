//! A minimal blocking HTTP API over an [crate::abacus_service::Service].
//!
//! Exposes the same capability as the MCP server ([crate::mcp]) as plain REST/JSON, for clients that
//! aren't MCP-aware. Built on the synchronous `tiny_http` crate to match the rest of the crate (no
//! async runtime).
//!
//! Endpoints:
//! - `GET  /health`
//! - `GET  /datasets`
//! - `GET  /variables/search?dataset=<d>&q=<query>&max=<n>`
//! - `GET  /variables?dataset=<d>&names=A,B,C`
//! - `GET  /catalog?dataset=<d>`
//! - `POST /tabulate` — body is the intent envelope (+ optional `datasets`/`dataset`, `detailed`).

use serde_json::{json, Value};
use tiny_http::{Header, Response, Server};

use crate::abacus_service::{Service, TabRequest, DEFAULT_CATEGORY_CATALOG_MAX};
use crate::mderror::MdError;

/// Serve the HTTP API on `0.0.0.0:<port>` until the process is stopped.
pub fn serve(service: Service, port: u16) -> Result<(), Box<dyn std::error::Error>> {
    let addr = format!("0.0.0.0:{port}");
    let server = Server::http(&addr).map_err(|e| format!("could not bind {addr}: {e}"))?;
    eprintln!(
        "abacus-server listening on http://{addr} (product '{}', data root {:?})",
        service.product, service.data_root
    );

    for mut request in server.incoming_requests() {
        let method = request.method().to_string().to_uppercase();
        let url = request.url().to_string();
        let mut body = String::new();
        if method == "POST" || method == "PUT" {
            let _ = request.as_reader().read_to_string(&mut body);
        }
        let (status, payload) = route(&service, &method, &url, &body);
        let header = Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..])
            .expect("static header is valid");
        let response = Response::from_string(payload)
            .with_status_code(status)
            .with_header(header);
        let _ = request.respond(response);
    }
    Ok(())
}

/// Route one request to a handler, returning an HTTP status and a JSON body. Pure (no I/O beyond the
/// service's metadata reads), so it is directly unit-testable.
pub fn route(service: &Service, method: &str, url: &str, body: &str) -> (u16, String) {
    let (path, query) = match url.split_once('?') {
        Some((p, q)) => (p, q),
        None => (url, ""),
    };

    let result: Result<Value, (u16, MdError)> = match (method, path) {
        ("GET", "/health") => Ok(json!({ "status": "ok" })),
        ("GET", "/datasets") => handle_datasets(service),
        ("GET", "/variables/search") => handle_search(service, query),
        ("GET", "/variables") => handle_variables(service, query),
        ("GET", "/catalog") => handle_catalog(service, query),
        ("POST", "/tabulate") => handle_tabulate(service, body),
        _ => {
            return (
                404,
                json!({ "error": format!("no route for {method} {path}") }).to_string(),
            )
        }
    };

    match result {
        Ok(value) => (200, value.to_string()),
        Err((status, err)) => (status, json!({ "error": err.to_string() }).to_string()),
    }
}

fn handle_datasets(service: &Service) -> Result<Value, (u16, MdError)> {
    let datasets = service.list_datasets().map_err(server_error)?;
    Ok(json!({ "datasets": datasets }))
}

fn handle_search(service: &Service, query: &str) -> Result<Value, (u16, MdError)> {
    let datasets = resolve_datasets(service, &query_datasets(query))?;
    let q = param(query, "q").unwrap_or_default();
    let max = param(query, "max")
        .and_then(|m| m.parse::<usize>().ok())
        .unwrap_or(50);
    let result = service
        .search_variables(&datasets, &q, max)
        .map_err(server_error)?;
    Ok(json!({ "datasets": datasets, "query": q, "result": result }))
}

fn handle_variables(service: &Service, query: &str) -> Result<Value, (u16, MdError)> {
    let datasets = resolve_datasets(service, &query_datasets(query))?;
    let names = param(query, "names")
        .map(|n| {
            n.split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    if names.is_empty() {
        return Err((
            400,
            MdError::Msg("the `names` query parameter is required (comma-separated)".to_string()),
        ));
    }
    let docs = service
        .get_variables(&datasets, &names)
        .map_err(server_error)?;
    Ok(json!({ "datasets": datasets, "variables": docs }))
}

fn handle_catalog(service: &Service, query: &str) -> Result<Value, (u16, MdError)> {
    let datasets = resolve_datasets(service, &query_datasets(query))?;
    let max = param(query, "max")
        .and_then(|m| m.parse::<usize>().ok())
        .unwrap_or(DEFAULT_CATEGORY_CATALOG_MAX);
    let catalog = service.catalog(&datasets, max).map_err(server_error)?;
    Ok(json!({ "datasets": datasets, "catalog": catalog }))
}

fn handle_tabulate(service: &Service, body: &str) -> Result<Value, (u16, MdError)> {
    let parsed: Value = serde_json::from_str(body).map_err(|err| {
        (
            400,
            MdError::Msg(format!("request body is not valid JSON: {err}")),
        )
    })?;
    let datasets = resolve_datasets(service, &value_datasets(&parsed))?;
    let detailed = parsed
        .get("detailed")
        .and_then(|d| d.as_bool())
        .unwrap_or(false);
    let req: TabRequest = serde_json::from_value(parsed).map_err(|err| {
        (
            400,
            MdError::Msg(format!("could not parse the tabulation request: {err}")),
        )
    })?;
    if req.request_variables.is_empty() {
        return Err((
            400,
            MdError::Msg("`request_variables` must have at least one entry".to_string()),
        ));
    }
    let outcome = service
        .tabulate(&req, &datasets, detailed)
        .map_err(server_error)?;
    Ok(json!({ "datasets": datasets, "result": outcome }))
}

/// Map a service error to a 500 (the metadata/execution layer doesn't distinguish client vs. server
/// faults; explicit 400s are returned at the parse sites above).
fn server_error(err: MdError) -> (u16, MdError) {
    (500, err)
}

/// Resolve requested datasets, falling back to the sole available dataset when none were given.
fn resolve_datasets(service: &Service, requested: &[String]) -> Result<Vec<String>, (u16, MdError)> {
    let cleaned: Vec<String> = requested
        .iter()
        .map(|d| d.trim().to_string())
        .filter(|d| !d.is_empty())
        .collect();
    if !cleaned.is_empty() {
        return Ok(cleaned);
    }
    let available = service.list_datasets().map_err(server_error)?;
    match available.as_slice() {
        [only] => Ok(vec![only.name.clone()]),
        many => Err((
            400,
            MdError::Msg(format!(
                "specify a `dataset` — available: {}",
                many.iter()
                    .map(|d| d.name.clone())
                    .collect::<Vec<_>>()
                    .join(", ")
            )),
        )),
    }
}

/// Dataset names from a query string (`dataset=` and/or `datasets=` comma list).
fn query_datasets(query: &str) -> Vec<String> {
    let mut out = Vec::new();
    if let Some(d) = param(query, "dataset") {
        out.push(d);
    }
    if let Some(ds) = param(query, "datasets") {
        out.extend(ds.split(',').map(|s| s.trim().to_string()));
    }
    out
}

/// Dataset names from a JSON body (`dataset` string and/or `datasets` array).
fn value_datasets(value: &Value) -> Vec<String> {
    let mut out = Vec::new();
    if let Some(d) = value.get("dataset").and_then(|d| d.as_str()) {
        out.push(d.to_string());
    }
    if let Some(arr) = value.get("datasets").and_then(|d| d.as_array()) {
        out.extend(arr.iter().filter_map(|v| v.as_str()).map(String::from));
    }
    out
}

/// Look up a query parameter by key, URL-decoding its value.
fn param(query: &str, key: &str) -> Option<String> {
    query.split('&').find_map(|pair| {
        let (k, v) = pair.split_once('=')?;
        if url_decode(k) == key {
            Some(url_decode(v))
        } else {
            None
        }
    })
}

/// Minimal URL decoding: `+` to space and `%XX` escapes.
fn url_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            b'%' if i + 2 < bytes.len() => {
                let hex = std::str::from_utf8(&bytes[i + 1..i + 3]).ok();
                match hex.and_then(|h| u8::from_str_radix(h, 16).ok()) {
                    Some(byte) => {
                        out.push(byte);
                        i += 3;
                    }
                    None => {
                        out.push(bytes[i]);
                        i += 1;
                    }
                }
            }
            b => {
                out.push(b);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_param_and_url_decode() {
        let q = "dataset=us2019b&q=marital+status&names=A%2CB";
        assert_eq!(param(q, "dataset").as_deref(), Some("us2019b"));
        assert_eq!(param(q, "q").as_deref(), Some("marital status"));
        assert_eq!(param(q, "names").as_deref(), Some("A,B"));
        assert_eq!(param(q, "missing"), None);
    }

    #[test]
    fn test_query_datasets() {
        assert_eq!(query_datasets("dataset=us2019b"), vec!["us2019b".to_string()]);
        assert_eq!(
            query_datasets("datasets=a,b,c"),
            vec!["a".to_string(), "b".to_string(), "c".to_string()]
        );
    }
}
