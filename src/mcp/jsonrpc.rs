//! Minimal JSON-RPC 2.0 helpers for the MCP stdio transport.
//!
//! MCP frames messages as newline-delimited JSON-RPC 2.0 on stdin/stdout (one JSON object per line).
//! This module is just the envelope: success/error constructors and the standard error codes. The
//! method dispatch lives in [super::server].

use serde_json::{json, Value};

pub const JSONRPC_VERSION: &str = "2.0";

// Standard JSON-RPC 2.0 error codes.
pub const PARSE_ERROR: i64 = -32700;
pub const INVALID_REQUEST: i64 = -32600;
pub const METHOD_NOT_FOUND: i64 = -32601;
pub const INVALID_PARAMS: i64 = -32602;
pub const INTERNAL_ERROR: i64 = -32603;

/// A successful JSON-RPC response carrying `result`.
pub fn success(id: Value, result: Value) -> Value {
    json!({ "jsonrpc": JSONRPC_VERSION, "id": id, "result": result })
}

/// A JSON-RPC error response.
pub fn error(id: Value, code: i64, message: impl Into<String>) -> Value {
    json!({
        "jsonrpc": JSONRPC_VERSION,
        "id": id,
        "error": { "code": code, "message": message.into() }
    })
}
