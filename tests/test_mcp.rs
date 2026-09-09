//! Tests that drive the MCP dispatcher directly (no stdio), against the offline sample data.

use cimdea::abacus_service::Service;
use cimdea::mcp::{McpServer, PROTOCOL_VERSION};
use serde_json::json;

fn server() -> McpServer {
    McpServer::new(Service::new("usa", Some("tests/data_root".to_string())))
}

#[test]
fn test_initialize_advertises_tools_and_instructions() {
    let resp = server()
        .handle(&json!({ "jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {} }))
        .expect("initialize should reply");
    let result = &resp["result"];
    assert_eq!(result["protocolVersion"], PROTOCOL_VERSION);
    assert!(result["capabilities"]["tools"].is_object());
    let instructions = result["instructions"].as_str().unwrap_or("");
    assert!(instructions.contains("search_variables"), "instructions should teach the workflow");
    assert!(instructions.contains("HISPAN"), "instructions should carry IPUMS conventions");
}

#[test]
fn test_initialized_notification_has_no_reply() {
    let resp = server().handle(&json!({ "jsonrpc": "2.0", "method": "notifications/initialized" }));
    assert!(resp.is_none(), "a notification must not produce a response");
}

#[test]
fn test_tools_list_returns_four_tools() {
    let resp = server()
        .handle(&json!({ "jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": {} }))
        .expect("tools/list should reply");
    let tools = resp["result"]["tools"].as_array().expect("tools array");
    let names: Vec<&str> = tools.iter().filter_map(|t| t["name"].as_str()).collect();
    assert_eq!(names.len(), 4, "got {names:?}");
    for expected in ["list_datasets", "search_variables", "get_variables", "tabulate"] {
        assert!(names.contains(&expected), "missing tool {expected}");
    }
}

#[test]
fn test_call_list_datasets() {
    let resp = server()
        .handle(&json!({
            "jsonrpc": "2.0", "id": 3, "method": "tools/call",
            "params": { "name": "list_datasets", "arguments": {} }
        }))
        .expect("tools/call should reply");
    let result = &resp["result"];
    assert_eq!(result["isError"], false);
    let text = result["content"][0]["text"].as_str().unwrap_or("");
    assert!(text.contains("us2015b"), "dataset listing should include us2015b: {text}");
}

#[test]
fn test_call_tabulate_returns_a_table() {
    let resp = server()
        .handle(&json!({
            "jsonrpc": "2.0", "id": 4, "method": "tools/call",
            "params": {
                "name": "tabulate",
                "arguments": {
                    "dataset": "us2015b",
                    "uoa": "P",
                    "request_variables": [{ "variable_mnemonic": "MARST" }]
                }
            }
        }))
        .expect("tabulate should reply");
    let result = &resp["result"];
    assert_eq!(result["isError"], false, "tabulate should succeed: {result}");
    let text = result["content"][0]["text"].as_str().unwrap_or("");
    assert!(text.contains("MARST"), "result should mention MARST: {text}");
    assert!(text.contains("Generated Abacus request"), "result should include the request");
}

#[test]
fn test_call_unknown_tool_is_protocol_error() {
    let resp = server()
        .handle(&json!({
            "jsonrpc": "2.0", "id": 5, "method": "tools/call",
            "params": { "name": "nope", "arguments": {} }
        }))
        .expect("should reply");
    assert!(resp.get("error").is_some(), "unknown tool should be a JSON-RPC error");
}

#[test]
fn test_tabulate_unknown_variable_is_tool_error() {
    let resp = server()
        .handle(&json!({
            "jsonrpc": "2.0", "id": 6, "method": "tools/call",
            "params": {
                "name": "tabulate",
                "arguments": {
                    "dataset": "us2015b",
                    "request_variables": [{ "variable_mnemonic": "NOTAVAR" }]
                }
            }
        }))
        .expect("should reply");
    let result = &resp["result"];
    // A tool-execution failure is reported in-band with isError, not as a protocol error.
    assert_eq!(result["isError"], true);
    let text = result["content"][0]["text"].as_str().unwrap_or("");
    assert!(text.contains("NOTAVAR"), "error text should name the bad variable: {text}");
}
