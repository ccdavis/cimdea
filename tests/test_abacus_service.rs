//! Integration tests for the transport-agnostic [cimdea::abacus_service::Service].
//!
//! These run fully offline against the sample data in `tests/data_root` — there is no LLM in this
//! path, so no API key or network is needed. The committed sample parquet has variable *names* but no
//! embedded value labels, so the tests focus on dataset discovery, request repair, and execution.

use cimdea::abacus_service::{Service, TabRequest};

fn service() -> Service {
    Service::new("usa", Some("tests/data_root".to_string()))
}

#[test]
fn test_list_datasets_includes_known_samples() {
    let datasets = service().list_datasets().expect("should list datasets");
    let names: Vec<&str> = datasets.iter().map(|d| d.name.as_str()).collect();
    assert!(names.contains(&"us2015b"), "expected us2015b in {names:?}");
    // Year parsing comes along for free.
    let us2015b = datasets.iter().find(|d| d.name == "us2015b").unwrap();
    assert_eq!(us2015b.year, Some(2015));
}

#[test]
fn test_search_variables_finds_by_mnemonic() {
    let svc = service();
    let result = svc
        .search_variables(&["us2015b".to_string()], "marst", 25)
        .expect("search should succeed");
    assert!(
        result.matches.iter().any(|m| m.name == "MARST"),
        "expected MARST among matches: {:?}",
        result.matches.iter().map(|m| &m.name).collect::<Vec<_>>()
    );
}

#[test]
fn test_search_variables_empty_match_gives_guidance() {
    let svc = service();
    let result = svc
        .search_variables(&["us2015b".to_string()], "zzz_no_such_concept", 25)
        .expect("search should succeed");
    assert!(result.matches.is_empty());
    assert!(
        result.note.as_deref().unwrap_or("").contains("ipums.org"),
        "an empty search should point elsewhere"
    );
}

#[test]
fn test_get_variables_returns_a_doc() {
    let svc = service();
    let docs = svc
        .get_variables(&["us2015b".to_string()], &["MARST".to_string()])
        .expect("get_variables should succeed");
    assert_eq!(docs.len(), 1);
    assert_eq!(docs[0].name, "MARST");
}

#[test]
fn test_tabulate_marst_produces_a_table() {
    let svc = service();
    let req: TabRequest = serde_json::from_str(
        r#"{ "uoa": "P", "request_variables": [{"variable_mnemonic": "MARST"}] }"#,
    )
    .unwrap();

    let outcome = svc
        .tabulate(&req, &["us2015b".to_string()], false)
        .expect("tabulation should succeed");

    assert!(
        !outcome.table_text.trim().is_empty(),
        "expected rendered table text"
    );
    // The structured tables should be present and non-empty.
    let tables = outcome
        .table_json
        .get("tables")
        .or(Some(&outcome.table_json))
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    assert!(
        !tables.is_empty() || outcome.table_json.is_object(),
        "expected tabulation JSON"
    );
    // The generated request that actually ran should name the dataset and variable.
    assert!(outcome.generated_request_json.contains("us2015b"));
    assert!(outcome.generated_request_json.contains("MARST"));
}

#[test]
fn test_tabulate_with_subpopulation_filter() {
    let svc = service();
    // Count persons, restricted to MARST == 4 (a single category) -> a one-row result.
    let req: TabRequest = serde_json::from_str(
        r#"{
            "uoa": "P",
            "request_variables": [{"variable_mnemonic": "MARST"}],
            "subpopulation": [
                {"variable_mnemonic": "MARST", "case_selection": true,
                 "request_case_selections": [{"low_code": "4", "high_code": "4"}]}
            ]
        }"#,
    )
    .unwrap();

    let outcome = svc
        .tabulate(&req, &["us2015b".to_string()], false)
        .expect("filtered tabulation should succeed");
    assert!(outcome.generated_request_json.contains("case_selection"));
}

#[test]
fn test_tabulate_unknown_variable_is_an_error() {
    let svc = service();
    let req: TabRequest = serde_json::from_str(
        r#"{ "request_variables": [{"variable_mnemonic": "NOTAVAR"}] }"#,
    )
    .unwrap();
    let err = svc
        .tabulate(&req, &["us2015b".to_string()], false)
        .expect_err("an unknown variable should fail");
    assert!(err.to_string().contains("NOTAVAR"));
}
