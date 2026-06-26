//! Integration tests for the natural-language → tabulation pipeline.
//!
//! These use a [MockLlmProvider] so they run offline (no API key, no network). They exercise the
//! validation/repair and execution path against the sample data in `tests/data_root`. That sample
//! parquet has no embedded variable labels, so these tests focus on request handling and
//! tabulation rather than value-label documentation (which needs production parquet metadata).

use cimdea::llm::MockLlmProvider;
use cimdea::nl_tabulation::{self, NlConfig};
use cimdea::tabulate::TableFormat;

fn usa_config() -> NlConfig {
    NlConfig {
        product: "usa".to_string(),
        data_root: Some("tests/data_root".to_string()),
        datasets: vec!["us2015b".to_string()],
        category_catalog_max: None,
        detailed: false,
    }
}

#[test]
fn test_simple_tabulation_produces_a_table() {
    let response = r#"{
        "request_kind": "tabulation",
        "abacus_request": {
            "uoa": "P",
            "request_variables": [
                {"variable_mnemonic": "MARST", "general_detailed_selection": ""}
            ],
            "subpopulation": [],
            "category_bins": {}
        },
        "explanation": "Counts persons by marital status.",
        "assumptions": ""
    }"#;

    let provider = MockLlmProvider::new(response);
    let result = nl_tabulation::run(&provider, "count people by marital status", &usa_config())
        .expect("the pipeline should succeed");

    assert_eq!(result.request_kind, "tabulation");
    let tab = result.tabulation.as_ref().expect("there should be a tabulation");
    assert!(!tab.0.is_empty(), "expected at least one table");
    assert!(!tab.0[0].rows.is_empty(), "expected at least one row");

    // The rendered text output should include the explanation and the result section.
    let rendered = result.render(TableFormat::TextTable).expect("should render");
    assert!(rendered.contains("marital status"));
    assert!(rendered.contains("## Result"));
}

#[test]
fn test_subpopulation_filter_runs() {
    // Tabulate marital status only for one marital-status value isn't meaningful, so filter on a
    // different variable: count persons by SEX where MARST = 1 (married, spouse present).
    let response = r#"{
        "request_kind": "tabulation",
        "abacus_request": {
            "uoa": "P",
            "request_variables": [
                {"variable_mnemonic": "SEX", "general_detailed_selection": ""}
            ],
            "subpopulation": [
                {"variable_mnemonic": "MARST", "case_selection": true,
                 "request_case_selections": [{"low_code": "1", "high_code": "1"}]}
            ],
            "category_bins": {}
        },
        "explanation": "Counts persons by sex among the married-spouse-present population.",
        "assumptions": "Defined the subpopulation as MARST = 1."
    }"#;

    let provider = MockLlmProvider::new(response);
    let result = nl_tabulation::run(&provider, "sex breakdown of married people", &usa_config())
        .expect("the pipeline should succeed with a subpopulation filter");

    let tab = result.tabulation.expect("there should be a tabulation");
    assert!(!tab.0[0].rows.is_empty());
}

#[test]
fn test_unknown_variable_is_an_error() {
    let response = r#"{
        "request_kind": "tabulation",
        "abacus_request": {
            "uoa": "P",
            "request_variables": [
                {"variable_mnemonic": "NOTAVARIABLE", "general_detailed_selection": ""}
            ]
        },
        "explanation": "x",
        "assumptions": ""
    }"#;

    let provider = MockLlmProvider::new(response);
    let result = nl_tabulation::run(&provider, "tabulate a made-up variable", &usa_config());
    assert!(
        result.is_err(),
        "referencing a variable not in the data should be an error"
    );
}

/// Verifies that variable + value-label documentation is pulled from production parquet embedded
/// metadata. The committed sample data has no embedded labels, so this test is `#[ignore]`d and
/// only runs when `CIMDEA_NL_DATA_ROOT` points at a data root with a label-carrying `us2019b`
/// parquet (e.g. a copy from gp1). Run with:
///   CIMDEA_NL_DATA_ROOT=/path/to/data_root cargo test --release --test test_nl_tabulation -- --ignored
#[test]
#[ignore]
fn test_value_labels_from_production_parquet() {
    let data_root = std::env::var("CIMDEA_NL_DATA_ROOT")
        .expect("set CIMDEA_NL_DATA_ROOT to a data root with label-carrying us2019b parquet");

    let cfg = NlConfig {
        product: "usa".to_string(),
        data_root: Some(data_root),
        datasets: vec!["us2019b".to_string()],
        category_catalog_max: None,
        detailed: false,
    };

    let response = r#"{
        "request_kind": "tabulation",
        "abacus_request": {
            "uoa": "P",
            "request_variables": [
                {"variable_mnemonic": "MARST", "general_detailed_selection": ""}
            ]
        },
        "explanation": "Counts persons by marital status.",
        "assumptions": ""
    }"#;

    let provider = MockLlmProvider::new(response);
    let result = nl_tabulation::run(&provider, "count people by marital status", &cfg)
        .expect("the pipeline should succeed against production parquet");

    let marst = result
        .variable_docs
        .iter()
        .find(|d| d.name == "MARST")
        .expect("MARST should be documented");
    assert_eq!(marst.label.as_deref(), Some("Marital status"));
    assert!(
        marst
            .categories
            .iter()
            .any(|(code, label)| code == "1" && label == "Married, spouse present"),
        "MARST value labels should be present, got {:?}",
        marst.categories
    );

    let rendered = result.render(TableFormat::TextTable).expect("should render");
    assert!(rendered.contains("Married, spouse present"));
    // The result table should carry an inlined value-label column (not just the legend).
    assert!(
        rendered.contains("MARST_label"),
        "the result table should include a MARST_label column, got:\n{rendered}"
    );
}

/// Verifies general (collapsed) value labels are derived and shown for a `"G"` selection, using the
/// production parquet metadata. Uses a [MockLlmProvider] (no API key/quota) plus real parquet, so it
/// is `#[ignore]`d and gated on `CIMDEA_NL_DATA_ROOT` like the test above. Run with:
///   CIMDEA_NL_DATA_ROOT=/path/to/data_root cargo test --release --test test_nl_tabulation -- --ignored
#[test]
#[ignore]
fn test_general_value_labels_from_production_parquet() {
    let data_root = std::env::var("CIMDEA_NL_DATA_ROOT")
        .expect("set CIMDEA_NL_DATA_ROOT to a data root with label-carrying us2019b parquet");

    let cfg = NlConfig {
        product: "usa".to_string(),
        data_root: Some(data_root),
        datasets: vec!["us2019b".to_string()],
        category_catalog_max: None,
        detailed: false,
    };

    // Tabulate RELATE with the general selection; general code 1 should be labeled "Head/householder".
    let response = r#"{
        "request_kind": "tabulation",
        "abacus_request": {
            "uoa": "P",
            "request_variables": [
                {"variable_mnemonic": "RELATE", "general_detailed_selection": "G"}
            ]
        },
        "explanation": "Counts persons by general relationship to household head.",
        "assumptions": ""
    }"#;

    let provider = MockLlmProvider::new(response);
    let result = nl_tabulation::run(&provider, "people by general relationship", &cfg)
        .expect("the pipeline should succeed against production parquet");

    let relate = result
        .variable_docs
        .iter()
        .find(|d| d.name == "RELATE")
        .expect("RELATE should be documented");
    assert!(relate.general, "RELATE should be marked as a general selection");
    assert!(
        relate
            .categories
            .iter()
            .any(|(code, label)| code == "1" && label == "Head/householder"),
        "general code 1 should be labeled Head/householder, got {:?}",
        relate.categories
    );

    let rendered = result.render(TableFormat::TextTable).expect("should render");
    // The result table should carry a general label column with the derived labels.
    assert!(rendered.contains("RELATE_label"), "expected a RELATE_label column:\n{rendered}");
    assert!(rendered.contains("Head/householder"));
}

#[test]
fn test_microdata_extract_is_recognized_but_not_executed() {
    let response = r#"{
        "request_kind": "microdata_extract",
        "abacus_request": null,
        "explanation": "The user needs row-level microdata for further processing.",
        "assumptions": ""
    }"#;

    let provider = MockLlmProvider::new(response);
    let result = nl_tabulation::run(
        &provider,
        "give me a microdata extract of age and sex to process locally",
        &usa_config(),
    )
    .expect("recognizing an extract request should not error");

    assert_eq!(result.request_kind, "microdata_extract");
    assert!(result.tabulation.is_none());
    assert!(!result.warnings.is_empty());
}
