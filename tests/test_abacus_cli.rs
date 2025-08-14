//! This module contains tests for the abacus binary itself, using the assert_cmd
//! crate. I followed the "Rust Command Line Apps" book, which has a helpful
//! tutorial for this sort of testing. Check it out at
//! https://rust-cli.github.io/book/tutorial/testing.html.

use assert_cmd::Command;
use predicates::prelude::*;

#[test]
fn test_help() {
    let mut command = Command::cargo_bin("abacus").unwrap();
    let assert = command.args(["--help"]).assert();
    let pred = predicate::str::contains("Usage: abacus")
        .and(predicate::str::contains("JSON Abacus request"));
    assert
        .success()
        .stdout(pred)
        .stderr(predicate::str::is_empty());
}

#[test]
fn test_request_help() {
    let mut command = Command::cargo_bin("abacus").unwrap();
    let assert = command.args(["request", "--help"]).assert();

    let pred = predicate::str::contains(
        "Given a JSON Abacus request, compute the tabulation it describes",
    );
    assert
        .success()
        .stdout(pred)
        .stderr(predicate::str::is_empty());
}

#[test]
fn test_tab_help() {
    let mut command = Command::cargo_bin("abacus").unwrap();
    let assert = command.args(["tab", "--help"]).assert();

    let pred = predicate::str::contains("Compute a tabulation of one or more variables");
    assert
        .success()
        .stdout(pred)
        .stderr(predicate::str::is_empty());
}

/// Abacus can process the incwage_marst_example.json example and outputs text by default.
#[test]
fn test_request_incwage_marst_example() {
    let mut command = Command::cargo_bin("abacus").unwrap();
    let assert = command
        .args(["request", "tests/requests/incwage_marst_example.json"])
        .assert();
    let pred = predicate::str::starts_with("|         ct | weighted_ct | INCWAGE | MARST |\n");
    assert
        .success()
        .stdout(pred)
        .stderr(predicate::str::is_empty());
}

/// Abacus outputs JSON when passed '-f json' on the command line.
#[test]
fn test_request_json_output() {
    let mut command = Command::cargo_bin("abacus").unwrap();
    let assert = command
        .args([
            "request",
            "tests/requests/incwage_marst_example.json",
            "-f",
            "json",
        ])
        .assert();

    let assert = assert.success();
    let stdout = &assert.get_output().stdout;

    let _: serde_json::Value = serde_json::from_slice(stdout)
        .expect("abacus should output valid JSON when passed '-f json'");
}

/// By default the output format is text, but you can also explicitly request
/// this on the command line.
#[test]
fn test_request_text_output() {
    let mut command = Command::cargo_bin("abacus").unwrap();
    let assert = command
        .args([
            "request",
            "tests/requests/incwage_marst_example.json",
            "-f",
            "text",
        ])
        .assert();

    let pred = predicate::str::starts_with(
        "|         ct | weighted_ct | INCWAGE | MARST |\n\
         |--------------------------------------------|\n\
         |          1 |          84 |       0 |     6 |\n",
    );
    assert.success().stdout(pred);
}

/// Abacus returns an error when it can't find the input file for a request.
#[test]
fn test_request_missing_input_file_error() {
    let mut command = Command::cargo_bin("abacus").unwrap();
    let assert = command
        .args(["request", "tests/requests/this_file_is_not_there.json"])
        .assert();
    let pred = predicate::str::contains("Can't access Abacus request file");
    assert.failure().stderr(pred);
}

/// Without an input file, 'abacus request' reads from stdin. Passing invalid JSON
/// results in an error.
#[test]
fn test_request_invalid_json_on_stdin() {
    let mut command = Command::cargo_bin("abacus").unwrap();
    let assert = command.arg("request").write_stdin("{").assert();

    let pred = predicate::str::starts_with("Error parsing input JSON")
        .and(predicate::str::contains("EOF while parsing an object"));
    assert.failure().stderr(pred);
}

/// Valid JSON is not always a valid Abacus request.
#[test]
fn test_request_valid_json_invalid_request_on_stdin() {
    let mut command = Command::cargo_bin("abacus").unwrap();
    let assert = command.arg("request").write_stdin("{}").assert();

    let pred = predicate::str::starts_with("Error parsing input JSON")
        .and(predicate::str::contains("missing field"));
    assert.failure().stderr(pred);
}

/// 'abacus tab' accepts a -d argument which tells it where to look for data.
#[test]
fn test_tab_specify_data_root() {
    let mut command = Command::cargo_bin("abacus").unwrap();
    let assert = command
        .args(["tab", "usa", "us2015b", "AGE", "-d", "tests/data_root"])
        .assert();

    let pred = predicates::str::starts_with(
        "|         ct | weighted_ct | AGE |\n\
         |--------------------------------|\n\
         |        226 |       30958 |   0 |\n",
    );

    assert.success().stdout(pred);
}

/// 'abacus tab' returns an error if it can't find the provided data root.
#[test]
fn test_tab_missing_data_root_error() {
    let mut command = Command::cargo_bin("abacus").unwrap();
    let assert = command
        .args([
            "tab",
            "usa",
            "us2015b",
            "-d",
            "tests/data_root/does/not/exist",
        ])
        .assert();

    let pred =
        predicate::str::contains("Error while setting up tabulation: Cannot create CSV reader");
    assert.failure().stderr(pred);
}

/// It's an error if you don't specify any variables for 'abacus tab'.
#[test]
fn test_tab_zero_variables() {
    let mut command = Command::cargo_bin("abacus").unwrap();
    let assert = command
        .args(["tab", "usa", "us2015b", "-d", "tests/data_root"])
        .assert();
    let pred = predicate::str::contains("Must supply at least one request variable");
    assert.failure().stderr(pred);
}
