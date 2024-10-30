//! This module contains tests for the abacus binary itself, using the assert_cmd
//! crate. I followed the "Rust Command Line Apps" book, which has a helpful
//! tutorial for this sort of testing. Check it out at
//! https://rust-cli.github.io/book/tutorial/testing.html.

use assert_cmd::Command;
use predicates::prelude::*;
use serde_json;

#[test]
fn test_help() {
    let mut command = Command::cargo_bin("abacus").unwrap();
    let assert = command.args(["--help"]).assert();
    let pred = predicate::str::contains("Usage: abacus")
        .and(predicate::str::contains("JSON Abacus request"));
    assert
        .code(0)
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
        .code(0)
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

    let assert = assert.code(0);
    let stdout = &assert.get_output().stdout;

    let _: serde_json::Value = serde_json::from_slice(stdout)
        .expect("abacus should output valid JSON when passed '-f json'");
}

/// Abacus returns an error when it can't find the input file for a request.
#[test]
fn test_request_missing_input_file_error() {
    let mut command = Command::cargo_bin("abacus").unwrap();
    let assert = command
        .args(["request", "tests/requests/this_file_is_not_there.json"])
        .assert();
    let pred = predicate::str::contains("Can't access Abacus request file");
    assert.code(1).stderr(pred);
}
