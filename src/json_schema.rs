//! Code for modeling and parsing the incoming JSON schema for extract requests

use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize)]
pub struct CategoryBin {
    code: usize,
    value_label: String,
    low: usize,
    high: usize,
}

#[derive(Deserialize, Serialize)]
pub struct RequestVariable {
    variable_mnemonic: String,
    mnemonic: String,
    general_detailed_selection: (),
    attached_variable_pointer: (),
    case_selection: bool,
    request_case_selections: Vec<()>,
    extract_start: usize,
    extract_width: usize,
}

#[derive(Deserialize, Serialize)]
pub struct RequestSample {
    name: String,
    custom_sampling_ratio: String,
    first_household_sampled: usize,
}
