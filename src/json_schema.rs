//! Code for modeling and parsing the incoming JSON schema for extract requests

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize)]
pub struct AbacusRequest {
    product: String,
    data_root: String,
    uoa: String,
    output_format: String,
    subpopulation: Vec<RequestVariable>,
    category_bins: BTreeMap<String, Vec<CategoryBin>>,
    request_samples: Vec<RequestSample>,
    request_variables: Vec<RequestVariable>,
}

#[derive(Deserialize, Serialize)]
pub struct CategoryBin {
    code: usize,
    value_label: String,
    low: Option<usize>,
    high: Option<usize>,
}

#[derive(Deserialize, Serialize)]
pub struct RequestVariable {
    variable_mnemonic: String,
    mnemonic: String,
    general_detailed_selection: String,
    attached_variable_pointer: (),
    case_selection: bool,
    request_case_selections: Vec<RequestCaseSelection>,
    extract_start: usize,
    extract_width: usize,
}

#[derive(Deserialize, Serialize)]
pub struct RequestSample {
    name: String,
    custom_sampling_ratio: Option<String>,
    first_household_sampled: Option<usize>,
}

#[derive(Deserialize, Serialize)]
pub struct RequestCaseSelection {
    low_code: String,
    high_code: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Deserialize a real example AbacusRequest with the request variables
    /// INCWAGE and MARST.
    #[test]
    fn test_deserialize_incwage_marst_example() {
        let json_str = include_str!("../test/requests/incwage_marst_example.json");
        let request: AbacusRequest =
            serde_json::from_str(json_str).expect("should deserialize into an AbacusRequest");

        assert_eq!(request.product, "usa");
        assert_eq!(request.category_bins["INCWAGE"].len(), 17);
    }
}
