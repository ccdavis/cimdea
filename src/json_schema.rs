//! Code for modeling and parsing the incoming JSON schema for extract requests

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::mderror::MdError;

#[derive(Debug, Deserialize, Serialize)]
pub struct AbacusRequest {
    pub product: String,
    pub data_root: Option<String>,
    pub uoa: String,
    pub output_format: String,
    pub subpopulation: Vec<RequestVariable>,
    pub category_bins: BTreeMap<String, Vec<CategoryBin>>,
    pub request_samples: Vec<RequestSample>,
    pub request_variables: Vec<RequestVariable>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(try_from = "CategoryBinRaw")]
pub enum CategoryBin {
    LessThan {
        value: i64,
        code: u64,
        label: String,
    },
    Range {
        low: i64,
        high: i64,
        code: u64,
        label: String,
    },
    MoreThan {
        value: i64,
        code: u64,
        label: String,
    },
}

impl TryFrom<CategoryBinRaw> for CategoryBin {
    type Error = MdError;

    fn try_from(value: CategoryBinRaw) -> Result<Self, Self::Error> {
        let code = value.code;
        let label = &value.value_label;
        match (value.low, value.high) {
            (Some(low), Some(high)) if high < low => Err(MdError::Msg(format!(
                "category_bins: a low of {} and high of {} do not satisfy low <= high",
                low, high
            ))),
            (Some(low), Some(high)) => Ok(Self::Range {
                low,
                high,
                code,
                label: label.to_owned(),
            }),
            (None, Some(high)) => Ok(Self::LessThan {
                value: high,
                code,
                label: label.to_owned(),
            }),
            (Some(low), None) => Ok(Self::MoreThan {
                value: low,
                code,
                label: label.to_owned(),
            }),
            (None, None) => Err(MdError::Msg(
                "category_bins: must have low, high, or both set to some value".to_string(),
            )),
        }
    }
}

impl CategoryBin {
    pub fn within(&self, test_value: i64) -> bool {
        match self {
            Self::LessThan { value, .. } => test_value < *value,
            Self::Range { low, high, .. } => test_value >= *low && test_value <= *high,
            Self::MoreThan { value, .. } => test_value > *value,
        }
    }
}

#[derive(Deserialize, Serialize)]
struct CategoryBinRaw {
    code: u64,
    value_label: String,
    low: Option<i64>,
    high: Option<i64>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct RequestVariable {
    pub variable_mnemonic: String,
    pub mnemonic: String,
    pub general_detailed_selection: Option<String>,
    pub attached_variable_pointer: (),
    pub case_selection: bool,
    pub request_case_selections: Vec<RequestCaseSelection>,
    pub extract_start: usize,
    pub extract_width: usize,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct RequestSample {
    pub name: String,
    pub custom_sampling_ratio: Option<String>,
    pub first_household_sampled: Option<usize>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(try_from = "RequestCaseSelectionRaw")]
pub struct RequestCaseSelection {
    pub low_code: u64,
    pub high_code: u64,
}

impl TryFrom<RequestCaseSelectionRaw> for RequestCaseSelection {
    type Error = MdError;

    fn try_from(value: RequestCaseSelectionRaw) -> Result<Self, Self::Error> {
        let Ok(low_code) = value.low_code.parse() else {
            return Err(MdError::ParsingError(
                "request_case_selections: cannot parse low_code as an unsigned integer".to_string(),
            ));
        };

        let Ok(high_code) = value.high_code.parse() else {
            return Err(MdError::ParsingError(
                "request_case_selections: cannot parse high_code as an unsigned integer"
                    .to_string(),
            ));
        };

        if high_code < low_code {
            Err(MdError::Msg(format!("request_case_selections: a low_code of {low_code} and high_code of {high_code} do not satisfy low_code <= high_code")))
        } else {
            Ok(Self {
                low_code,
                high_code,
            })
        }
    }
}

#[derive(Deserialize, Serialize)]
struct RequestCaseSelectionRaw {
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

    #[test]
    fn test_category_bin_try_from_less_than() {
        let raw_bin = CategoryBinRaw {
            code: 0,
            value_label: "less than 3".to_string(),
            low: None,
            high: Some(3),
        };
        let bin = CategoryBin::try_from(raw_bin)
            .expect("should successfully convert from CategoryBinRaw");
        assert!(matches!(bin, CategoryBin::LessThan { .. }))
    }

    #[test]
    fn test_category_bin_try_from_more_than() {
        let raw_bin = CategoryBinRaw {
            code: 0,
            value_label: "more than 3".to_string(),
            low: Some(3),
            high: None,
        };
        let bin = CategoryBin::try_from(raw_bin)
            .expect("should successfully convert from CategoryBinRaw");
        assert!(matches!(bin, CategoryBin::MoreThan { .. }));
    }

    #[test]
    fn test_category_bin_try_from_range() {
        let raw_bin = CategoryBinRaw {
            code: 0,
            value_label: "between 3 and 5".to_string(),
            low: Some(3),
            high: Some(5),
        };
        let bin = CategoryBin::try_from(raw_bin)
            .expect("should successfully convert from CategoryBinRaw");
        assert!(matches!(bin, CategoryBin::Range { .. }));
    }

    #[test]
    fn test_category_bin_try_from_no_boundaries_error() {
        let raw_bin = CategoryBinRaw {
            code: 0,
            value_label: "no boundaries!".to_string(),
            low: None,
            high: None,
        };
        let result = CategoryBin::try_from(raw_bin);
        assert!(
            result.is_err(),
            "it should be an error if neither low nor high is provided"
        );
    }

    #[test]
    fn test_category_bin_try_from_high_less_than_low_error() {
        let raw_bin = CategoryBinRaw {
            code: 0,
            value_label: "that's not possible".to_string(),
            low: Some(10),
            high: Some(2),
        };
        let result = CategoryBin::try_from(raw_bin);
        assert!(result.is_err(), "it should be an error if high < low");
    }

    #[test]
    fn test_category_bin_deserialize_range() {
        let json_str =
            "{\"code\": 0, \"value_label\": \"between 3 and 5\", \"low\": 3, \"high\": 5}";
        let category_bin: CategoryBin =
            serde_json::from_str(json_str).expect("should deserialize into CategoryBin");
        assert!(matches!(category_bin, CategoryBin::Range { .. }));
    }

    #[test]
    fn test_category_bin_deserialize_high_less_than_low_error() {
        let json_str =
            "{\"code\": 0, \"value_label\": \"that's not possible\", \"low\": 10, \"high\": 2}";
        let result: Result<CategoryBin, _> = serde_json::from_str(json_str);
        assert!(result.is_err());
    }

    /// Although we represent the low and high codes as strings in the JSON, we
    /// automatically convert them to integers during deserialization.
    #[test]
    fn test_request_case_selection_deserialize() {
        let json_str = "{\"low_code\": \"060\", \"high_code\": \"065\"}";
        let rcs: RequestCaseSelection =
            serde_json::from_str(json_str).expect("should parse into a RequestCaseSelection");
        assert_eq!(rcs.low_code, 60);
        assert_eq!(rcs.high_code, 65);
    }

    #[test]
    fn test_request_case_selection_high_less_than_low_error() {
        let json_str = "{\"low_code\": \"065\", \"high_code\": \"060\"}";
        let result: Result<RequestCaseSelection, _> = serde_json::from_str(json_str);
        assert!(result.is_err());
    }

    #[test]
    fn test_request_case_selection_cannot_convert_int_error() {
        let json_str = "{\"low_code\": \"A\", \"high_code\": \"B\"}";
        let result: Result<RequestCaseSelection, _> = serde_json::from_str(json_str);
        assert!(result.is_err());
    }
}
