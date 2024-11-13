//! Models and parsing logic for incoming JSON tabulation requests.

use std::collections::BTreeMap;

use serde::{Deserialize, Deserializer, Serialize};

use crate::mderror::{parsing_error, MdError};

#[derive(Debug, Deserialize, Eq, PartialEq, Serialize)]
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

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(try_from = "CategoryBinRaw", into = "CategoryBinRaw")]
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

impl From<CategoryBin> for CategoryBinRaw {
    fn from(value: CategoryBin) -> Self {
        match value {
            CategoryBin::LessThan { value, code, label } => Self {
                code,
                value_label: label,
                low: None,
                high: Some(value),
            },
            CategoryBin::MoreThan { value, code, label } => Self {
                code,
                value_label: label,
                low: Some(value),
                high: None,
            },
            CategoryBin::Range {
                low,
                high,
                code,
                label,
            } => Self {
                code,
                value_label: label,
                low: Some(low),
                high: Some(high),
            },
        }
    }
}

#[derive(Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RequestVariable {
    pub variable_mnemonic: String,
    pub mnemonic: String,
    #[serde(deserialize_with = "general_detailed_selection_from_nullable_field")]
    pub general_detailed_selection: GeneralDetailedSelection,
    pub attached_variable_pointer: (),
    pub case_selection: bool,
    pub request_case_selections: Vec<RequestCaseSelection>,
    pub extract_start: usize,
    pub extract_width: usize,
}

#[derive(Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RequestSample {
    pub name: String,
    pub custom_sampling_ratio: Option<String>,
    pub first_household_sampled: Option<usize>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(try_from = "RequestCaseSelectionRaw", into = "RequestCaseSelectionRaw")]
pub enum RequestCaseSelection {
    LessEqual(u64),
    GreaterEqual(u64),
    Between(u64, u64),
}

impl RequestCaseSelection {
    pub fn try_new(low_code: Option<u64>, high_code: Option<u64>) -> Result<Self, MdError> {
        match (low_code, high_code) {
            (None, None) => Err(parsing_error!(
                "at most one of request_case_selections low_code and high_code may be null"
            )),
            (Some(low_code), None) => Ok(Self::GreaterEqual(low_code)),
            (None, Some(high_code)) => Ok(Self::LessEqual(high_code)),
            (Some(low_code), Some(high_code)) if low_code <= high_code => {
                Ok(Self::Between(low_code, high_code))
            }
            (Some(low_code), Some(high_code)) => Err(parsing_error!("request_case_selections low_code must be <= high_code; got low_code={low_code}, high_code={high_code}")),
        }
    }
}
impl TryFrom<RequestCaseSelectionRaw> for RequestCaseSelection {
    type Error = MdError;

    fn try_from(value: RequestCaseSelectionRaw) -> Result<Self, Self::Error> {
        let low_code: Option<u64> = value
            .low_code
            .map(|s| {
                s.parse().map_err(|err| {
                    parsing_error!(
                    "cannot parse request_case_selections low_code as an unsigned integer: {err}"
                )
                })
            })
            .transpose()?;

        let high_code: Option<u64> = value
            .high_code
            .map(|s| {
                s.parse().map_err(|err| {
                    parsing_error!(
                "cannot parse request_case_selections high_code as an unsigned integer: {err}"
                )
                })
            })
            .transpose()?;

        Self::try_new(low_code, high_code)
    }
}

#[derive(Deserialize, Serialize)]
struct RequestCaseSelectionRaw {
    low_code: Option<String>,
    high_code: Option<String>,
}

impl From<RequestCaseSelection> for RequestCaseSelectionRaw {
    fn from(value: RequestCaseSelection) -> Self {
        match value {
            RequestCaseSelection::LessEqual(code) => Self {
                low_code: None,
                high_code: Some(code.to_string()),
            },
            RequestCaseSelection::GreaterEqual(code) => Self {
                low_code: Some(code.to_string()),
                high_code: None,
            },
            RequestCaseSelection::Between(low_code, high_code) => Self {
                low_code: Some(low_code.to_string()),
                high_code: Some(high_code.to_string()),
            },
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub enum GeneralDetailedSelection {
    #[serde(rename = "G")]
    General,
    #[default]
    #[serde(rename = "")]
    Detailed,
}

/// This is because missing attributes and attributes that are null are handled differently by
/// serde, so we can't just say #[serde(default)] for general_detailed_selection.
/// Check out this GitHub issue: https://github.com/serde-rs/serde/issues/1098.
fn general_detailed_selection_from_nullable_field<'de, D>(
    deserializer: D,
) -> Result<GeneralDetailedSelection, D::Error>
where
    D: Deserializer<'de>,
{
    let maybe_gendet = Option::deserialize(deserializer)?;
    Ok(maybe_gendet.unwrap_or_default())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Deserialize a real example AbacusRequest with the request variables
    /// INCWAGE and MARST.
    #[test]
    fn test_deserialize_incwage_marst_example() {
        let json_str = include_str!("../tests/requests/incwage_marst_example.json");
        let request: AbacusRequest =
            serde_json::from_str(json_str).expect("should deserialize into an AbacusRequest");

        assert_eq!(request.product, "usa");
        assert_eq!(request.category_bins["INCWAGE"].len(), 17);
        assert_eq!(
            request.subpopulation[0].general_detailed_selection,
            GeneralDetailedSelection::General
        );
        assert_eq!(
            request.request_variables[0].general_detailed_selection,
            GeneralDetailedSelection::Detailed
        );
    }

    /// Make sure that AbacusRequest serializes in a way that it can also deserialize.
    ///
    /// The serialized string may not be exactly equal to the input string, but
    /// it should deserialize to the same AbacusRequest as the input string does.
    #[test]
    fn test_json_request_round_trip() {
        let json_str = include_str!("../tests/requests/incwage_marst_example.json");
        let deserialized1: AbacusRequest =
            serde_json::from_str(json_str).expect("should deserialize into an AbacusRequest");
        let serialized =
            serde_json::to_string(&deserialized1).expect("should serialize back to a string");
        let deserialized2: AbacusRequest =
            serde_json::from_str(&serialized).expect("should serialize back into an AbacusRequest");
        assert_eq!(deserialized1, deserialized2);
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
        assert_eq!(rcs, RequestCaseSelection::Between(60, 65));
    }

    #[test]
    fn test_request_case_selection_low_code_may_be_null() {
        let json_str = "{\"low_code\": null, \"high_code\": \"9999\"}";
        let rcs: RequestCaseSelection =
            serde_json::from_str(json_str).expect("should parse into a RequestCaseSelection");

        assert_eq!(rcs, RequestCaseSelection::LessEqual(9999));
    }

    #[test]
    fn test_request_case_selection_high_code_may_be_null() {
        let json_str = "{\"low_code\": \"200000\", \"high_code\": null}";
        let rcs: RequestCaseSelection =
            serde_json::from_str(json_str).expect("should parse into a RequestCaseSelection");

        assert_eq!(rcs, RequestCaseSelection::GreaterEqual(200000));
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

    /// If both low_code and high_code are null, then the request case selection
    /// doesn't contain any information and doesn't really make sense. This is
    /// an error.
    #[test]
    fn test_request_case_selection_must_have_a_bound_error() {
        let json_str = "{\"low_code\": null, \"high_code\": null}";
        let result: Result<RequestCaseSelection, _> = serde_json::from_str(json_str);
        assert!(
            result.is_err(),
            "expected an error because both low_code and high_code are null, got {result:?}"
        );
    }

    #[test]
    fn test_deserialize_general_detailed_selection_g() {
        let gen_det: GeneralDetailedSelection = serde_json::from_str("\"G\"")
            .expect("should deserialize into a GeneralDetailedSelection");
        assert_eq!(gen_det, GeneralDetailedSelection::General);
    }

    #[test]
    fn test_deserialize_general_detailed_selection_empty() {
        let gen_det: GeneralDetailedSelection = serde_json::from_str("\"\"")
            .expect("should deserialize into a GeneralDetailedSelection");
        assert_eq!(gen_det, GeneralDetailedSelection::Detailed);
    }
}
