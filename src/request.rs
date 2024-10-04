//! Requests describe requested IPUMS data either as an extract -- multiple records -- or an aggregation of
//! data  from those records. We use names of IPUMS "variables" and "datasets" in the requests along with other details.
//!
//! The code associated with the Request structs will produce queries but not include code to perform the
//! extracts. This work will be handled by "Extract" or "Tabulate" modules which will be responsible for reading
//! data and formatting output. Request objects will connect with metadata storage if needed in order to set up
//! the request object to get handed off to "Extract" or "Tabulate" code.
//!

use serde::de::IntoDeserializer;
use serde::Serialize;
use serde_json::{to_string, Error};

use crate::ipums_data_model::{self, RecordType};
use crate::{
    conventions,
    conventions::Context,
    ipums_metadata_model::{CategoryBin, IpumsDataType, IpumsDataset, IpumsVariable},
    query_gen::Condition,
};

#[derive(Clone, Debug)]
pub struct RequestVariable {
    pub variable: IpumsVariable,
    pub is_general: bool,
    pub general_divisor: usize, // for instance, 100 for RELATE vs RELATED
    pub name: String,
    pub case_selection: Option<Condition>,
    pub attached_variable_pointer: Option<IpumsVariable>,
    pub category_bins: Option<Vec<CategoryBin>>,
}

impl RequestVariable {
    pub fn from_ipums_variable(var: &IpumsVariable, use_general: bool) -> Self {
        let general_divisor: usize = if let Some((_, w)) = var.formatting {
            if w == var.general_width {
                1
            } else if (w < var.general_width) {
                let exponent: u32 = (w - var.general_width).try_into().unwrap();
                let base: i32 = 10;
                base.pow(exponent).try_into().unwrap()
            } else {
                panic!(
                    "Bad metadata, general width can't be larger than detailed width on {}",
                    &var.name
                );
            }
        } else {
            1
        };

        Self {
            variable: var.clone(),
            is_general: use_general,
            general_divisor,
            name: var.name.clone(),
            case_selection: None,
            attached_variable_pointer: None,
            category_bins: var.categoryBins.clone(),
        }
    }

    pub fn detailed_width(&self) -> Result<usize, String> {
        if let Some((_, w)) = self.variable.formatting {
            Ok(w)
        } else {
            Err(format!("No width metadata available for {}", self.name))
        }
    }

    pub fn general_width(&self) -> Result<usize, String> {
        if self.is_general {
            Ok(self.variable.general_width)
        } else {
            Err(format!("General width not available for {}", self.name))
        }
    }

    pub fn requested_width(&self) -> Result<usize, String> {
        if self.is_general {
            self.general_width()
        } else {
            self.detailed_width()
        }
    }

    pub fn data_type(&self) -> Option<IpumsDataType> {
        self.variable.data_type.clone()
    }

    pub fn variable_name(&self) -> String {
        self.variable.name.clone()
    }
}

#[derive(Clone, Debug)]
pub struct RequestSample {
    pub sample: IpumsDataset,
    pub name: String,
}

impl RequestSample {
    pub fn from_ipums_dataset(ds: &IpumsDataset) -> Self {
        Self {
            sample: ds.clone(),
            name: ds.name.clone(),
        }
    }
}

/// Every data request should serialize, deserialize, and produce SQL
/// queries for what it's requesting.
pub trait DataRequest {
    fn get_request_variables(&self) -> Vec<RequestVariable>;
    fn get_request_samples(&self) -> Vec<RequestSample>;
    fn get_conditions(&self) -> Option<Vec<Condition>>;

    /// To the Tractor / generic IPUMS representation
    fn serialize_to_IPUMS_JSON(&self) -> String;

    /// Convert from the Tractor / generic JSON representation.
    fn deserialize_from_ipums_json(
        ctx: &conventions::Context,
        request_type: RequestType,
        json_request: &str,
    ) -> Result<Self, String>
    where
        Self: std::marker::Sized;

    /// Build request from a basic set of variable and dataset names and data locations.
    fn from_names(
        product_name: &str,
        requested_datasets: &[&str],
        requested_variables: &[&str],
        unit_of_analysis: Option<String>,
        optional_product_root: Option<String>,
        optional_data_root: Option<String>,
    ) -> (conventions::Context, Self);

    /// Print a human readable codebook
    fn print_codebook(&self) -> String;

    /// Print a machine readable Stata codebook
    fn print_stata(&self) -> String;
}

#[derive(Clone, Debug)]
pub enum RequestType {
    Tabulation,
    Extract,
}

#[derive(Clone, Debug)]
pub enum OutputFormat {
    CSV,
    FW,
    Json,
    Html,
}

#[derive(Clone, Debug)]
pub enum InputType {
    Fw,
    Parquet,
    Csv,
    NativeDb,
}

impl InputType {
    pub fn data_sub_directory(&self) -> Option<String> {
        match self {
            Self::Csv => Some("csv".to_string()),
            Self::Parquet => Some("parquet".to_string()),
            Self::Fw => None,
            Self::NativeDb => None,
        }
    }
}

// The key point is you can take an impl of a DataRequest and do something with it.
pub fn perform_request(rq: impl DataRequest) -> Result<(), String> {
    Ok(())
}

fn validated_unit_of_analysis(ctx: &Context, unit_of_analysis: Option<String>) -> RecordType {
    let uoa = unit_of_analysis.unwrap_or(ctx.settings.default_unit_of_analysis.value.clone());

    // Check that uoa is present for the current context
    let unit_rectype = match ctx.settings.record_types.get(&uoa) {
        Some(urt) => urt.clone(),
        None => {
            let rectype_names = ctx.settings.record_types.keys().cloned();
            let msg = format!("Record type '{}' not available for use as unit of analysis; the record type is not present in the current context with record types '{:?}'",
            &uoa,
            rectype_names);
            panic!("{}", msg);
        }
    };
    unit_rectype
}

/// The Abacus Request type contains variables to tabulate, variables used for conditions and datasets.
#[derive(Clone, Debug)]
pub struct AbacusRequest {
    pub product: String,                         // name of data collection
    pub request_variables: Vec<RequestVariable>, //  Tabulate these variables, and use general / detailed and the category bins
    pub subpopulation: Vec<RequestVariable>,     // These will provide the data for the conditions
    pub request_samples: Vec<RequestSample>,
    pub unit_rectype: ipums_data_model::RecordType,
    pub output_format: OutputFormat,
    pub use_general_variables: bool,
    pub data_root: Option<String>,
}

impl DataRequest for AbacusRequest {


}

impl AbacusRequest {
    /// Accepts a single JSON with keys for Request, Subpop and any other arguments.
    /// ///
    ///  { "product": "usa",
    ///  "data_root" : "/pkg/ipums/usa/output_data/current",
    /// "request_variables": [...],
    /// request_samples: [...],
    ///  "subpop" : [ {...}, {...}],
    /// "uoa" : "P"}
    pub fn from_json(input: &str) -> Result<(conventions::Context, Self), String> {
        let parsed: serde_json::Value = match serde_json::from_str(input) {
            Ok(parsed) => parsed,
            Err(e) => return Err(format!("Error deserializing request: '{}'", e)),
        };

        let Some(product) = parsed["product"].as_str() else {
            return Err("No 'product' in request".to_string());
        };

        let optional_data_root = if let Some(ref r) = parsed["data_root"].as_str() {
            Some(r.to_string())
        } else {
            None
        };

        let mut ctx = conventions::Context::from_ipums_collection_name(
            product,
            None,
            optional_data_root.clone(),
        );

        let Some(parsed_request_samples) = parsed["request_samples"].as_array() else {
            return Err("No 'request_samples' in request.".to_string());
        };
        let Some(parsed_request_variables) = parsed["request_variables"].as_array() else {
            return Err("Request must have 'request_variables' field.".to_string());
        };

        let Some(parsed_subpop) = parsed["subpopulation"].as_array() else {
            return Err("No subpopulation key.".to_string());
        };

        let Some(parsed_uoa) = parsed["uoa"].as_str() else {
            return Err("'uoa' (unit of analysis) required.".to_string());
        };

        let mut requested_dataset_names = Vec::new();
        for rs in parsed_request_samples {
            let Some(name) = rs["name"].as_str() else {
                return Err("Missing name field in RequestSample object.".to_string());
            };
            requested_dataset_names.push(name);
        }

        // Use the names of the requested samples to load partial metadata
        ctx.load_metadata_for_datasets(&requested_dataset_names);

        // With metadata loaded, we can fully instantiate the RequestVariables and RequestSamples
        let uoa  = if let Some(u) = ctx.settings.record_types.clone().get(parsed_uoa){
            u.clone()
        } else {
            return Err("No record type for uoa.".to_string());
        };

        let Some(ref md) = &ctx.settings.metadata else {
            return Err("Insufficient metadata loaded to deserialize request.".to_string());
        };

        let mut rqs = Vec::new();
        for p in parsed_request_samples {
            let name = p["name"].as_str().unwrap();
            let Some(ipums_ds) = md.cloned_dataset_from_name(name) else {
                return Err(format!("No metadata for dataset named {}", &name));
            };
            rqs.push(RequestSample {
                name: name.to_string(),
                sample: ipums_ds,
            });
        }

        let mut rqv = Vec::new();
        for v in parsed_request_variables {
            let name = v["mnemonic"].as_str() else {
                return Err("Missing mnemonic on request variable.".to_string());
            };

            let Some(variable_mnemonic) = v["variable_mnemonic"].as_str() else {
                return Err("Missing variable_mnemonic in RequestVariable.".to_string());
            };

            let Some(ipums_var) = md.cloned_variable_from_name(variable_mnemonic) else {
                return Err(format!(
                    "No variable named '{}' in loaded metadata.",
                    variable_mnemonic
                ));
            };

            let use_general = if v["general_detailed_selection"].is_null() {
                false
            } else {
                if let Some(gendet) = v["general_detail_selection"].as_str() {
                    gendet == "G"
                } else {
                    false
                }
            };

            let mut request_var = RequestVariable::from_ipums_variable(&ipums_var, use_general);

            // TODO add category bins

            rqv.push(request_var);
        }

        let mut subpop = Vec::new();
        for s in parsed_subpop {}

        Ok((ctx, Self {
            product: product.to_string(),
            request_variables: rqv,
            request_samples: rqs,
            subpopulation: subpop,
            output_format: OutputFormat::Json,
            use_general_variables: true,
            unit_rectype: uoa.clone(),
            data_root: optional_data_root,
        }))
    }
}

/// The `SimpleRequest` probably can describe 90% of IPUMS tabulation and extraction requests.
///
/// In a ComplexRequest, Variables could have attached variables or monetary standardization adjustment factors,
/// datasets could have sub-sample sizes or other attrributes. Here with a SimpleRequest we're requesting either a tabulation from
/// the given sources or an extract of data of same.
///
/// When constructing a request or simple request, we may begin with only variable names and dataset names. We must have a minimum
/// set of metadata to build the IpumsVariable and IpumsDataset values out of those names. The IPUMS conventions combined with
/// data file metadata (Parquet) or IPUMS fixed-width layout files will have enough metadata to complete a "Simple" tabulation or
/// extraction.  If we have access to the IPUMS metadata database the IpumsVariable and IpumsDataset values can be enriched with
/// category labels, variable labels and extra dataset information.
#[derive(Clone, Debug)]
pub struct SimpleRequest {
    pub product: String, // name of data collection
    pub datasets: Vec<IpumsDataset>,
    pub variables: Vec<IpumsVariable>,
    pub unit_rectype: ipums_data_model::RecordType,
    pub request_type: RequestType,
    pub output_format: OutputFormat,
    pub conditions: Option<Vec<Condition>>,
    pub use_general_variables: bool,
}

// The new() and some setup stuff is particular to the SimpleRequest or the more complex types of requests.

impl DataRequest for SimpleRequest {
    // A simple builder if we don't have serialized JSON, for tests and CLI use cases.
    // Returns a new context.
    fn from_names(
        product: &str,
        requested_datasets: &[&str],
        requested_variables: &[&str],
        unit_of_analysis: Option<String>,
        optional_product_root: Option<String>,
        optional_data_root: Option<String>,
    ) -> (conventions::Context, Self) {
        let mut ctx =
            conventions::Context::from_ipums_collection_name(product, None, optional_data_root);
        ctx.load_metadata_for_datasets(requested_datasets);
        let unit_rectype = validated_unit_of_analysis(&ctx, unit_of_analysis);

        // Get variables from selections
        let variables = if let Some(ref md) = ctx.settings.metadata {
            let mut loaded_vars = Vec::new();
            for rv in requested_variables {
                if let Some(id) = md.variables_by_name.get(&*rv.to_ascii_uppercase()) {
                    loaded_vars.push(md.variables_index[*id].clone());
                } else {
                    panic!("Variable {} not in any loaded metadata.", rv);
                }
            }
            loaded_vars
        } else {
            Vec::new()
        };

        let datasets = if let Some(ref md) = ctx.settings.metadata {
            let mut loaded_datasets = Vec::new();
            for rd in requested_datasets {
                if let Some(id) = md.datasets_by_name.get(*rd) {
                    loaded_datasets.push(md.datasets_index[*id].clone());
                } else {
                    panic!("No dataset named {} found in metadata or layouts!", rd);
                }
            }
            loaded_datasets
        } else {
            Vec::new()
        };
        // get datasets from selections
        (
            ctx,
            Self {
                product: product.to_string(),
                datasets,
                variables,
                unit_rectype,
                request_type: RequestType::Tabulation,
                output_format: OutputFormat::CSV,
                conditions: None,
                use_general_variables: false,
            },
        )
    }

    fn get_request_variables(&self) -> Vec<RequestVariable> {
        self.variables
            .iter()
            .map(|v| RequestVariable::from_ipums_variable(v, self.use_general_variables))
            .collect()
    }

    fn get_request_samples(&self) -> Vec<RequestSample> {
        self.datasets
            .iter()
            .map(|d| RequestSample::from_ipums_dataset(d))
            .collect()
    }

    fn get_conditions(&self) -> Option<Vec<Condition>> {
        self.conditions.clone()
    }

    #[allow(refining_impl_trait)]
    fn deserialize_from_ipums_json(
        ctx: &conventions::Context,
        request_type: RequestType,
        json_request: &str,
    ) -> Result<Self, String> {
        let parsed: serde_json::Value = match serde_json::from_str(json_request) {
            Ok(parsed) => parsed,
            Err(e) => return Err(format!("Error deserializing request: '{}'", e)),
        };

        let product = parsed["product"].as_str().expect("No 'product' in request");
        let details = parsed["details"]
            .as_object()
            .expect("No 'details' in request.");

        let request_samples = details["request_samples"]
            .as_array()
            .expect("Expected request_samples array.");
        let request_variables = details["request_variables"]
            .as_array()
            .expect("Expected a request_variables array.");
        let output_format = details["output_format"]
            .as_str()
            .expect("No 'output_format' in request.");

        let case_select_logic = details["case_select_logic"]
            .as_str()
            .expect("No case_select_logic in request.");
        let variables = if let Some(ref md) = ctx.settings.metadata {
            let mut checked_vars = Vec::new();
            for v in request_variables.iter() {
                let variable_mnemonic = v["variable_mnemonic"]
                    .as_str()
                    .expect("No 'variable_mnemonic'");
                if let Some(var_value) = md.cloned_variable_from_name(variable_mnemonic) {
                    checked_vars.push(var_value);
                } else {
                    let msg = format!("No variable '{}' in metadata.", variable_mnemonic);
                    return Err(msg);
                }
            }
            checked_vars
        } else {
            panic!("Metadata for context not yet set up.");
        };

        let datasets = if let Some(ref md) = ctx.settings.metadata {
            let mut checked_samples = Vec::new();
            for d in request_samples.iter() {
                let ds_name = d["name"].as_str().expect("missing sample 'name'.");
                if let Some(ipums_ds) = md.cloned_dataset_from_name(ds_name) {
                    checked_samples.push(ipums_ds);
                } else {
                    let msg = format!("No dataset '{}' in metadata.", ds_name);
                    return Err(msg);
                }
            }
            checked_samples
        } else {
            panic!("Metadata for context not yet set up.");
        };

        let output_format = OutputFormat::CSV;

        let unit_of_analysis = None;
        let unit_rectype = validated_unit_of_analysis(&ctx, unit_of_analysis);

        Ok(Self {
            product: product.to_string(),
            datasets,
            variables,
            unit_rectype,
            request_type,
            output_format,
            conditions: None,
            use_general_variables: false,
        })
    }

    fn serialize_to_IPUMS_JSON(&self) -> String {
        "".to_string()
    }

    fn print_stata(&self) -> String {
        "".to_string()
    }

    fn print_codebook(&self) -> String {
        "".to_string()
    }
}

mod test {
    use std::fs;

    use super::*;

    #[test]
    pub fn test_deserialize_into_simple_request() {
        let data_root = String::from("test/data_root");
        let mut ctx =
            conventions::Context::from_ipums_collection_name("usa", None, Some(data_root));

        // Load the mentioned datasets and all their associated variables into metadata
        ctx.load_metadata_for_datasets(&["us2016c", "us2014d"]);
        if let Some(ref md) = ctx.settings.metadata {
            println!("loaded {} variables.", md.variables_index.len());
            println!("{:?}", md.variables_by_name.get("YEAR"));
            for v in &md.variables_index {
                //println!("{}",v.name);
            }
        }

        let json_request = fs::read_to_string("test/requests/usa_extract.json")
            .expect("Error reading test fixture in test/requests");
        let simple_request =
            SimpleRequest::deserialize_from_ipums_json(&ctx, RequestType::Extract, &json_request);
        if let Err(ref e) = simple_request {
            eprintln!("Parsing error in test: '{}'", e);
        }
        assert!(simple_request.is_ok());
        if let Ok(rq) = simple_request {
            assert_eq!(rq.product, "usa");
        }
    }

    #[test]
    pub fn test_from_names() {
        let data_root = String::from("test/data_root");
        let (ctx, rq) = SimpleRequest::from_names(
            "usa",
            &["us2015b"],
            &["AGE", "MARST", "GQ", "YEAR"],
            Some("P".to_string()),
            None,
            Some(data_root),
        );

        assert_eq!(4, rq.variables.len());
        assert_eq!(rq.product, "usa");
        assert_eq!(1, rq.datasets.len());
    }

    #[test]
    pub fn test_abacus_request_from_json() {
        let json_request = fs::read_to_string("test/requests/usa_abacus_request.json")
            .expect("Error reading test fixture in test/requests");

        let abacus_request = AbacusRequest::from_json(&json_request);
        match abacus_request {
            Err(ref e) => eprintln!("Error was '{}'", e),
            _ => (),
        }
        assert!(abacus_request.is_ok());
    }
}
