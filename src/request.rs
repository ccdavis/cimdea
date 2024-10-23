//! Requests describe requested IPUMS data either as an extract -- multiple records -- or an aggregation of
//! data  from those records. We use names of IPUMS "variables" and "datasets" to make the requests along with other details.
//!
//! The code associated with the Request structs will produce queries but not include code to perform the
//! extracts. This work will be handled by "Extract" or "Tabulate" modules which will be responsible for reading
//! data and formatting output. Request objects will connect with metadata storage if needed in order to set up
//! the request object to get handed off to "Extract" or "Tabulate" code.
//!
//use serde_json::{to_string, Error};
use crate::ipums_data_model::{self, RecordType};
use crate::{
    conventions,
    conventions::Context,
    input_schema_tabulation,
    input_schema_tabulation::{CategoryBin, GeneralDetailedSelection},
    ipums_metadata_model::{IpumsDataType, IpumsDataset, IpumsVariable},
    mderror::MdError,
    query_gen::Condition,
};

// Given a set of variable and dataset names and a product name, produce a context loaded
// with metadata just for those named parts and return copies of the IpumsVariable and IpumsSample structs.
fn context_from_names_helper(
    product: &str,
    requested_datasets: &[&str],
    requested_variables: &[&str],
    _optional_product_root: Option<String>,
    optional_data_root: Option<String>,
) -> Result<(conventions::Context, Vec<IpumsVariable>, Vec<IpumsDataset>), MdError> {
    let mut ctx =
        conventions::Context::from_ipums_collection_name(product, None, optional_data_root);
    ctx.load_metadata_for_datasets(requested_datasets);

    // Get variables from selections
    let variables = if let Some(ref md) = ctx.settings.metadata {
        let mut loaded_vars = Vec::new();
        for rv in requested_variables {
            if let Some(id) = md.variables_by_name.get(&*rv.to_ascii_uppercase()) {
                loaded_vars.push(md.variables_index[*id].clone());
            } else {
                return Err(MdError::NotInMetadata(format!(
                    "Variable {} not in any loaded metadata.",
                    rv
                )));
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
                return Err(MdError::NotInMetadata(format!(
                    "No dataset named {} found in metadata or layouts!",
                    rd
                )));
            }
        }
        loaded_datasets
    } else {
        Vec::new()
    };
    Ok((ctx, variables, datasets))
}

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
    pub fn from_ipums_variable(var: &IpumsVariable, use_general: bool) -> Result<Self, MdError> {
        let general_divisor: usize = if let Some((_, w)) = var.formatting {
            if w == var.general_width {
                1
            } else if var.general_width < w {
                let exponent: u32 = (w - var.general_width).try_into().unwrap();
                let base: usize = 10;
                base.pow(exponent)
            } else {
                return Err(MdError::InvalidMetadata(format!(
                    "Bad metadata, general width can't be larger than detailed width on {}",
                    &var.name
                )));
            }
        } else {
            1
        };

        Ok(Self {
            variable: var.clone(),
            is_general: use_general,
            general_divisor,
            name: var.name.clone(),
            case_selection: None,
            attached_variable_pointer: None,
            category_bins: var.category_bins.clone(),
        })
    }

    pub fn detailed_width(&self) -> Result<usize, MdError> {
        if let Some((_, w)) = self.variable.formatting {
            Ok(w)
        } else {
            Err(MdError::Msg(format!(
                "No width metadata available for {}",
                self.name
            )))
        }
    }

    pub fn general_width(&self) -> Result<usize, MdError> {
        if self.is_general {
            Ok(self.variable.general_width)
        } else {
            Err(MdError::Msg(format!(
                "General width not available for {}",
                self.name
            )))
        }
    }

    pub fn requested_width(&self) -> Result<usize, MdError> {
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

    /// Convert to the Tractor / generic IPUMS representation
    fn serialize_to_ipums_json(&self) -> String;

    /// Convert from the Tractor / generic JSON representation.
    fn deserialize_from_ipums_json(
        ctx: &conventions::Context,
        request_type: RequestType,
        json_request: &str,
    ) -> Result<Self, MdError>
    where
        Self: std::marker::Sized;

    /// Build request from a basic set of variable and dataset names and data locations.
    /// Get back a SimpleRequest and a Context needed to execute the request or future requests.
    fn from_names(
        product_name: &str,
        requested_datasets: &[&str],
        requested_variables: &[&str],
        unit_of_analysis: Option<String>,
        optional_product_root: Option<String>,
        optional_data_root: Option<String>,
    ) -> Result<(conventions::Context, Self), MdError>
    where
        Self: std::marker::Sized;

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
pub fn perform_request(rq: impl DataRequest) -> Result<(), MdError> {
    todo!("Implement");
}

fn validated_unit_of_analysis(
    ctx: &Context,
    unit_of_analysis: Option<String>,
) -> Result<RecordType, MdError> {
    let uoa = unit_of_analysis.unwrap_or(ctx.settings.default_unit_of_analysis.value.clone());

    // Check that uoa is present for the current context
    let unit_rectype = match ctx.settings.record_types.get(&uoa) {
        Some(urt) => urt.clone(),
        None => {
            let mut rectype_names = ctx
                .settings
                .record_types
                .keys()
                .map(|k| k.as_str())
                .collect::<Vec<_>>();
            rectype_names.sort();
            let rectype_names = rectype_names.join(", ");
            let msg = format!(
                "Record type '{uoa}' not available for use as unit of analysis; \
                 the record type is not present in the current context with record types {rectype_names}"
            );
            return Err(MdError::NotInMetadata(msg));
        }
    };
    Ok(unit_rectype)
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
    fn get_request_variables(&self) -> Vec<RequestVariable> {
        self.request_variables.clone()
    }

    fn get_request_samples(&self) -> Vec<RequestSample> {
        self.request_samples.clone()
    }

    fn get_conditions(&self) -> Option<Vec<Condition>> {
        let conditions = self
            .subpopulation
            .iter()
            .filter_map(|rv| rv.case_selection.clone())
            .collect::<Vec<Condition>>();
        if conditions.len() > 0 {
            Some(conditions)
        } else {
            None
        }
    }

    #[allow(dead_code)]
    fn deserialize_from_ipums_json(
        ctx: &conventions::Context,
        request_type: RequestType,
        json_request: &str,
    ) -> Result<Self, MdError>
    where
        Self: std::marker::Sized,
    {
        todo!("Not implemented yet")
    }

    fn print_codebook(&self) -> String {
        todo!("Not implemented!");
    }

    fn print_stata(&self) -> String {
        todo!("Not implemented");
    }
    /// Inteded for command line utilities. Construct an Abacus Request from variable and dataset names and return
    /// the AbacusRequest as well as the Context needed to run it.
    fn from_names(
        product: &str,
        requested_datasets: &[&str],
        requested_variables: &[&str],
        unit_of_analysis: Option<String>,
        optional_product_root: Option<String>,
        optional_data_root: Option<String>,
    ) -> Result<(conventions::Context, Self), MdError> {
        // get datasets from selections
        let (ctx, variables, datasets) = context_from_names_helper(
            product,
            requested_datasets,
            requested_variables,
            optional_product_root,
            optional_data_root.clone(),
        )?;
        let request_variables = variables
            .iter()
            .map(|v| RequestVariable::from_ipums_variable(v, false))
            .collect::<Result<Vec<RequestVariable>, MdError>>()?;

        let request_samples = datasets
            .iter()
            .map(|d| RequestSample::from_ipums_dataset(d))
            .collect();

        let unit_rectype = validated_unit_of_analysis(&ctx, unit_of_analysis)?;
        Ok((
            ctx,
            Self {
                product: product.to_string(),
                request_samples,
                request_variables,
                unit_rectype,
                output_format: OutputFormat::CSV,
                subpopulation: Vec::new(),
                use_general_variables: false,
                data_root: optional_data_root,
            },
        ))
    }

    fn serialize_to_ipums_json(&self) -> String {
        todo!("Serialization is not yet implemented.")
    }
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
    pub fn from_json(input: &str) -> Result<(conventions::Context, Self), MdError> {
        let request: input_schema_tabulation::AbacusRequest = match serde_json::from_str(input) {
            Ok(request) => request,
            Err(err) => {
                return Err(MdError::Msg(format!(
                    "Error deserializing request: '{err}'"
                )));
            }
        };

        let mut ctx = conventions::Context::from_ipums_collection_name(
            &request.product,
            None,
            request.data_root.clone(),
        );

        let requested_dataset_names: Vec<_> = request
            .request_samples
            .iter()
            .map(|rs| rs.name.as_str())
            .collect();

        // Use the names of the requested samples to load partial metadata
        ctx.load_metadata_for_datasets(requested_dataset_names.as_slice());

        // With metadata loaded, we can fully instantiate the RequestVariables and RequestSamples
        let uoa = if let Some(u) = ctx.settings.record_types.clone().get(&request.uoa) {
            u.clone()
        } else {
            return Err(MdError::NotInMetadata(
                "No record type for uoa.".to_string(),
            ));
        };

        let Some(ref md) = &ctx.settings.metadata else {
            return Err(MdError::Msg(
                "Insufficient metadata loaded to deserialize request.".to_string(),
            ));
        };

        let mut rqs = Vec::new();
        for p in request.request_samples {
            let name = p.name;
            let Some(ipums_ds) = md.cloned_dataset_from_name(&name) else {
                return Err(MdError::NotInMetadata(format!(
                    "No metadata for dataset named {}",
                    &name
                )));
            };
            rqs.push(RequestSample {
                name: name.to_string(),
                sample: ipums_ds,
            });
        }

        let mut rqv = Vec::new();
        for v in request.request_variables {
            let name = v.mnemonic;
            let variable_mnemonic = v.variable_mnemonic;

            let Some(ipums_var) = md.cloned_variable_from_name(&variable_mnemonic) else {
                return Err(MdError::NotInMetadata(format!(
                    "No variable named '{}' in loaded metadata.",
                    variable_mnemonic
                )));
            };

            let use_general = matches!(
                v.general_detailed_selection,
                GeneralDetailedSelection::General
            );

            let mut request_var = RequestVariable::from_ipums_variable(&ipums_var, use_general)?;

            // TODO add category bins

            rqv.push(request_var);
        }

        let mut subpop = Vec::new();
        for s in request.subpopulation {}

        Ok((
            ctx,
            Self {
                product: request.product,
                request_variables: rqv,
                request_samples: rqs,
                subpopulation: subpop,
                output_format: OutputFormat::Json,
                use_general_variables: true,
                unit_rectype: uoa.clone(),
                data_root: request.data_root,
            },
        ))
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
    ) -> Result<(conventions::Context, Self), MdError> {
        // get datasets from selections
        let (ctx, variables, datasets) = context_from_names_helper(
            product,
            requested_datasets,
            requested_variables,
            optional_product_root,
            optional_data_root,
        )?;
        let unit_rectype = validated_unit_of_analysis(&ctx, unit_of_analysis)?;
        Ok((
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
        ))
    }

    fn get_request_variables(&self) -> Vec<RequestVariable> {
        // Note the .expect() below: If we got here from the from_names() then
        // if the metadata is broken (general detailed probably incorrect), we
        // simply can't proceed.
        self.variables
            .iter()
            .map(|v| {
                RequestVariable::from_ipums_variable(v, self.use_general_variables)
                    .expect("Broken metadata.")
            })
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
    ) -> Result<Self, MdError> {
        let parsed: serde_json::Value = match serde_json::from_str(json_request) {
            Ok(parsed) => parsed,
            Err(e) => {
                return Err(MdError::Msg(format!(
                    "Error deserializing request: '{}'",
                    e
                )))
            }
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
                    return Err(MdError::NotInMetadata(msg));
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
                    return Err(MdError::NotInMetadata(msg));
                }
            }
            checked_samples
        } else {
            panic!("Metadata for context not yet set up.");
        };

        let output_format = OutputFormat::CSV;

        let unit_of_analysis = None;
        let unit_rectype =
            validated_unit_of_analysis(&ctx, unit_of_analysis).expect("invalid unit of analysis");

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

    fn serialize_to_ipums_json(&self) -> String {
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
        ).expect("This construction of a request is for setting up a subsequent test and should always work.");

        assert_eq!(4, rq.variables.len());
        assert_eq!(rq.product, "usa");
        assert_eq!(1, rq.datasets.len());
    }

    #[test]
    fn test_abacus_request_from_names() {
        let data_root = String::from("test/data_root");
        let (_ctx, abacus_request) = AbacusRequest::from_names(
            "usa",
            &["us2015b"],
            &["AGE", "MARST", "GQ", "YEAR"],
            Some("P".to_string()),
            None,
            Some(data_root),
        )
        .expect("should be able to construct an AbacusRequest from the given names");

        assert_eq!(abacus_request.request_variables.len(), 4);
        assert_eq!(abacus_request.product, "usa");
        assert_eq!(abacus_request.request_samples.len(), 1);
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

    /// It's an error if the given unit of analysis is not present as a record
    /// type in the context.
    #[test]
    fn test_validated_unit_of_analysis_unknown_rectype_error() {
        let context =
            Context::from_ipums_collection_name("usa", None, Some("test/data_root".to_string()));
        let uoa = "Z";
        assert!(
            !context.settings.record_types.contains_key(uoa),
            "Z should not be a default record type for USA"
        );
        let result = validated_unit_of_analysis(&context, Some(uoa.to_string()));
        match result {
            Ok(record_type) => {
                panic!("expected an error, got back an Ok with RecordType {record_type:?}")
            }
            Err(err) => {
                assert!(err
                    .to_string()
                    .contains("Record type 'Z' not available for use as unit of analysis"));
            }
        }
    }

    #[test]
    fn test_request_variable_from_ipums_variable_invalid_widths_error() {
        let variable = IpumsVariable {
            id: 0,
            name: "RELATE".to_string(),
            data_type: None,
            label: None,
            record_type: "P".to_string(),
            categories: None,
            formatting: Some((100, 4)),
            // This is invalid because it's greater than the width in the formatting
            // field. This will cause the error later.
            general_width: 5,
            description: None,
            category_bins: None,
        };

        let result = RequestVariable::from_ipums_variable(&variable, true);
        assert!(result.is_err(), "expected an error but got {result:?}");
    }

    #[test]
    fn test_request_variable_from_ipums_variable_valid_general_width() {
        let variable = IpumsVariable {
            id: 0,
            name: "RELATE".to_string(),
            data_type: None,
            label: None,
            record_type: "P".to_string(),
            categories: None,
            formatting: Some((100, 4)),
            general_width: 2,
            description: None,
            category_bins: None,
        };

        let rqv = RequestVariable::from_ipums_variable(&variable, true)
            .expect("should convert into a RequestVariable");
        assert_eq!(
            rqv.general_divisor, 100,
            "expected a general divisor of 10^(4-2) = 10^2 = 100"
        );
    }

    #[test]
    fn test_request_variable_from_ipums_variable_equal_widths() {
        let variable = IpumsVariable {
            id: 0,
            name: "AGE".to_string(),
            data_type: None,
            label: None,
            record_type: "P".to_string(),
            categories: None,
            formatting: Some((5, 2)),
            general_width: 2,
            description: None,
            category_bins: None,
        };

        let rqv = RequestVariable::from_ipums_variable(&variable, true)
            .expect("should convert into a RequestVariable");
        assert_eq!(
            rqv.general_divisor, 1,
            "expected a general divisor of 1 because the general and detailed widths are the same"
        );
    }

    #[test]
    fn test_request_variable_from_ipums_variable_no_formatting() {
        let variable = IpumsVariable {
            id: 0,
            name: "AGE".to_string(),
            data_type: None,
            label: None,
            record_type: "P".to_string(),
            categories: None,
            formatting: None,
            general_width: 2,
            description: None,
            category_bins: None,
        };

        let rqv = RequestVariable::from_ipums_variable(&variable, true)
            .expect("should convert into a RequestVariable");
        assert_eq!(
            rqv.general_divisor, 1,
            "expected a general divisor of 1 because there was no detailed width provided"
        );
    }
}
