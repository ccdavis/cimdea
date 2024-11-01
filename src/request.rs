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
    mderror::{metadata_error, parsing_error, MdError},
    query_gen::Condition,
};

// Given a set of variable and dataset names and a product name, produce a context loaded
// with metadata just for those named parts and return copies of the IpumsVariable and IpumsSample structs.
// This is public so it can be used as a test helper.
pub fn context_from_names_helper(
    product: &str,
    requested_datasets: &[&str],
    requested_variables: &[&str],
    _optional_product_root: Option<String>,
    optional_data_root: Option<String>,
) -> Result<(conventions::Context, Vec<IpumsVariable>, Vec<IpumsDataset>), MdError> {
    let mut ctx =
        conventions::Context::from_ipums_collection_name(product, None, optional_data_root)?;
    ctx.load_metadata_for_datasets(requested_datasets)?;

    // Get variables from selections
    let variables = if let Some(ref md) = ctx.settings.metadata {
        let mut loaded_vars = Vec::new();
        for rv in requested_variables {
            if let Some(id) = md.variables_by_name.get(&*rv.to_ascii_uppercase()) {
                loaded_vars.push(md.variables_index[*id].clone());
            } else {
                return Err(metadata_error!("Variable {rv} not in any loaded metadata."));
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
                return Err(metadata_error!(
                    "No dataset named {rd} found in metadata or layouts!"
                ));
            }
        }
        loaded_datasets
    } else {
        Vec::new()
    };
    Ok((ctx, variables, datasets))
}

#[allow(unused)]
#[derive(Clone, Debug)]
pub struct RequestVariable {
    pub variable: IpumsVariable,
    pub general_detailed_selection: GeneralDetailedSelection,
    pub general_divisor: usize, // for instance, 100 for RELATE vs RELATED
    pub name: String,
    pub case_selection: Option<Condition>,
    pub attached_variable_pointer: Option<IpumsVariable>,
    pub category_bins: Option<Vec<CategoryBin>>,
    // extract_start is only useful to help order the request variables and
    // for producing a fixed-width output which we generally don't want.
    extract_start: Option<usize>,

    // extract_width is useful for detecting if a request variable is 'general' vs
    // 'detailed'  and we can derive the 'general_divisor' by comparing 'extract_width'
    // to the var.formatting[1] (start, width) which has the full detailed width.
    // General width isn't available from all metadata sources like layout files.
    extract_width: Option<usize>,
}

impl RequestVariable {
    // Can't impl 'From' directly  with just input_schema_tabulation::RequestVariable because it takes a context
    // as well ...
    fn try_from_input_request_variable(
        ctx: &Context,
        category_bins: &Option<&Vec<CategoryBin>>,
        input_rq: input_schema_tabulation::RequestVariable,
    ) -> Result<Self, MdError> {
        let var = ctx.get_md_variable_by_name(&input_rq.variable_mnemonic)?;
        let mut rq = Self::try_from_ipums_variable(&var, input_rq.general_detailed_selection)?;

        // This is optional; the category bins could have been attached already by way of the IpumsVariable from ctx. If
        // we pass Some() then we're asking to over-ride anything coming from context.
        if let Some(ref bins) = category_bins {
            rq.category_bins = Some(bins.to_vec().clone());
        }

        if input_rq.case_selection {
            rq.case_selection = Condition::try_from_request_case_selections(
                &var,
                &input_rq.request_case_selections,
            )?;
        } else {
            rq.case_selection = None;
        }

        Ok(rq)
    }

    pub fn try_from_ipums_variable(
        var: &IpumsVariable,
        use_general: GeneralDetailedSelection,
    ) -> Result<Self, MdError> {
        let general_divisor: usize = if let Some((_, w)) = var.formatting {
            if w == var.general_width {
                1
            } else if var.general_width < w {
                // We could avoid this unwrap() by using u32s instead of usizes
                // during parsing and metadata loading. But as it is, this is
                // technically a fallible conversion. It's not likely to fail
                // with normal metadata; we'd need to overflow a u32.
                let exponent: u32 = (w - var.general_width).try_into().unwrap();
                let base: usize = 10;
                base.pow(exponent)
            } else {
                return Err(metadata_error!(
                    "variable {} has general width {}, which is larger than its detailed width {}",
                    var.name,
                    var.general_width,
                    w
                ));
            }
        } else {
            1
        };

        Ok(Self {
            variable: var.clone(),
            general_detailed_selection: use_general,
            general_divisor,
            name: var.name.clone(),
            case_selection: None,
            attached_variable_pointer: None,
            category_bins: var.category_bins.clone(),
            extract_start: None,
            extract_width: None,
        })
    }

    pub fn is_general(&self) -> bool {
        GeneralDetailedSelection::General == self.general_detailed_selection
    }

    pub fn detailed_width(&self) -> Result<usize, MdError> {
        if let Some((_, w)) = self.variable.formatting {
            Ok(w)
        } else {
            Err(metadata_error!(
                "No width metadata available for {}",
                self.name
            ))
        }
    }

    pub fn general_width(&self) -> Result<usize, MdError> {
        match (&self.general_detailed_selection, self.extract_width) {
            (GeneralDetailedSelection::General, Some(w)) => Ok(w),
            (GeneralDetailedSelection::General, None) => Err(metadata_error!(
                "'{}' requires 'extract_width' from request currently to determine the general width; not represented in IpumsVariable or not available from current metadata either.",self.name)),
            _ => Err(metadata_error!(
                "General width not available for {}",
                self.name
            ))
        }
    }

    pub fn requested_width(&self) -> Result<usize, MdError> {
        if let GeneralDetailedSelection::General = self.general_detailed_selection {
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

    pub fn is_bucketed(&self) -> bool {
        self.category_bins.is_some()
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

pub enum CaseSelectLogic {
    And,
    Or,
}

// We only ever apply CaseSelectUnit  to household-person but theoretically this is a way
// to select all members of a given unit of analysis contained in the 'unit' if it's
// not the current unit when one record matches. For instance 'EntireHousehold' means
// include all people / person records from the current household if any person records match.
// NOTE: The behavior when the case selection is on a household variable but the extract or
// tabulation is using 'person' as the unit of analysis isn't well defined. In our old code
// we include all persons in a household if a household variable matches even if there's no
// person level variables with case selection. The interaction with the 'and' and 'or' of the case select logic
// across record types and hierarchies is complicated. The old extract engine has a complex approach probably not worth
// reproducing in full here.
pub enum CaseSelectUnit {
    Individual,
    EntireHousehold,
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

    fn case_select_logic(&self) -> CaseSelectLogic;
    fn case_select_unit(&self) -> CaseSelectUnit;
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
#[allow(unused)]
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
            let err = metadata_error!(
                "Record type '{uoa}' not available for use as unit of analysis; \
                 the record type is not present in the current context with record types {rectype_names}"
            );
            return Err(err);
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
    fn case_select_logic(&self) -> CaseSelectLogic {
        CaseSelectLogic::And
    }

    fn case_select_unit(&self) -> CaseSelectUnit {
        CaseSelectUnit::Individual
    }

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

    #[allow(unused)]
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
        let mut lines = Vec::new();
        lines.push("Tabulation\n\n".to_string());

        lines.push(format!("Datasets:"));
        for s in self.get_request_samples() {
            let label = s.sample.label.unwrap_or("".to_string());
            let sample_pct = if let Some(density) = s.sample.sampling_density {
                format!("{}", density * 100.0)
            } else {
                "N/A".to_string()
            };

            lines.push(format!(
                "{}: \"{}\" sample: {} ",
                &s.name, label, sample_pct
            ));
        }

        lines.push("\n\nVariables:".to_string());

        for v in self.get_request_variables() {
            let label = &v.variable.label.clone().unwrap_or("NO LABEL".to_string());
            let general_detailed = if v.is_general() {
                "General".to_string()
            } else {
                "detailed".to_string()
            };

            lines.push(format!("{}\t\t{} -- {}", v.name, &label, &general_detailed));
        }

        lines.push("\n\nSubpopulation filters:\n".to_string());
        if let Some(ref conditions) = self.get_conditions() {
            if conditions.len() > 0 {
                let logic = match self.case_select_logic() {
                    CaseSelectLogic::And => " 'AND'",
                    CaseSelectLogic::Or => " 'OR' ",
                };
                lines.push(format!("Logic across variables: {}\n", logic));

                for c in conditions {
                    let compare_to_list = c
                        .comparison
                        .iter()
                        .map(|cs| cs.print())
                        .collect::<Vec<String>>();
                    lines.push(format!("{} : {}", &c.var.name, &compare_to_list.join("\n")));
                }
            }
        }

        lines.join("\n")
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
            .map(|v| {
                RequestVariable::try_from_ipums_variable(v, GeneralDetailedSelection::Detailed)
            })
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
    pub fn try_from_json(input: &str) -> Result<(conventions::Context, Self), MdError> {
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
        )?;

        let requested_dataset_names: Vec<_> = request
            .request_samples
            .iter()
            .map(|rs| rs.name.as_str())
            .collect();

        // Use the names of the requested samples to load partial metadata
        ctx.load_metadata_for_datasets(requested_dataset_names.as_slice())?;

        // With metadata loaded, we can fully instantiate the RequestVariables and RequestSamples
        let uoa = if let Some(u) = ctx.settings.record_types.clone().get(&request.uoa) {
            u.clone()
        } else {
            return Err(metadata_error!("No record type for uoa."));
        };

        let Some(ref md) = &ctx.settings.metadata else {
            return Err(metadata_error!(
                "Insufficient metadata loaded to deserialize request."
            ));
        };

        let mut rqs = Vec::new();
        for p in request.request_samples {
            let name = p.name;
            let Some(ipums_ds) = md.cloned_dataset_from_name(&name) else {
                return Err(metadata_error!("No metadata for dataset named {name}"));
            };

            rqs.push(RequestSample {
                name: name.to_string(),
                sample: ipums_ds,
            });
        }

        let mut rqv = Vec::new();
        for v in request.request_variables {
            // The category_bins can also come from the IpumsVariable as it's properly part of metadata. However in the request
            // for Abacus we pass category bins on each request for all request variables that need them.
            let bins = request.category_bins.get(&v.variable_mnemonic);
            let request_var = RequestVariable::try_from_input_request_variable(&ctx, &bins, v)?;
            rqv.push(request_var);
        }

        let mut subpop = Vec::new();
        for s in request.subpopulation {
            let bins = request.category_bins.get(&s.variable_mnemonic);
            let spv = RequestVariable::try_from_input_request_variable(&ctx, &bins, s)?;
            subpop.push(spv);
        }

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
    pub use_general_variables: GeneralDetailedSelection,
}

// The new() and some setup stuff is particular to the SimpleRequest or the more complex types of requests.

impl DataRequest for SimpleRequest {
    fn case_select_logic(&self) -> CaseSelectLogic {
        CaseSelectLogic::And
    }

    fn case_select_unit(&self) -> CaseSelectUnit {
        CaseSelectUnit::Individual
    }

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
                use_general_variables: GeneralDetailedSelection::Detailed,
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
                RequestVariable::try_from_ipums_variable(v, self.use_general_variables.clone())
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

        let Some(product) = parsed["product"].as_str() else {
            return Err(parsing_error!("no 'product' in request"));
        };

        let Some(details) = parsed["details"].as_object() else {
            return Err(parsing_error!("no 'details' in request"));
        };

        let Some(request_samples) = details["request_samples"].as_array() else {
            return Err(parsing_error!("expected 'request_samples' array"));
        };

        let Some(request_variables) = details["request_variables"].as_array() else {
            return Err(parsing_error!("expected a request_variables array"));
        };

        let Some(_output_format) = details["output_format"].as_str() else {
            return Err(parsing_error!("no 'output_format' in request"));
        };

        let Some(_case_select_logic) = details["case_select_logic"].as_str() else {
            return Err(parsing_error!("no 'case_select_logic' in request"));
        };

        let variables = if let Some(ref md) = ctx.settings.metadata {
            let mut checked_vars = Vec::new();
            for (index, v) in request_variables.iter().enumerate() {
                let Some(variable_mnemonic) = v["variable_mnemonic"].as_str() else {
                    return Err(parsing_error!(
                        "no 'variable_mnemonic' for request variable {index}"
                    ));
                };

                if let Some(var_value) = md.cloned_variable_from_name(variable_mnemonic) {
                    checked_vars.push(var_value);
                } else {
                    return Err(metadata_error!(
                        "No variable '{variable_mnemonic}' in metadata."
                    ));
                }
            }
            checked_vars
        } else {
            return Err(metadata_error!("Metadata for context not yet set up."));
        };

        let datasets = if let Some(ref md) = ctx.settings.metadata {
            let mut checked_samples = Vec::new();
            for (index, d) in request_samples.iter().enumerate() {
                let Some(ds_name) = d["name"].as_str() else {
                    return Err(parsing_error!("no 'name' for request sample {index}"));
                };

                if let Some(ipums_ds) = md.cloned_dataset_from_name(ds_name) {
                    checked_samples.push(ipums_ds);
                } else {
                    return Err(metadata_error!("No dataset '{ds_name}' in metadata."));
                }
            }
            checked_samples
        } else {
            return Err(metadata_error!("Metadata for context not yet set up."));
        };

        let output_format = OutputFormat::CSV;

        let unit_of_analysis = None;
        let unit_rectype = validated_unit_of_analysis(&ctx, unit_of_analysis)?;

        Ok(Self {
            product: product.to_string(),
            datasets,
            variables,
            unit_rectype,
            request_type,
            output_format,
            conditions: None,
            use_general_variables: GeneralDetailedSelection::Detailed,
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

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    pub fn test_deserialize_into_simple_request() {
        let data_root = String::from("tests/data_root");
        let mut ctx =
            conventions::Context::from_ipums_collection_name("usa", None, Some(data_root))
                .expect("should be able to load context for USA");

        // Load the mentioned datasets and all their associated variables into metadata
        ctx.load_metadata_for_datasets(&["us2016c", "us2014d"])
            .expect("should be able to load metadata for datasets");
        if let Some(ref md) = ctx.settings.metadata {
            println!("loaded {} variables.", md.variables_index.len());

            for _v in &md.variables_index {
                //println!("{}",v.name);
            }
        }

        let json_request = include_str!("../tests/requests/usa_extract.json");
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
        let data_root = String::from("tests/data_root");
        let (_ctx, rq) = SimpleRequest::from_names(
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
        let data_root = String::from("tests/data_root");
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
        let json_request = include_str!("../tests/requests/usa_abacus_request.json");

        let abacus_request = AbacusRequest::try_from_json(&json_request);
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
            Context::from_ipums_collection_name("usa", None, Some("tests/data_root".to_string()))
                .expect("should be able to load context for USA");
        let uoa = "Z";
        assert!(
            !context.settings.record_types.contains_key(uoa),
            "Z should not be a default record type for USA"
        );
        let err = validated_unit_of_analysis(&context, Some(uoa.to_string()))
            .expect_err("expected an error because Z is not a valid record type");
        assert!(err
            .to_string()
            .contains("Record type 'Z' not available for use as unit of analysis"));
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

        let result =
            RequestVariable::try_from_ipums_variable(&variable, GeneralDetailedSelection::General);
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

        let rqv =
            RequestVariable::try_from_ipums_variable(&variable, GeneralDetailedSelection::General)
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

        let rqv =
            RequestVariable::try_from_ipums_variable(&variable, GeneralDetailedSelection::General)
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

        let rqv =
            RequestVariable::try_from_ipums_variable(&variable, GeneralDetailedSelection::General)
                .expect("should convert into a RequestVariable");
        assert_eq!(
            rqv.general_divisor, 1,
            "expected a general divisor of 1 because there was no detailed width provided"
        );
    }
}
