use crate::query_gen::Condition;
use crate::{
    conventions,
    ipums_metadata_model::{self, *},
};

pub trait DataRequest {
    fn extract_query(&self) -> String;
    fn aggregate_query(&self) -> String;
    fn serialize_to_IPUMS_JSON(&self) -> String;
    fn deserialize_from_ipums_json(json_request: &str) -> Self;
    fn from_simple_request(variable_names: &[&str], dataset_names: &[&str]);
    fn print_codebook(&self) -> String;
    fn print_stata(&self) -> String;
}

pub enum RequestType {
    Tabulation,
    Extract,
}

pub enum OutputFormat {
    CSV,
    FW,
    Json,
}

pub enum InputType {
    Fw,
    Parquet,
    Csv,
}

/// In a ComplexRequest, Variables could have attached variables or monetary standardization adjustment factors,
/// datasets could have sub-sample sizes or other attrributes. Here with a SimpleRequest we're requesting either a tabulation from
/// the given sources or an extract of data of same.
///
/// When constructing a request or simple request, we may begin with only variable names and dataset names. We must have a minimum
/// set of metadata to build the IpumsVariable and IpumsDataset values out of those names. The IPUMS conventions combined with
/// data file metadata (Parquet) or IPUMS fixed-width layout files will have enough metadata. If we have access to the IPUMS
/// metadata database the IpumsVariable and IpumsDataset values can be enriched with category labels, variable labels and extra
/// dataset information.
pub struct SimpleRequest {
    pub product: String, // name of data collection
    pub variables: Vec<IpumsVariable>,
    pub datasets: Vec<IpumsDataset>,
    pub request_type: RequestType,
    pub output_format: OutputFormat,
    pub conditions: Option<Vec<Condition>>,
}

// The new() and some setup stuff is particular to the SimpleRequest or the more complex types of requests.
impl SimpleRequest {
    // A simple builder if we don't have serialized JSON, for tests and CLI use cases.
    pub fn from_names_no_context(
        product: &str,
        requested_datasets: &Vec<String>,
        requested_variables: &Vec<String>,
    ) -> Self {
        let mut ctx = conventions::Context::default_from_name(product, None, None);
        ctx.load_metadata_for_datasets(requested_datasets);

        // Get variables from selections
        let variables = if let Some(ref md) = ctx.settings.metadata {
            let mut loaded_vars = Vec::new();
            for rv in requested_variables {
                if let Some(id) = md.variables_by_name.get(rv) {
                    loaded_vars.push(md.variables_index[*id].clone());
                } else {
                    panic!("Variable {} not in any loaded metadata.", &rv);
                }
            }
            loaded_vars
        } else {
            Vec::new()
        };

        let datasets = if let Some(ref md) = ctx.settings.metadata {
            let mut loaded_datasets = Vec::new();
            for rd in requested_datasets {
                if let Some(id) = md.datasets_by_name.get(rd) {
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
        Self {
            product: product.to_string(),
            datasets,
            variables,
            request_type: RequestType::Tabulation,
            output_format: OutputFormat::CSV,
            conditions: None,
        }
    }
}
