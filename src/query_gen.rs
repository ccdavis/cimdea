//! Generate queries from a DataRequest . Currently supports cross-tab style queries.
//!
//! Instead of the DB specific query builders and parameter binding, see if we can do it in a generic way
//! TODO For Duck DB we need the table name if it's parquet to looke like ` 'table_name.parquet' ` and it needs to be a valid
//! file, or if the parquet is multiple files it would look like ` table_name/*.parquet' `. /
//!
//! The IPUMS conventions have been applied earlier; the table / filenames have been checked and determined and
//! weight variables have been checked. We're assuming inputs here are valid.
//!
//! The `Condition` and `CompareOperation` will support the modeling of aggregation and extraction requests which will be converted to
//! SQL.
use crate::conventions::Context;
use crate::ipums_data_model::RecordWeight;
use crate::ipums_metadata_model::{self, IpumsDataType};
use crate::request::DataRequest;
use crate::request::InputType;
use crate::request::RequestSample;
use crate::request::RequestVariable;
use parquet::column::reader::get_column_reader;
use sql_builder::{prelude::Bind, SqlBuilder};
use std::collections::HashMap;
use std::collections::HashSet;
use std::path::PathBuf;

/// The TabBuilder is meant to assist with one or more tabulations from the same data product.
struct TabBuilder {
    platform: DataPlatform,
    input_format: InputType,
    dataset: String,
    data_sources: HashMap<String, DataSource>,
}

impl TabBuilder {
    pub fn new(
        ctx: &Context,
        dataset: &str,
        platform: &DataPlatform,
        input_format: &InputType,
    ) -> Result<Self, String> {
        let data_sources = DataSource::for_dataset(ctx, dataset, input_format)?;
        Ok(Self {
            data_sources,
            dataset: dataset.to_string(),
            platform: platform.clone(),
            input_format: input_format.clone(),
        })
    }
}

impl TabBuilder {
    fn build_from_clause(
        &self,
        ctx: &Context,
        dataset: &str,
        uoa: &str,
        all_rectypes: &HashSet<String>,
    ) -> Result<String, String> {
        let lhs = &self.data_sources.get(uoa).unwrap();
        let left_platform_specific_path = lhs.for_platform(&self.platform);
        let left_alias = lhs.table_name();

        let mut q = format!("from {} as {}", left_platform_specific_path, left_alias);

        // TODO: Handle the remaining tables. Currently the connections between the joined tables are only
        // generated to connect any two tables where we have foreign and primary keys. Three or more
        // correct joins aren't yet supported.
        if self.data_sources.len() > 2 {
            return Err(
                "Tabulations across more than two record types not yet supported!".to_string(),
            );
        }
        for (rt, ds) in &self.data_sources {
            if rt != uoa && all_rectypes.contains(rt) {
                // The uoa should be the lowest record in the hierarchy by definition. The 'foreign_key' will point to the record
                // type directly above in the hierarchy. Note this breaks down for sibling records. Variables from sibling records
                // should not be allowed in the same tabulation.
                let left_foreign_key = Self::get_connecting_foreign_key(ctx, uoa, rt)?;

                let platform_specific_path = ds.for_platform(&self.platform);
                let table_alias = ds.table_name();
                let table_id = Self::get_id_for_record_type(ctx, rt)?;
                q = q + &format!(
                    "\n left join  {} {} on {}.{} = {}.{}",
                    platform_specific_path,
                    table_alias,
                    left_alias,
                    left_foreign_key,
                    table_alias,
                    table_id
                );
            }
        }

        Ok(q)
    }

    fn build_select_clause(
        &self,
        request_variables: &[RequestVariable],
        weight_name: Option<String>,
        weight_divisor: Option<usize>,
    ) -> String {
        let mut select_clause = "select count(*) as ct".to_string();

        if let Some(ref wt) = weight_name {
            select_clause += &format!(
                ", sum({}/{}) as weighted_ct",
                wt,
                weight_divisor.unwrap_or(1)
            );
        }

        for rq in request_variables {
            select_clause += &if !rq.is_general {
                format!(", {} as {}", &rq.variable.name, &rq.name)
            } else {
                format!(
                    ", {}/{} as {}",
                    &rq.variable.name, &rq.general_divisor, &rq.name
                )
            };
        }

        select_clause
    }

    pub fn make_query(
        &self,
        ctx: &Context,
        request_variables: &[RequestVariable],
        request_sample: &RequestSample,
    ) -> Result<String, String> {
        if request_variables.len() == 0 {
            return Err("Must supply at least one request variable.".to_string());
        }
        // Find all rectypes used by the requested variables
        let rectypes_vec = request_variables
            .iter()
            .map(|v| v.variable.record_type.clone())
            .collect::<Vec<String>>();

        let rectypes: HashSet<String> = HashSet::from_iter(rectypes_vec.iter().cloned());

        // TODO: Decide the unit of analysis based on variable selection?
        let mut uoa = ctx.settings.default_unit_of_analysis.value.clone();

        if !self.data_sources.contains_key(&uoa) {
            let msg = format!("Can't use unit of analysis '{}' to generate 'from' clause, not in set of record types in '{}'", uoa, ctx.settings.name);
            return Err(msg);
        }

        // What if the default unit of analysis isn't in the requested variables. This covers the common case
        // where only household type variables are in the request. It doesn't handle all cases, such as
        // a request with "activity" and "person" variables, where the uoa (could) be "activity". If we
        // have more than one rectype therefore, we error out.
        if !rectypes.contains(&uoa) {
            if rectypes.len() == 1 {
                uoa = rectypes_vec[0].clone();
            }
        }

        let weight_name = ctx.settings.weight_for_rectype(&uoa);
        let weight_divisor = ctx.settings.weight_divisor(&uoa);

        let select_clause =
            self.build_select_clause(request_variables, weight_name, weight_divisor);
        let from_clause = &self.build_from_clause(ctx, &request_sample.name, &uoa, &rectypes)?;

        // Build this from '.case_selection' on each RequestVariable or other conditions
        let mut where_clause = "".to_string();

        let vars_in_order = &request_variables
            .iter()
            .map(|v| v.name.clone())
            .collect::<Vec<_>>()
            .join(", ");

        let group_by_clause = "group by ".to_string() + &vars_in_order;
        let order_by_clause = "order by ".to_string() + &vars_in_order;
        Ok(format!(
            "{}\n{}\n{}\n{}\n{}",
            &select_clause, &from_clause, &where_clause, &group_by_clause, &order_by_clause
        ))
    }

    fn get_connecting_foreign_key(
        ctx: &Context,
        from_rt: &str,
        to_parent: &str,
    ) -> Result<String, String> {
        if let Some(ref child_rt) = ctx.settings.record_types.get(from_rt) {
            let fkey_name = child_rt
                .foreign_keys
                .iter()
                .find(|(to_rt, f_)| to_rt == to_parent);
            if let Some(key_name) = fkey_name {
                Ok(key_name.1.clone())
            } else {
                Err(format!(
                    "Cannot find a connection between '{}' and a parent record type of '{}'",
                    from_rt, to_parent
                ))
            }
        } else {
            Err(format!(
                "Cannot find a connection between '{}' and a parent record type of '{}'",
                from_rt, to_parent
            ))
        }
    }

    fn get_id_for_record_type(ctx: &Context, rt: &str) -> Result<String, String> {
        if let Some(ref record_type) = ctx.settings.record_types.get(rt) {
            Ok(record_type.unique_id.clone())
        } else {
            Err(format!("No record type '{}' in current context.", rt))
        }
    }
}

#[derive(Debug, Clone)]
pub enum DataSource {
    Parquet { name: String, full_path: PathBuf },
    NativeTable { name: String },
    Csv { name: String, full_path: PathBuf },
}

#[derive(Clone, Debug)]
pub enum DataPlatform {
    Duckdb,
    DataFusion,
}

impl DataSource {
    pub fn for_dataset(
        ctx: &Context,
        dataset: &str,
        input_format: &InputType,
    ) -> Result<HashMap<String, DataSource>, String> {
        let paths_by_rectypes = ctx.paths_from_dataset_name(dataset, &input_format);
        let mut data_sources = HashMap::new();
        for rt in ctx.settings.record_types.keys() {
            let table_alias = ctx.settings.default_table_name(dataset, rt);
            let p = paths_by_rectypes.get(rt).cloned();
            let ds = DataSource::new(table_alias, p)?;
            data_sources.insert(rt.to_string(), ds);
        }

        Ok(data_sources)
    }

    pub fn new(name: String, full_path: Option<PathBuf>) -> Result<Self, String> {
        if let Some(p) = full_path {
            if p.to_string_lossy().ends_with(".parquet") {
                Ok(Self::Parquet { name, full_path: p })
            } else if p.to_string_lossy().ends_with(".csv") {
                Ok(Self::Csv { name, full_path: p })
            } else {
                let msg = format!(
                    "Can't construct DataSource '{}' from {}",
                    &name,
                    &p.display()
                );
                Err(msg)
            }
        } else {
            Ok(Self::NativeTable { name })
        }
    }

    // The table in the 'from' clause needs to be represented differently
    // depending on the platform and if it's an external table or part
    // of a database.
    pub fn for_platform(&self, platform: &DataPlatform) -> String {
        match platform {
            DataPlatform::Duckdb => match self {
                Self::Parquet { name, full_path } => {
                    // Check if full path points to a directory
                    if full_path.is_dir() {
                        // Duckdb can query a directory of parquet files
                        // as if they're a single logical file as long as
                        // the schema matches on all of them.
                        format!("'{}/*.parquet'", &full_path.display())
                    } else {
                        format!("'{}'", &full_path.display())
                    }
                }
                Self::Csv { name, full_path } => format!("'{}'", &full_path.display()),
                Self::NativeTable { name } => name.to_owned(),
            },
            // DataFusion expects the data tables to have been registered already
            // using the full path.
            DataPlatform::DataFusion => match self {
                Self::Parquet { name, .. } => name.to_owned(),
                Self::Csv { name, .. } => name.to_owned(),
                Self::NativeTable { name } => {
                    panic!("No native table type for '{}' in DataFusion.", &name)
                }
            },
        }
    }

    pub fn table_name(&self) -> String {
        match self {
            Self::Parquet { name, .. } => name.clone(),
            Self::Csv { name, .. } => name.clone(),
            Self::NativeTable { name } => name.clone(),
        }
    }
}

// TODO not yet dealing with escaping string values
#[derive(Clone, Debug)]
pub enum CompareOperation {
    Equal,
    Less,
    Greater,
    LessEqual,
    GreaterEqual,
    Between,
    In,
}

#[derive(Clone, Debug)]
pub struct Condition {
    pub var: ipums_metadata_model::IpumsVariable,
    pub comparison: CompareOperation,
    pub compare_to: Vec<String>, // one or more values depending on context
}

impl Condition {
    pub fn new(
        var: &ipums_metadata_model::IpumsVariable,
        data_type: IpumsDataType,
        comparison: CompareOperation,
        compare_to: Vec<String>,
    ) -> Self {
        // TODO check with data type and compare_to for a  valid representation (parse  into i32 for example)
        // If values are string type add appropriate escaping and quotes (possibly)
        Self {
            var: var.clone(),
            comparison,
            compare_to,
        }
    }
}

// Returns one query per dataset in the request; if you wanted to tabulate across
// datasets that would be a different query that unions thetables of the same record type...
// You can accomplish the same thing by combining the results of each query.
pub fn tab_queries(
    ctx: &Context,
    request: impl DataRequest,
    input_format: &InputType,
    platform: &DataPlatform,
) -> Result<Vec<String>, String> {
    let mut queries = Vec::new();
    for dataset in request.get_request_samples() {
        let tb = TabBuilder::new(ctx, &dataset.name, platform, input_format)?;
        let q = tb.make_query(ctx, &request.get_request_variables(), &dataset)?;
        queries.push(q);
    }
    Ok(queries)
}

pub fn frequency(
    table_name: &str,
    variable_name: &str,
    weight: Option<String>,
    divisor: Option<usize>,
) -> String {
    // frequency field will differ if we are weighting and if there's a divisor
    let freq_field: String = if let Some(w) = weight {
        if let Some(d) = divisor {
            format!("sum({} / :divisor:)", &w)
        } else {
            format!("sum({} )", &w)
        }
    } else {
        "count(*)".to_string()
    } + " as frequency";

    let sql = SqlBuilder::select_from(table_name)
        .field(variable_name)
        .field(freq_field)
        .group_by(variable_name)
        .sql()
        .unwrap();
    if let Some(d) = divisor {
        sql.bind_name(&"divisor", &d)
    } else {
        sql
    }
}

mod test {
    use super::*;
    use crate::request::SimpleRequest;

    #[test]
    fn test_frequency_duckdb_parquet() {
        let data_root = String::from("test/data_root");
        let (ctx, rq) = SimpleRequest::from_names(
            "usa",
            &["us2015b"],
            &["AGE", "MARST", "GQ", "YEAR"],
            Some("P".to_string()),
            None,
            Some(data_root),
        );

        let queries = tab_queries(&ctx, rq, &InputType::Parquet, &DataPlatform::Duckdb);
        match queries {
            // print the error whatever it is.
            Err(ref e) => assert_eq!("abc", e),
            _ => (),
        }
        assert!(queries.is_ok());
        if let Ok(qs) = queries {
            assert_eq!(1, qs.len());
            assert!(qs[0].contains("from"));
        }
    }
}
