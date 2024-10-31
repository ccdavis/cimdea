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

use crate::input_schema_tabulation::{self, CategoryBin};
use crate::ipums_metadata_model::{self, IpumsDataType, IpumsVariable};
use crate::request::CaseSelectLogic;
use crate::mderror::{metadata_error, MdError};
use crate::request::DataRequest;
use crate::request::InputType;
use crate::request::RequestVariable;
use std::collections::HashMap;
use std::collections::HashSet;
use std::path::PathBuf;

/// The TabBuilder is meant to assist with one or more tabulations from the same data product.
#[allow(dead_code)]
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
    ) -> Result<Self, MdError> {
        let data_sources = DataSource::for_dataset(ctx, dataset, input_format)?;
        Ok(Self {
            data_sources,
            dataset: dataset.to_string(),
            platform: platform.clone(),
            input_format: input_format.clone(),
        })
    }

    #[allow(dead_code)]
    fn build_from_clause(
        &self,
        ctx: &Context,
        _dataset: &str,
        uoa: &str,
        all_rectypes: &HashSet<String>,
    ) -> Result<String, MdError> {
        let lhs = match self.data_sources.get(uoa) {
            Some(lhs) => lhs,
            None => {
                return Err(MdError::Msg(format!(
                    "no data source for unit of analysis '{uoa}'"
                )));
            }
        };

        let left_platform_specific_path = lhs.for_platform(&self.platform);
        let left_alias = lhs.table_name();

        let mut q = format!("{} as {}", left_platform_specific_path, left_alias);

        // TODO: Handle the remaining tables. Currently the connections between the joined tables are only
        // generated to connect any two tables where we have foreign and primary keys. Three or more
        // correct joins aren't yet supported.
        if self.data_sources.len() > 2 {
            return Err(MdError::Msg(
                "Tabulations across more than two record types not yet supported!".to_string(),
            ));
        }
        for (rt, ds) in &self.data_sources {
            if rt != uoa && all_rectypes.contains(rt) {
                // The uoa should be the lowest record in the hierarchy of record types from requested variables by definition. The 'foreign_key' will point to the record
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

    fn bucket(&self, rq: &RequestVariable) -> Result<String, MdError> {
        let Some(ref bins) = rq.category_bins else {
            return Err(MdError::Msg("No category bins available.".to_string()));
        };
        if bins.len() == 0 {
            return Err(MdError::Msg("Metadata marks this variable as having category bins but the list of bins is empty.".to_string()));
        }
        let mut sql = "case\n".to_string();
        let cases = bins
            .iter()
            .map(|b| match b {
                CategoryBin::LessThan { value, code, .. } => {
                    format!("\twhen {} <= {} then '{:03}'", rq.name, value, code)
                }
                CategoryBin::MoreThan { value, code, .. } => {
                    format!("\twhen {} >= {} then '{:03}'", rq.name, value, code)
                }
                CategoryBin::Range {
                    low, high, code, ..
                } => format!(
                    "\twhen {} >= {} and {} <= {} then '{:03}'",
                    rq.name, low, rq.name, high, code
                ),
            })
            .collect::<Vec<String>>()
            .join("\n");
        sql.push_str(&cases);
        sql.push_str("\nelse '999' end ");
        sql.push_str(&format!("as {}_bucketed", &rq.name));
        Ok(sql)
    }

    fn build_select_clause(
        &self,
        request_variables: &[RequestVariable],
        weight_name: Option<String>,
        weight_divisor: Option<usize>,
    ) -> Result<String, MdError> {
        let mut select_clause = "count(*) as ct".to_string();

        if let Some(ref wt) = weight_name {
            select_clause += &format!(
                ", sum({}/{}) as weighted_ct",
                wt,
                weight_divisor.unwrap_or(1)
            );
        }

        for rq in request_variables {
            // A request variable can be 'general' or 'bucketed' but not both.
            if rq.is_general() && rq.is_bucketed() {
                let msg = format!(
                    "The variable {} can't be both a general variable and use category bins.",
                    &rq.name
                );
                return Err(MdError::Msg(msg));
            }
            select_clause += &if rq.is_general() {
                format!(
                    ", {}/{} as {}",
                    &rq.variable.name, &rq.general_divisor, &rq.name
                )
            } else if rq.is_bucketed() {
                format!(", {} ", &self.bucket(&rq)?)
            } else {
                format!(", {} as {}", &rq.variable.name, &rq.name)
            };
        }

        Ok(select_clause)
    }

    fn build_where_clause(
        &self,
        conditions: &[Condition],
        case_select_logic: CaseSelectLogic,
    ) -> Result<String, MdError> {
        let w: Vec<String> = conditions
            .iter()
            .map(|c| format!("({})", c.to_sql()))
            .collect();

        // The case selection logic can be 'or' or 'and' but typically is 'and'.
        // NOTE: This will apply to the unit of analysis record types / individual. The 'entire household'
        // behavior isn't here.
        match case_select_logic {
            CaseSelectLogic::And => Ok(w.join(" and ")),
            CaseSelectLogic::Or => Ok(w.join(" or ")),
        }
    }

    pub fn make_query(
        &self,
        ctx: &Context,
        abacus_request: &impl DataRequest,
    ) -> Result<String, MdError> {
        let request_variables = abacus_request.get_request_variables();
        let conditions = abacus_request.get_conditions();
        let case_select_logic = abacus_request.case_select_logic();

        if request_variables.len() == 0 {
            return Err(MdError::Msg(
                "Must supply at least one request variable.".to_string(),
            ));
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
            return Err(MdError::Msg(msg));
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
            self.build_select_clause(&request_variables, weight_name, weight_divisor);
        let from_clause = &self.build_from_clause(ctx, &self.dataset, &uoa, &rectypes)?;

        let vars_in_order = &request_variables
            .iter()
            .map(|v| {
                if v.is_bucketed() {
                    format!("{}_bucketed", &v.name)
                } else {
                    v.name.to_string()
                }
            })
            .collect::<Vec<_>>()
            .join(", ");

        let group_by_clause = vars_in_order;
        let order_by_clause = vars_in_order;
        if let Some(conds) = conditions {
            let where_clause = &self.build_where_clause(&conds, case_select_logic)?;
            Ok(format!(
                "select \n{}\nfrom {}\nwhere {}\ngroup by {}\norder by {}",
                &select_clause?, &from_clause, &where_clause, &group_by_clause, &order_by_clause
            ))
        } else {
            Ok(format!(
                "select \n{}\nfrom {}\ngroup by {}\norder by {}",
                &select_clause?, &from_clause, &group_by_clause, &order_by_clause
            ))
        }
    }

    fn get_connecting_foreign_key(
        ctx: &Context,
        from_rt: &str,
        to_parent: &str,
    ) -> Result<String, MdError> {
        if let Some(ref child_rt) = ctx.settings.record_types.get(from_rt) {
            let fkey_name = child_rt
                .foreign_keys
                .iter()
                .find(|(to_rt, _f)| to_rt == to_parent);
            if let Some(key_name) = fkey_name {
                Ok(key_name.1.clone())
            } else {
                Err(MdError::Msg(format!(
                    "Cannot find a connection between '{}' and a parent record type of '{}'",
                    from_rt, to_parent
                )))
            }
        } else {
            Err(MdError::Msg(format!(
                "Cannot find a connection between '{}' and a parent record type of '{}'",
                from_rt, to_parent
            )))
        }
    }

    fn get_id_for_record_type(ctx: &Context, rt: &str) -> Result<String, MdError> {
        if let Some(ref record_type) = ctx.settings.record_types.get(rt) {
            Ok(record_type.unique_id.clone())
        } else {
            Err(metadata_error!("No record type '{rt}' in current context.",))
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
    ) -> Result<HashMap<String, DataSource>, MdError> {
        let paths_by_rectypes = ctx.paths_from_dataset_name(dataset, &input_format)?;
        let mut data_sources = HashMap::new();
        for rt in ctx.settings.record_types.keys() {
            let table_alias = ctx.settings.default_table_name(dataset, rt)?;
            let p = paths_by_rectypes.get(rt).cloned();
            let ds = DataSource::new(table_alias, p)?;
            data_sources.insert(rt.to_string(), ds);
        }

        Ok(data_sources)
    }

    pub fn new(name: String, full_path: Option<PathBuf>) -> Result<Self, MdError> {
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
                Err(MdError::Msg(msg))
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
                Self::Parquet { full_path, .. } => {
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
                Self::Csv { full_path, .. } => format!("'{}'", &full_path.display()),
                Self::NativeTable { name } => name.to_owned(),
            },
            // DataFusion expects the data tables to have been registered already
            // using the full path.
            DataPlatform::DataFusion => match self {
                Self::Parquet { name, .. } => name.to_owned(),
                Self::Csv { name, .. } => name.to_owned(),
                Self::NativeTable { name } => {
                    todo!("No native table type for '{}' in DataFusion yet.", &name)
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
    Equal(String),
    Less(String),
    Greater(String),
    LessEqual(String),
    GreaterEqual(String),
    NotEqual(String),
    Between(String, String),
    In(Vec<String>),
}

impl CompareOperation {
    pub fn name(&self) -> String {
        match self {
            Self::Equal(_) => "equal to",
            Self::Less(_) => "less than",
            Self::Between(_, _) => "between",
            Self::In(_) => "in",
            Self::Greater(_) => "more than",
            Self::GreaterEqual(_) => "greater or equal to",
            Self::LessEqual(_) => "less than or equal to",
            Self::NotEqual(_) => "not equal to",
        }
        .to_string()
    }

    // Mostly to support printing codebooks
    fn values(&self) -> Vec<String> {
        match self {
            Self::Equal(rhs) => vec![rhs.to_string()],
            Self::Less(rhs) => vec![rhs.to_string()],
            Self::Greater(rhs) => vec![rhs.to_string()],
            Self::LessEqual(rhs) => vec![rhs.to_string()],
            Self::GreaterEqual(rhs) => vec![rhs.to_string()],
            Self::NotEqual(rhs) => vec![rhs.to_string()],
            Self::Between(rhsl, rhsr) => vec![rhsl.to_string(), rhsr.to_string()],
            Self::In(rhs_list) => rhs_list.to_vec(),
        }
    }

    // The human readable version of the comparison.
    pub fn print(&self) -> String {
        format!("{} {}", &self.name(), self.values().join(", "))
    }

    pub fn to_sql(&self, lhs: &str) -> String {
        match self {
            Self::Equal(rhs) => format!("{} = {}", lhs, &rhs),
            Self::Less(rhs) => format!("{} < {}", lhs, &rhs),
            Self::Greater(rhs) => format!("{} > {}", lhs, &rhs),
            Self::LessEqual(rhs) => format!("{} <= {}", lhs, &rhs),
            Self::GreaterEqual(rhs) => format!("{} >= {}", lhs, &rhs),
            Self::NotEqual(rhs) => format!("{} != {}", lhs, &rhs),
            Self::Between(rhsl, rhsr) => format!("{} between {} and {}", lhs, &rhsl, &rhsr),
            Self::In(rhs_list) => format!("{} in ({})", lhs, &rhs_list.join(",")),
        }
    }
}

#[derive(Clone, Debug)]
pub struct Condition {
    pub var: ipums_metadata_model::IpumsVariable,
    pub comparison: Vec<CompareOperation>,
    pub data_type: IpumsDataType,
}

impl Condition {
    pub fn new(
        var: &ipums_metadata_model::IpumsVariable,
        comparison: &[CompareOperation],
    ) -> Result<Self, MdError> {
        let data_type = if let Some(ref dt) = var.data_type {
            dt.clone()
        } else {
            IpumsDataType::Integer
        };

        // TODO check with data type and compare_to for a  valid representation (parse  into i32 for example)
        // If values are string type add appropriate escaping and quotes (possibly)
        Ok(Self {
            var: var.clone(),
            comparison: comparison.to_vec(),
            data_type,
        })
    }

    pub fn try_from_request_case_selections(
        var: &IpumsVariable,
        rcs: &[input_schema_tabulation::RequestCaseSelection],
    ) -> Result<Option<Self>, MdError> {
        let data_type = if let Some(ref dt) = var.data_type {
            dt.clone()
        } else {
            IpumsDataType::Integer
        };
        let maybe_comparisons = rcs.iter()
            .map(|cs| {
                if cs.low_code < cs.high_code {
                    Ok(CompareOperation::Between(format!("{}",cs.low_code), format!("{}",cs.high_code)))
                } else if cs.low_code == cs.high_code {
                    Ok(CompareOperation::Equal(format!("{}", cs.low_code)))
                } else {
                    Err(MdError::Msg(format!("Case selection low code must be lower or equal to high code on '{}': {}, {}", &var.name, cs.low_code, cs.high_code)))
                }
            })
            .collect::<Result<Vec<CompareOperation>, MdError>>();
        let comparisons = maybe_comparisons?;
        if comparisons.len() == 0 {
            Ok(None)
        } else {
            Ok(Some(Self {
                var: var.clone(),
                comparison: comparisons,
                data_type,
            }))
        }
    }

    // Pretty sure we'll need this at some point not too far off
    #[allow(dead_code)]
    fn lit(&self, v: &str) -> String {
        match self.data_type {
            IpumsDataType::String => format!("'{}'", v),
            _ => format!("{}", v),
        }
    }

    // A helper method to generate part of an SQL  'where' clause.
    pub fn to_sql(&self) -> String {
        self.comparison
            .iter()
            .map(|c| format!("({})", c.to_sql(&self.var.name)))
            .collect::<Vec<String>>()
            .join(" or ") // by the definition of Condition, 'or' is, always correct.
    }
}

// Returns one query per dataset in the request; if you wanted to tabulate across
// datasets that would be a different query that unions thetables of the same record type...
// You can accomplish the same thing by combining the results of each query.
pub fn tab_queries<R>(
    ctx: &Context,
    request: R,
    input_format: &InputType,
    platform: &DataPlatform,
) -> Result<Vec<String>, MdError>
where
    R: DataRequest,
{
    let mut queries = Vec::new();
    for dataset in request.get_request_samples() {
        let tb = TabBuilder::new(ctx, &dataset.name, platform, input_format)?;
        let q = tb.make_query(ctx, &request)?;
        queries.push(q);
    }
    Ok(queries)
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::request::context_from_names_helper;
    use crate::request::SimpleRequest;

    #[test]
    fn test_bucketing() {
        let data_root = String::from("test/data_root");
        let (ctx, _, _) = context_from_names_helper(
            "usa",
            &["us2015b"],
            &["AGE", "MARST", "GQ", "YEAR", "UHRSWORK"],
            None,
            Some(data_root),
        )
        .expect("Should be able to construct this test context.");

        let tab_builder =
            TabBuilder::new(&ctx, "us2015b", &DataPlatform::Duckdb, &InputType::Parquet)
                .expect("TabBuilder new() for testing should never error out.");

        let uhrswork = ctx
            .get_md_variable_by_name("UHRSWORK")
            .expect("Expected UHRSWORK to be in the test context.");

        let mut uhrswork_rq = RequestVariable::try_from_ipums_variable(
            &uhrswork,
            input_schema_tabulation::GeneralDetailedSelection::Detailed,
        )
        .expect("UHRSWORK should be in the test context.");

        let mut bins = Vec::new();
        bins.push(CategoryBin::LessThan {
            value: 0,
            code: 0,
            label: "N/A".to_string(),
        });
        bins.push(CategoryBin::Range {
            low: 1,
            high: 14,
            code: 1,
            label: "1 to 14 hours worked per week".to_string(),
        });

        bins.push(CategoryBin::Range {
            low: 15,
            high: 34,
            code: 2,
            label: "15 to 34 hours worked per week".to_string(),
        });

        bins.push(CategoryBin::Range {
            low: 35,
            high: 99,
            code: 3,
            label: "35 or more hours worked per week".to_string(),
        });

        uhrswork_rq.category_bins = Some(bins);

        let bucket_fragment_result = tab_builder.bucket(&uhrswork_rq);
        assert!(bucket_fragment_result.is_ok());
        if let Ok(sql) = bucket_fragment_result {
            let correct = r"case
	when UHRSWORK <= 0 then '000'
	when UHRSWORK >= 1 and UHRSWORK <= 14 then '001'
	when UHRSWORK >= 15 and UHRSWORK <= 34 then '002'
	when UHRSWORK >= 35 and UHRSWORK <= 99 then '003'
else '999' end as UHRSWORK_bucketed";

            assert_eq!(correct, &sql);
        }
    }

    #[test]
    fn test_new_condition() {
        let data_root = String::from("test/data_root");
        let (ctx, _) = SimpleRequest::from_names(
            "usa",
            &["us2015b"],
            &["AGE", "MARST", "GQ", "YEAR"],
            Some("P".to_string()),
            None,
            Some(data_root),
        )
        .unwrap();
        let age_var = ctx
            .settings
            .metadata
            .unwrap()
            .cloned_variable_from_name("AGE")
            .expect("'AGE' variable required for tests.");

        let cond1_age = Condition::new(
            &age_var,
            &vec![CompareOperation::In(vec![
                "1".to_string(),
                "2".to_string(),
                "3".to_string(),
            ])],
        );

        assert!(cond1_age.is_ok());
        let cond2_age = Condition::new(
            &age_var,
            &vec![
                CompareOperation::Equal("1".to_string()),
                CompareOperation::Equal("2".to_string()),
                CompareOperation::Equal("3".to_string()),
            ],
        );

        assert!(cond2_age.is_ok());

        let cond3_age = Condition::new(&age_var, &vec![CompareOperation::Equal("1".to_string())]);

        assert!(cond3_age.is_ok());

        let cond4_age = Condition::new(
            &age_var,
            &vec![CompareOperation::Between("1".to_string(), "9".to_string())],
        );

        assert!(cond4_age.is_ok());
    }

    #[test]
    fn test_build_where_clause() {
        let data_root = String::from("test/data_root");
        let (ctx, _) = SimpleRequest::from_names(
            "usa",
            &["us2015b"],
            &["AGE", "MARST", "GQ", "YEAR"],
            Some("P".to_string()),
            None,
            Some(data_root),
        )
        .unwrap();

        let tab_builder =
            TabBuilder::new(&ctx, "us2015b", &DataPlatform::Duckdb, &InputType::Parquet)
                .expect("TabBuilder new() for testing should never error out.");

        let mut test_conditions: Vec<Condition> = Vec::new();

        let age_var = ctx
            .get_md_variable_by_name("AGE")
            .expect("'AGE' variable required for tests.");

        let gq_var = ctx
            .get_md_variable_by_name("GQ")
            .expect("'GQ' variable required for tests.");

        let cond1 = Condition::new(
            &age_var,
            &vec![CompareOperation::In(vec![
                "1".to_string(),
                "2".to_string(),
                "3".to_string(),
            ])],
        )
        .expect("Condition should always be  constructed for testing.");

        assert_eq!("(AGE in (1,2,3))", &cond1.to_sql());

        test_conditions.push(cond1);
        let maybe_where_clause =
            tab_builder.build_where_clause(&test_conditions, CaseSelectLogic::And);
        assert!(maybe_where_clause.is_ok());
        assert_eq!("((AGE in (1,2,3)))", &maybe_where_clause.unwrap());

        let cond2 = Condition::new(&gq_var, &vec![CompareOperation::Equal("1".to_string())])
            .expect("Condition should always be  constructed for testing.");

        test_conditions.push(cond2);

        let maybe_bigger_where_clause =
            tab_builder.build_where_clause(&test_conditions, CaseSelectLogic::And);
        assert!(maybe_bigger_where_clause.is_ok());
        assert_eq!(
            "((AGE in (1,2,3))) and ((GQ = 1))",
            &maybe_bigger_where_clause.unwrap()
        );
    }

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
        )
        .unwrap();

        let queries = tab_queries(&ctx, rq, &InputType::Parquet, &DataPlatform::Duckdb);
        match queries {
            // print the error whatever it is.
            Err(ref e) => {
                println!("{}", e);
                assert_eq!(1, 2);
            }
            _ => (),
        }
        assert!(queries.is_ok());
        if let Ok(qs) = queries {
            assert_eq!(1, qs.len());
            assert!(qs[0].contains("from"));
        }
    }
}
