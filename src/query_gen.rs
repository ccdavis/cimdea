/// This is an exploratory module to try out ideas for aggregating IPUMS data with generic SQL.
/// Instead of the DB specific query builders and parameter binding, see if we can do it in a generic way
/// TODO For Duck DB we need the table name if it's parquet to looke like ` 'table_name.parquet' ` and it needs to be a valid
/// file, or if the parquet is multiple files it would look like ` table_name/*.parquet' `. /
///
/// The IPUMS conventions have been applied earlier; the table / filenames have been checked and determined and
/// weight variables have been checked. We're assuming inputs here are valid.

/// The `Condition` and `CompareOperation` will support the modeling of aggregation and extraction requests which will be converted to
/// SQL.
use crate::ipums_metadata_model::{self, IpumsDataType};
use sql_builder::{prelude::Bind, SqlBuilder};
use std::path::Path;
use std::path::PathBuf;

#[derive(Debug, Clone)]
pub enum DataSource {
    Parquet { name: String, full_path: PathBuf },
    NativeTable { name: String },
    Csv { name: String, full_path: PathBuf },
}

pub enum DataPlatform {
    Duckdb,
    DataFusion,
}

impl DataSource {
    pub fn new(name: String, full_path: Option<PathBuf>) -> Self {
        if let Some(p) = full_path {
            if p.ends_with(".parquet") {
                Self::Parquet { name, full_path: p }
            } else if p.ends_with(".csv") {
                Self::Csv { name, full_path: p }
            } else {
                panic!(
                    "Can't construct DataSource '{}' from {}",
                    &name,
                    &p.display()
                );
            }
        } else {
            Self::NativeTable { name }
        }
    }

    // The table in the 'from' clause needs to be represented differently
    // depending on the platform and if it's an external table or part
    // of a database.
    pub fn for_platform(&self, platform: DataPlatform) -> String {
        match platform {
            DataPlatform::Duckdb => match self {
                Self::Parquet { name, full_path } => {
                    // Check if full path points to a directory

                    format!("'{}'", &full_path.display())
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
}

// TODO not yet dealing with escaping string values
pub enum CompareOperation {
    Equal,
    Less,
    Greater,
    LessEqual,
    GreaterEqual,
    Between,
    In,
}

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

// A generalization of frequency()
// tables: List of table name and alias, like ('us2015b_usa.H.parquet', h_recs), ('us2015b_usa.P.parquet', p_recs)]
//  join_keys: Pairs of keys to use in a join either in where clause like "where h_recs.SERIAL = p_recs.SERIALP "
pub fn cross_tab(
    tables: &[(&str, &str)],
    join_keys: &[(&str, &str)],
    vars: &[&str],
    weight: Option<String>,
    divisor: Option<usize>,
) -> String {
    let freq_field: String = if let Some(w) = weight {
        if let Some(d) = divisor {
            format!("sum({} / :divisor:)", &w)
        } else {
            format!("sum({} )", &w)
        }
    } else {
        "count(*)".to_string()
    } + " as frequency";

    "Not implemented".to_string()
}

// In theory this version can also generate the two simpler versions. I'm building up to it.
pub fn cross_tab_subpopulation(
    tables: &[&str],
    vars: &[&str],
    weight: Option<String>,
    divisor: Option<usize>,
    subpop: &[Condition],
) -> String {
    "Not implemented".to_string()
}
mod test {
    use super::*;

    #[test]
    fn test_frequency_duckdb_parquet() {
        // Determination of specific table names based on dataset happens outside the query generation

        // These are in single quotes to match what Duck DB expects for parquet files
        let us2015b_people = "'us2015b_usa.P.parquet'";
        let us2015b_households = "'us2015b_usa.H.parquet'";

        let q = frequency(us2015b_people, "AGE", None, None);
        assert!(q.len() > 1);

        let expected =
            "SELECT AGE, count(*) as frequency FROM 'us2015b_usa.P.parquet' GROUP BY AGE;";
        assert_eq!(expected, q);

        let hh_q = frequency(
            us2015b_households,
            "VEHICLES",
            Some("HHWT".to_string()),
            Some(100),
        );
        //assert_eq!("",hh_q);
    }
}
