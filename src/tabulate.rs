//! The high level module for executing and formatting tabulations.
//!
//!   The result of the tabulations are tabulation::Table structs that
//! carry some metadata information with them to be used by formatters or even codebook
//! generators.
//!

use crate::conventions::Context;
use crate::ipums_metadata_model::IpumsDataType;
use crate::mderror::MdError;
use crate::query_gen::tab_queries;
use crate::query_gen::DataPlatform;
use crate::request::DataRequest;
use crate::request::InputType;
use crate::request::RequestVariable;
use duckdb::Connection;

use serde::Serialize;

#[derive(Clone, Debug)]
pub enum TableFormat {
    Csv,
    Html,
    Json,
    TextTable,
}

impl TableFormat {
    pub fn from_str(name: &str) -> Result<Self, MdError> {
        let tf = match name.to_ascii_lowercase().as_str() {
            "csv" => Self::Csv,
            "json" => Self::Json,
            "text" => Self::TextTable,
            "html" => Self::Html,
            _ => return Err(MdError::Msg("unknown format name.".to_string())),
        };
        Ok(tf)
    }
}

#[derive(Clone, Debug)]
pub enum OutputColumn {
    Constructed {
        name: String,
        width: usize,
        data_type: IpumsDataType,
    },
    RequestVar(RequestVariable),
}

/// The RequestVar variant on OutputColumn has a real RequestVariable struct because there is a lot of useful information in there
/// to help format or generate codebooks etc. However for basic table serialization we only want to capture the
/// name, type and format width.We don't want to serialize the whole content of the RequestVar varient into JSON.
/// This serialization exists to convert an tabulate::Table into JSON for outside consumption.
impl Serialize for OutputColumn {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStructVariant;
        match &self {
            Self::Constructed {
                name,
                width,
                data_type,
            } => {
                let mut ser =
                    serializer.serialize_struct_variant("OutputColumn", 0, "Constructed", 3)?;
                ser.serialize_field("name", &name)?;
                ser.serialize_field("width", &width)?;
                ser.serialize_field("data_type", &format!("{}", data_type))?;
                ser.end()
            }
            Self::RequestVar(ref v) => {
                let mut ser =
                    serializer.serialize_struct_variant("OutputColumn", 1, "RequestVar", 3)?;
                ser.serialize_field("name", &v.name)?;
                ser.serialize_field("width", &v.requested_width().expect("Width not available."))?;
                ser.serialize_field(
                    "data_type",
                    &format!(
                        "{}",
                        &v.variable.data_type.clone().expect(
                            "Variables must have data types to allow serializing of table data."
                        )
                    ),
                )?;
                ser.end()
            }
        }
    } // serialize trait
} // impl

impl OutputColumn {
    pub fn name(&self) -> String {
        match self {
            Self::Constructed { ref name, .. } => name.clone(),
            Self::RequestVar(ref v) => v.name.clone(),
        }
    }

    pub fn width(&self) -> Result<usize, MdError> {
        match self {
            Self::Constructed { ref width, .. } => Ok(*width),
            Self::RequestVar(ref v) => {
                if !v.is_general {
                    if let Some((_, wid)) = v.variable.formatting {
                        Ok(wid)
                    } else {
                        Err(MdError::InvalidMetadata(
                            "width from metadata variable required".to_string(),
                        ))
                    }
                } else {
                    Ok(v.variable.general_width)
                }
            }
        }
    }
} // impl

// If we want we can use the IpumsVariable categories to replace the numbers in the results (rows)
// with category labels and use the data type and width information to better format the table.

#[derive(Clone, Debug, Serialize)]
pub struct Table {
    pub heading: Vec<OutputColumn>, // variable name columns
    pub rows: Vec<Vec<String>>,
}

impl Table {
    pub fn output(&self, format: TableFormat) -> Result<String, MdError> {
        match format {
            TableFormat::Html | TableFormat::Csv => {
                todo!("Output format {:?} not implemented yet.", format)
            }
            TableFormat::Json => self.format_as_json(),
            TableFormat::TextTable => self.format_as_text(),
        }
    }

    pub fn format_as_json(&self) -> Result<String, MdError> {
        match serde_json::to_string_pretty(&self) {
            Ok(j) => Ok(j),
            Err(e) => Err(MdError::Msg(format!(
                "Cannot serialize result into json: {e}"
            ))),
        }
    }

    pub fn format_as_text(&self) -> Result<String, MdError> {
        let mut out = String::new();
        let widths = self.column_widths()?;
        for (column, _v) in self.heading.iter().enumerate() {
            let name = self.heading[column].name();
            let column_header = format!("| {n:>w$} ", n = &name, w = widths[column]);
            out.push_str(&column_header);
        }
        out.push_str("|\n");
        out.push_str(&format!(
            "|{:}|",
            str::repeat(&"-", self.text_table_width()? - 2)
        ));
        out.push_str("\n");

        for r in &self.rows {
            for (column, item) in r.iter().enumerate() {
                let w = widths[column];
                let formatted_item = format!("| {value:>width$} ", value = &item, width = w);
                out.push_str(&formatted_item);
            }
            out.push_str("|\n");
        }
        Ok(out)
    }

    pub fn text_table_width(&self) -> Result<usize, MdError> {
        Ok(1 + 3 * self.heading.len() + self.column_widths()?.iter().sum::<usize>())
    }

    fn column_widths(&self) -> Result<Vec<usize>, MdError> {
        let mut widths = Vec::new();
        for (_column, var) in self.heading.iter().enumerate() {
            let name_width = var.name().len();
            let width = var.width()?;
            if name_width < width {
                widths.push(width);
            } else {
                widths.push(name_width);
            }
            /*
            else  if let Some(w) = self.width_from_data(column) {
                    if name_width < w {
                        widths.push(w);
                    } else {
                        widths.push(name_width);
                    }
                } else {
                    panic!("Can't determine column width of data.");
                }
            }
            */
        }
        Ok(widths)
    }

    fn width_from_data(&self, column: usize) -> Option<usize> {
        self.rows.iter().map(|r| r[column].len()).max()
    }

    pub fn empty() -> Self {
        Self {
            rows: Vec::new(),
            heading: Vec::new(),
        }
    }
}

/// A single request can result in multiple tables. Normally there's one table per IPUMS dataset in
/// the request.Right now the InputType::Parquet and  DataPlatform::Duckdb are hard-coded in; they're the main
/// use-case for now. InputType::Csv ought to be pretty interchangable except for performance implications.
/// The DataPlatform::DataFusion alternative would require minor additions to the query generation module.
/// DataPlatform::Polars is also planned and shouldn't require too much additional query gen updates but is unimplemented for now.
pub fn tabulate(ctx: &Context, rq: impl DataRequest) -> Result<Vec<Table>, MdError> {
    let requested_output_columns = &rq
        .get_request_variables()
        .iter()
        .map(|v| OutputColumn::RequestVar(v.clone()))
        .collect::<Vec<OutputColumn>>();

    let mut tables: Vec<Table> = Vec::new();
    let sql_queries = tab_queries(ctx, rq, &InputType::Parquet, &DataPlatform::Duckdb)?;
    let conn = match Connection::open_in_memory() {
        Ok(c) => c,
        Err(e) => return Err(MdError::Msg(format!("{}", e))),
    };
    for q in sql_queries {
        let mut stmt = match conn.prepare(&q) {
            Ok(results) => results,
            Err(e) => return Err(MdError::Msg(format!("{}", e))),
        };

        let mut rows = match stmt.query([]) {
            Ok(r) => r,
            Err(e) => return Err(MdError::Msg(format!("{}", e))),
        };

        let mut output = Table {
            heading: Vec::new(),
            rows: Vec::new(),
        };
        output.heading.push(OutputColumn::Constructed {
            name: "ct".to_string(),
            width: 10,
            data_type: IpumsDataType::Integer,
        });
        output.heading.push(OutputColumn::Constructed {
            name: "weighted_ct".to_string(),
            width: 10,
            data_type: IpumsDataType::Integer,
        });
        output.heading.extend(requested_output_columns.clone());

        while let Some(row) = rows.next().expect("Error reading row.") {
            let mut this_row = Vec::new();
            // Must do this here on row rather than getting column_names() from
            // stmt.column_names() because of a bug in the DuckDB API -- it
            // works on rsqlite but not DuckDB.
            // See https://github.com/duckdb/duckdb-rs/issues/251
            let column_names = row.as_ref().column_names();
            for (column_number, column_name) in column_names.iter().enumerate() {
                let item: usize = match row.get(column_number) {
                    Ok(i) => i,
                    Err(e) => {
                        return Err(MdError::Msg(format!(
                            "Can't extract value for '{}', error was '{}'",
                            &column_name, e
                        )))
                    }
                };
                this_row.push(format!("{}", item));
            }
            output.rows.push(this_row);
        }
        tables.push(output);
    }

    Ok(tables)
}

mod test {
    #[cfg(test)]
    use super::*;
    #[cfg(test)]
    use crate::request::SimpleRequest;
    #[cfg(test)]
    use std::time::*;

    #[test]
    fn test_tabulation() {
        let start = Instant::now();

        let data_root = String::from("test/data_root");
        let (ctx, rq) = SimpleRequest::from_names(
            "usa",
            &["us2015b"],
            &["MARST", "GQ"],
            Some("P".to_string()),
            None,
            Some(data_root),
        )
        .expect(
            "Setting up this request and context is for a subsequent test and should always work.",
        );

        println!(
            "tabulation test startup took {} ms",
            start.elapsed().as_millis()
        );

        let tabtime = Instant::now();

        let result = tabulate(&ctx, rq);
        println!("Test tabulation took {} ms", tabtime.elapsed().as_millis());
        if let Err(ref e) = result {
            println!("{}", e);
        }

        assert!(result.is_ok(), "Should have tabulated.");
        if let Ok(tables) = result {
            assert_eq!(1, tables.len());
            for t in tables {
                println!(
                    "{}",
                    t.format_as_text()
                        .expect("should be able to format as text")
                );
                assert_eq!(18, t.rows.len());
                assert_eq!(4, t.rows[0].len());
            }
        }
    }
}
