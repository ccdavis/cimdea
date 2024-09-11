use std::io::empty;

use crate::conventions::Context;
use crate::ipums_metadata_model::IpumsDataType;
use crate::request::InputType;
use crate::request::RequestVariable;
use crate::request::DataRequest;
use crate::query_gen::tab_queries;
use crate::query_gen::DataPlatform;
use duckdb::{params, Connection, Result};

pub enum TableFormat {
    Csv,
    Html,
    Json,
    TextTable,
}
#[derive(Clone,Debug)]
pub enum OutputColumn {
    Constructed { name: String, width: usize, data_type:IpumsDataType },
    RequestVar(RequestVariable),
}

impl OutputColumn {
    pub fn name(&self) -> String {
        match self {
            Self::Constructed { ref name, ..} => name.clone(),
            Self::RequestVar(ref v) => v.name.clone(),
        }
    }

    pub fn width(&self) -> usize {
        match self {
            Self::Constructed { ref width, ..} => *width,
            Self::RequestVar(ref v) => {
                if v.is_detailed {
                    if let Some((_,wid)) = v.variable.formatting {
                        wid
                    } else {
                        panic!("Width from metadata Variable required.");
                    }
                } else {
                    v.variable.general_width
                }

            }
        }
    }
} // impl

// If we want we can use the IpumsVariable categories to replace the numbers in the results (rows)
// with category labels and use the data type and width information to better format the table.
pub struct Table {
    pub heading: Vec<OutputColumn>, // variable name columns
    pub rows: Vec<Vec<String>>,
}

impl Table {
    pub fn output(&self, format: TableFormat) -> String {
        match format {
            TableFormat::Html | TableFormat::Csv | TableFormat::Json => {
                panic!("Output format not implemented yet.")
            }
            TableFormat::TextTable => self.formatAsText(),
        }
    }

    pub fn formatAsText(&self) -> String {
        let mut out = String::new();
        let widths = self.column_widths();

        for (column, v) in self.heading.iter().enumerate() {
            let name = self.heading[column].name();
            let column_header = format!("| {:>1$} |", &name, widths[column]);
            out.push_str(&column_header);
        }
        out.push_str("\n");
        out.push_str(&format!("{:-0$}", self.text_table_width()));

        for r in &self.rows {
            for (column, item) in r.iter().enumerate() {
                let w = widths[column];
                let formatted_item = format!("| {:>1$} ", &item, w);
                out.push_str(&formatted_item);
            }
            out.push_str("|\n");
        }
        return out;
    }

    pub fn text_table_width(&self) -> usize {
        1 + 3 * self.heading.len() + self.column_widths().iter().sum::<usize>()
    }

    fn column_widths(&self) -> Vec<usize> {
        let mut widths = Vec::new();
        for (column, var) in self.heading.iter().enumerate() {
            let name_width = var.name().len();
            let width = var.width();
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
        widths
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

pub fn tabulate(ctx: &Context, rq: impl DataRequest) -> Result<Vec<Table>, String> {
    let requested_output_columns = &rq.get_request_variables().iter()
        .map(|v| OutputColumn::RequestVar(v.clone()))
        .collect::<Vec<OutputColumn>>();

        let mut tables: Vec<Table> = Vec::new();

    let sql_queries =tab_queries(ctx, rq, &InputType::Parquet, &DataPlatform::Duckdb)?;
    let conn = match Connection::open_in_memory() {
        Ok(c) => c,
        Err(e) => return Err(format!("{}",e),)
    };
    for q in sql_queries {
        let mut stmt = match conn.prepare(&q) {
            Ok(results) => results,
            Err(e) => return Err(format!("{}",e)),
        };

        let column_names = stmt.column_names();
        let mut output = Table { heading: Vec::new(), rows: Vec::new()};
        output.heading.push(OutputColumn::Constructed{ name: "ct".to_string(), width:10, data_type: IpumsDataType::Integer});
        output.heading.push(OutputColumn::Constructed{ name: "weighted_ct".to_string(), width:10, data_type: IpumsDataType::Integer});
        output.heading.extend(requested_output_columns.clone());

        let mut rows = match stmt.query([]) {
            Ok(r) => r,
            Err(e) => return Err(format!("{}",e)),
        };

        while let Some(row) = rows.next().expect("Error reading row.") {
            let mut this_row = Vec::new();
            for (column_number, column_name)  in column_names.iter().enumerate() {
                let item = row.get_unwrap(column_number);
                this_row.push(item);
            }
            output.rows.push(this_row);
        }
        tables.push(output);
    }

    Ok(tables)
}
