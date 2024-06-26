use  request::SimpleRequest;
use  conventions::Context;
use ipums_metadata_model::IpumsVariable;

// If we want we can use the IpumsVariable categories to replace the numbers in the results (rows)
// with category labels and use the data type and width information to better format the table.
pub struct Table {
    pub heading: Vec<IpumsVariable>, // variable name columns
    pub count:  String,
    pub weighted_count: String,
    pub weight_variable: Option<IpumsVariable>,
    pub rows: Vec<String>,
}

impl Table {
    pub fn empty() -> Self {
        Self {
            rows: Vec::new(),
            header: Vec::new(),
            count: "count".to_string(),
            weighted_count: None,
            weight_variable: None,
        }
    }
}

pub fn tabulate(ctx: &Context, rq: &SimpleRequest) -> Result<Table, String>{
    let mut tb = Table::empty();
    Ok(tb)
}