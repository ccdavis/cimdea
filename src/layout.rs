//! Supports reading the "layout" metadata files from IPUMS microdata.
//!
//! These files describe fixed-width
//! formatted IPUMS datasets. While they were developed to act as minimal record layouts for fixed-width
//! they can be useful for getting basic metadata for the dataset.

use crate::ipums_metadata_model::IpumsDataType;
use std::collections::HashMap;
use std::path::Path;
use std::str;

/// An entry (a single line) from a layout file. There's one line per variable (column.)
#[derive(Clone, Debug)]
pub struct LayoutVar {
    pub name: String,
    pub rectype: String,
    pub start: usize,
    pub width: usize,
    pub col: usize, // column number when CSV rather than fixed-width
    pub data_type: IpumsDataType,
}

#[derive(Clone)]
pub struct RecordLayout {
    pub vars: Vec<LayoutVar>,
}

impl RecordLayout {
    pub fn add(&mut self, var: LayoutVar) {
        self.vars.push(var);
    }

    // A no-cost accessor
    pub fn vars(&self) -> &Vec<LayoutVar> {
        &self.vars
    }

    pub fn sorted_vars_by_start(&self) -> Vec<LayoutVar> {
        let mut ordered_vars = self.vars.clone();
        ordered_vars.sort_by(|a, b| a.start.cmp(&b.start));
        ordered_vars
    }

    pub fn new(var: LayoutVar) -> Self {
        Self { vars: vec![var] }
    }

    pub fn new_from_vars(vars: Vec<LayoutVar>) -> Self {
        Self { vars }
    }

    // When we filter, we also apply alphabetical order to match the default parquet
    // schema order; additionally TODO we should really force column order on both
    // fixed-width and parquet to use the order of the selected columns.
    pub fn filtered(&self, selections: &[String]) -> Self {
        let mut ordered_vars = self.vars.clone();
        ordered_vars.sort_by(|a, b| a.name.cmp(&b.name));

        let filtered_vars = ordered_vars
            .iter()
            .filter(|var| selections.contains(&var.name.to_uppercase()))
            .enumerate()
            .map(|(index, v)| {
                let mut renumbered_v = v.clone();
                renumbered_v.col = index;
                renumbered_v
            })
            .collect::<Vec<LayoutVar>>();

        Self {
            vars: filtered_vars,
        }
    }
}

// Layouts for all record types in a file
#[derive(Clone)]
pub struct DatasetLayout {
    layouts: HashMap<String, RecordLayout>,
}

impl DatasetLayout {
    pub fn record_types(&self) -> Vec<String> {
        self.layouts.keys().cloned().collect()
    }

    pub fn all_variables(&self) -> Vec<LayoutVar> {
        self.record_types()
            .iter()
            .flat_map(|rt| self.for_rectype(rt).vars.clone())
            .collect()
    }

    pub fn find_variables(&self, names: &[String]) -> Vec<LayoutVar> {
        self.record_types()
            .iter()
            .flat_map(|rt| self.for_rectype(rt).filtered(names).vars)
            .collect()
    }

    pub fn for_rectype(&self, rt: &str) -> &RecordLayout {
        match self.layouts.get(rt) {
            None => {
                panic!("No records of type {} in layout.", rt);
            }
            Some(vars) => vars,
        }
    }

    // If you have a Vec of mixed record type LayoutVars, perhaps read in
    // from some non-DCP layout format file elsewhere. Returns the
    // layouts organized by record type and with column numbers assigned.
    pub fn from_layout_vars(all_vars: Vec<LayoutVar>) -> Self {
        let mut layouts: HashMap<String, RecordLayout> = HashMap::new();

        for mut var in all_vars {
            match layouts.get_mut(&var.rectype) {
                Some(layout) => {
                    var.col = layout.vars.len();
                    layout.add(var);
                }
                None => {
                    var.col = 0;
                    layouts.insert(var.rectype.to_owned(), RecordLayout::new(var));
                }
            }
        }
        Self { layouts }
    }

    pub fn from_layout_file(filename: &Path) -> Self {
        let layouts: HashMap<String, RecordLayout> = HashMap::new();
        let rdr = csv::ReaderBuilder::new()
            .has_headers(false)
            .delimiter(b' ')
            .comment(Some(b'#'))
            .from_path(filename);

        let mut reader = match rdr {
            Err(msg) => panic!(
                "Cannot create CSV reader on {}, error was {}.",
                filename.display(),
                &msg
            ),
            Ok(r) => r,
        };

        let mut all_vars = reader
            .records()
            .filter(|r| r.is_ok())
            .map(|r| r.unwrap())
            .filter(|r| r.len() > 1)
            .map(|record| LayoutVar {
                name: record[0].to_string(),
                rectype: record[1].to_string(),
                start: record[2].parse().unwrap(),
                width: record[3].parse().unwrap(),
                data_type: IpumsDataType::from(&record[4]),
                col: 0,
            })
            .collect::<Vec<LayoutVar>>();

        // While sorting in 'start' order would yield an order that's slightly
        // faster to process, defaulting vars to alphabetical order ensures
        // a known schema order that is easy to match with other files or
        // data sources with different natural orderings.
        all_vars.sort_by(|a, b| b.name.cmp(&a.name));
        DatasetLayout::from_layout_vars(all_vars)
    }

    // Return a new DatasetLayout containing only the requested variables or an error message.
    // Doing it this way so that we can retain the full layout for reuse.
    pub fn select_only(&self, selections: Vec<String>) -> Result<DatasetLayout, String> {
        let mut filtered_layouts: HashMap<String, RecordLayout> = HashMap::new();
        let upcased_selections = selections
            .iter()
            .map(|s| s.to_uppercase())
            .collect::<Vec<String>>();

        for rectype in self.layouts.keys() {
            filtered_layouts.insert(
                rectype.clone(),
                self.layouts
                    .get(rectype)
                    .unwrap()
                    .filtered(&upcased_selections),
            );
        }
        Ok(DatasetLayout {
            layouts: filtered_layouts,
        })
    }
}
