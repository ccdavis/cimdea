//! Supports reading the "layout" metadata files from IPUMS microdata.
//!
//! These files describe fixed-width
//! formatted IPUMS datasets. While they were developed to act as minimal record layouts for fixed-width
//! they can be useful for getting basic metadata for the dataset.

use crate::ipums_metadata_model::IpumsDataType;
use crate::mderror::MdError;
use std::collections::HashMap;
use std::io::Read;
use std::path::Path;
use std::str;

/// An entry (a single line) from a layout file, describing the layout of one variable.
#[derive(Clone, Debug)]
pub struct LayoutVar {
    pub name: String,
    pub rectype: String,
    pub start: usize,
    pub width: usize,
    /// The column number of the variable when using CSV rather than fixed-width
    pub col: usize,
    pub data_type: IpumsDataType,
}

/// The layout for all variables of a particular record type in a dataset.
#[derive(Clone, Debug)]
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

/// The layout for an entire dataset.
///
/// This includes layouts for all record types in a layout file.
///
/// ```
/// use std::path::Path;
/// use cimdea::layout::DatasetLayout;
///
/// let layout_file = Path::new("tests/data_root/layouts/us2015b.layout.txt");
/// let layout = DatasetLayout::try_from_layout_file(layout_file).unwrap();
///
/// let mut vars = layout.find_variables(&["RECTYPE".to_string(), "MOMLOC".to_string()]);
/// vars.sort_by_key(|v| v.name.clone());
///
/// assert_eq!(vars[0].name, "MOMLOC");
/// assert_eq!(vars[0].rectype, "P");
///
/// assert_eq!(vars[1].name, "RECTYPE");
/// assert_eq!(vars[1].rectype, "H");
/// ```
#[derive(Clone, Debug)]
pub struct DatasetLayout {
    layouts: HashMap<String, RecordLayout>,
}

impl DatasetLayout {
    pub fn record_types(&self) -> Vec<String> {
        self.layouts.keys().cloned().collect()
    }

    pub fn all_variables(&self) -> Vec<LayoutVar> {
        self.layouts
            .values()
            .flat_map(|record_layout| record_layout.vars.clone())
            .collect()
    }

    pub fn find_variables(&self, names: &[String]) -> Vec<LayoutVar> {
        self.layouts
            .values()
            .flat_map(|record_layout| record_layout.filtered(names).vars)
            .collect()
    }

    /// Returns the RecordLayout for the given record type, or None if there is
    /// no layout for that record type.
    pub fn for_rectype(&self, rt: &str) -> Option<&RecordLayout> {
        self.layouts.get(rt)
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

    fn try_from_layout_reader<R: Read>(mut reader: csv::Reader<R>) -> Result<Self, MdError> {
        let mut all_vars = reader
            .records()
            .filter_map(|r| r.ok())
            .filter(|r| r.len() > 1)
            .map(|record| {
                if record.len() < 5 {
                    let fields = record.iter().collect::<Vec<_>>().join(" ");
                    return Err(MdError::ParsingError(format!(
                        "not enough fields in layout record '{fields}'"
                    )));
                }
                let name = record[0].to_string();
                let start_str = &record[2];
                let start: usize = start_str.parse().map_err(|err| {
                    let msg = format!(
                        "could not parse layout start '{start_str}' for variable \
                         '{name}' as a non-negative integer: {err}"
                    );
                    MdError::ParsingError(msg)
                })?;

                let width_str = &record[3];
                let width: usize = width_str.parse().map_err(|err| {
                    let msg = format!(
                        "could not parse layout width '{width_str}' for variable \
                            '{name}' as a non-negative integer: {err}"
                    );
                    MdError::ParsingError(msg)
                })?;

                Ok(LayoutVar {
                    name,
                    rectype: record[1].to_string(),
                    start,
                    width,
                    data_type: IpumsDataType::from(&record[4]),
                    col: 0,
                })
            })
            .collect::<Result<Vec<LayoutVar>, MdError>>()?;

        // While sorting in 'start' order would yield an order that's slightly
        // faster to process, defaulting vars to alphabetical order ensures
        // a known schema order that is easy to match with other files or
        // data sources with different natural orderings.
        all_vars.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(DatasetLayout::from_layout_vars(all_vars))
    }

    pub fn try_from_layout_file(filename: &Path) -> Result<Self, MdError> {
        let rdr = csv::ReaderBuilder::new()
            .has_headers(false)
            .delimiter(b' ')
            .comment(Some(b'#'))
            .from_path(filename);

        let reader = match rdr {
            Err(msg) => {
                return Err(MdError::Msg(format!(
                    "Cannot create CSV reader on {}, error was {}.",
                    filename.display(),
                    &msg
                )))
            }
            Ok(r) => r,
        };

        DatasetLayout::try_from_layout_reader(reader)
    }

    // Return a new DatasetLayout containing only the requested variables or an error.
    // Doing it this way so that we can retain the full layout for reuse.
    pub fn select_only(&self, selections: Vec<String>) -> Result<DatasetLayout, MdError> {
        let mut filtered_layouts: HashMap<String, RecordLayout> = HashMap::new();
        let upcased_selections = selections
            .iter()
            .map(|s| s.to_uppercase())
            .collect::<Vec<String>>();

        for (rectype, layout) in self.layouts.iter() {
            filtered_layouts.insert(rectype.clone(), layout.filtered(&upcased_selections));
        }

        Ok(DatasetLayout {
            layouts: filtered_layouts,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::HashSet;
    use std::io::Cursor;

    fn csv_reader_from_bytes(layout_data: &[u8]) -> csv::Reader<Cursor<&[u8]>> {
        let cursor = Cursor::new(layout_data);
        csv::ReaderBuilder::new()
            .has_headers(false)
            .delimiter(b' ')
            .from_reader(cursor)
    }

    #[test]
    fn test_dataset_layout_try_from_layout_file() {
        let layout_file = Path::new("tests/data_root/layouts/us1850a.layout.txt");
        let layout = DatasetLayout::try_from_layout_file(layout_file)
            .expect("should be able to create DatasetLayout from file");

        let h_vars = layout.layouts["H"].vars.len();
        let p_vars = layout.layouts["P"].vars.len();
        assert_eq!(
            h_vars + p_vars,
            339,
            "there should be 339 total P and H variables in the layout"
        );
    }

    #[test]
    fn test_dataset_layout_try_from_layout_file_no_such_file_error() {
        // This is not a real layout file
        let layout_file = Path::new("tests/data_root/layouts/us0000a.layout.txt");
        let result = DatasetLayout::try_from_layout_file(layout_file);
        assert!(result.is_err(), "expected an error but got {result:?}");
    }

    #[test]
    fn test_dataset_layout_try_from_layout_reader_variables_sorted_by_name() {
        let layout_data = b"RECTYPE H 1 1 string\n\
        CITY H 60 4 integer\n\
        CITYPOP H 64 7 integer\n";
        let reader = csv_reader_from_bytes(layout_data);

        let layout = DatasetLayout::try_from_layout_reader(reader)
            .expect("should parse into a DatasetLayout");

        let h_layout = &layout.layouts["H"];
        let variable_names: Vec<_> = h_layout.vars.iter().map(|v| v.name.as_str()).collect();
        assert_eq!(variable_names, vec!["CITY", "CITYPOP", "RECTYPE"]);
    }

    #[test]
    fn test_dataset_layout_try_from_layout_reader_non_integer_start_error() {
        let layout_data = b"RECTYPE H a 1 string\n";
        let reader = csv_reader_from_bytes(layout_data);
        let result = DatasetLayout::try_from_layout_reader(reader);

        assert!(
            matches!(result, Err(MdError::ParsingError(_))),
            "expected a parsing error, got {result:?}"
        );
    }

    #[test]
    fn test_dataset_layout_try_from_layout_reader_non_integer_width_error() {
        let layout_data = b"RECTYPE H 1 a string\n";
        let reader = csv_reader_from_bytes(layout_data);
        let result = DatasetLayout::try_from_layout_reader(reader);

        assert!(
            matches!(result, Err(MdError::ParsingError(_))),
            "expected a parsing error, got {result:?}"
        );
    }

    #[test]
    fn test_dataset_layout_try_from_layout_reader_missing_fields_error() {
        let layout_data = b"RECTYPE H\n";
        let reader = csv_reader_from_bytes(layout_data);
        let result = DatasetLayout::try_from_layout_reader(reader);

        assert!(
            matches!(result, Err(MdError::ParsingError(_))),
            "expected a parsing error, got {result:?}"
        );
    }

    /// Variables are split into RecordLayouts by record type.
    #[test]
    fn test_dataset_layout_try_from_layout_reader_multiple_rectypes() {
        let layout_data = b"YEAR H 2 4 integer\n\
        AGE P 58 3 integer\n";
        let reader = csv_reader_from_bytes(layout_data);
        let layout = DatasetLayout::try_from_layout_reader(reader)
            .expect("should parse into a DatasetLayout");

        let h_layout = &layout.layouts["H"];
        let p_layout = &layout.layouts["P"];

        assert_eq!(h_layout.vars[0].name, "YEAR");
        assert_eq!(p_layout.vars[0].name, "AGE");
    }

    #[test]
    fn test_dataset_layout_all_variables() {
        let layout_file = Path::new("tests/data_root/layouts/us1850a.layout.txt");
        let layout = DatasetLayout::try_from_layout_file(layout_file)
            .expect("should be able to create DatasetLayout from file");

        let all_vars = layout.all_variables();
        let all_var_names: Vec<_> = all_vars.iter().map(|v| v.name.as_str()).collect();
        assert_eq!(all_vars.len(), 345, "there should be 339 total variables");
        assert!(all_var_names.contains(&"AGE"), "should have P variable AGE");
        assert!(
            all_var_names.contains(&"METRO"),
            "should have H variable METRO"
        );
        assert!(
            all_var_names.contains(&"CORE_VERS_RELEASE_NUMBER"),
            "should have # variable CORE_VERS_RELEASE_NUMBER"
        );
    }

    #[test]
    fn test_dataset_layout_find_variables() {
        let layout_file = Path::new("tests/data_root/layouts/us1850a.layout.txt");
        let layout = DatasetLayout::try_from_layout_file(layout_file)
            .expect("should be able to create DatasetLayout from file");

        let vars = layout.find_variables(&[
            "METRO".to_string(),
            "PERNUM".to_string(),
            "AGE".to_string(),
            "NOTAVAR".to_string(),
        ]);
        let var_names: HashSet<_> = vars.iter().map(|v| v.name.as_str()).collect();

        // Any unrecognized variables (like NOTAVAR) should be left out
        assert_eq!(var_names, ["AGE", "METRO", "PERNUM"].into());
    }
}
