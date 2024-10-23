//! A support module for reading fixed-width IPUMS files and their layout files. Layouts are required as a minimum level of metadata to do all advanced Abacus tabulations and formatting.
//!
//!  The 'HFLR" type models the "Hierarchical Fixed-Length Record" data IPUMS uses.
use crate::layout;
//use duckdb::arrow::datatypes::ToByteSlice;
use ascii;
use std::path;

const TRACE: bool = false;
// Hierarchical fixed-length records
#[derive(Clone)]
pub struct Hflr {
    pub layout: layout::DatasetLayout,
    _filename: Option<String>,
    pub rectype_start: Option<usize>,
    pub rectype_width: Option<usize>,
}

impl Hflr {
    pub fn new_from_layout(layout: layout::DatasetLayout) -> Self {
        Self {
            layout,
            _filename: None,
            rectype_start: None,
            rectype_width: None,
        }
    }

    pub fn new(filename: &str, selection_filter: Option<Vec<String>>) -> Self {
        let l = layout::DatasetLayout::try_from_layout_file(filename.to_owned()).unwrap();
        // Decide how to handle problems with the selection_filter
        match selection_filter {
            None => Self {
                _filename: Some(filename.to_string()),
                layout: l,
                rectype_start: None,
                rectype_width: None,
            },
            Some(selections) => match l.select_only(selections) {
                Ok(new_layout) => Self {
                    _filename: Some(filename.to_string()),
                    layout: new_layout,
                    rectype_start: None,
                    rectype_width: None,
                },
                Err(msg) => {
                    eprintln!("Can't create layout for file {} because {}", filename, &msg);
                    std::process::exit(1);
                }
            },
        }
    } // fn
} // impl

pub fn dataset_from_path(fw_data_filename: &str) -> String {
    let fw_data_path = path::Path::new(fw_data_filename);
    if let Some(filename) = fw_data_path.file_name() {
        if let Some((left, _)) = filename.to_string_lossy().rsplit_once('_') {
            left.to_string()
        } else {
            panic!(
                "File name '{}' has no '_' to delimit the dataset name.",
                fw_data_filename
            );
        }
    } else {
        panic!(
            "Can't get dataset from a path with no filename in it. Path was {}",
            fw_data_filename
        );
    }
}

// Given a relative or absolute path of a fixed-width IPUMS file,
// determine the conventional location of the layout, check and
// return it if it exists. If nothing is in ../current/layouts/
// then check the directory where the data file is, to account for
// the -l DCP mode.
pub fn layout_file_for(fw_file: &str) -> String {
    let dataset = dataset_from_path(fw_file);
    let layout_filename = dataset + ".layout.txt";

    let fw_data_file = path::Path::new(fw_file);
    if TRACE {
        println!("fw file: {}", fw_data_file.display());
    }
    let fw_data_path = fw_data_file
        .parent()
        .unwrap_or_else(|| panic!("Can't read directory  of {}", fw_file));

    if TRACE {
        println!("parent layout path {}", fw_data_path.display());
    }
    let layout_path = fw_data_path.join("layouts");
    let layout_file = layout_path.join(&layout_filename);
    if !layout_file.exists() {
        let local_layout_file = fw_data_path.join(layout_filename);
        if !local_layout_file.exists() {
            eprintln!(
                "ERROR: Couldn't find layout file '{}' for data in '{}'.",
                &layout_file.display(),
                &fw_data_file.display()
            );
            std::process::exit(1);
        }
        local_layout_file.into_os_string().into_string().unwrap()
    } else {
        layout_file.into_os_string().into_string().unwrap()
    }
}

// This  takes an already formatted ASCII string and replaces one char with another ('0' for ' ' typically.)
// Additionally the '-' sign has to be put on the very left instead of embedded after the '0' chars.
pub fn left_pad_in_place(
    code: &mut [u8],
    replace: ascii::AsciiChar,
    replace_with: ascii::AsciiChar,
) {
    const negative_sign: u8 = b'-';

    let mut pos = 0;
    loop {
        if code[pos] == replace {
            code[pos] = replace_with.as_byte();
        }
        if pos > 0 && code[pos] == negative_sign {
            code[0] = negative_sign;
            code[pos] = replace_with.as_byte();
        }
        pos += 1;
        if pos == code.len() {
            break;
        }
        if code[pos] != replace && code[pos] != negative_sign {
            break;
        }
    }
}

pub fn make_zero_padded_numeric(code: &[u8]) -> Vec<u8> {
    let mut new_code = code.to_vec();
    left_pad_in_place(&mut new_code, ascii::AsciiChar::Space, ascii::AsciiChar::_0);
    new_code
}

mod tests {


    #[test]
    fn test_make_zero_padded_numeric() {
        use super::*;
        use bstr::*;

        let t1 = B("   123");
        let t2 = "   -123".as_bytes();
        let t3 = "-   12".as_bytes();
        let t4 = "   -  12".as_bytes();

        assert_eq!("000123".as_bytes(), make_zero_padded_numeric(t1));

        assert_eq!("-000123".as_bytes(), make_zero_padded_numeric(t2));
        assert_eq!("-00012".as_bytes(), make_zero_padded_numeric(t3));
        assert_ne!("-00  12".as_bytes(), make_zero_padded_numeric(t4));
        assert_eq!("-0000012".as_bytes(), make_zero_padded_numeric(t4));
    }

    #[test]
    fn test_hflr() {
        use super::*;
        let hflr = Hflr::new("test_data/us2015b.layout.txt", None);
        let person_layout = hflr.layout.for_rectype("P");
        assert_eq!(628, person_layout.vars.len());
        let hh_layout = hflr.layout.for_rectype("H");
        assert_eq!(469, hh_layout.vars.len());
    }

    #[test]

    fn test_with_variable_selections() {
        use super::*;
        let selections = vec!["AGE".to_string(), "GQ".to_string(), "SERIAL".to_string()];
        let hflr = Hflr::new("test_data/us2015b.layout.txt", Some(selections));
        let person_layout = hflr.layout.for_rectype("P");
        assert_eq!(1, person_layout.vars().len());
        let hh_layout = hflr.layout.for_rectype("H");

        assert_eq!(2, hh_layout.vars().len());
    }
}
