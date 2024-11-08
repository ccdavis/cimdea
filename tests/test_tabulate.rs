//! Tabulation integration tests
use cimdea::request::AbacusRequest;
use cimdea::tabulate::{tabulate, Table};

/// This test tabulates a single P variable MARST, which does not have category
/// bins. There are no subpopulations applied.
#[test]
fn test_no_category_bins_no_subpops() {
    let input_json = include_str!("requests/no_category_bins_no_subpops.json");
    let (ctx, request) =
        AbacusRequest::try_from_json(input_json).expect("should be able to parse input JSON");
    let tab = tabulate(&ctx, request).expect("tabulation should run without errors");
    let tables = tab.into_inner();

    assert_eq!(tables.len(), 1, "expected exactly one output table");
    let table = tables[0].clone();

    let key = KeyTable {
        column_names: ["ct", "weighted_ct", "MARST"],
        rows: [
            [10050, 998208, 1],
            [499, 54103, 2],
            [707, 82407, 3],
            [3670, 404131, 4],
            [2267, 204365, 5],
            [13574, 1730968, 6],
        ],
    };
    key.check(&table);
}

/// This test tabulates the P variable MARST with no category bins. It restricts
/// to the subpopulation SEX = 2, so just women.
#[test]
fn test_no_category_bins_subpop_p_variable() {
    let input_json = include_str!("requests/no_category_bins_subpop_P_variable.json");
    let (ctx, rq) =
        AbacusRequest::try_from_json(input_json).expect("should be able to parse input JSON");
    let tab = tabulate(&ctx, rq).expect("tabulation should run without errors");
    let tables = tab.into_inner();

    assert_eq!(tables.len(), 1, "expected exactly one output table");
    let table = tables[0].clone();

    let key = KeyTable {
        column_names: ["ct", "weighted_ct", "MARST"],
        rows: [
            [5048, 496088, 1],
            [270, 30965, 2],
            [432, 50255, 3],
            [2256, 240264, 4],
            [1831, 162628, 5],
            [6622, 836520, 6],
        ],
    };
    key.check(&table);
}

#[test]
fn test_no_category_bins_subpop_h_variable() {
    let input_json = include_str!("requests/no_category_bins_subpop_H_variable.json");
    let (ctx, rq) =
        AbacusRequest::try_from_json(input_json).expect("should be able to parse input JSON");
    let tab = tabulate(&ctx, rq).expect("tabulation should run without errors");

    let tables = tab.into_inner();
    assert_eq!(tables.len(), 1, "expected exactly one output table");
    let table = tables[0].clone();

    let key = KeyTable {
        column_names: ["ct", "weighted_ct", "MARST"],
        rows: [
            [570, 44647, 1],
            [71, 7056, 2],
            [69, 6657, 3],
            [372, 32910, 4],
            [237, 16834, 5],
            [1329, 137628, 6],
        ],
    };

    key.check(&table);
}

/// The variable RELATE has a detailed version which has a width of 4 bytes and
/// a general version with a width of 2 bytes. When a request specifies the general
/// version for RELATE, the results are aggregated on the general codes, which
/// each have width 2.
#[test]
fn test_general_selection_for_general_detailed_variable() {
    let input_json = include_str!("requests/relate_general_detailed.json");
    let (ctx, rq) =
        AbacusRequest::try_from_json(input_json).expect("should be able to parse input JSON");
    let tab = tabulate(&ctx, rq).expect("tabulation should run without errors");

    let tables = tab.into_inner();
    assert_eq!(tables.len(), 1, "expected exactly one table");
    let table = tables[0].clone();

    let key = KeyTable {
        column_names: ["ct", "weighted_ct", "RELATE"],
        rows: [
            [12408, 1221850, 1],
            [4844, 471843, 2],
            [8757, 1249147, 3],
            [205, 30813, 4],
            [351, 47899, 5],
            [53, 7590, 6],
            [392, 54531, 7],
            [31, 3992, 8],
            [1297, 161163, 9],
            [358, 46511, 10],
            [1006, 117082, 11],
            [478, 36484, 12],
            [587, 25277, 13],
        ],
    };

    key.check(&table);
}

/// The low_code attribute can be null for request_case_selections. This indicates
/// that there is no lower bound, so it's a <= comparison.
#[test]
fn test_request_case_selections_no_low_code() {
    let input_json = include_str!("requests/request_case_selections_no_low_code.json");
    let (ctx, rq) =
        AbacusRequest::try_from_json(input_json).expect("should be able to parse input JSON");
    let tab = tabulate(&ctx, rq).expect("should tabulate without errors");
    let tables = tab.into_inner();
    assert_eq!(tables.len(), 1, "expected exactly 1 output table");
    let table = tables[0].clone();

    let key = KeyTable {
        column_names: ["ct", "weighted_ct", "FARM"],
        rows: [[7525, 879598, 1], [7, 498, 2]],
    };

    key.check(&table);
}

/// A helpful struct for simplifying comparisons of a tabulation result to a key
/// table. Uses const generics W (width) and H (height) to keep track of the width
/// and height of the table. Has its own tests in this file.
///
/// Rows are type usize for convenience. If necessary we can switch this to &'a str
/// to preserve formatting of integers. Or we could create a new type parameter
/// T: ToString and make the rows contain &'a T.
#[derive(Debug)]
struct KeyTable<'a, const W: usize, const H: usize> {
    column_names: [&'a str; W],
    rows: [[usize; W]; H],
}

impl<'a, const W: usize, const H: usize> KeyTable<'a, W, H> {
    pub fn check(&self, table: &Table) {
        self.check_heading(table);
        self.check_row_dimensions(table);
        self.check_row_entries(table);
    }

    fn check_heading(&self, table: &Table) {
        let num_table_headers = table.heading.len();
        assert_eq!(
            num_table_headers, W,
            "number of columns in heading differs: key has {W}, \
            table has {num_table_headers}"
        );
        for index in 0..W {
            let table_column_name = table.heading[index].name();
            let key_column_name = self.column_names[index];
            assert_eq!(
                table_column_name, key_column_name,
                "name of column {index} differs: key has '{key_column_name}', \
                table has '{table_column_name}'"
            );
        }
    }

    fn check_row_dimensions(&self, table: &Table) {
        let num_table_rows = table.rows.len();

        assert_eq!(
            num_table_rows, H,
            "number of rows differs: key has {H}, table has {num_table_rows}"
        );
        for index in 0..H {
            let row_len = table.rows[index].len();
            assert_eq!(
                row_len, W,
                "length of row {index} differs: key has {W}, table has {row_len}"
            );
        }
    }

    fn check_row_entries(&self, table: &Table) {
        for column_index in 0..W {
            let column_name = self.column_names[column_index];
            for row_index in 0..H {
                let key_entry = self.rows[row_index][column_index].to_string();
                let table_entry = &table.rows[row_index][column_index];
                assert_eq!(
                    &key_entry, table_entry,
                    "entry in column {column_index} ('{column_name}') and row \
                    {row_index} differs: key has {key_entry}, table has {table_entry}"
                );
            }
        }
    }
}
