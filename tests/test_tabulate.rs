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

    assert_eq!(tables.len(), 1);
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

    assert_eq!(tables.len(), 1);
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
