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

    assert_eq!(table.heading.len(), 3);
    assert_eq!(table.heading[0].name(), "ct");
    assert_eq!(table.heading[1].name(), "weighted_ct");
    assert_eq!(table.heading[2].name(), "MARST");

    assert_eq!(table.rows.len(), 6);
    for row in &table.rows {
        assert_eq!(row.len(), 3);
    }

    // Check ct
    assert_eq!(table.rows[0][0], "10050");
    assert_eq!(table.rows[1][0], "499");
    assert_eq!(table.rows[2][0], "707");
    assert_eq!(table.rows[3][0], "3670");
    assert_eq!(table.rows[4][0], "2267");
    assert_eq!(table.rows[5][0], "13574");

    // Check weighted_ct
    assert_eq!(table.rows[0][1], "998208");
    assert_eq!(table.rows[1][1], "54103");
    assert_eq!(table.rows[2][1], "82407");
    assert_eq!(table.rows[3][1], "404131");
    assert_eq!(table.rows[4][1], "204365");
    assert_eq!(table.rows[5][1], "1730968");

    // Check MARST
    assert_eq!(table.rows[0][2], "1");
    assert_eq!(table.rows[1][2], "2");
    assert_eq!(table.rows[2][2], "3");
    assert_eq!(table.rows[3][2], "4");
    assert_eq!(table.rows[4][2], "5");
    assert_eq!(table.rows[5][2], "6");
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

    assert_eq!(table.heading.len(), 3);
    assert_eq!(table.heading[0].name(), "ct");
    assert_eq!(table.heading[1].name(), "weighted_ct");
    assert_eq!(table.heading[2].name(), "MARST");

    assert_eq!(table.rows.len(), 6);
    for row in &table.rows {
        assert_eq!(row.len(), 3);
    }

    // Check ct
    assert_eq!(table.rows[0][0], "5048");
    assert_eq!(table.rows[1][0], "270");
    assert_eq!(table.rows[2][0], "432");
    assert_eq!(table.rows[3][0], "2256");
    assert_eq!(table.rows[4][0], "1831");
    assert_eq!(table.rows[5][0], "6622");

    // Check weighted_ct
    assert_eq!(table.rows[0][1], "496088");
    assert_eq!(table.rows[1][1], "30965");
    assert_eq!(table.rows[2][1], "50255");
    assert_eq!(table.rows[3][1], "240264");
    assert_eq!(table.rows[4][1], "162628");
    assert_eq!(table.rows[5][1], "836520");

    // Check MARST
    assert_eq!(table.rows[0][2], "1");
    assert_eq!(table.rows[1][2], "2");
    assert_eq!(table.rows[2][2], "3");
    assert_eq!(table.rows[3][2], "4");
    assert_eq!(table.rows[4][2], "5");
    assert_eq!(table.rows[5][2], "6");
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
        assert_eq!(table.heading.len(), W);
        for index in 0..W {
            assert_eq!(table.heading[index].name(), self.column_names[index]);
        }
    }

    fn check_row_dimensions(&self, table: &Table) {
        assert_eq!(table.rows.len(), H);
        for row in &table.rows {
            assert_eq!(row.len(), W);
        }
    }

    fn check_row_entries(&self, table: &Table) {
        for column_index in 0..W {
            for row_index in 0..H {
                let key_entry = self.rows[row_index][column_index].to_string();
                let table_entry = &table.rows[row_index][column_index];
                assert_eq!(&key_entry, table_entry);
            }
        }
    }
}
