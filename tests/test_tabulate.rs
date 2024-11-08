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

/// This request has a complex subpopulation of "METRO (an H variable) is either
/// 1 or 3, and SEX (a P variable) is 2".
#[test]
fn test_no_category_bins_complex_subpop() {
    let input_json = include_str!("requests/no_category_bins_complex_subpop.json");

    let (ctx, rq) =
        AbacusRequest::try_from_json(input_json).expect("should be able to parse input JSON");
    let tab = tabulate(&ctx, rq).expect("tabulation should run without errors");

    let tables = tab.into_inner();
    assert_eq!(tables.len(), 1, "expected exactly 1 output table");
    let table = tables[0].clone();

    let key = KeyTable {
        column_names: ["ct", "weighted_ct", "MARST"],
        rows: [
            [2270, 234510, 1],
            [127, 016015, 2],
            [186, 23503, 3],
            [922, 104773, 4],
            [752, 66727, 5],
            [2663, 365164, 6],
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

/// The variable FTOTINC is a P variable, and in this request it has 17 category
/// bins.
#[test]
fn test_category_bins_no_subpops() {
    let input_json = include_str!("requests/ftotinc_category_bins_no_subpops.json");
    let (ctx, rq) =
        AbacusRequest::try_from_json(input_json).expect("should be able to parse input JSON");
    let tab = tabulate(&ctx, rq).expect("should be able to tabulate without errors");

    let tables = tab.into_inner();
    assert_eq!(tables.len(), 1, "expected exactly one output table");
    let table = tables[0].clone();

    let key = KeyTable {
        column_names: ["ct", "weighted_ct", "FTOTINC"],
        rows: [
            [908, 40725, 0],
            [7532, 880096, 1],
            [3264, 371129, 2],
            [2871, 327968, 3],
            [2355, 271148, 4],
            [1821, 214895, 5],
            [1803, 200163, 6],
            [1565, 179681, 7],
            [1342, 147771, 8],
            [1118, 126565, 9],
            [1682, 190799, 10],
            [1601, 195258, 11],
            [1334, 155015, 12],
            [608, 65219, 13],
            [243, 29161, 14],
            [179, 18680, 15],
            [541, 59909, 16],
        ],
    };

    key.check(&table);
}

/// This request tabulates FTOTINC with the subpopulation 60 <= EDUC <= 65.
#[test]
fn test_category_bins_subpop_p_variable() {
    let input_json = include_str!("requests/ftotinc_category_bins_subpop_P_variable.json");
    let (ctx, rq) =
        AbacusRequest::try_from_json(input_json).expect("should be able to parse input JSON");
    let tab = tabulate(&ctx, rq).expect("should be able to tabulate without errors");

    let tables = tab.into_inner();
    assert_eq!(tables.len(), 1, "expected exactly one input table");
    let table = tables[0].clone();

    let key = KeyTable {
        column_names: ["ct", "weighted_ct", "FTOTINC"],
        rows: [
            [283, 11554, 0],
            [2128, 238373, 1],
            [990, 107469, 2],
            [918, 100487, 3],
            [678, 69712, 4],
            [532, 58371, 5],
            [473, 53488, 6],
            [394, 42347, 7],
            [300, 30003, 8],
            [254, 27070, 9],
            [373, 37396, 10],
            [273, 31653, 11],
            [216, 22396, 12],
            [89, 9190, 13],
            [20, 2171, 14],
            [13, 1008, 15],
            [50, 4954, 16],
        ],
    };

    key.check(&table);
}

/// This test tabulates FTOTINC over the subpopulation FARM = 2 (which is households
/// that are on farms; FARM is an H variable). Since us2015b is a relatively
/// small sample, there are several category bins of FTOTINC which have a count
/// of 0. These categories do not appear in the output table.
#[test]
fn test_category_bins_subpop_h_variable() {
    let input_json = include_str!("requests/ftotinc_category_bins_subpop_H_variable.json");
    let (ctx, rq) =
        AbacusRequest::try_from_json(input_json).expect("should be able to parse input JSON");
    let tab = tabulate(&ctx, rq).expect("should tabulate without errors");

    let tables = tab.into_inner();
    assert_eq!(tables.len(), 1, "expected exactly one output table");
    let table = tables[0].clone();

    let key = KeyTable {
        column_names: ["ct", "weighted_ct", "FTOTINC"],
        rows: [
            [7, 498, 1],
            [9, 1187, 4],
            [6, 507, 7],
            [2, 259, 8],
            [6, 389, 10],
            [4, 925, 12],
        ],
    };

    key.check(&table);
}

/// This test tabulates the FTOTINC variable with category bins and a fairly
/// complex subpopulation. The subpopulation is "people who don't live on a farm, and
/// whose educational attainment is either Grade 12 or 1-4 years of college".
/// This subpopulation involves the H variable FARM and multiple conditions on
/// the P variable EDUC.
#[test]
fn test_category_bins_complex_subpop() {
    let input_json = include_str!("requests/ftotinc_category_bins_complex_subpop.json");
    let (ctx, rq) =
        AbacusRequest::try_from_json(input_json).expect("should be able to parse input JSON");
    let tab = tabulate(&ctx, rq).expect("should be able to tabulate without errors");

    let tables = tab.into_inner();
    assert_eq!(tables.len(), 1, "expected exactly 1 output table");
    let table = tables[0].clone();

    let key = KeyTable {
        column_names: ["ct", "weighted_ct", "FTOTINC"],
        rows: [
            [438, 18273, 0],
            [3464, 397424, 1],
            [1721, 191838, 2],
            [1674, 191139, 3],
            [1447, 161257, 4],
            [1195, 139253, 5],
            [1164, 127114, 6],
            [1077, 121966, 7],
            [885, 96782, 8],
            [749, 84157, 9],
            [1118, 125347, 10],
            [1042, 125185, 11],
            [838, 96101, 12],
            [377, 39820, 13],
            [142, 17098, 14],
            [84, 8735, 15],
            [265, 29556, 16],
        ],
    };

    key.check(&table);
}

/// Each request sample gets its own output table.
#[test]
fn test_multiple_request_samples() {
    let input_json = include_str!("requests/multiple_request_samples.json");
    let (ctx, rq) =
        AbacusRequest::try_from_json(input_json).expect("should be able to parse input JSON");
    let tab = tabulate(&ctx, rq).expect("should run tabulation without errors");

    let tables = tab.into_inner();
    assert_eq!(tables.len(), 2, "expected exactly 2 output tables");
    let table_us2015b = tables[0].clone();
    let table_us2016b = tables[1].clone();

    let key_us2015b = KeyTable {
        column_names: ["ct", "weighted_ct", "CINETHH"],
        rows: [
            [897, 39460, 0],
            [17698, 2042631, 1],
            [1262, 162305, 2],
            [10910, 1229786, 3],
        ],
    };

    let key_us2016b = KeyTable {
        column_names: ["ct", "weighted_ct", "CINETHH"],
        rows: [
            [801, 39617, 0],
            [19634, 2271203, 1],
            [598, 70448, 2],
            [9191, 1030039, 3],
        ],
    };

    key_us2015b.check(&table_us2015b);
    key_us2016b.check(&table_us2016b);
}

/// This test tabulates the two variables GQ and UHRSWORK. GQ does not have
/// category bins applied, but UHRSWORK does. There is no subpopulation requested.
#[test]
fn test_multiple_variables_mixed_category_bins_no_subpops() {
    let input_json =
        include_str!("requests/multiple_variables_mixed_category_bins_no_subpops.json");
    let (ctx, rq) =
        AbacusRequest::try_from_json(input_json).expect("should be able to parse input JSON");
    let tab = tabulate(&ctx, rq).expect("should tabulate without errors");

    let tables = tab.into_inner();
    assert_eq!(tables.len(), 1, "expected exactly 1 output table");
    let table = tables[0].clone();

    let key = KeyTable {
        column_names: ["ct", "weighted_ct", "GQ", "UHRSWORK"],
        rows: [
            [20527, 2328045, 1, 0],
            [549, 60884, 1, 1],
            [2318, 279919, 1, 2],
            [6452, 762702, 1, 3],
            [19, 2369, 2, 0],
            [2, 435, 2, 1],
            [1, 112, 2, 2],
            [2, 256, 2, 3],
            [484, 21902, 3, 0],
            [8, 258, 3, 1],
            [38, 1257, 3, 2],
            [57, 1860, 3, 3],
            [242, 10719, 4, 0],
            [15, 676, 4, 1],
            [37, 1850, 4, 2],
            [16, 938, 4, 3],
        ],
    };

    key.check(&table);
}

/// This test tabulates the GQ and UHRSWORK variables over the subpopulation
/// LOOKING = 2.
#[test]
fn test_multiple_variables_mixed_category_bins_subpop_p_variable() {
    let input_json =
        include_str!("requests/multiple_variables_mixed_category_bins_subpop_P_variable.json");
    let (ctx, rq) =
        AbacusRequest::try_from_json(input_json).expect("should be able to parse input JSON");
    let tab = tabulate(&ctx, rq).expect("should tabulate without errors");

    let tables = tab.into_inner();
    assert_eq!(tables.len(), 1, "expected exactly one output table");
    let table = tables[0].clone();

    let key = KeyTable {
        column_names: ["ct", "weighted_ct", "GQ", "UHRSWORK"],
        rows: [
            [1419, 184886, 1, 0],
            [62, 6410, 1, 1],
            [179, 21363, 1, 2],
            [224, 26152, 1, 3],
            [1, 85, 3, 0],
            [34, 1524, 4, 0],
        ],
    };

    key.check(&table);
}

#[test]
fn test_multiple_variables_mixed_category_bins_subpop_h_variable() {
    let input_json =
        include_str!("requests/multiple_variables_mixed_category_bins_subpop_H_variable.json");
    let (ctx, rq) =
        AbacusRequest::try_from_json(input_json).expect("should be able to parse input JSON");
    let tab = tabulate(&ctx, rq).expect("should tabulate without errors");

    let tables = tab.into_inner();
    assert_eq!(tables.len(), 1, "expected exactly 1 output table");
    let table = tables[0].clone();

    let key = KeyTable {
        column_names: ["ct", "weighted_ct", "GQ", "UHRSWORK"],
        rows: [
            [9109, 1043079, 1, 0],
            [329, 35376, 1, 1],
            [1339, 161551, 1, 2],
            [4406, 509826, 1, 3],
            [14, 1922, 2, 0],
            [2, 435, 2, 1],
            [1, 112, 2, 2],
        ],
    };

    key.check(&table);
}

/// This test tabulates GQ and UHRSWORK with the subpopulation CILAPTOP = 1 and
/// LOOKING = 2.
#[test]
fn test_multiple_variables_mixed_category_bins_complex_subpop() {
    let input_json =
        include_str!("requests/multiple_variables_mixed_category_bins_complex_subpop.json");
    let (ctx, rq) =
        AbacusRequest::try_from_json(input_json).expect("should be able to parse input JSON");
    let tab = tabulate(&ctx, rq).expect("should tabulate without errors");

    let tables = tab.into_inner();
    assert_eq!(tables.len(), 1, "expected exactly one output table");
    let table = tables[0].clone();

    let key = KeyTable {
        column_names: ["ct", "weighted_ct", "GQ", "UHRSWORK"],
        rows: [
            [703, 88849, 1, 0],
            [36, 3457, 1, 1],
            [107, 12433, 1, 2],
            [137, 15493, 1, 3],
        ],
    };

    key.check(&table);
}

/// A helpful struct for simplifying comparisons of a tabulation result to a key
/// table. Uses const generics W (width) and H (height) to keep track of the width
/// and height of the table.
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
        dbg!(self);
        dbg!(table);

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
