use cimdea::request::AbacusRequest;
use cimdea::tabulate::{self, TableFormat};

/// Computes and serializes into JSON a tabulation on the EDUC variable, with
/// general width 2 and detailed width 3.
#[test]
fn test_serialize_general_detailed_to_json() {
    let input_json = include_str!("requests/educ_general_detailed_selection.json");
    let (ctx, rq) =
        AbacusRequest::try_from_json(input_json).expect("should be able to deserialize input JSON");
    let tab = tabulate::tabulate(&ctx, rq).expect("tabulation should run without errors");
    let output_json = tab
        .output(TableFormat::Json)
        .expect("tabulation should serialize into JSON");
    let _: serde_json::Value =
        serde_json::from_str(&output_json).expect("serialized tabulation should be valid JSON");
}
