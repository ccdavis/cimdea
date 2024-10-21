//! Code for modeling and parsing the incoming JSON schema for extract requests

use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize)]
pub struct AbacusRequest {
    product: String,
    data_root: String,
    uoa: String,
    output_format: String,
    subpopulation: (),
    category_bins: (),
    request_variables: (),
    request_samples: (),
}
