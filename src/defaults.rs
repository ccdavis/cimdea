use crate::conventions::*;
use crate::ipums_data_model::*;
use std::collections::HashMap;

/// The Household - Person record structure is the default for much IPUMS data. Here we have some
///  functions to support setting up such a structure without needing any external configuration.
///  
///  A generic record type generator could use Cow instead of String, as in https://stackoverflow.com/questions/63201351/writing-a-rust-struct-type-that-contains-a-string-and-can-be-used-in-a-constant
///

fn household() -> RecordType {
    RecordType {
        name: "Household".to_string(),
        value: "H".to_string(),
        unique_id: "SERIAL".to_string(),
        foreign_keys: Vec::new(),
        weight: Some(default_household_weight()),
    }
}

fn person() -> RecordType {
    RecordType {
        name: "Person".to_string(),
        value: "P".to_string(),
        unique_id: "PSERIAL".to_string(),
        foreign_keys: vec![("H".to_string(), "SERIALP".to_string())],
        weight: Some(default_person_weight()),
    }
}

fn default_record_types() -> HashMap<String, RecordType> {
    HashMap::from([("H".to_string(), household()), ("P".to_string(), person())])
}

fn default_household_weight() -> RecordWeight {
    RecordWeight::new("HHWT", 1)
}

fn default_person_weight() -> RecordWeight {
    RecordWeight::new("PERWT", 1)
}

fn default_hierarchy() -> RecordHierarchy {
    let mut hierarchy = RecordHierarchy::new("H");
    hierarchy.add_member("P", "H");
    hierarchy
}

fn default_settings_named(name: &str) -> MicroDataCollection {
    MicroDataCollection {
        name: name.to_string(),
        record_hierarchy: default_hierarchy(),
        record_types: default_record_types(),
        metadata: None,
    }
}

/// There are default configurations for USA, IPUMSI and CPS currently.
/// Get them like
/// ```
/// use cimdea::defaults::defaults_for;
/// let current_settings = defaults_for("usa");
/// ```
///
///
///

pub fn defaults_for(product: &str) -> MicroDataCollection {
    // Right now we only set defaults but in future this should set some additional
    // properties particular to products or stuff loaded in from
    // an external configuration.
    match product.to_lowercase().as_ref() {
        "usa" => default_settings_named("USA"),
        "cps" => default_settings_named("cps"),
        "ipumsi" => default_settings_named("ipumsi"),
        _ => panic!("Product not supported"),
    }
}
