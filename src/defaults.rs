use crate::conventions::*;
use crate::ipums_data_model::*;
use std::collections::HashMap;

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

mod default_usa {
    use super::*;

    fn settings() -> MicroDataCollection {
        default_settings_named("USA")
    }
}

mod default_cps {
    use super::*;
    fn settings() -> MicroDataCollection {
        default_settings_named("CPS")
    }
}

mod default_ipumsi {
    use super::*;
    fn settings() -> MicroDataCollection {
        default_settings_named("IPUMSI")
    }
}

use lazy_static::*;

lazy_static! {}
