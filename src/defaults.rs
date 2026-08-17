//! Definitions of default IPUMS values.
//!
//! IPUMS data products are built out of record types arranged in a hierarchy. The household -
//! person structure covers many products; the time use products add activity records, and the
//! health products add a long list of records hanging off of a person. Here we set up that
//! structure for any supported product without needing any external configuration.
//!
//! [ALL_RECORD_TYPES] and [handled_record_types] mirror `ProductConfig::record_type_to_char`,
//! `ProductConfig::hierarchy` and `ProductConfig::handled_record_types` in the DCP settings
//! (`code/settings/Settings.cpp`), which is where IPUMS defines the record structure of each
//! product, so keep them in step with DCP.
//!
//! These tables are only defaults. The record types on a [MicroDataCollection] are a plain map
//! built at run time, so anything with better information can replace or extend them. When a
//! product's `metadata.db` is available, its "rectypes" (or "record_types") table has the real
//! record types, including the columns that link them, which is where all of this belongs
//! eventually.
//!
//!  A generic record type generator could use Cow instead of String, as in
//!  <https://stackoverflow.com/questions/63201351/writing-a-rust-struct-type-that-contains-a-string-and-can-be-used-in-a-constant>

use crate::conventions::*;
use crate::ipums_data_model::*;
use crate::mderror::MdError;
use std::collections::HashMap;

/// A record type in the IPUMS record hierarchy, as DCP defines it.
struct RecordTypeSpec {
    /// The record type name, like "person" or "prescription_medication".
    name: &'static str,
    /// The letter identifying the record type in data and file names.
    value: &'static str,
    /// The record type this one hangs off of. A product that doesn't use the parent -- person
    /// only products have no household records -- gets no parent link, which makes its topmost
    /// record type the root of its hierarchy.
    parent: Option<&'static str>,
}

impl RecordTypeSpec {
    const fn new(name: &'static str, value: &'static str, parent: Option<&'static str>) -> Self {
        Self {
            name,
            value,
            parent,
        }
    }
}

/// Every record type IPUMS data products are built from, parents before children.
///
/// Letters are only unique within a product: 'A' is an activity record in the time use products
/// but a hospitalization record in the health products, and 'R' is elder care in ATUS but a round
/// record in health. [defaults_for] keys record types by letter and errors out if a product ever
/// repeats one.
///
/// DCP also has a "poisoning" ('Z') record type, but it has no place in the hierarchy and no
/// product handles it, so it's left out here.
const ALL_RECORD_TYPES: &[RecordTypeSpec] = &[
    RecordTypeSpec::new("household", "H", None),
    RecordTypeSpec::new("person", "P", Some("household")),
    RecordTypeSpec::new("activity", "A", Some("person")),
    RecordTypeSpec::new("who", "W", Some("activity")),
    RecordTypeSpec::new("elder_care", "R", Some("person")),
    RecordTypeSpec::new("injury", "I", Some("person")),
    RecordTypeSpec::new("round", "R", Some("person")),
    RecordTypeSpec::new("event", "E", Some("person")),
    RecordTypeSpec::new("event_medication_refill", "K", Some("event")),
    RecordTypeSpec::new("condition", "X", Some("person")),
    RecordTypeSpec::new("condition_event_refill", "J", Some("condition")),
    RecordTypeSpec::new("xray", "S", Some("person")),
    RecordTypeSpec::new("hospitalization", "A", Some("person")),
    RecordTypeSpec::new("prescription_medication", "M", Some("person")),
    RecordTypeSpec::new("medication_refill", "F", Some("prescription_medication")),
    RecordTypeSpec::new("non_prescription_medication", "O", Some("person")),
    RecordTypeSpec::new("doctor_visit", "D", Some("person")),
    RecordTypeSpec::new("motor_vehicle_accident", "V", Some("person")),
    RecordTypeSpec::new("pregnancy", "G", Some("person")),
    RecordTypeSpec::new("vitamin_mineral", "L", Some("person")),
    RecordTypeSpec::new("soa_condition", "Y", Some("person")),
    RecordTypeSpec::new("disability_condition", "B", Some("person")),
    RecordTypeSpec::new("device_implant_joint", "1", Some("person")),
    RecordTypeSpec::new("device_implant_fixation", "2", Some("person")),
    RecordTypeSpec::new("device_implant_heart_valve", "3", Some("person")),
    RecordTypeSpec::new("device_implant_lens", "4", Some("person")),
    RecordTypeSpec::new("device_implant_pacemaker", "5", Some("person")),
    RecordTypeSpec::new("device_implant_other", "6", Some("person")),
];

/// The record types a collection uses, or None for a collection we don't know about.
///
/// DCP comments out the pregnancy, disability_condition, soa_condition and vitamin_mineral record
/// types of the health products, so they're left out here too.
fn handled_record_types(collection: &str) -> Option<&'static [&'static str]> {
    let record_types: &[&str] = match collection {
        "usa" | "fullusa" | "ipumsi" | "cps" => &["household", "person"],
        "dhs" | "pma" | "mics" | "highered" | "brfss" | "yrbss" | "nyts" | "nsduh" => &["person"],
        "ahtus" | "mtus" => &["person", "activity"],
        "atus" => &["household", "person", "activity", "who", "elder_care"],
        "health" => &[
            "household",
            "person",
            "injury",
            "round",
            "event",
            "event_medication_refill",
            "condition",
            "condition_event_refill",
            "xray",
            "hospitalization",
            "prescription_medication",
            "non_prescription_medication",
            "medication_refill",
            "doctor_visit",
            "motor_vehicle_accident",
            "device_implant_joint",
            "device_implant_fixation",
            "device_implant_heart_valve",
            "device_implant_lens",
            "device_implant_pacemaker",
            "device_implant_other",
        ],
        _ => return None,
    };
    Some(record_types)
}

/// The collection a product name belongs to, which is almost always the product name itself.
///
/// DCP builds MEPS and NHIS together as one "health" product and names their data files with a
/// `_health` suffix, so both map to "health" here.
fn collection_name(product: &str) -> String {
    match product.to_lowercase().as_ref() {
        "meps" | "nhis" | "health" => "health".to_string(),
        other => other.to_string(),
    }
}

/// Build one record type of a collection.
///
/// Only the household and person id, link and weight columns follow naming conventions we can
/// count on without metadata. The others are left empty rather than invented, and query
/// generation reports the missing id column instead of building a join it can't name.
// TODO: DCP's ProductConfig::weights has per-product weights but only fills in usa, ipumsi and
// mics, whose person weight is WEIGHTWM rather than PERWT. Weights belong in metadata; take them
// from there rather than growing another hard coded table here.
fn record_type(
    collection: &str,
    spec: &RecordTypeSpec,
    parent: Option<&RecordTypeSpec>,
) -> RecordType {
    let (unique_id, weight) = match spec.name {
        "household" => ("SERIAL", Some(RecordWeight::new("HHWT", 100))),
        "person" => ("PSERIAL", Some(RecordWeight::new("PERWT", 100))),
        _ => ("", None),
    };

    // A person record points at the household it belongs to with SERIALP.
    let foreign_keys = match (spec.name, parent) {
        ("person", Some(household)) => vec![(household.value.to_string(), "SERIALP".to_string())],
        _ => Vec::new(),
    };

    // Sample line weights apply to the USA 1940 and 1950 samples.
    let sample_weight = match (collection, spec.name) {
        ("usa", "person") => Some(RecordWeight::new("SLWT", 100)),
        _ => None,
    };

    RecordType {
        name: spec.name.to_string(),
        value: spec.value.to_string(),
        unique_id: unique_id.to_string(),
        foreign_keys,
        weight,
        sample_weight,
    }
}

fn find_record_type_spec(name: &str) -> Option<&'static RecordTypeSpec> {
    ALL_RECORD_TYPES.iter().find(|spec| spec.name == name)
}

/// Get the default configuration for a data collection.
///
/// There are default configurations for every IPUMS data product DCP builds: usa, fullusa,
/// ipumsi, cps, atus, ahtus, mtus, health (also reachable as meps or nhis), dhs, pma, mics,
/// highered, brfss, yrbss, nyts and nsduh. Product names are not case sensitive.
///
/// ```
/// use cimdea::defaults::defaults_for;
///
/// let settings = defaults_for("usa").unwrap();
/// ```
///
/// A collection's record types are keyed by the letter identifying them in the data.
///
/// ```
/// use cimdea::defaults::defaults_for;
///
/// let settings = defaults_for("atus").unwrap();
/// let mut record_types: Vec<_> = settings.record_types.keys().cloned().collect();
/// record_types.sort();
///
/// // Household, Person, Activity, Who and elder caRe records
/// assert_eq!(record_types, ["A", "H", "P", "R", "W"]);
/// ```
///
/// Passing an unknown data collection name causes an error.
///
/// ```
/// use cimdea::defaults::defaults_for;
///
/// let maybe_settings = defaults_for("notarealcollection");
/// assert!(maybe_settings.is_err());
/// ```
pub fn defaults_for(product: &str) -> Result<MicroDataCollection, MdError> {
    // Right now we only set defaults programmatically but in the future this should set some
    // additional properties particular to products or stuff loaded in from an external
    // configuration.
    let collection = collection_name(product);
    let Some(handled) = handled_record_types(&collection) else {
        return Err(MdError::Msg(format!("Product '{product}' not supported")));
    };

    let mut record_types = HashMap::new();
    let mut hierarchy: Option<RecordHierarchy> = None;

    // ALL_RECORD_TYPES lists parents before children, so the first record type the collection
    // uses is the root of its hierarchy and every other one already has its parent in place.
    for spec in ALL_RECORD_TYPES
        .iter()
        .filter(|s| handled.contains(&s.name))
    {
        let parent = spec
            .parent
            .filter(|parent| handled.contains(parent))
            .and_then(find_record_type_spec);

        match (hierarchy.as_mut(), parent) {
            (None, None) => hierarchy = Some(RecordHierarchy::new(spec.value)),
            (Some(hierarchy), Some(parent)) => hierarchy.add_member(spec.value, parent.value)?,
            _ => {
                return Err(MdError::Msg(format!(
                    "Record type '{}' does not connect to the rest of the record hierarchy of collection '{collection}'.",
                    spec.name
                )))
            }
        }

        let already_there = record_types.insert(
            spec.value.to_string(),
            record_type(&collection, spec, parent),
        );
        if let Some(already_there) = already_there {
            return Err(MdError::Msg(format!(
                "Collection '{collection}' has two record types identified by '{}': '{}' and '{}'.",
                spec.value, already_there.name, spec.name
            )));
        }
    }

    let Some(record_hierarchy) = hierarchy else {
        return Err(MdError::Msg(format!(
            "Collection '{collection}' has no record types."
        )));
    };

    // Every collection has person records, and person is the unit of analysis for all of them.
    let Some(default_unit_of_analysis) = record_types.get("P").cloned() else {
        return Err(MdError::Msg(format!(
            "Collection '{collection}' has no person record type to use as a unit of analysis."
        )));
    };

    Ok(MicroDataCollection {
        name: collection,
        record_hierarchy,
        record_types,
        default_unit_of_analysis,
        metadata: None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every product, with the root of its hierarchy and how many record types it uses, from
    /// ProductConfig::rootRecord and ProductConfig::handled_record_types in the DCP settings.
    const ALL_PRODUCTS: &[(&str, &str, usize)] = &[
        ("ahtus", "P", 2),
        ("atus", "H", 5),
        ("brfss", "P", 1),
        ("cps", "H", 2),
        ("dhs", "P", 1),
        ("fullusa", "H", 2),
        ("health", "H", 21),
        ("highered", "P", 1),
        ("ipumsi", "H", 2),
        ("meps", "H", 21),
        ("mics", "P", 1),
        ("mtus", "P", 2),
        ("nhis", "H", 21),
        ("nsduh", "P", 1),
        ("nyts", "P", 1),
        ("pma", "P", 1),
        ("usa", "H", 2),
        ("yrbss", "P", 1),
    ];

    #[test]
    fn test_defaults_for_unknown_product() {
        let result = defaults_for("????");
        assert!(
            result.is_err(),
            "there should not be any defaults for product '????'"
        );
    }

    /// Every product has defaults with the record types and hierarchy DCP gives it. Since record
    /// types are keyed by letter and defaults_for() errors out on a repeated letter, the record
    /// type count also checks that no two record types of a product collide.
    #[test]
    fn test_defaults_for_every_product() {
        for (product, root, record_type_count) in ALL_PRODUCTS {
            let settings = defaults_for(product)
                .unwrap_or_else(|err| panic!("should have defaults for '{product}': {err}"));

            assert_eq!(
                settings.record_types.len(),
                *record_type_count,
                "'{product}' should have {record_type_count} record types"
            );
            assert_eq!(
                settings.record_hierarchy.levels.len(),
                *record_type_count,
                "every record type of '{product}' should be in its hierarchy"
            );
            assert_eq!(
                settings.default_unit_of_analysis.value, "P",
                "person should be the default unit of analysis for '{product}'"
            );

            // Only the root record type has no parent; every other one hangs off a record type
            // the product also uses.
            for (value, member) in &settings.record_hierarchy.levels {
                match &member.parent {
                    None => assert_eq!(value, root, "'{product}' should be rooted at '{root}'"),
                    Some(parent) => assert!(
                        settings.record_types.contains_key(parent),
                        "the parent '{parent}' of '{value}' should be a record type of '{product}'"
                    ),
                }
            }
        }
    }

    /// The household - person products keep the record structure and weights they've always had.
    #[test]
    fn test_household_person_products() {
        for product in ["usa", "cps", "ipumsi", "fullusa"] {
            let settings = defaults_for(product).expect("should have defaults");

            assert_eq!(settings.record_types["H"].unique_id, "SERIAL");
            assert_eq!(settings.record_types["P"].unique_id, "PSERIAL");
            assert_eq!(
                settings.record_types["P"].foreign_keys,
                vec![("H".to_string(), "SERIALP".to_string())],
                "a person record points at its household with SERIALP"
            );
            assert_eq!(settings.weight_for_rectype("H").as_deref(), Some("HHWT"));
            assert_eq!(settings.weight_for_rectype("P").as_deref(), Some("PERWT"));
            assert_eq!(settings.weight_divisor("P"), Some(100));

            // Only USA has a sample line weight
            let sample_line_weight = settings.sample_line_weight_for_rectype("P");
            if product == "usa" {
                assert_eq!(sample_line_weight.as_deref(), Some("SLWT"));
            } else {
                assert_eq!(sample_line_weight, None);
            }
        }
    }

    /// A person-only product has no household records, so person is the root of its hierarchy and
    /// has no foreign key pointing anywhere.
    #[test]
    fn test_person_only_product() {
        let settings = defaults_for("dhs").expect("should have defaults for DHS");

        assert!(!settings.record_types.contains_key("H"));
        assert_eq!(settings.record_hierarchy.root, "P");
        assert!(settings.record_types["P"].foreign_keys.is_empty());
    }

    /// ATUS activity records belong to a person and who records belong to an activity. Record
    /// types we have no link columns for get an empty unique id rather than an invented one.
    #[test]
    fn test_atus_hierarchy() {
        let settings = defaults_for("atus").expect("should have defaults for ATUS");
        let levels = &settings.record_hierarchy.levels;

        assert_eq!(levels["A"].parent.as_deref(), Some("P"));
        assert_eq!(levels["W"].parent.as_deref(), Some("A"));
        assert_eq!(levels["R"].parent.as_deref(), Some("P"));

        let activity = &settings.record_types["A"];
        assert_eq!(activity.name, "activity");
        assert!(
            activity.unique_id.is_empty(),
            "we don't know an activity record's id column without metadata"
        );
        assert!(activity.foreign_keys.is_empty());
    }

    /// The health record types hang off of a person, except the refill records, which hang off the
    /// record they refill. MEPS and NHIS are both built as the "health" collection, and their data
    /// files are named with a _health suffix.
    #[test]
    fn test_health_hierarchy() {
        for product in ["meps", "NHIS", "health"] {
            let settings = defaults_for(product).expect("should have defaults");
            let levels = &settings.record_hierarchy.levels;

            assert_eq!(settings.name, "health");
            assert_eq!(
                settings.base_filename_for_dataset("mp2015"),
                "mp2015_health"
            );
            assert_eq!(
                settings.record_types["A"].name, "hospitalization",
                "'A' is a hospitalization record here, not an activity record"
            );
            assert_eq!(settings.record_types["R"].name, "round");
            assert_eq!(
                levels["F"].parent.as_deref(),
                Some("M"),
                "a medication refill belongs to a prescription medication record"
            );
            assert_eq!(levels["K"].parent.as_deref(), Some("E"));
            assert_eq!(levels["J"].parent.as_deref(), Some("X"));
        }
    }
}
