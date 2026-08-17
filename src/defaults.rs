//! Definitions of default IPUMS values.
//!
//! IPUMS data products are built out of record types arranged in a hierarchy. The Household -
//! Person structure covers many products, but time use products add activity records, and the
//! health products add a long list of records that hang off of a person. Here we have functions
//! to set up that structure for any supported product without needing any external configuration.
//!
//! The record type names, the letters that identify them in data and file names, the hierarchy
//! among them, and which record types each product uses all come from the DCP settings in
//! `code/settings/Settings.cpp` (`ProductConfig::record_type_to_char`, `ProductConfig::hierarchy`,
//! `ProductConfig::handled_record_types` and `ProductConfig::rootRecord`). DCP is where IPUMS
//! defines the record structure of every data product, so this module should be kept in step
//! with it.
//!
//! We would rather build all of this from metadata, which is where it really belongs, but that
//! metadata often isn't available. When it is, it's in the "rectypes" or "record_types" table of
//! the current product's `metadata.db`. Loading it from there is worth doing later; until then
//! everything modeled here could also originate from a run-time configuration process.
//!
//!  A generic record type generator could use Cow instead of String, as in
//!  <https://stackoverflow.com/questions/63201351/writing-a-rust-struct-type-that-contains-a-string-and-can-be-used-in-a-constant>

use crate::conventions::*;
use crate::ipums_data_model::*;
use crate::mderror::MdError;
use std::collections::HashMap;

/// One record type in the IPUMS record hierarchy.
struct RecordTypeSpec {
    /// The record type name, like "person" or "prescription_medication".
    name: &'static str,
    /// The letter identifying this record type in data and file names.
    value: &'static str,
    /// The record type this one hangs off of, by name.
    ///
    /// This is the parent across all products. A product that doesn't use the parent record type
    /// -- person-only products have no household records, for instance -- gets no parent link,
    /// and its topmost record type becomes the root of its hierarchy.
    parent: Option<&'static str>,
}

/// Every record type IPUMS data products are built from, parents before children.
///
/// Letters are only unique within a product: 'A' is an activity record in the time use products
/// and a hospitalization record in the health products, and 'R' is a round record in health but
/// an elder care record in ATUS. No product uses two record types with the same letter, which
/// [record_types_for_product] relies on since it keys record types by letter.
///
/// DCP also defines a "poisoning" ('Z') record type, but it has no place in the hierarchy and no
/// product handles it, so it's left out here.
const ALL_RECORD_TYPES: &[RecordTypeSpec] = &[
    RecordTypeSpec {
        name: "household",
        value: "H",
        parent: None,
    },
    RecordTypeSpec {
        name: "person",
        value: "P",
        parent: Some("household"),
    },
    RecordTypeSpec {
        name: "activity",
        value: "A",
        parent: Some("person"),
    },
    RecordTypeSpec {
        name: "who",
        value: "W",
        parent: Some("activity"),
    },
    RecordTypeSpec {
        name: "elder_care",
        value: "R",
        parent: Some("person"),
    },
    RecordTypeSpec {
        name: "injury",
        value: "I",
        parent: Some("person"),
    },
    RecordTypeSpec {
        name: "round",
        value: "R",
        parent: Some("person"),
    },
    RecordTypeSpec {
        name: "event",
        value: "E",
        parent: Some("person"),
    },
    RecordTypeSpec {
        name: "event_medication_refill",
        value: "K",
        parent: Some("event"),
    },
    RecordTypeSpec {
        name: "condition",
        value: "X",
        parent: Some("person"),
    },
    RecordTypeSpec {
        name: "condition_event_refill",
        value: "J",
        parent: Some("condition"),
    },
    RecordTypeSpec {
        name: "xray",
        value: "S",
        parent: Some("person"),
    },
    RecordTypeSpec {
        name: "hospitalization",
        value: "A",
        parent: Some("person"),
    },
    RecordTypeSpec {
        name: "prescription_medication",
        value: "M",
        parent: Some("person"),
    },
    RecordTypeSpec {
        name: "medication_refill",
        value: "F",
        parent: Some("prescription_medication"),
    },
    RecordTypeSpec {
        name: "non_prescription_medication",
        value: "O",
        parent: Some("person"),
    },
    RecordTypeSpec {
        name: "doctor_visit",
        value: "D",
        parent: Some("person"),
    },
    RecordTypeSpec {
        name: "motor_vehicle_accident",
        value: "V",
        parent: Some("person"),
    },
    RecordTypeSpec {
        name: "pregnancy",
        value: "G",
        parent: Some("person"),
    },
    RecordTypeSpec {
        name: "vitamin_mineral",
        value: "L",
        parent: Some("person"),
    },
    RecordTypeSpec {
        name: "soa_condition",
        value: "Y",
        parent: Some("person"),
    },
    RecordTypeSpec {
        name: "disability_condition",
        value: "B",
        parent: Some("person"),
    },
    RecordTypeSpec {
        name: "device_implant_joint",
        value: "1",
        parent: Some("person"),
    },
    RecordTypeSpec {
        name: "device_implant_fixation",
        value: "2",
        parent: Some("person"),
    },
    RecordTypeSpec {
        name: "device_implant_heart_valve",
        value: "3",
        parent: Some("person"),
    },
    RecordTypeSpec {
        name: "device_implant_lens",
        value: "4",
        parent: Some("person"),
    },
    RecordTypeSpec {
        name: "device_implant_pacemaker",
        value: "5",
        parent: Some("person"),
    },
    RecordTypeSpec {
        name: "device_implant_other",
        value: "6",
        parent: Some("person"),
    },
];

/// The record types of the household - person products: USA, IPUMSI, CPS and friends.
const HOUSEHOLD_PERSON: &[&str] = &["household", "person"];

/// The record types of the person-only products: DHS, PMA, MICS, HIGHERED and the survey products.
const PERSON_ONLY: &[&str] = &["person"];

/// The record types of the time use products which have activity records but no households.
const PERSON_ACTIVITY: &[&str] = &["person", "activity"];

/// The record types of ATUS, which has households along with activity, who and elder care records.
const ATUS_RECORD_TYPES: &[&str] = &["household", "person", "activity", "who", "elder_care"];

/// The record types of the health products (MEPS and NHIS, which DCP builds as one "health"
/// product).
///
/// DCP currently comments out the pregnancy, disability_condition, soa_condition and
/// vitamin_mineral record types, so they're left out here too.
const HEALTH_RECORD_TYPES: &[&str] = &[
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
];

/// The name of the data collection a product name belongs to.
///
/// This is almost always the product name itself. The exception is the health products: DCP builds
/// MEPS and NHIS together as one "health" product, and their data files are named with a `_health`
/// suffix, so both map to "health" here.
fn collection_name(product: &str) -> String {
    match product.to_lowercase().as_ref() {
        "meps" | "nhis" | "health" => "health".to_string(),
        other => other.to_string(),
    }
}

/// The record types a data collection uses, or None if the collection isn't one we know about.
fn handled_record_types(collection: &str) -> Option<&'static [&'static str]> {
    let record_types = match collection {
        "usa" | "fullusa" | "napp" | "ipumsi" | "cps" | "mock" => HOUSEHOLD_PERSON,
        "atus" => ATUS_RECORD_TYPES,
        "ahtus" | "mtus" => PERSON_ACTIVITY,
        "health" => HEALTH_RECORD_TYPES,
        "dhs" | "pma" | "mics" | "highered" | "brfss" | "yrbss" | "nyts" | "nsduh" => PERSON_ONLY,
        _ => return None,
    };
    Some(record_types)
}

/// The column holding a record type's own unique id.
///
/// Only the household and person conventions hold without metadata. The columns linking the other
/// record types vary by product, so they're empty here and query generation reports an error
/// instead of building a join it can't name. These belong in metadata: see the module
/// documentation on the "record_types" metadata table.
fn unique_id_for(record_type_name: &str) -> String {
    match record_type_name {
        "household" => "SERIAL".to_string(),
        "person" => "PSERIAL".to_string(),
        _ => String::new(),
    }
}

/// The columns pointing from a record type to the record type above it, as (parent letter, column).
///
/// Like [unique_id_for], only the person to household link follows a convention we can count on
/// without metadata.
fn foreign_keys_for(
    record_type_name: &str,
    parent: Option<&RecordTypeSpec>,
) -> Vec<(String, String)> {
    match (record_type_name, parent) {
        ("person", Some(parent)) if parent.name == "household" => {
            vec![(parent.value.to_string(), "SERIALP".to_string())]
        }
        _ => Vec::new(),
    }
}

/// The default weight variable for a record type, if it has one.
///
/// The household and person weights follow the usual IPUMS naming. Other record types don't get a
/// weight by default, so tabulations of them come back unweighted rather than referring to a
/// column that may not exist.
// TODO: DCP's ProductConfig::weights has per-product weights, but only fills in usa, ipumsi and
// mics (whose person weight is WEIGHTWM, not PERWT). Weights really belong in metadata; take them
// from there rather than growing another hard coded table here.
fn weight_for(record_type_name: &str) -> Option<RecordWeight> {
    match record_type_name {
        "household" => Some(default_household_weight()),
        "person" => Some(default_person_weight()),
        _ => None,
    }
}

/// The sample line weight for a record type, if the collection has one.
fn sample_weight_for(collection: &str, record_type_name: &str) -> Option<RecordWeight> {
    match (collection, record_type_name) {
        ("usa", "person") => Some(usa_sample_line_weight()),
        _ => None,
    }
}

fn find_record_type_spec(name: &str) -> Option<&'static RecordTypeSpec> {
    ALL_RECORD_TYPES.iter().find(|spec| spec.name == name)
}

/// Build a record type, linking it to its parent only if the collection uses the parent too.
fn record_type_from_spec(
    collection: &str,
    spec: &RecordTypeSpec,
    handled: &[&str],
) -> Result<RecordType, MdError> {
    let parent = match spec.parent {
        Some(parent_name) if handled.contains(&parent_name) => {
            let parent_spec = find_record_type_spec(parent_name).ok_or_else(|| {
                MdError::Msg(format!(
                    "Record type '{}' has the parent record type '{parent_name}', which is not a known record type.",
                    spec.name
                ))
            })?;
            Some(parent_spec)
        }
        _ => None,
    };

    Ok(RecordType {
        name: spec.name.to_string(),
        value: spec.value.to_string(),
        unique_id: unique_id_for(spec.name),
        foreign_keys: foreign_keys_for(spec.name, parent),
        weight: weight_for(spec.name),
        sample_weight: sample_weight_for(collection, spec.name),
    })
}

/// All record types of a collection, keyed by the letter identifying each one.
fn record_types_for_product(
    collection: &str,
    handled: &[&str],
) -> Result<HashMap<String, RecordType>, MdError> {
    let mut record_types = HashMap::new();
    for name in handled {
        let spec = find_record_type_spec(name).ok_or_else(|| {
            MdError::Msg(format!(
                "Collection '{collection}' uses record type '{name}', which is not a known record type."
            ))
        })?;
        let record_type = record_type_from_spec(collection, spec, handled)?;
        if let Some(already_there) = record_types.insert(spec.value.to_string(), record_type) {
            return Err(MdError::Msg(format!(
                "Collection '{collection}' has two record types identified by '{}': '{}' and '{}'.",
                spec.value, already_there.name, spec.name
            )));
        }
    }
    Ok(record_types)
}

/// The root of a collection's record hierarchy: the record type with no parent in the collection.
fn root_record_type(
    collection: &str,
    handled: &[&str],
) -> Result<&'static RecordTypeSpec, MdError> {
    let mut roots = ALL_RECORD_TYPES
        .iter()
        .filter(|spec| handled.contains(&spec.name))
        .filter(|spec| !spec.parent.is_some_and(|parent| handled.contains(&parent)));

    let Some(root) = roots.next() else {
        return Err(MdError::Msg(format!(
            "Collection '{collection}' has no record type at the top of its hierarchy."
        )));
    };

    if let Some(extra_root) = roots.next() {
        return Err(MdError::Msg(format!(
            "Collection '{collection}' has more than one record type at the top of its hierarchy: '{}' and '{}'.",
            root.name, extra_root.name
        )));
    }

    Ok(root)
}

/// The hierarchy of a collection's record types.
///
/// [ALL_RECORD_TYPES] lists parents before their children, so walking it in order always adds a
/// parent to the hierarchy before any of its children.
fn hierarchy_for_product(collection: &str, handled: &[&str]) -> Result<RecordHierarchy, MdError> {
    let root = root_record_type(collection, handled)?;
    let mut hierarchy = RecordHierarchy::new(root.value);

    for spec in ALL_RECORD_TYPES {
        if spec.name == root.name || !handled.contains(&spec.name) {
            continue;
        }

        let parent_name = spec.parent.filter(|parent| handled.contains(parent));
        let Some(parent_name) = parent_name else {
            return Err(MdError::Msg(format!(
                "Record type '{}' of collection '{collection}' has no parent record type, but '{}' is already the root of the hierarchy.",
                spec.name, root.name
            )));
        };
        let parent = find_record_type_spec(parent_name).ok_or_else(|| {
            MdError::Msg(format!(
                "Record type '{}' has the parent record type '{parent_name}', which is not a known record type.",
                spec.name
            ))
        })?;

        hierarchy.add_member(spec.value, parent.value)?;
    }

    Ok(hierarchy)
}

/// The record type a collection tabulates by default.
///
/// Person is the unit of analysis for every collection we support; they all have person records.
fn default_unit_of_analysis(
    collection: &str,
    handled: &[&str],
    record_types: &HashMap<String, RecordType>,
) -> Result<RecordType, MdError> {
    let unit = if handled.contains(&"person") {
        find_record_type_spec("person")
    } else {
        Some(root_record_type(collection, handled)?)
    };

    let Some(unit) = unit else {
        return Err(MdError::Msg(format!(
            "Collection '{collection}' has no record type to use as a default unit of analysis."
        )));
    };

    record_types.get(unit.value).cloned().ok_or_else(|| {
        MdError::Msg(format!(
            "Collection '{collection}' has no '{}' record type to use as a default unit of analysis.",
            unit.name
        ))
    })
}

fn default_household_weight() -> RecordWeight {
    RecordWeight::new("HHWT", 100)
}

fn default_person_weight() -> RecordWeight {
    RecordWeight::new("PERWT", 100)
}

fn usa_sample_line_weight() -> RecordWeight {
    RecordWeight::new("SLWT", 100)
}

/// Get the default configuration for a data collection.
///
/// There are default configurations for every IPUMS data product DCP builds: usa, fullusa, napp,
/// ipumsi, cps, mock, atus, ahtus, mtus, health (also reachable as meps or nhis), dhs, pma, mics,
/// highered, brfss, yrbss, nyts and nsduh. Product names are not case sensitive.
///
/// ```
/// use cimdea::defaults::defaults_for;
///
/// let settings = defaults_for("usa").unwrap();
/// ```
///
/// A collection's record types are keyed by the letter that identifies them in the data.
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

    let record_types = record_types_for_product(&collection, handled)?;
    let record_hierarchy = hierarchy_for_product(&collection, handled)?;
    let default_unit_of_analysis = default_unit_of_analysis(&collection, handled, &record_types)?;

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

    /// Every product DCP builds, along with the record type at the top of its hierarchy and how
    /// many record types it has. These come from ProductConfig::rootRecord and
    /// ProductConfig::handled_record_types in the DCP settings.
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
        ("mics", "P", 1),
        ("mock", "H", 2),
        ("mtus", "P", 2),
        ("napp", "H", 2),
        ("nhis", "H", 21),
        ("nsduh", "P", 1),
        ("nyts", "P", 1),
        ("meps", "H", 21),
        ("pma", "P", 1),
        ("usa", "H", 2),
        ("yrbss", "P", 1),
    ];

    #[test]
    fn test_defaults_for_usa() {
        let result = defaults_for("usa");
        assert!(
            result.is_ok(),
            "should be able to get defaults for product USA"
        );
    }

    #[test]
    fn test_defaults_for_unknown_product() {
        let result = defaults_for("????");
        assert!(
            result.is_err(),
            "there should not be any defaults for product '????'"
        );
    }

    /// Every product has defaults, with the record types and root record type DCP gives it.
    #[test]
    fn test_defaults_for_every_product() {
        for (product, root, number_of_record_types) in ALL_PRODUCTS {
            let settings = defaults_for(product)
                .unwrap_or_else(|err| panic!("should have defaults for '{product}': {err}"));

            assert_eq!(
                settings.record_types.len(),
                *number_of_record_types,
                "'{product}' should have {number_of_record_types} record types"
            );
            assert_eq!(
                settings.record_hierarchy.root, *root,
                "'{product}' should have '{root}' at the root of its hierarchy"
            );
            assert_eq!(
                settings.record_hierarchy.levels.len(),
                *number_of_record_types,
                "every record type of '{product}' should be in its hierarchy"
            );
            assert!(
                settings.record_types.contains_key("P"),
                "every product has person records"
            );
            assert_eq!(
                settings.default_unit_of_analysis.value, "P",
                "person should be the default unit of analysis for '{product}'"
            );
        }
    }

    /// Record types are keyed by letter, so no product may have two record types with the same
    /// letter. Letters do repeat across products: 'A' is an activity record in ATUS and a
    /// hospitalization record in the health products.
    #[test]
    fn test_record_type_letters_are_unique_within_a_product() {
        for (product, _, number_of_record_types) in ALL_PRODUCTS {
            let collection = collection_name(product);
            let handled = handled_record_types(&collection).expect("should be a known collection");
            assert_eq!(
                handled.len(),
                *number_of_record_types,
                "'{product}' should use {number_of_record_types} record types"
            );

            // record_types_for_product() errors out on a repeated letter
            let record_types = record_types_for_product(&collection, handled)
                .unwrap_or_else(|err| panic!("record types for '{product}': {err}"));
            assert_eq!(record_types.len(), handled.len());
        }
    }

    /// Every record type in a product's hierarchy, other than the root, has a parent that the
    /// product also uses.
    #[test]
    fn test_hierarchy_members_have_parents() {
        for (product, root, _) in ALL_PRODUCTS {
            let settings = defaults_for(product).expect("should have defaults");
            for (value, member) in &settings.record_hierarchy.levels {
                if value.as_str() == *root {
                    assert!(
                        member.parent.is_none(),
                        "the root record type of '{product}' should have no parent"
                    );
                    continue;
                }
                let parent = member
                    .parent
                    .as_ref()
                    .unwrap_or_else(|| panic!("'{value}' in '{product}' should have a parent"));
                assert!(
                    settings.record_types.contains_key(parent),
                    "the parent '{parent}' of '{value}' should be a record type of '{product}'"
                );
            }
        }
    }

    /// The household - person products keep the record structure and weights they've always had.
    #[test]
    fn test_household_person_products() {
        for product in ["usa", "cps", "ipumsi", "napp", "fullusa", "mock"] {
            let settings = defaults_for(product).expect("should have defaults");
            let household = &settings.record_types["H"];
            let person = &settings.record_types["P"];

            assert_eq!(household.unique_id, "SERIAL");
            assert_eq!(person.unique_id, "PSERIAL");
            assert_eq!(
                person.foreign_keys,
                vec![("H".to_string(), "SERIALP".to_string())],
                "person records point at their household with SERIALP"
            );
            assert_eq!(settings.weight_for_rectype("H").as_deref(), Some("HHWT"));
            assert_eq!(settings.weight_for_rectype("P").as_deref(), Some("PERWT"));
            assert_eq!(settings.weight_divisor("H"), Some(100));
        }
    }

    /// Only USA has a sample line weight.
    #[test]
    fn test_usa_sample_line_weight() {
        let usa = defaults_for("usa").expect("should have defaults for USA");
        assert_eq!(
            usa.sample_line_weight_for_rectype("P").as_deref(),
            Some("SLWT")
        );

        let cps = defaults_for("cps").expect("should have defaults for CPS");
        assert_eq!(cps.sample_line_weight_for_rectype("P"), None);
    }

    /// The person-only products have no household records, so person is the root of the hierarchy
    /// and has no foreign key pointing anywhere.
    #[test]
    fn test_person_only_product() {
        let settings = defaults_for("dhs").expect("should have defaults for DHS");
        assert!(!settings.record_types.contains_key("H"));

        let person = &settings.record_types["P"];
        assert!(
            person.foreign_keys.is_empty(),
            "a person record with no household to belong to has no foreign key"
        );
        assert_eq!(settings.record_hierarchy.root, "P");
    }

    /// ATUS activity records belong to a person, and who records belong to an activity.
    #[test]
    fn test_atus_hierarchy() {
        let settings = defaults_for("atus").expect("should have defaults for ATUS");

        let activity = &settings.record_hierarchy.levels["A"];
        assert_eq!(activity.parent.as_deref(), Some("P"));

        let who = &settings.record_hierarchy.levels["W"];
        assert_eq!(who.parent.as_deref(), Some("A"));

        let person = &settings.record_hierarchy.levels["P"];
        let children = person
            .children
            .as_ref()
            .expect("person should have child record types in ATUS");
        assert!(
            children.contains("A"),
            "activity records belong to a person"
        );
        assert!(
            children.contains("R"),
            "elder care records belong to a person"
        );
    }

    /// The health record types hang off of a person, except for the refill records, which hang off
    /// of the record they refill.
    #[test]
    fn test_health_hierarchy() {
        let settings = defaults_for("nhis").expect("should have defaults for NHIS");

        assert_eq!(
            settings.record_types["A"].name, "hospitalization",
            "'A' is a hospitalization record in the health products, not an activity record"
        );
        assert_eq!(settings.record_types["R"].name, "round");

        let medication_refill = &settings.record_hierarchy.levels["F"];
        assert_eq!(
            medication_refill.parent.as_deref(),
            Some("M"),
            "medication refills belong to a prescription medication record"
        );

        let event_refill = &settings.record_hierarchy.levels["K"];
        assert_eq!(event_refill.parent.as_deref(), Some("E"));

        let condition_refill = &settings.record_hierarchy.levels["J"];
        assert_eq!(condition_refill.parent.as_deref(), Some("X"));
    }

    /// MEPS and NHIS are both built as the "health" collection, and their data files are named
    /// with a _health suffix.
    #[test]
    fn test_meps_and_nhis_are_the_health_collection() {
        for product in ["meps", "nhis", "health"] {
            let settings = defaults_for(product).expect("should have defaults");
            assert_eq!(settings.name, "health");
            assert_eq!(
                settings.base_filename_for_dataset("mp2015"),
                "mp2015_health"
            );
        }
    }

    /// Product names are not case sensitive.
    #[test]
    fn test_product_name_is_not_case_sensitive() {
        let settings = defaults_for("IPUMSI").expect("should have defaults for IPUMSI");
        assert_eq!(settings.name, "ipumsi");
        assert_eq!(settings.record_types.len(), 2);
    }

    /// Record types we don't have link columns for get an empty unique id rather than a made up
    /// column name. Query generation reports an error instead of joining on it.
    #[test]
    fn test_unknown_link_columns_are_empty() {
        let settings = defaults_for("atus").expect("should have defaults for ATUS");
        let activity = &settings.record_types["A"];
        assert!(
            activity.unique_id.is_empty(),
            "we don't know the activity record's id column without metadata"
        );
        assert!(activity.foreign_keys.is_empty());
    }
}
