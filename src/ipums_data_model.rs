//! Models for record types, record type relationships, and weights in IPUMS datasets.
//!
//! Model the IPUMS datasets (sometimes called 'samples'.)  This module models the record types, weighting and record type relationship for IPUMS data, not the samples themselves.
//!
//! Every dataset in a collection will have these same characteristics:
//! It will be a collection of record types and each record type has
//! some information about it and its relationships to the other record types.
//! Every collection has a single hierarchy of record types.
//! A record type on a particular data product may have a default weight variable -- or it may not.
//!
use crate::mderror::MdError;
use std::collections::HashMap;
use std::collections::HashSet;

#[derive(Clone, Debug)]
pub struct RecordType {
    pub name: String,                        // Person, Household, Activity, etc
    pub value: String,                       // like 'H', 'P', 'A' etc
    pub unique_id: String,                   // Like SERIAL for household, PSERIAL for Person etc
    pub foreign_keys: Vec<(String, String)>, // RecordType name,  key name: like 'Household', 'serialp'
    pub weight: Option<RecordWeight>,

    // Some datasets will have a "sample line weight" where only certain records / people
    // have non N/A values for some variables. This type of weight will be 0 for many
    // records. When using this weight instead of the main weight you will get a correctly
    // weighted subsample.
    pub sample_weight: Option<RecordWeight>,
}

impl RecordType {
    /// The name of the key column DCP generates for this record type, like `PERSON_KEY`.
    ///
    /// DCP's metadata cache adds one of these to every record type of a product, along with the
    /// foreign keys and counting variables below (`ProductConfig::name_primary_key` and
    /// `Cache::create_key_and_counting_variables` in the DCP source). The names are mechanically
    /// produced from the record type names, so we can produce them here too instead of storing
    /// them.
    ///
    /// Data built before DCP started generating these columns doesn't have them, so treat this as
    /// the name to look for rather than a promise the column is there.
    ///
    /// ```
    /// use cimdea::defaults::defaults_for;
    ///
    /// let settings = defaults_for("atus").unwrap();
    /// assert_eq!(settings.record_types["A"].generated_key(), "ACTIVITY_KEY");
    /// ```
    pub fn generated_key(&self) -> String {
        format!("{}_KEY", self.name.to_ascii_uppercase())
    }

    /// The name of the column on this record type pointing at its parent, like
    /// `HOUSEHOLD_KEY_ON_PERSON`.
    ///
    /// The column lives on the child record and names the parent first, so a person record
    /// carries `HOUSEHOLD_KEY_ON_PERSON` and an activity record carries `PERSON_KEY_ON_ACTIVITY`.
    ///
    /// ```
    /// use cimdea::defaults::defaults_for;
    ///
    /// let settings = defaults_for("atus").unwrap();
    /// let person = &settings.record_types["P"];
    /// let activity = &settings.record_types["A"];
    /// assert_eq!(activity.generated_foreign_key(person), "PERSON_KEY_ON_ACTIVITY");
    /// ```
    pub fn generated_foreign_key(&self, parent: &RecordType) -> String {
        format!(
            "{}_KEY_ON_{}",
            parent.name.to_ascii_uppercase(),
            self.name.to_ascii_uppercase()
        )
    }

    /// The name of the column on this record type counting the child records of one type that
    /// follow it, like `COUNT_OF_P`.
    ///
    /// Note DCP names these with the child record type's letter, not its name.
    ///
    /// ```
    /// use cimdea::defaults::defaults_for;
    ///
    /// let settings = defaults_for("atus").unwrap();
    /// let household = &settings.record_types["H"];
    /// let person = &settings.record_types["P"];
    /// assert_eq!(household.generated_count_of(person), "COUNT_OF_P");
    /// ```
    pub fn generated_count_of(&self, child: &RecordType) -> String {
        format!("COUNT_OF_{}", child.value.to_ascii_uppercase())
    }
}

#[derive(Clone, Debug)]
pub struct RecordWeight {
    pub name: String,
    pub divisor: usize,
}

impl RecordWeight {
    pub fn new(name: &str, divisor: usize) -> Self {
        Self {
            name: name.to_string(),
            divisor,
        }
    }
}
#[derive(Clone, Debug)]
pub struct RecordHierarchyMember {
    pub name: String,
    pub children: Option<HashSet<String>>,
    pub parent: Option<String>,
}

impl RecordHierarchyMember {
    pub fn add_child(&mut self, rectype: &str) {
        let children = self.children.get_or_insert_with(HashSet::new);
        children.insert(rectype.to_string());
    }
}

#[derive(Clone, Debug)]
pub struct RecordHierarchy {
    pub root: String,
    pub levels: HashMap<String, RecordHierarchyMember>,
}

impl RecordHierarchy {
    pub fn new(rectype: &str) -> Self {
        let root_level = RecordHierarchyMember {
            name: rectype.to_string(),
            parent: None,
            children: None,
        };
        Self {
            root: rectype.to_string(),
            levels: HashMap::from([(rectype.to_string(), root_level)]),
        }
    }

    pub fn add_member(&mut self, rectype: &str, parent: &str) -> Result<(), MdError> {
        let member = RecordHierarchyMember {
            name: rectype.to_string(),
            parent: Some(parent.to_string()),
            children: None,
        };

        // Update the parent level to include this as a child
        match self.levels.get_mut(parent) {
            Some(p) =>  p.add_child(rectype),
            None => return Err(MdError::Msg(format!("You tried to add a child record of type {} with a parent '{}' but no such parent is in the hierarchy yet.", rectype, parent))),

        }
        self.levels.insert(rectype.to_string(), member);
        Ok(())
    }
}

mod test {
    #[cfg(test)]
    use super::*;

    #[test]
    fn test_hierarchy() {
        let mut rh = RecordHierarchy::new("H");
        assert_eq!(1, rh.levels.len());
        let result = rh.add_member("P", "H");
        assert!(
            result.is_ok(),
            "Should be able to add P with H as parent to a record hierarchy."
        );
        assert_eq!(2, rh.levels.len());

        let bad_result = rh.add_member("X", "Y");
        assert!(
            bad_result.is_err(),
            "Should error out when adding a member with a parent type that doesn't exist."
        );
    }

    #[test]
    fn test_record_hierarchy_member_add_child_no_children_yet() {
        let mut member = RecordHierarchyMember {
            name: "H".to_string(),
            children: None,
            parent: None,
        };

        member.add_child("P");

        let children = member
            .children
            .expect("should create a new set when adding the first child");
        assert!(
            children.contains("P"),
            "P should be added to the new set of children"
        );
    }

    #[test]
    fn test_record_hierarchy_member_add_child_multiple() {
        let children = HashSet::from(["I".to_string(), "X".to_string()]);
        let mut member = RecordHierarchyMember {
            name: "P".to_string(),
            children: Some(children),
            parent: Some("H".to_string()),
        };

        member.add_child("D");
        let children = member.children.expect("should have a set of children");
        let expected = HashSet::from(["I".to_string(), "X".to_string(), "D".to_string()]);
        assert_eq!(expected, children);
    }
}
