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
        let children = self.children.get_or_insert_with(|| HashSet::new());
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
