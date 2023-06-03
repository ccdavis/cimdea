use std::collections::HashMap;
use std::collections::HashSet;

// Every dataset in a collection will have these same characteristics:
//  It will be a collection of record types and each record type has
// some information about it and its relationships to the other record types.
// Every collection has a single hierarchy of record types.

pub struct RecordType {
    pub name: String,                        // Person, Household, Activity, etc
    pub value: String,                       // like 'H', 'P', 'A' etc
    pub unique_id: String,                   // Like SERIAL for household, PSERIAL for Person etc
    pub foreign_keys: Vec<(String, String)>, // RecordType name,  key name: like 'Household', 'serialp'
    pub weight_name: String,
    pub weight_divisor: usize,
}

pub struct RecordHierarchy {
    pub name: String,
    pub children: Option<HashSet<RecordHierarchy>>,
    pub parent: Option<Box<RecordHierarchy>>,
}
