# Change Log

This is the change log for cimdea. It lists changes chronologically from most
recent to least recent. Each cimdea version has its own heading below which
corresponds to a git tag in the repository. Some of the most recent changes may
not yet be released in a cimdea version, and they are listed under the "Not Yet
Released" heading at the top.

## Not Yet Released

### Bug fixes

* Tabulations now use the unit of analysis from the request. Previously the `uoa`
  field of an Abacus request and the `unit_of_analysis` argument of
  `DataRequest::from_names()` were accepted but ignored during query generation,
  and every tabulation ran at the collection's default unit of analysis (person,
  for the built-in collections).
* A record type with no weight variable no longer produces a broken query. The
  generated `group by` used to assume a `weighted_ct` column was always present,
  which shifted the grouping columns, and the output table always got a
  `weighted_ct` heading whether or not the query selected one.
* `context_from_names_helper()` (and so `DataRequest::from_names()`) now uses the
  product root it's passed instead of silently dropping it.
* Loading metadata for datasets from layouts adds to metadata already loaded
  rather than replacing all of it.
* `CategoryBin::within()` now treats its bounds as inclusive, matching the
  bucketing SQL that gets generated for the same bins.
* `MicroDataCollection::clear_metadata()` clears the metadata; it used to do
  nothing.
* Tabulating a value that isn't a number, or is NULL, no longer fails the whole
  tabulation.
* The "more than two record types" check in query generation now counts the
  record types the request needs, instead of every record type the collection
  has.
* Reading a layout file reports errors instead of silently dropping the records
  it can't read.
* Requesting a not-yet-implemented output format (CSV or HTML) returns an error
  instead of panicking.
* Metadata indexes grow to fit instead of panicking if dataset or variable ids
  arrive out of order.
* `ParquetMetadataReader::get_schema_info()` returns fields in a stable order.
* Remote paths are quoted before they're sent to a remote shell.

### Other changes

* `defaults_for()` now covers every IPUMS data product, with the record types,
  record type letters, and record hierarchy each one uses: the household -
  person products (usa, fullusa, ipumsi, cps), the time use products (atus,
  ahtus, mtus) with their activity, who and elder care records, the health
  products (meps and nhis, which are built as one "health" product) with their
  21 record types, and the person-only products (dhs, pma, mics, highered,
  brfss, yrbss, nyts, nsduh). These come from the DCP settings; eventually they
  should come from the record types metadata instead.
* `RecordType` can produce the key and counting column names DCP generates from
  the record type names: `generated_key()` (`PERSON_KEY`),
  `generated_foreign_key()` (`HOUSEHOLD_KEY_ON_PERSON`, which lives on the child
  record) and `generated_count_of()` (`COUNT_OF_P`, the number of child records
  that follow). `MicroDataCollection::counting_variables_for_rectype()` lists
  the counting columns of a record type, and `parent_rectype()` gives the record
  type a record type belongs to.
* Query generation joins record types on those generated keys whenever the data
  has them, which it checks against the loaded metadata. The conventional SERIAL,
  PSERIAL and SERIALP columns are the fall back for data built before DCP
  started generating keys. This also means tabulations across the time use and
  health record types, which have no conventionally named keys at all, can now
  be generated. Both columns of a join are resolved together, so a query can't
  join a generated key on one side to a conventionally named id on the other.
* `Context::has_md_variable()` and `MetadataEntities::has_variable()` report
  whether a variable is in the loaded metadata.
* DCP renamed the health products' event medication refill record type in July
  2025, so data built before then names its keys
  `EVENT_MEDICATION_EVENT_REFILL_KEY` and
  `EVENT_KEY_ON_EVENT_MEDICATION_EVENT_REFILL`. Both spellings resolve.
* The collection name in `MicroDataCollection` is now the lower case product
  name for every product. It used to be "USA" for usa but lower case for the
  other collections. Data file names are unaffected because they were already
  built from the lower cased name. Requesting meps or nhis gives a collection
  named "health", which matches how their data files are named.
* Dropped the `sql-builder`, `extended`, and `encoding` dependencies, none of
  which the crate uses.

### Breaking changes

* `DataRequest::get_request_variables()` returns `Result<Vec<RequestVariable>, MdError>`.
  It used to panic on inconsistent metadata.
* `DataRequest` has a new required method, `unit_of_analysis()`.
* `query_gen::tab_queries()` returns `Vec<TabQuery>` rather than `Vec<String>`.
  A `TabQuery` has the query's `sql` plus whether it selects a weighted count.
* `ParquetMetadataReader::get_schema_info()` returns a `BTreeMap` instead of a
  `HashMap`, and `RecordLayout::vars()` returns a slice instead of a `&Vec`.

## v0.3.2 (2025-02-19)

* Include lockfile for reproducible builds. when using `cargo install`.
* Updated documentation, including more details and examples.

## v0.3.1 (2024-11-13)

* Added support for optional bounds in request_case_selections. Now `low_code` may
  be null to indicate that there is no lower bound, or similarly `high_code` may
  be null to indicate that there is no upper bound. At least one of the two attributes
  must not be null.
* Fixed a flaky test that occasionally failed on certain machines.
* Automated documentation generation with GitHub Actions.

## v0.3.0 (2024-11-04)

* Fixed a bug that caused requests with sub-populations that used only household
  variables to fail.
* Added full support for general versions of variables, and fixed some bugs
  which prevented the feature from working. In particular, the `general_width`
  field on `IpumsVariable` is now optional, since we actually cannot determine
  the general width from layout files. Currently, incoming requests specify the
  general width with the `extract_width` field on request variables.
* Added partial support for sample-line weights, which apply to USA 1940 and 1950 samples.
* Updated the DuckDB library to version 1.1.1.

## v0.2.0 (2024-10-31)

This is the first release of cimdea.
