# cimdea

A Rust package for working with IPUMS-like microdata both for aggregation, extraction and processing.

The library supports high-level  tabulation interfaces to IPUMS-like data and data processing and extraction.  See the [docs](https://ccdavis.github.io/cimdea/cimdea/index.html).

The `abacus` binary creates cross-tabs from the command line, taking care of some complexities like joining multiple record types and allowing for bucketing continuous value variables, and applying record weights if available.

## CIMDEA = Convenient IPUMS-like Microdata Extraction and Aggregation

The main idea here is not to make a general data processing tool but instead take advantage of all the conventions in IPUMS datasets. These are demographic data at the individual level from surveys or censuses. By assuming IPUMS conventions and a bit of (optional) configuration we could provide a powerful, high-level, easy to use set of features.

Metadata in varying amounts accompanies IPUMS datasets. A minimal metadata level ought to provide enough information to power most features in this library. With more metadata (from the IPUMS API or internal databases) the library should produce the same data outputs but more richly documented.

The tabulation interface should take a variables list, datasets list and some filtering criteria. The data extraction interface should take a similar input but deliver records rather than summaries. The processing interface should provide a hierarchical representation of individual level data in a way that's easy to work with programmatically.

Possible uses: 
* Create a high-speed tabulation tool on the command line or an API to serve up tabulations to a front-end user facing application.
* Build a fast, low-resource extract engine and server for user driven or automated IPUMS data testing 
*  Use the processing interface to make a rich, responsive  data browser and search tool.


### Goals

* Explore modern data engineering libraries. The primary tool will be DuckDB but Polars may prove useful as well. Data Fusion might end up in the mix. Currently I have working code with DuckDB for aggregation.
* Support "low metadata" environments (nothing but file names and any built-in schema on the data files) or normal metadata environments with extra variable and dataset level metadata including integrated variable category metadata. Identify which library features can run in which environment and expose these feature flags at run-time.
* Support the legacy fixed-width format. I have some working, performant code to do this. Primarily this support will be to allow conversion to Parquet or importing to DuckDB.
* Support CSV
* Multithreading support: In many cases processing IPUMS data is embarrassingly parallel so some effort should be given to threading in the library design. If it's hard, don't bother. Data Fusion is designed ground-up with concurrency in mind and Polars and DuckDB take advantage of multiple cores in places.

Experimenting with data libraries: DuckDB can interoperate with Polars: You can make a Polars data frame from an Arrow data frame returned by a DuckDB query; and you can query a Polars data frame with DuckDB SQL. Also, Polars has a (maybe nascent ) SQL module. Data Fusion has good SQL support and a data frame interface as well. So there are going to be quite a few combinations to try, balancing performance and flexibility.

### IPUMS Conventions assumed by Cimdea

You can count on a few things:

* A "dataset" will have one ore more record sets; if more than one there's a hierarchical relationship between all records and linking keys in a typical primary / foreign key arrangement. For instance census data usually has household records and person records; each person belongs to one household, and every household has zero or more people.
* A common set of column ("variable") names: IPUMS data "integrates" data across time to make the most comparable data collection possible. So while not all information is available in every dataset in a collection, if it  is, names of the columns will match and values will retain the same meaning across datasets. 
* Record types and the relationships between them are the same for all datasets in a collection.
* If there are weight variables they are the same across a collection.
* Data has one of three types: Integer, String or floating point. Most data is 64-bit integer; with modern compression and column oriented storage compression for variables with small ranges is just as good as using a mixture of integer sizes. Strings are UTF-8 in IPUMS column storage (parquet) but are Latin-1 in the legacy fixed-width format. In code they should always be UTF-8.
* There are conventional directory arrangements for data and metadata. Users outside IPUMS won't need to use the default but adopting some convention is quite useful. This is configurable.
* Datasets within a collection have conventional names: DATASET_COLLECTION.RecordType.Format. For instance the U.S. Census IPUMS data for 1950 files would be `us1950a_usa.H.parquet` and `us1950a_usa.P.parquet`.
* If metadata is available (it may not be) we can count on some variables being "categorical" with a defined set of values and labels for those values. We can also count on the variables being split into "source" and "integrated" types; "source" variables provide the raw materials for the integrated variables. For most functionality this detail isn't needed but can be very useful for documentation and auditing purposes.



