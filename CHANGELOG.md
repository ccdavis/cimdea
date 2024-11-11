# Change Log

This is the change log for cimdea. It lists changes chronologically from most
recent to least recent. Each cimdea version has its own heading below which
corresponds to a git tag in the repository. Some of the most recent changes may
not yet be released in a cimdea version, and they are listed under the "Not Yet
Released" heading at the top.

## Not Yet Released

* Added support for optional bounds in request_case_selections. Now `low_code` may
  be null to indicate that there is no lower bound, or similarly `high_code` may
  be null to indicate that there is no upper bound. At least one of the two attributes
  must not be null.

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
