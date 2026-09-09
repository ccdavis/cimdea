//! Shared, transport-agnostic Abacus service.
//!
//! This module holds the non-LLM core that turns a *friendly intent envelope* ([TabRequest]) into an
//! executed Abacus tabulation, plus the metadata discovery / search / documentation primitives around
//! it. It is the reuse layer behind two very different front ends:
//!
//! - [crate::nl_tabulation] (the `ask` CLI), where a Gemini model produces the envelope, and
//! - the MCP / HTTP servers ([crate::mcp], [crate::http_api]), where the *connecting* AI produces the
//!   envelope by calling tools — so there is no LLM in this crate's path at all.
//!
//! The envelope only carries intent (which variables, general vs. detailed, subpopulation filters,
//! category bins, unit of analysis). Product, datasets, and all byte-offset / width mechanics are
//! filled in here from metadata, so a caller can never get them wrong.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::conventions::Context;
use crate::input_schema_tabulation as ist;
use crate::ipums_metadata_model::{IpumsCategory, IpumsValue, IpumsVariable};
use crate::mderror::MdError;
use crate::request::AbacusRequest;
use crate::tabulate::{self, OutputColumn, Table, TableFormat, Tabulation};

/// How many value labels a variable may have before they are omitted from the prompt catalog.
///
/// The catalog inlines value labels only for variables with at most this many categories; larger
/// variables show just a count, and a focused follow-up (the `get_variables` tool, or `ask`'s second
/// refinement pass) resolves their codes on demand. This trims the bulk of the catalog tokens
/// (high-cardinality variables) while keeping common demographic variables (SEX, MARST, RACE, ...)
/// inline.
pub const DEFAULT_CATEGORY_CATALOG_MAX: usize = 12;

/// Domain knowledge about how IPUMS variables encode concepts. Shared between `ask`'s system prompt
/// and the MCP server's instructions so both steer a model the same way.
pub const IPUMS_CONVENTIONS: &str = r#"IPUMS conventions (domain knowledge about how these variables encode concepts — follow them when mapping the request to variables):
- Hispanic/Latino origin is an ETHNICITY, captured by the separate variable HISPAN — it is NOT a RACE category. Race and Hispanic ethnicity are independent dimensions (a person of any race may or may not be Hispanic). For "Hispanic"/"Latino", use HISPAN and select its Hispanic categories (i.e. exclude the "Not Hispanic" code), not a race variable. Use RACE (or a race-detail variable) only for racial categories such as White, Black, or Asian. If the user asks for both race and Hispanic origin, use both variables."#;

// ---------------------------------------------------------------------------
// The friendly intent envelope. Only the fields that carry intent are present;
// mechanical fields are filled later from metadata.
// ---------------------------------------------------------------------------

/// A tabulation request expressed as intent: which variables to tabulate (general or detailed), which
/// subpopulation filters to apply, and how to bin continuous variables.
#[derive(Debug, Default, Deserialize)]
pub struct TabRequest {
    /// Unit of analysis: "P" to count persons, "H" to count households. Defaults to "P".
    #[serde(default)]
    pub uoa: String,
    /// The variables to tabulate.
    #[serde(default)]
    pub request_variables: Vec<RequestVar>,
    /// Optional filters restricting which records are counted (AND-ed together).
    #[serde(default)]
    pub subpopulation: Vec<RequestVar>,
    /// Optional groupings, one entry per continuous variable being binned.
    #[serde(default)]
    pub category_bins: Vec<CategoryBinGroup>,
}

/// One variable in a request, used both for tabulation variables and subpopulation filters.
#[derive(Debug, Deserialize)]
pub struct RequestVar {
    #[serde(default)]
    pub variable_mnemonic: String,
    /// Accepted as an alias for `variable_mnemonic`.
    #[serde(default)]
    pub mnemonic: String,
    /// "G" for the general (simplified) categories, "" (or absent) for detailed.
    #[serde(default)]
    pub general_detailed_selection: String,
    #[serde(default)]
    pub case_selection: bool,
    #[serde(default)]
    pub request_case_selections: Vec<CaseSelection>,
}

impl RequestVar {
    /// The variable name, accepting either `variable_mnemonic` or `mnemonic`, trimmed and uppercased.
    pub fn name(&self) -> String {
        let raw = if !self.variable_mnemonic.is_empty() {
            &self.variable_mnemonic
        } else {
            &self.mnemonic
        };
        raw.trim().to_uppercase()
    }

    pub fn is_general(&self) -> bool {
        self.general_detailed_selection.eq_ignore_ascii_case("G")
    }
}

/// One subpopulation selection: keep records whose value is between `low_code` and `high_code`
/// inclusive. Either bound may be null (or a number, or a numeric string) for an open-ended range.
#[derive(Debug, Deserialize)]
pub struct CaseSelection {
    #[serde(default)]
    pub low_code: Option<serde_json::Value>,
    #[serde(default)]
    pub high_code: Option<serde_json::Value>,
}

/// A single bin grouping a continuous variable: codes between `low` and `high` map to result `code`
/// (labeled `value_label`). Omit a bound for an open-ended end.
#[derive(Debug, Clone, Deserialize)]
pub struct CategoryBinSpec {
    #[serde(default)]
    pub code: i64,
    #[serde(default)]
    pub value_label: String,
    #[serde(default)]
    pub low: Option<i64>,
    #[serde(default)]
    pub high: Option<i64>,
}

/// All the bins for one variable.
#[derive(Debug, Clone, Deserialize)]
pub struct CategoryBinGroup {
    #[serde(default)]
    pub variable: String,
    #[serde(default)]
    pub bins: Vec<CategoryBinSpec>,
}

/// Documentation for a single variable, drawn from metadata rather than from any model.
#[derive(Debug, Clone, Serialize)]
pub struct VariableDoc {
    pub name: String,
    pub label: Option<String>,
    pub record_type: String,
    /// Whether this variable was tabulated with its "general" (collapsed) categories. When true, the
    /// result codes are general groupings and the detailed `categories` below do not describe them.
    pub general: bool,
    /// (code, label) pairs for the variable's value labels.
    pub categories: Vec<(String, String)>,
}

// ---------------------------------------------------------------------------
// The service
// ---------------------------------------------------------------------------

/// A configured entry point for metadata and tabulation against one IPUMS product + data root.
pub struct Service {
    pub product: String,
    /// Path to the data root (containing `parquet/` and `layouts/`). `None` uses product defaults.
    pub data_root: Option<String>,
}

/// A dataset available under the data root, with its parsed sample year (if any).
#[derive(Debug, Clone, Serialize)]
pub struct DatasetInfo {
    pub name: String,
    pub year: Option<u32>,
}

/// A variable matched by [Service::search_variables].
#[derive(Debug, Clone, Serialize)]
pub struct VariableMatch {
    pub name: String,
    pub label: Option<String>,
    pub record_type: String,
    /// Whether the variable has a distinct general (collapsed) form.
    pub has_general: bool,
    /// How many value labels the variable carries (0 if none / continuous).
    pub n_categories: usize,
}

/// The result of a variable search: the matches plus an optional note guiding the next step.
#[derive(Debug, Clone, Serialize)]
pub struct SearchResult {
    pub matches: Vec<VariableMatch>,
    /// Guidance shown when the result is empty or weak (e.g. "no match; try a broader term, or this
    /// concept may not exist in this product — see ipums.org").
    pub note: Option<String>,
}

/// The result of a tabulation.
#[derive(Debug, Clone, Serialize)]
pub struct TabOutcome {
    /// The result rendered as text (a single number for a one-row result, else a labeled table).
    pub table_text: String,
    /// The tabulation tables as JSON (the same shape `abacus`/`ask` emit).
    pub table_json: serde_json::Value,
    /// Documentation for each tabulated / filtered variable.
    pub variable_docs: Vec<VariableDoc>,
    /// The repaired Abacus request that was executed, as pretty JSON.
    pub generated_request_json: String,
    /// Non-fatal issues encountered while repairing the request.
    pub warnings: Vec<String>,
}

impl Service {
    pub fn new(product: impl Into<String>, data_root: Option<String>) -> Self {
        Self {
            product: product.into(),
            data_root,
        }
    }

    /// List datasets available under the data root, each with its parsed year.
    pub fn list_datasets(&self) -> Result<Vec<DatasetInfo>, MdError> {
        let names = list_available_datasets(self.data_root.as_deref())?;
        Ok(names
            .into_iter()
            .map(|name| {
                let year = parse_year(&name);
                DatasetInfo { name, year }
            })
            .collect())
    }

    /// Load variable metadata for `datasets` into a context (parquet metadata preferred, layouts as a
    /// fallback).
    fn load(&self, datasets: &[String]) -> Result<Context, MdError> {
        load_catalog_context(&self.product, self.data_root.clone(), datasets)
    }

    /// Search the variables in `datasets` for `query` (case-insensitive substring over mnemonic,
    /// label, and value-label text), returning up to `max` matches plus guidance.
    pub fn search_variables(
        &self,
        datasets: &[String],
        query: &str,
        max: usize,
    ) -> Result<SearchResult, MdError> {
        let ctx = self.load(datasets)?;
        let variables = loaded_variables(&ctx)?;
        let needle = query.trim().to_lowercase();

        let mut matches: Vec<VariableMatch> = Vec::new();
        for var in variables {
            if needle.is_empty() || variable_matches(var, &needle) {
                matches.push(VariableMatch {
                    name: var.name.clone(),
                    label: var.label.clone(),
                    record_type: var.record_type.clone(),
                    has_general: general_divisor(var) > 1,
                    n_categories: var.categories.as_ref().map(|c| c.len()).unwrap_or(0),
                });
            }
        }
        // Stable order: exact name match first, then by name.
        let upper = needle.to_uppercase();
        matches.sort_by(|a, b| {
            let a_exact = a.name == upper;
            let b_exact = b.name == upper;
            b_exact.cmp(&a_exact).then_with(|| a.name.cmp(&b.name))
        });

        let total = matches.len();
        matches.truncate(max);

        let note = if total == 0 {
            Some(format!(
                "No variable in {} matched \"{}\". Try a broader or different term (e.g. a synonym), \
                 or the concept may not be available in this product — see https://www.ipums.org for \
                 the full variable documentation.",
                datasets.join(", "),
                query.trim()
            ))
        } else if total > max {
            Some(format!(
                "{total} variables matched; showing the first {max}. Narrow the query for fewer, \
                 then call get_variables for a chosen variable's full value codes."
            ))
        } else {
            None
        };

        Ok(SearchResult { matches, note })
    }

    /// Full (code, label) value lists for the named variables in `datasets`, for resolving the exact
    /// codes of a subpopulation filter or a category-bin grouping.
    pub fn get_variables(
        &self,
        datasets: &[String],
        names: &[String],
    ) -> Result<Vec<VariableDoc>, MdError> {
        let ctx = self.load(datasets)?;
        let variables = loaded_variables(&ctx)?;
        let by_name: HashMap<String, &IpumsVariable> = variables
            .iter()
            .map(|v| (v.name.to_uppercase(), v))
            .collect();
        let upper: Vec<String> = names.iter().map(|n| n.trim().to_uppercase()).collect();
        // No variable is general here: get_variables shows detailed value labels for code resolution.
        Ok(build_variable_docs(&upper, &by_name, &HashSet::new()))
    }

    /// The full catalog text for `datasets` (one line per variable, with inline value labels for
    /// small variables). Useful as a first-contact overview for compact products.
    pub fn catalog(&self, datasets: &[String], category_max: usize) -> Result<String, MdError> {
        let ctx = self.load(datasets)?;
        let variables = loaded_variables(&ctx)?;
        Ok(build_catalog(variables, category_max))
    }

    /// Validate + repair `req` against `datasets`' metadata, execute it, and return the table plus
    /// documentation. `detailed` forces detailed categories for every tabulation variable.
    pub fn tabulate(
        &self,
        req: &TabRequest,
        datasets: &[String],
        detailed: bool,
    ) -> Result<TabOutcome, MdError> {
        let ctx = self.load(datasets)?;
        let variables = loaded_variables(&ctx)?;
        let by_name: HashMap<String, &IpumsVariable> = variables
            .iter()
            .map(|v| (v.name.to_uppercase(), v))
            .collect();

        let mut warnings = Vec::new();
        let strict = build_strict_request(
            req,
            &self.product,
            &self.data_root,
            detailed,
            datasets,
            &by_name,
            &mut warnings,
        )?;

        // Collect the variables to document (tabulation order, then subpopulation).
        let mut doc_names: Vec<String> = strict
            .request_variables
            .iter()
            .map(|v| v.variable_mnemonic.clone())
            .collect();
        for v in &strict.subpopulation {
            if !doc_names.contains(&v.variable_mnemonic) {
                doc_names.push(v.variable_mnemonic.clone());
            }
        }
        let general_names: HashSet<String> = strict
            .request_variables
            .iter()
            .chain(strict.subpopulation.iter())
            .filter(|v| {
                matches!(
                    v.general_detailed_selection,
                    ist::GeneralDetailedSelection::General
                )
            })
            .map(|v| v.variable_mnemonic.clone())
            .collect();
        let mut variable_docs = build_variable_docs(&doc_names, &by_name, &general_names);
        apply_bin_labels(&mut variable_docs, req);

        let request_json = serde_json::to_string(&strict).map_err(|err| {
            MdError::Msg(format!(
                "could not serialize the generated Abacus request: {err}"
            ))
        })?;
        let pretty = serde_json::to_string_pretty(&strict).unwrap_or_else(|_| request_json.clone());

        let (exec_ctx, exec_request) = AbacusRequest::try_from_json(&request_json)?;
        let tabulation = tabulate::tabulate(&exec_ctx, exec_request)?;

        let table_text = match render_single_number(&tabulation, &variable_docs) {
            Some(single) => single,
            None => render_tables_with_labels(&tabulation, &variable_docs)?,
        };
        let table_json: serde_json::Value = {
            let s = tabulation.output(TableFormat::Json)?;
            serde_json::from_str(&s)
                .map_err(|err| MdError::Msg(format!("could not re-parse tabulation JSON: {err}")))?
        };

        Ok(TabOutcome {
            table_text,
            table_json,
            variable_docs,
            generated_request_json: pretty,
            warnings,
        })
    }
}

/// Whether `var` matches a lowercase `needle` by mnemonic, label, or any value-label text.
fn variable_matches(var: &IpumsVariable, needle: &str) -> bool {
    if var.name.to_lowercase().contains(needle) {
        return true;
    }
    if let Some(label) = &var.label {
        if label.to_lowercase().contains(needle) {
            return true;
        }
    }
    if let Some(cats) = &var.categories {
        if cats.iter().any(|c| c.label().to_lowercase().contains(needle)) {
            return true;
        }
    }
    false
}

// ---------------------------------------------------------------------------
// Metadata loading + dataset discovery
// ---------------------------------------------------------------------------

/// Load metadata for `datasets`, preferring parquet embedded metadata (labels, value labels, general
/// widths) and falling back to layout files (names + widths only). On the fallback a fresh context is
/// used so a partial parquet load doesn't double up the catalog.
pub fn load_catalog_context(
    product: &str,
    data_root: Option<String>,
    datasets: &[String],
) -> Result<Context, MdError> {
    let mut ctx = Context::from_ipums_collection_name(product, None, data_root.clone())?;
    let ds_refs: Vec<&str> = datasets.iter().map(|s| s.as_str()).collect();

    match ctx.load_metadata_for_datasets_from_parquet(&ds_refs) {
        Ok(()) => Ok(ctx),
        Err(_) => {
            let mut ctx = Context::from_ipums_collection_name(product, None, data_root)?;
            ctx.load_metadata_for_datasets(&ds_refs)?;
            Ok(ctx)
        }
    }
}

/// Borrow the loaded variable metadata from a context.
pub fn loaded_variables(ctx: &Context) -> Result<&[IpumsVariable], MdError> {
    ctx.settings
        .metadata
        .as_ref()
        .map(|md| md.variables_index.as_slice())
        .ok_or_else(|| MdError::MetadataError("no variable metadata loaded".to_string()))
}

/// List datasets available under the data root: each `parquet/<name>` that also has a
/// `layouts/<name>.layout.txt` (so it is executable). Sorted, de-duplicated.
pub fn list_available_datasets(data_root: Option<&str>) -> Result<Vec<String>, MdError> {
    let data_root = data_root.ok_or_else(|| {
        MdError::Msg(
            "no dataset was given and no data root is configured, so datasets cannot be \
             discovered; specify a dataset"
                .to_string(),
        )
    })?;
    let parquet_dir = Path::new(data_root).join("parquet");
    let layouts_dir = Path::new(data_root).join("layouts");

    let entries = std::fs::read_dir(&parquet_dir).map_err(|e| {
        MdError::Msg(format!(
            "could not list datasets in {}: {e}",
            parquet_dir.display()
        ))
    })?;

    let mut names = Vec::new();
    for entry in entries.flatten() {
        let path = entry.path();
        let name = match path.file_name().and_then(|n| n.to_str()) {
            Some(n) => n.trim_end_matches(".parquet").to_string(),
            None => continue,
        };
        if layouts_dir.join(format!("{name}.layout.txt")).is_file() {
            names.push(name);
        }
    }
    names.sort();
    names.dedup();

    if names.is_empty() {
        return Err(MdError::Msg(format!(
            "no usable datasets found under {data_root} (need parquet/<dataset> with a matching \
             layouts/<dataset>.layout.txt)"
        )));
    }
    Ok(names)
}

/// Extract a 4-digit year from a dataset name (e.g. "us1900m" -> 1900), if present.
pub fn parse_year(name: &str) -> Option<u32> {
    let bytes = name.as_bytes();
    let mut i = 0;
    while i + 4 <= bytes.len() {
        let window = &bytes[i..i + 4];
        let is_four_digits = window.iter().all(u8::is_ascii_digit);
        let bounded_left = i == 0 || !bytes[i - 1].is_ascii_digit();
        let bounded_right = i + 4 == bytes.len() || !bytes[i + 4].is_ascii_digit();
        if is_four_digits && bounded_left && bounded_right {
            return std::str::from_utf8(window)
                .ok()
                .and_then(|s| s.parse().ok());
        }
        i += 1;
    }
    None
}

// ---------------------------------------------------------------------------
// Catalog text
// ---------------------------------------------------------------------------

/// Render the variable catalog: one line per variable, with inline value labels for variables with at
/// most `category_max` categories. Sorted for determinism.
pub fn build_catalog(variables: &[IpumsVariable], category_max: usize) -> String {
    let mut lines = Vec::with_capacity(variables.len());
    for var in variables {
        let label = var.label.as_deref().unwrap_or("(no label)");
        let record_type = if general_divisor(var) > 1 {
            format!("{}; general", var.record_type)
        } else {
            var.record_type.clone()
        };
        let mut line = format!("{} — {} ({})", var.name, label, record_type);
        if let Some(cats) = &var.categories {
            if !cats.is_empty() && cats.len() <= category_max {
                let rendered = render_categories_inline(cats);
                line.push_str(&format!(" [{rendered}]"));
            } else if cats.len() > category_max {
                line.push_str(&format!(" [{} value labels]", cats.len()));
            }
        }
        lines.push(line);
    }
    lines.sort();
    lines.join("\n")
}

pub fn render_categories_inline(categories: &[IpumsCategory]) -> String {
    categories
        .iter()
        .map(|c| format!("{}={}", ipums_value_code(&c.value), c.label()))
        .collect::<Vec<_>>()
        .join("; ")
}

// ---------------------------------------------------------------------------
// Validation + repair into a strict Abacus request
// ---------------------------------------------------------------------------

/// Validate `req` against the loaded metadata and repair it into a strict [ist::AbacusRequest],
/// filling the mechanical fields (`extract_start`/`extract_width`/`mnemonic`) the caller can't know.
pub fn build_strict_request(
    req: &TabRequest,
    product: &str,
    data_root: &Option<String>,
    detailed: bool,
    datasets: &[String],
    by_name: &HashMap<String, &IpumsVariable>,
    warnings: &mut Vec<String>,
) -> Result<ist::AbacusRequest, MdError> {
    // Confirm every requested variable actually exists in the loaded metadata.
    let mut unknown = Vec::new();
    for v in req.request_variables.iter().chain(req.subpopulation.iter()) {
        let name = v.name();
        if name.is_empty() {
            return Err(MdError::LlmError(
                "the request has a variable with no mnemonic".to_string(),
            ));
        }
        if !by_name.contains_key(&name) {
            unknown.push(name);
        }
    }
    if !unknown.is_empty() {
        unknown.sort();
        unknown.dedup();
        return Err(MdError::MetadataError(format!(
            "the request referenced variable(s) not present in dataset(s) {}: {}",
            datasets.join(", "),
            unknown.join(", ")
        )));
    }

    // Tabulation variables honor the detailed override; subpopulation filters are ALWAYS detailed,
    // with filter codes expressed in general categories expanded to the detailed range they cover.
    let request_variables = req
        .request_variables
        .iter()
        .map(|v| build_request_variable(v, by_name, detailed))
        .collect::<Result<Vec<_>, _>>()?;
    let subpopulation = req
        .subpopulation
        .iter()
        .map(|v| build_filter_variable(v, by_name, warnings))
        .collect::<Result<Vec<_>, _>>()?;

    let mut category_bins = BTreeMap::new();
    for group in &req.category_bins {
        let key = group.variable.trim().to_uppercase();
        if key.is_empty() {
            continue;
        }
        let mut seen_codes: HashSet<i64> = HashSet::new();
        let mut converted = Vec::new();
        for b in &group.bins {
            if b.code < 0 {
                warnings.push(format!(
                    "{key}: dropped a category bin with a negative code ({}).",
                    b.code
                ));
                continue;
            }
            if !seen_codes.insert(b.code) {
                warnings.push(format!(
                    "{key}: dropped a category bin with a duplicate code ({}).",
                    b.code
                ));
                continue;
            }
            if let Some(bin) = convert_category_bin(b, &key, warnings) {
                converted.push(bin);
            }
        }
        category_bins.insert(key, converted);
    }

    let uoa = if req.uoa.trim().is_empty() {
        "P".to_string()
    } else {
        req.uoa.trim().to_uppercase()
    };

    let request_samples = datasets
        .iter()
        .map(|name| ist::RequestSample {
            name: name.clone(),
            custom_sampling_ratio: None,
            first_household_sampled: None,
        })
        .collect();

    Ok(ist::AbacusRequest {
        product: product.to_string(),
        data_root: data_root.clone(),
        uoa,
        output_format: "json".to_string(),
        subpopulation,
        category_bins,
        request_samples,
        request_variables,
    })
}

pub fn build_request_variable(
    v: &RequestVar,
    by_name: &HashMap<String, &IpumsVariable>,
    force_detailed: bool,
) -> Result<ist::RequestVariable, MdError> {
    let name = v.name();
    let md = by_name.get(&name).copied();

    // Honor a general selection only for variables that actually have a general form.
    let (selection, extract_width) = if v.is_general() && !force_detailed && has_general_form(md) {
        let general_width = md
            .and_then(|m| m.general_width)
            .expect("has_general_form guarantees a general width");
        (ist::GeneralDetailedSelection::General, general_width)
    } else {
        (ist::GeneralDetailedSelection::Detailed, detailed_width(md))
    };

    let request_case_selections = v
        .request_case_selections
        .iter()
        .map(convert_case_selection)
        .collect::<Result<Vec<_>, _>>()?;

    // A subpopulation entry filters when it has selections, even if the flag was omitted.
    let case_selection = v.case_selection || !request_case_selections.is_empty();

    Ok(ist::RequestVariable {
        variable_mnemonic: name.clone(),
        mnemonic: name,
        general_detailed_selection: selection,
        attached_variable_pointer: (),
        case_selection,
        request_case_selections,
        extract_start: 1,
        extract_width,
    })
}

/// Build one subpopulation filter entry.
///
/// Filters execute against the DETAILED codes stored in the data, so a filter expressed in general
/// category codes would silently match nothing (e.g. RACE general 2 "Black" vs. stored detailed
/// codes 200-299). Selections are expanded to the detailed range each general code covers
/// (general g -> [g*divisor, g*divisor + divisor - 1]) when the caller marked the filter "G", or —
/// with a warning — when the codes can only be read as general codes: they match none of the
/// variable's detailed category codes but all match general ones.
pub fn build_filter_variable(
    v: &RequestVar,
    by_name: &HashMap<String, &IpumsVariable>,
    warnings: &mut Vec<String>,
) -> Result<ist::RequestVariable, MdError> {
    let mut rv = build_request_variable(v, by_name, true)?;
    let Some(md) = by_name.get(&rv.variable_mnemonic).copied() else {
        return Ok(rv);
    };
    let divisor = general_divisor(md) as u64;
    if divisor <= 1 {
        return Ok(rv);
    }

    let detailed_codes = detailed_category_codes(md);
    let general_codes: HashSet<u64> = detailed_codes.iter().map(|c| c / divisor).collect();

    rv.request_case_selections = rv
        .request_case_selections
        .iter()
        .map(|cs| {
            let must_be_general = selection_must_be_general(cs, &detailed_codes, &general_codes);
            if !v.is_general() && !must_be_general {
                return *cs;
            }
            let expanded = expand_general_selection(cs, divisor);
            if must_be_general && !v.is_general() {
                warnings.push(format!(
                    "{name}: the filter code(s) {} match {name}'s general categories, not its \
                     detailed codes; expanded to the detailed range {}.",
                    describe_selection(cs),
                    describe_selection(&expanded),
                    name = rv.variable_mnemonic,
                ));
            }
            expanded
        })
        .collect();
    Ok(rv)
}

/// The variable's detailed category codes (integer categories only).
fn detailed_category_codes(var: &IpumsVariable) -> HashSet<u64> {
    var.categories
        .as_ref()
        .map(|cats| {
            cats.iter()
                .filter_map(|c| match c.value {
                    IpumsValue::Integer(v) if v >= 0 => Some(v as u64),
                    _ => None,
                })
                .collect()
        })
        .unwrap_or_default()
}

/// Whether every bound of `cs` can only be read as a general code: it matches no detailed category
/// code but does match a general one. With no known categories this can never be established.
fn selection_must_be_general(
    cs: &ist::RequestCaseSelection,
    detailed: &HashSet<u64>,
    general: &HashSet<u64>,
) -> bool {
    let (low, high) = match cs {
        ist::RequestCaseSelection::LessEqual(high) => (*high, *high),
        ist::RequestCaseSelection::GreaterEqual(low) => (*low, *low),
        ist::RequestCaseSelection::Between(low, high) => (*low, *high),
    };
    !detailed.is_empty()
        && [low, high]
            .iter()
            .all(|b| !detailed.contains(b) && general.contains(b))
}

/// Expand a selection over general codes to the detailed range it covers: general code g stands
/// for detailed codes [g*divisor, g*divisor + divisor - 1].
fn expand_general_selection(
    cs: &ist::RequestCaseSelection,
    divisor: u64,
) -> ist::RequestCaseSelection {
    let expand_low = |low: u64| low.saturating_mul(divisor);
    let expand_high = |high: u64| high.saturating_mul(divisor).saturating_add(divisor - 1);
    match cs {
        ist::RequestCaseSelection::LessEqual(high) => {
            ist::RequestCaseSelection::LessEqual(expand_high(*high))
        }
        ist::RequestCaseSelection::GreaterEqual(low) => {
            ist::RequestCaseSelection::GreaterEqual(expand_low(*low))
        }
        ist::RequestCaseSelection::Between(low, high) => {
            ist::RequestCaseSelection::Between(expand_low(*low), expand_high(*high))
        }
    }
}

/// A short human-readable form of a case selection, for warnings ("= 2", "200-299", ">= 65").
fn describe_selection(cs: &ist::RequestCaseSelection) -> String {
    match cs {
        ist::RequestCaseSelection::LessEqual(high) => format!("<= {high}"),
        ist::RequestCaseSelection::GreaterEqual(low) => format!(">= {low}"),
        ist::RequestCaseSelection::Between(low, high) if low == high => format!("= {low}"),
        ist::RequestCaseSelection::Between(low, high) => format!("{low}-{high}"),
    }
}

/// The detailed width from metadata, or 1 when unavailable (the value is unused for detailed
/// selections, so any placeholder is safe).
pub fn detailed_width(md: Option<&IpumsVariable>) -> usize {
    md.and_then(|m| m.formatting).map(|(_, w)| w).unwrap_or(1)
}

pub fn convert_case_selection(sel: &CaseSelection) -> Result<ist::RequestCaseSelection, MdError> {
    let low = value_to_code(sel.low_code.as_ref())?;
    let high = value_to_code(sel.high_code.as_ref())?;
    ist::RequestCaseSelection::try_new(low, high)
}

fn convert_category_bin(
    bin: &CategoryBinSpec,
    var: &str,
    warnings: &mut Vec<String>,
) -> Option<ist::CategoryBin> {
    let code = bin.code as u64;
    let label = bin.value_label.clone();
    match (bin.low, bin.high) {
        (Some(low), Some(high)) if high < low => {
            warnings.push(format!(
                "{var}: dropped a category bin with low {low} greater than high {high}."
            ));
            None
        }
        (Some(low), Some(high)) => Some(ist::CategoryBin::Range {
            low,
            high,
            code,
            label,
        }),
        (None, Some(high)) => Some(ist::CategoryBin::LessThan {
            value: high,
            code,
            label,
        }),
        (Some(low), None) => Some(ist::CategoryBin::MoreThan {
            value: low,
            code,
            label,
        }),
        (None, None) => {
            warnings.push(format!(
                "{var}: dropped a category bin with neither a low nor a high bound."
            ));
            None
        }
    }
}

/// Convert a JSON value (number, string, or null) into an optional `u64` code.
pub fn value_to_code(value: Option<&serde_json::Value>) -> Result<Option<u64>, MdError> {
    match value {
        None | Some(serde_json::Value::Null) => Ok(None),
        Some(serde_json::Value::Number(n)) => n.as_u64().map(Some).ok_or_else(|| {
            MdError::LlmError(format!(
                "case selection code {n} is not a non-negative integer"
            ))
        }),
        Some(serde_json::Value::String(s)) if s.trim().is_empty() => Ok(None),
        Some(serde_json::Value::String(s)) => s.trim().parse::<u64>().map(Some).map_err(|err| {
            MdError::LlmError(format!(
                "could not parse case selection code '{s}' as a non-negative integer: {err}"
            ))
        }),
        Some(other) => Err(MdError::LlmError(format!(
            "unexpected case selection code value: {other}"
        ))),
    }
}

// ---------------------------------------------------------------------------
// Documentation
// ---------------------------------------------------------------------------

/// Replace a binned variable's documented categories with its bin labels (code -> "0-9", "10-19",
/// ...), so the result table and legend describe the bins rather than the raw value codes.
pub fn apply_bin_labels(docs: &mut [VariableDoc], req: &TabRequest) {
    for group in &req.category_bins {
        let var = group.variable.trim().to_uppercase();
        let bins: Vec<(String, String)> = group
            .bins
            .iter()
            .map(|b| (b.code.to_string(), b.value_label.clone()))
            .collect();
        if bins.is_empty() {
            continue;
        }
        if let Some(doc) = docs.iter_mut().find(|d| d.name.to_uppercase() == var) {
            doc.categories = bins;
            doc.general = false; // bins are explicit groupings, not general categories
        }
    }
}

pub fn build_variable_docs(
    names: &[String],
    by_name: &HashMap<String, &IpumsVariable>,
    general_names: &HashSet<String>,
) -> Vec<VariableDoc> {
    names
        .iter()
        .filter_map(|name| by_name.get(name).copied().map(|var| (name, var)))
        .map(|(name, var)| {
            let general = general_names.contains(name);
            let categories = if general {
                general_categories(var)
            } else {
                detailed_categories(var)
            };
            VariableDoc {
                name: var.name.clone(),
                label: var.label.clone(),
                record_type: var.record_type.clone(),
                general,
                categories,
            }
        })
        .collect()
}

/// The detailed value labels as (code, label) pairs.
pub fn detailed_categories(var: &IpumsVariable) -> Vec<(String, String)> {
    var.categories
        .as_ref()
        .map(|cats| {
            cats.iter()
                .map(|c| (ipums_value_code(&c.value), c.label().to_string()))
                .collect()
        })
        .unwrap_or_default()
}

/// The integer divisor that collapses detailed codes into general codes:
/// `10^(detailed_width - general_width)`, or 1 when there is no distinct general width.
pub fn general_divisor(var: &IpumsVariable) -> usize {
    match (var.formatting, var.general_width) {
        (Some((_, w)), Some(gw)) if gw < w => 10usize.pow((w - gw) as u32),
        _ => 1,
    }
}

/// Whether a variable actually has a distinct general form (a general width strictly narrower than
/// the detailed width, equivalently a divisor `> 1`).
pub fn has_general_form(md: Option<&IpumsVariable>) -> bool {
    md.map(|v| general_divisor(v) > 1).unwrap_or(false)
}

/// Derive general (collapsed) category labels from the detailed value labels using the "first label
/// rule": group detailed codes by their general code (`code / divisor`) and take the label of the
/// smallest detailed code in each group. Returns (general_code, label) pairs sorted by code.
pub fn general_categories(var: &IpumsVariable) -> Vec<(String, String)> {
    let divisor = general_divisor(var) as i64;
    let cats = match &var.categories {
        Some(c) => c,
        None => return Vec::new(),
    };
    let mut groups: BTreeMap<i64, (i64, String)> = BTreeMap::new();
    for c in cats {
        if let IpumsValue::Integer(v) = c.value {
            let entry = groups
                .entry(v / divisor)
                .or_insert((i64::MAX, String::new()));
            if v < entry.0 {
                *entry = (v, c.label().to_string());
            }
        }
    }
    groups
        .into_iter()
        .map(|(general, (_, label))| (general.to_string(), label))
        .collect()
}

pub fn ipums_value_code(value: &IpumsValue) -> String {
    match value {
        IpumsValue::Integer(i) => i.to_string(),
        IpumsValue::Float(s) => s.clone(),
        IpumsValue::String { value, .. } => String::from_utf8_lossy(value).to_string(),
        IpumsValue::Fixed { base, .. } => base.to_string(),
    }
}

// ---------------------------------------------------------------------------
// Result rendering (shared by the service and by `ask`'s NlResult)
// ---------------------------------------------------------------------------

/// Group an integer string with thousands separators ("1234567" -> "1,234,567"). Non-integer input
/// is returned unchanged.
pub fn group_thousands(s: &str) -> String {
    let trimmed = s.trim();
    let (sign, digits) = match trimmed.strip_prefix('-') {
        Some(rest) => ("-", rest),
        None => ("", trimmed),
    };
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return s.to_string();
    }
    let mut grouped = String::with_capacity(digits.len() + digits.len() / 3);
    let n = digits.len();
    for (i, ch) in digits.chars().enumerate() {
        if i > 0 && (n - i) % 3 == 0 {
            grouped.push(',');
        }
        grouped.push(ch);
    }
    format!("{sign}{grouped}")
}

/// Build code -> label maps from documented variables' categories (keyed by the codes that appear in
/// the result — detailed, or derived general labels for general selections).
pub fn label_maps_from_docs(docs: &[VariableDoc]) -> HashMap<&str, HashMap<&str, &str>> {
    let mut label_maps: HashMap<&str, HashMap<&str, &str>> = HashMap::new();
    for doc in docs {
        if doc.categories.is_empty() {
            continue;
        }
        let map = doc
            .categories
            .iter()
            .map(|(code, label)| (code.as_str(), label.as_str()))
            .collect();
        label_maps.insert(doc.name.as_str(), map);
    }
    label_maps
}

/// If the result is a single value (one table, one row), render it as a number rather than a table.
/// Returns `None` otherwise.
pub fn render_single_number(tab: &Tabulation, docs: &[VariableDoc]) -> Option<String> {
    if tab.0.len() != 1 || tab.0[0].rows.len() != 1 {
        return None;
    }
    let table = &tab.0[0];
    let row = &table.rows[0];
    if row.len() < 2 {
        return None;
    }
    let unweighted = group_thousands(&row[0]);
    let weighted = group_thousands(&row[1]);

    let label_maps = label_maps_from_docs(docs);
    let mut parts = Vec::new();
    for (i, col) in table.heading.iter().enumerate() {
        if i < 2 {
            continue; // columns 0,1 are ct and weighted_ct
        }
        if let OutputColumn::RequestVar(v) = col {
            let code = row.get(i).map(String::as_str).unwrap_or("");
            match label_maps.get(v.name.as_str()).and_then(|m| m.get(code)) {
                Some(label) => parts.push(format!("{} = {} ({})", v.name, code, label)),
                None => parts.push(format!("{} = {}", v.name, code)),
            }
        }
    }
    let head = if parts.is_empty() {
        String::new()
    } else {
        format!("{}: ", parts.join(", "))
    };
    Some(format!(
        "{head}weighted estimate **{weighted}** (unweighted sample: {unweighted}).\n"
    ))
}

/// Render every table as text, inserting a `<VAR>_label` column after each coded variable column that
/// has labels. Raw codes are always kept; the labels are an additional column.
pub fn render_tables_with_labels(
    tab: &Tabulation,
    docs: &[VariableDoc],
) -> Result<String, MdError> {
    let label_maps = label_maps_from_docs(docs);
    let mut out = String::new();
    for table in &tab.0 {
        out.push_str(&format_table_with_labels(table, &label_maps)?);
        out.push('\n');
    }
    Ok(out)
}

/// Render a single result table as text, inserting a left-aligned `<VAR>_label` column after each
/// coded variable column that has an entry in `label_maps`.
fn format_table_with_labels(
    table: &Table,
    label_maps: &HashMap<&str, HashMap<&str, &str>>,
) -> Result<String, MdError> {
    let mut headers: Vec<String> = Vec::new();
    let mut align_left: Vec<bool> = Vec::new();
    let mut label_after: Vec<Option<&HashMap<&str, &str>>> = Vec::new();

    for col in &table.heading {
        headers.push(col.name());
        align_left.push(false);
        let mut lm = None;
        if let OutputColumn::RequestVar(v) = col {
            if let Some(map) = label_maps.get(v.name.as_str()) {
                lm = Some(map);
                headers.push(format!("{}_label", v.name));
                align_left.push(true);
            }
        }
        label_after.push(lm);
    }

    let mut display_rows: Vec<Vec<String>> = Vec::with_capacity(table.rows.len());
    for row in &table.rows {
        let mut new_row: Vec<String> = Vec::with_capacity(headers.len());
        for (i, cell) in row.iter().enumerate() {
            new_row.push(cell.clone());
            if let Some(Some(map)) = label_after.get(i) {
                let label = map.get(cell.as_str()).copied().unwrap_or("");
                new_row.push(label.to_string());
            }
        }
        display_rows.push(new_row);
    }

    let ncol = headers.len();
    let mut widths: Vec<usize> = headers.iter().map(|h| h.len()).collect();
    for r in &display_rows {
        for (i, c) in r.iter().enumerate() {
            if i < ncol && c.len() > widths[i] {
                widths[i] = c.len();
            }
        }
    }

    let render_cell = |buf: &mut String, value: &str, width: usize, left: bool| {
        if left {
            buf.push_str(&format!("| {value:<width$} "));
        } else {
            buf.push_str(&format!("| {value:>width$} "));
        }
    };

    let mut out = String::new();
    for (i, h) in headers.iter().enumerate() {
        render_cell(&mut out, h, widths[i], align_left[i]);
    }
    out.push_str("|\n");
    let total: usize = 1 + 3 * ncol + widths.iter().sum::<usize>();
    out.push_str(&format!("|{}|\n", "-".repeat(total.saturating_sub(2))));
    for r in &display_rows {
        for (i, c) in r.iter().enumerate() {
            render_cell(&mut out, c, widths[i], align_left[i]);
        }
        out.push_str("|\n");
    }
    Ok(out)
}

/// Strip a leading ```/```json fence and trailing ``` from a model response, if present.
pub fn strip_json_fences(s: &str) -> String {
    let t = s.trim();
    if let Some(after_open) = t.strip_prefix("```") {
        let body = match after_open.find('\n') {
            Some(i) => &after_open[i + 1..],
            None => "",
        };
        let body = match body.rfind("```") {
            Some(i) => &body[..i],
            None => body,
        };
        body.trim().to_string()
    } else {
        t.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ipums_metadata_model::{IpumsDataType, UniversalCategoryType};

    fn var_with_n_categories(name: &str, n: usize) -> IpumsVariable {
        let cats = (0..n)
            .map(|i| {
                IpumsCategory::new(
                    &format!("label {i}"),
                    UniversalCategoryType::Value,
                    IpumsValue::Integer(i as i64),
                )
            })
            .collect();
        IpumsVariable {
            name: name.to_string(),
            data_type: Some(IpumsDataType::Integer),
            label: Some(format!("{name} label")),
            record_type: "P".to_string(),
            categories: Some(cats),
            formatting: Some((1, 3)),
            general_width: None,
            description: None,
            category_bins: None,
            id: 0,
        }
    }

    fn general_request(name: &str) -> RequestVar {
        RequestVar {
            variable_mnemonic: name.to_string(),
            mnemonic: String::new(),
            general_detailed_selection: "G".to_string(),
            case_selection: false,
            request_case_selections: vec![],
        }
    }

    /// A RACE-like variable: detailed 3-digit codes, general 1-digit codes (divisor 100).
    fn race_like_var() -> IpumsVariable {
        let cats = [
            (100, "White"),
            (200, "Black"),
            (210, "Mulatto"),
            (300, "American Indian"),
        ]
        .iter()
        .map(|(code, label)| {
            IpumsCategory::new(label, UniversalCategoryType::Value, IpumsValue::Integer(*code))
        })
        .collect();
        IpumsVariable {
            name: "RACE".to_string(),
            data_type: Some(IpumsDataType::Integer),
            label: Some("Race".to_string()),
            record_type: "P".to_string(),
            categories: Some(cats),
            formatting: Some((1, 3)),
            general_width: Some(1),
            description: None,
            category_bins: None,
            id: 0,
        }
    }

    fn filter_request(
        name: &str,
        selection: &str,
        low: Option<&str>,
        high: Option<&str>,
    ) -> RequestVar {
        RequestVar {
            variable_mnemonic: name.to_string(),
            mnemonic: String::new(),
            general_detailed_selection: selection.to_string(),
            case_selection: true,
            request_case_selections: vec![CaseSelection {
                low_code: low.map(|v| serde_json::json!(v)),
                high_code: high.map(|v| serde_json::json!(v)),
            }],
        }
    }

    #[test]
    fn test_strip_json_fences_plain() {
        assert_eq!(strip_json_fences("{\"a\":1}"), "{\"a\":1}");
    }

    #[test]
    fn test_strip_json_fences_with_language_tag() {
        let fenced = "```json\n{\"a\":1}\n```";
        assert_eq!(strip_json_fences(fenced), "{\"a\":1}");
    }

    #[test]
    fn test_value_to_code_accepts_number_and_string() {
        assert_eq!(
            value_to_code(Some(&serde_json::json!(2))).unwrap(),
            Some(2u64)
        );
        assert_eq!(
            value_to_code(Some(&serde_json::json!("060"))).unwrap(),
            Some(60u64)
        );
        assert_eq!(value_to_code(Some(&serde_json::Value::Null)).unwrap(), None);
        assert_eq!(value_to_code(None).unwrap(), None);
    }

    #[test]
    fn test_general_categories_first_label_rule() {
        let cats = vec![
            IpumsCategory::new(
                "Head/householder",
                UniversalCategoryType::Value,
                IpumsValue::Integer(101),
            ),
            IpumsCategory::new("Spouse", UniversalCategoryType::Value, IpumsValue::Integer(201)),
            IpumsCategory::new(
                "2nd/3rd wife",
                UniversalCategoryType::Value,
                IpumsValue::Integer(202),
            ),
            IpumsCategory::new("Child", UniversalCategoryType::Value, IpumsValue::Integer(301)),
            IpumsCategory::new(
                "Adopted child",
                UniversalCategoryType::Value,
                IpumsValue::Integer(302),
            ),
        ];
        let var = IpumsVariable {
            name: "RELATE".to_string(),
            data_type: Some(IpumsDataType::Integer),
            label: Some("Relationship to household head".to_string()),
            record_type: "P".to_string(),
            categories: Some(cats),
            formatting: Some((1, 4)),
            general_width: Some(2),
            description: None,
            category_bins: None,
            id: 0,
        };

        assert_eq!(general_divisor(&var), 100);
        let general = general_categories(&var);
        assert_eq!(
            general,
            vec![
                ("1".to_string(), "Head/householder".to_string()),
                ("2".to_string(), "Spouse".to_string()),
                ("3".to_string(), "Child".to_string()),
            ]
        );
    }

    #[test]
    fn test_general_divisor_no_distinct_general_width() {
        let mut var = var_with_n_categories("X", 3);
        var.formatting = Some((1, 3));
        var.general_width = Some(3);
        assert_eq!(general_divisor(&var), 1);
        var.general_width = None;
        assert_eq!(general_divisor(&var), 1);
    }

    #[test]
    fn test_parse_year() {
        assert_eq!(parse_year("us1900m"), Some(1900));
        assert_eq!(parse_year("us2019b"), Some(2019));
        assert_eq!(parse_year("original_us2015a"), Some(2015));
        assert_eq!(parse_year("cps2020"), Some(2020));
        assert_eq!(parse_year("nodigits"), None);
    }

    #[test]
    fn test_group_thousands() {
        assert_eq!(group_thousands("1234567"), "1,234,567");
        assert_eq!(group_thousands("100"), "100");
        assert_eq!(group_thousands("1000"), "1,000");
        assert_eq!(group_thousands("0"), "0");
        assert_eq!(group_thousands("-12345"), "-12,345");
        assert_eq!(group_thousands("not-a-number"), "not-a-number");
    }

    #[test]
    fn test_catalog_marks_only_general_variables() {
        let mut edu = var_with_n_categories("EDUC", 5);
        edu.formatting = Some((1, 3));
        edu.general_width = Some(2);
        let mut sex = var_with_n_categories("SEX", 2);
        sex.formatting = Some((1, 1));
        sex.general_width = Some(1);

        let catalog = build_catalog(&[edu, sex], 25);
        let edu_line = catalog.lines().find(|l| l.starts_with("EDUC")).unwrap();
        let sex_line = catalog.lines().find(|l| l.starts_with("SEX")).unwrap();
        assert!(edu_line.contains("; general"), "EDUC should be marked general: {edu_line}");
        assert!(!sex_line.contains("; general"), "SEX should not be marked general: {sex_line}");
    }

    #[test]
    fn test_catalog_compact_omits_value_labels() {
        let marst = var_with_n_categories("MARST", 6);
        let catalog = build_catalog(std::slice::from_ref(&marst), 0);
        let line = catalog.lines().find(|l| l.starts_with("MARST")).unwrap();
        assert!(line.contains("value labels"), "should show a count hint: {line}");
        assert!(!line.contains("label 0"), "should NOT inline the actual labels: {line}");
    }

    #[test]
    fn test_force_detailed_overrides_general_selection() {
        let mut var = var_with_n_categories("EDUC", 40);
        var.formatting = Some((1, 3));
        var.general_width = Some(2);
        let mut by_name: HashMap<String, &IpumsVariable> = HashMap::new();
        by_name.insert("EDUC".to_string(), &var);

        let g = general_request("EDUC");

        let general = build_request_variable(&g, &by_name, false).unwrap();
        assert!(
            matches!(general.general_detailed_selection, ist::GeneralDetailedSelection::General),
            "without the override, a 'G' request should stay general"
        );

        let detailed = build_request_variable(&g, &by_name, true).unwrap();
        assert!(
            matches!(detailed.general_detailed_selection, ist::GeneralDetailedSelection::Detailed),
            "force_detailed should make the tabulation variable detailed"
        );
    }

    #[test]
    fn test_general_request_downgrades_without_general_form() {
        let mut var = var_with_n_categories("MARST", 6);
        var.formatting = Some((1, 1));
        var.general_width = Some(1);
        assert!(!has_general_form(Some(&var)));
        let mut by_name: HashMap<String, &IpumsVariable> = HashMap::new();
        by_name.insert("MARST".to_string(), &var);

        let rv = build_request_variable(&general_request("MARST"), &by_name, false).unwrap();
        assert!(
            matches!(rv.general_detailed_selection, ist::GeneralDetailedSelection::Detailed),
            "a 'G' request on a variable with no general form should become detailed"
        );
    }

    #[test]
    fn test_filter_general_code_auto_expands_to_detailed_range() {
        // The model claimed a detailed filter but RACE=2 can only be a general code (no detailed
        // category is 2; general codes are 1, 2, 3). It must expand to the stored detailed range.
        let race = race_like_var();
        let mut by_name: HashMap<String, &IpumsVariable> = HashMap::new();
        by_name.insert("RACE".to_string(), &race);
        let mut warnings = Vec::new();

        let rv = build_filter_variable(
            &filter_request("RACE", "", Some("2"), Some("2")),
            &by_name,
            &mut warnings,
        )
        .unwrap();

        assert_eq!(
            rv.request_case_selections,
            vec![ist::RequestCaseSelection::Between(200, 299)]
        );
        assert_eq!(warnings.len(), 1, "the repair should be surfaced: {warnings:?}");
        assert!(warnings[0].contains("RACE"), "warning names the variable: {}", warnings[0]);
    }

    #[test]
    fn test_filter_marked_general_expands_without_warning() {
        let race = race_like_var();
        let mut by_name: HashMap<String, &IpumsVariable> = HashMap::new();
        by_name.insert("RACE".to_string(), &race);
        let mut warnings = Vec::new();

        // Closed range of general codes 2-3 covers detailed 200-399.
        let rv = build_filter_variable(
            &filter_request("RACE", "G", Some("2"), Some("3")),
            &by_name,
            &mut warnings,
        )
        .unwrap();
        assert_eq!(
            rv.request_case_selections,
            vec![ist::RequestCaseSelection::Between(200, 399)]
        );
        // The filter itself always executes against detailed codes.
        assert!(matches!(
            rv.general_detailed_selection,
            ist::GeneralDetailedSelection::Detailed
        ));

        // Open-ended bounds expand on the correct side: >= 2 -> >= 200; <= 2 -> <= 299.
        let ge = build_filter_variable(
            &filter_request("RACE", "G", Some("2"), None),
            &by_name,
            &mut warnings,
        )
        .unwrap();
        assert_eq!(
            ge.request_case_selections,
            vec![ist::RequestCaseSelection::GreaterEqual(200)]
        );
        let le = build_filter_variable(
            &filter_request("RACE", "G", None, Some("2")),
            &by_name,
            &mut warnings,
        )
        .unwrap();
        assert_eq!(
            le.request_case_selections,
            vec![ist::RequestCaseSelection::LessEqual(299)]
        );

        assert!(warnings.is_empty(), "an explicit 'G' filter is not a repair: {warnings:?}");
    }

    #[test]
    fn test_filter_detailed_codes_pass_through() {
        let race = race_like_var();
        let mut by_name: HashMap<String, &IpumsVariable> = HashMap::new();
        by_name.insert("RACE".to_string(), &race);
        let mut warnings = Vec::new();

        // Valid detailed codes are untouched.
        let rv = build_filter_variable(
            &filter_request("RACE", "", Some("200"), Some("210")),
            &by_name,
            &mut warnings,
        )
        .unwrap();
        assert_eq!(
            rv.request_case_selections,
            vec![ist::RequestCaseSelection::Between(200, 210)]
        );

        // A range bound that is neither a detailed nor a general category (299) is left alone too.
        let rv = build_filter_variable(
            &filter_request("RACE", "", Some("200"), Some("299")),
            &by_name,
            &mut warnings,
        )
        .unwrap();
        assert_eq!(
            rv.request_case_selections,
            vec![ist::RequestCaseSelection::Between(200, 299)]
        );
        assert!(warnings.is_empty(), "{warnings:?}");
    }

    #[test]
    fn test_filter_without_general_form_untouched() {
        // A variable with no general form (divisor 1) never has its filter codes rewritten.
        let sex = var_with_n_categories("SEX", 3);
        let mut by_name: HashMap<String, &IpumsVariable> = HashMap::new();
        by_name.insert("SEX".to_string(), &sex);
        let mut warnings = Vec::new();

        let rv = build_filter_variable(
            &filter_request("SEX", "", Some("2"), Some("2")),
            &by_name,
            &mut warnings,
        )
        .unwrap();
        assert_eq!(
            rv.request_case_selections,
            vec![ist::RequestCaseSelection::Between(2, 2)]
        );
        assert!(warnings.is_empty(), "{warnings:?}");
    }

    #[test]
    fn test_tab_request_parses_minimal() {
        let json = r#"{
            "uoa": "P",
            "request_variables": [{"variable_mnemonic": "MARST", "general_detailed_selection": ""}]
        }"#;
        let req: TabRequest = serde_json::from_str(json).expect("should parse");
        assert_eq!(req.request_variables.len(), 1);
        assert_eq!(req.request_variables[0].name(), "MARST");
        assert!(!req.request_variables[0].is_general());
    }
}
