//! Turn an English description of a tabulation into an executed Abacus tabulation.
//!
//! The pipeline is:
//! 1. Load variable metadata (labels + value labels) for the requested dataset(s), preferring the
//!    embedded metadata in the parquet files and falling back to layout files.
//! 2. Build a prompt: a schema description, a one-shot example, and a catalog of the real variables
//!    (with value labels) available in the data.
//! 3. Ask an [LlmProvider](crate::llm::LlmProvider) to translate the user's English into a JSON
//!    response envelope whose `abacus_request` field holds the tabulation request.
//! 4. Validate and repair that request against the metadata — confirm the variables exist and fill
//!    in the mechanical fields (`extract_start`/`extract_width`/`mnemonic`) the model can't know.
//! 5. Execute it through the normal [crate::request::AbacusRequest] / [crate::tabulate] path.
//! 6. Assemble an explanation that pairs the model's prose with variable/value-label documentation
//!    pulled straight from the metadata (so the facts come from the data, not the model).

use std::collections::{BTreeMap, HashMap, HashSet};

use serde::Deserialize;

use crate::conventions::Context;
use crate::input_schema_tabulation as ist;
use crate::ipums_metadata_model::{IpumsCategory, IpumsValue, IpumsVariable};
use crate::llm::LlmProvider;
use crate::mderror::MdError;
use crate::request::AbacusRequest;
use crate::tabulate::{self, OutputColumn, Table, TableFormat, Tabulation};

/// How many value labels a variable may have before we omit them from the prompt catalog (to keep
/// the prompt from ballooning on continuous variables with thousands of distinct codes).
const DEFAULT_CATEGORY_CATALOG_MAX: usize = 25;

/// Inputs needed to translate and run a natural-language tabulation request.
pub struct NlConfig {
    /// IPUMS product/collection, e.g. "usa".
    pub product: String,
    /// Path to the data root (containing `parquet/` and `layouts/`). `None` uses product defaults.
    pub data_root: Option<String>,
    /// Dataset(s) whose metadata is offered to the model and which the tabulation runs against.
    pub datasets: Vec<String>,
    /// Max value labels to inline per variable in the catalog. `None` uses the default.
    pub category_catalog_max: Option<usize>,
}

/// The result of a natural-language tabulation: the model's interpretation, supporting variable
/// documentation, and (for tabulation requests) the computed table.
pub struct NlResult {
    /// "tabulation" or "microdata_extract" as classified by the model.
    pub request_kind: String,
    /// The model id that produced the request.
    pub model: String,
    /// The model's plain-English description of what the tabulation does.
    pub explanation: String,
    /// Assumptions/ambiguities the model noted.
    pub assumptions: String,
    /// Non-fatal issues encountered while repairing the request.
    pub warnings: Vec<String>,
    /// Documentation (label + value labels) for each tabulated/subpopulation variable.
    pub variable_docs: Vec<VariableDoc>,
    /// The repaired Abacus request as pretty JSON (for inspection / `--show-request`).
    pub generated_request_json: Option<String>,
    /// The computed tabulation, if this was an executable tabulation request.
    pub tabulation: Option<Tabulation>,
}

/// Documentation for a single variable, drawn from metadata rather than the model.
pub struct VariableDoc {
    pub name: String,
    pub label: Option<String>,
    pub record_type: String,
    /// Whether this variable was tabulated with its "general" (collapsed) categories. When true,
    /// the result codes are general groupings and the detailed `categories` below do not describe
    /// them (the metadata carries only detailed value labels).
    pub general: bool,
    /// (code, label) pairs for the variable's detailed value labels.
    pub categories: Vec<(String, String)>,
}

// ---------------------------------------------------------------------------
// The lenient shape we parse out of the model. The model only fills the fields
// that carry intent; mechanical fields are filled later from metadata.
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct LlmTabulationResponse {
    #[serde(default)]
    request_kind: String,
    #[serde(default)]
    abacus_request: Option<LlmAbacusRequest>,
    #[serde(default)]
    explanation: String,
    #[serde(default)]
    assumptions: String,
}

#[derive(Debug, Default, Deserialize)]
struct LlmAbacusRequest {
    #[serde(default)]
    uoa: String,
    #[serde(default)]
    request_variables: Vec<LlmRequestVariable>,
    #[serde(default)]
    subpopulation: Vec<LlmRequestVariable>,
    #[serde(default)]
    category_bins: BTreeMap<String, Vec<LlmCategoryBin>>,
}

#[derive(Debug, Deserialize)]
struct LlmRequestVariable {
    #[serde(default)]
    variable_mnemonic: String,
    #[serde(default)]
    mnemonic: String,
    #[serde(default)]
    general_detailed_selection: String,
    #[serde(default)]
    case_selection: bool,
    #[serde(default)]
    request_case_selections: Vec<LlmCaseSelection>,
}

impl LlmRequestVariable {
    /// The variable name, accepting either `variable_mnemonic` or `mnemonic`, uppercased.
    fn name(&self) -> String {
        let raw = if !self.variable_mnemonic.is_empty() {
            &self.variable_mnemonic
        } else {
            &self.mnemonic
        };
        raw.trim().to_uppercase()
    }

    fn is_general(&self) -> bool {
        self.general_detailed_selection.eq_ignore_ascii_case("G")
    }
}

#[derive(Debug, Deserialize)]
struct LlmCaseSelection {
    #[serde(default)]
    low_code: Option<serde_json::Value>,
    #[serde(default)]
    high_code: Option<serde_json::Value>,
}

#[derive(Debug, Deserialize)]
struct LlmCategoryBin {
    #[serde(default)]
    code: i64,
    #[serde(default)]
    value_label: String,
    #[serde(default)]
    low: Option<i64>,
    #[serde(default)]
    high: Option<i64>,
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

/// Translate `prompt` into a tabulation, run it, and return the result with documentation.
pub fn run(
    provider: &dyn LlmProvider,
    prompt: &str,
    cfg: &NlConfig,
) -> Result<NlResult, MdError> {
    if cfg.datasets.is_empty() {
        return Err(MdError::Msg(
            "at least one dataset is required to load metadata and tabulate".to_string(),
        ));
    }

    // 1. Load metadata for the catalog and for value-label documentation.
    let ctx = load_catalog_context(cfg)?;
    let variables = loaded_variables(&ctx)?;
    let by_name: HashMap<String, &IpumsVariable> = variables
        .iter()
        .map(|v| (v.name.to_uppercase(), v))
        .collect();

    // 2. Build the prompt and ask the model.
    let cat_max = cfg.category_catalog_max.unwrap_or(DEFAULT_CATEGORY_CATALOG_MAX);
    let catalog = build_catalog(variables, cat_max);
    let user_content = build_user_content(&cfg.product, &cfg.datasets, &catalog, prompt);

    let raw = provider.complete_json(SYSTEM_PROMPT, &user_content)?;
    let cleaned = strip_json_fences(&raw);
    let envelope: LlmTabulationResponse = serde_json::from_str(&cleaned).map_err(|err| {
        MdError::LlmError(format!(
            "could not parse the model's response as the expected JSON envelope ({err}); \
             response was: {cleaned}"
        ))
    })?;

    let request_kind = if envelope.request_kind.is_empty() {
        "tabulation".to_string()
    } else {
        envelope.request_kind.clone()
    };

    // A microdata extract is recognized but not executable yet (Phase 3).
    if request_kind != "tabulation" {
        return Ok(NlResult {
            request_kind,
            model: provider.model_name().to_string(),
            explanation: envelope.explanation,
            assumptions: envelope.assumptions,
            warnings: vec![
                "This request asks for a microdata extract, which is not implemented yet; \
                 no table was produced."
                    .to_string(),
            ],
            variable_docs: Vec::new(),
            generated_request_json: None,
            tabulation: None,
        });
    }

    let llm_request = envelope.abacus_request.ok_or_else(|| {
        MdError::LlmError(
            "the model classified this as a tabulation but did not provide an abacus_request"
                .to_string(),
        )
    })?;

    // 3. Validate + repair into a strict request, filling mechanical fields from metadata.
    let mut warnings = Vec::new();
    let strict = build_strict_request(&llm_request, cfg, &by_name, &mut warnings)?;

    // Collect the variables to document (in tabulation order, then subpopulation).
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
        .filter(|v| matches!(v.general_detailed_selection, ist::GeneralDetailedSelection::General))
        .map(|v| v.variable_mnemonic.clone())
        .collect();
    let variable_docs = build_variable_docs(&doc_names, &by_name, &general_names);

    // 4. Serialize and run through the normal Abacus path (which loads layout metadata itself).
    let request_json = serde_json::to_string(&strict).map_err(|err| {
        MdError::Msg(format!("could not serialize the generated Abacus request: {err}"))
    })?;
    let pretty = serde_json::to_string_pretty(&strict).ok();

    let (exec_ctx, exec_request) = AbacusRequest::try_from_json(&request_json)?;
    let tabulation = tabulate::tabulate(&exec_ctx, exec_request)?;

    Ok(NlResult {
        request_kind,
        model: provider.model_name().to_string(),
        explanation: envelope.explanation,
        assumptions: envelope.assumptions,
        warnings,
        variable_docs,
        generated_request_json: pretty,
        tabulation: Some(tabulation),
    })
}

// ---------------------------------------------------------------------------
// Metadata loading
// ---------------------------------------------------------------------------

fn load_catalog_context(cfg: &NlConfig) -> Result<Context, MdError> {
    let mut ctx =
        Context::from_ipums_collection_name(&cfg.product, None, cfg.data_root.clone())?;
    let ds_refs: Vec<&str> = cfg.datasets.iter().map(|s| s.as_str()).collect();

    // Prefer parquet embedded metadata (gives labels, value labels, general widths). Fall back to
    // layout files (names + widths only) if parquet metadata isn't available.
    match ctx.load_metadata_for_datasets_from_parquet(&ds_refs) {
        Ok(()) => Ok(ctx),
        Err(_) => {
            ctx.load_metadata_for_datasets(&ds_refs)?;
            Ok(ctx)
        }
    }
}

fn loaded_variables(ctx: &Context) -> Result<&[IpumsVariable], MdError> {
    ctx.settings
        .metadata
        .as_ref()
        .map(|md| md.variables_index.as_slice())
        .ok_or_else(|| MdError::MetadataError("no variable metadata loaded".to_string()))
}

// ---------------------------------------------------------------------------
// Prompt building
// ---------------------------------------------------------------------------

const SYSTEM_PROMPT: &str = r#"You convert an English description of a tabulation of IPUMS census/survey microdata into a JSON request for the "Abacus" tabulation engine.

Respond with ONLY a single JSON object (no markdown fences, no prose outside the JSON) with exactly these top-level keys:
- "request_kind": "tabulation" for a cross-tabulation or counts table (the usual case), or "microdata_extract" when the user needs row-level microdata records for further processing on their own machine (e.g. attaching characteristics, or constructing time-use variables). Only "tabulation" can be executed right now.
- "abacus_request": the tabulation request object described below. Required when request_kind is "tabulation"; may be null otherwise.
- "explanation": a short plain-English description of what the tabulation does and how you interpreted the request.
- "assumptions": any assumptions or ambiguities (which variable you chose, how you defined a subpopulation, etc.). Use an empty string if there are none.

The "abacus_request" object has these fields:
- "uoa": unit of analysis: "P" to count persons, "H" to count households.
- "request_variables": the variables to tabulate. Each is {"variable_mnemonic": "<NAME>", "general_detailed_selection": "" or "G"}. Use "G" only when the user wants the simplified "general" version of a variable; otherwise use "".
- "subpopulation": OPTIONAL array of filters restricting which records are counted. Each filter is {"variable_mnemonic": "<NAME>", "case_selection": true, "request_case_selections": [{"low_code": "<code>" or null, "high_code": "<code>" or null}]}. A selection keeps records whose value is between low_code and high_code inclusive; set one bound to null for an open-ended range.
- "category_bins": OPTIONAL object mapping a variable name to bins that group a continuous variable. Each bin is {"code": <int>, "value_label": "<text>", "low": <int> or null, "high": <int> or null}.

Rules:
- Use ONLY variable mnemonics from the provided catalog. Never invent variable names.
- For subpopulation filters and category bins, use the integer value codes shown in the catalog.
- Do NOT include byte offsets, widths, "mnemonic", or "attached_variable_pointer"; those are filled in from metadata.
- Keep the request minimal: only include "subpopulation" or "category_bins" when the user asks for a filter or a grouping.

Example user request: "Count people by marital status, but only women, in the 2019 ACS."
Example response:
{"request_kind":"tabulation","abacus_request":{"uoa":"P","request_variables":[{"variable_mnemonic":"MARST","general_detailed_selection":""}],"subpopulation":[{"variable_mnemonic":"SEX","case_selection":true,"request_case_selections":[{"low_code":"2","high_code":"2"}]}],"category_bins":{}},"explanation":"Tabulates persons by marital status (MARST), restricted to females (SEX=2).","assumptions":"Interpreted 'women' as SEX=2."}"#;

fn build_user_content(
    product: &str,
    datasets: &[String],
    catalog: &str,
    prompt: &str,
) -> String {
    format!(
        "IPUMS product: {product}\n\
         Dataset(s) to tabulate: {datasets}\n\n\
         Variable catalog (MNEMONIC — label (record type) [code=label; ...]):\n\
         {catalog}\n\n\
         User request: {prompt}\n",
        datasets = datasets.join(", "),
    )
}

fn build_catalog(variables: &[IpumsVariable], category_max: usize) -> String {
    let mut lines = Vec::with_capacity(variables.len());
    for var in variables {
        let label = var.label.as_deref().unwrap_or("(no label)");
        let mut line = format!("{} — {} ({})", var.name, label, var.record_type);
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
    lines.join("\n")
}

fn render_categories_inline(categories: &[IpumsCategory]) -> String {
    categories
        .iter()
        .map(|c| format!("{}={}", ipums_value_code(&c.value), c.label()))
        .collect::<Vec<_>>()
        .join("; ")
}

// ---------------------------------------------------------------------------
// Validation + repair
// ---------------------------------------------------------------------------

fn build_strict_request(
    llm: &LlmAbacusRequest,
    cfg: &NlConfig,
    by_name: &HashMap<String, &IpumsVariable>,
    warnings: &mut Vec<String>,
) -> Result<ist::AbacusRequest, MdError> {
    // Confirm every requested variable actually exists in the loaded metadata.
    let mut unknown = Vec::new();
    for v in llm.request_variables.iter().chain(llm.subpopulation.iter()) {
        let name = v.name();
        if name.is_empty() {
            return Err(MdError::LlmError(
                "the model produced a request variable with no mnemonic".to_string(),
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
            "the model referenced variable(s) not present in dataset(s) {}: {}",
            cfg.datasets.join(", "),
            unknown.join(", ")
        )));
    }

    let request_variables = llm
        .request_variables
        .iter()
        .map(|v| build_request_variable(v, by_name, warnings))
        .collect::<Result<Vec<_>, _>>()?;
    let subpopulation = llm
        .subpopulation
        .iter()
        .map(|v| build_request_variable(v, by_name, warnings))
        .collect::<Result<Vec<_>, _>>()?;

    let mut category_bins = BTreeMap::new();
    for (var, bins) in &llm.category_bins {
        let key = var.to_uppercase();
        let converted = bins
            .iter()
            .filter_map(|b| convert_category_bin(b, &key, warnings))
            .collect();
        category_bins.insert(key, converted);
    }

    let uoa = if llm.uoa.trim().is_empty() {
        "P".to_string()
    } else {
        llm.uoa.trim().to_uppercase()
    };

    let request_samples = cfg
        .datasets
        .iter()
        .map(|name| ist::RequestSample {
            name: name.clone(),
            custom_sampling_ratio: None,
            first_household_sampled: None,
        })
        .collect();

    Ok(ist::AbacusRequest {
        product: cfg.product.clone(),
        data_root: cfg.data_root.clone(),
        uoa,
        output_format: "json".to_string(),
        subpopulation,
        category_bins,
        request_samples,
        request_variables,
    })
}

fn build_request_variable(
    v: &LlmRequestVariable,
    by_name: &HashMap<String, &IpumsVariable>,
    warnings: &mut Vec<String>,
) -> Result<ist::RequestVariable, MdError> {
    let name = v.name();
    let md = by_name.get(&name).copied();

    // Determine the general/detailed selection and the extract_width it implies. For a detailed
    // selection extract_width is unused by tabulation; for a general selection it carries the
    // general width that drives code collapsing, so it must be correct.
    let (selection, extract_width) = if v.is_general() {
        match md.and_then(|m| m.general_width) {
            Some(w) => (ist::GeneralDetailedSelection::General, w),
            None => {
                warnings.push(format!(
                    "{name}: requested the general version but no general width is available in \
                     metadata; using the detailed version instead."
                ));
                (ist::GeneralDetailedSelection::Detailed, detailed_width(md))
            }
        }
    } else {
        (ist::GeneralDetailedSelection::Detailed, detailed_width(md))
    };

    let request_case_selections = v
        .request_case_selections
        .iter()
        .map(convert_case_selection)
        .collect::<Result<Vec<_>, _>>()?;

    // A subpopulation entry filters when it has selections, even if the model forgot the flag.
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

/// The detailed width from metadata, or 1 when unavailable (the value is unused for detailed
/// selections, so any placeholder is safe).
fn detailed_width(md: Option<&IpumsVariable>) -> usize {
    md.and_then(|m| m.formatting).map(|(_, w)| w).unwrap_or(1)
}

fn convert_case_selection(
    sel: &LlmCaseSelection,
) -> Result<ist::RequestCaseSelection, MdError> {
    let low = value_to_code(sel.low_code.as_ref())?;
    let high = value_to_code(sel.high_code.as_ref())?;
    ist::RequestCaseSelection::try_new(low, high)
}

fn convert_category_bin(
    bin: &LlmCategoryBin,
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
fn value_to_code(value: Option<&serde_json::Value>) -> Result<Option<u64>, MdError> {
    match value {
        None | Some(serde_json::Value::Null) => Ok(None),
        Some(serde_json::Value::Number(n)) => n.as_u64().map(Some).ok_or_else(|| {
            MdError::LlmError(format!("case selection code {n} is not a non-negative integer"))
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

fn build_variable_docs(
    names: &[String],
    by_name: &HashMap<String, &IpumsVariable>,
    general_names: &HashSet<String>,
) -> Vec<VariableDoc> {
    names
        .iter()
        .filter_map(|name| by_name.get(name).copied().map(|var| (name, var)))
        .map(|(name, var)| VariableDoc {
            name: var.name.clone(),
            label: var.label.clone(),
            record_type: var.record_type.clone(),
            general: general_names.contains(name),
            categories: var
                .categories
                .as_ref()
                .map(|cats| {
                    cats.iter()
                        .map(|c| (ipums_value_code(&c.value), c.label().to_string()))
                        .collect()
                })
                .unwrap_or_default(),
        })
        .collect()
}

fn ipums_value_code(value: &IpumsValue) -> String {
    match value {
        IpumsValue::Integer(i) => i.to_string(),
        IpumsValue::Float(s) => s.clone(),
        IpumsValue::String { value, .. } => String::from_utf8_lossy(value).to_string(),
        IpumsValue::Fixed { base, .. } => base.to_string(),
    }
}

// ---------------------------------------------------------------------------
// Helpers + rendering
// ---------------------------------------------------------------------------

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

impl NlResult {
    /// Render the result for display. `format` controls the table; the surrounding explanation is
    /// rendered as Markdown-ish text for `TextTable` and embedded in a JSON object otherwise.
    pub fn render(&self, format: TableFormat) -> Result<String, MdError> {
        match format {
            TableFormat::Json => self.render_json(),
            _ => self.render_text(format),
        }
    }

    fn render_text(&self, format: TableFormat) -> Result<String, MdError> {
        let mut out = String::new();
        out.push_str(&format!("## What this does\n{}\n", self.explanation.trim()));

        if !self.assumptions.trim().is_empty() {
            out.push_str(&format!("\n## Assumptions\n{}\n", self.assumptions.trim()));
        }

        if !self.warnings.is_empty() {
            out.push_str("\n## Warnings\n");
            for w in &self.warnings {
                out.push_str(&format!("- {w}\n"));
            }
        }

        if !self.variable_docs.is_empty() {
            out.push_str("\n## Variables\n");
            for doc in &self.variable_docs {
                let label = doc.label.as_deref().unwrap_or("(no label)");
                if doc.general {
                    // The result codes are general groupings; metadata has only detailed value
                    // labels, so listing them next to general codes would be misleading.
                    out.push_str(&format!(
                        "- {} ({}): {} — tabulated as general (collapsed) categories; \
                         the result shows general grouping codes.\n",
                        doc.name, doc.record_type, label
                    ));
                } else {
                    out.push_str(&format!("- {} ({}): {}\n", doc.name, doc.record_type, label));
                    for (code, clabel) in &doc.categories {
                        out.push_str(&format!("    {code} = {clabel}\n"));
                    }
                }
            }
        }

        if let Some(tab) = &self.tabulation {
            out.push_str("\n## Result\n");
            match format {
                // For the text table, inline value labels next to the raw codes.
                TableFormat::TextTable => out.push_str(&self.render_tables_with_labels(tab)?),
                _ => out.push_str(&tab.output(format)?),
            }
        }

        Ok(out)
    }

    /// Render every table as text, inserting a `<VAR>_label` column after each detailed coded
    /// variable column. General columns are left as raw codes (the metadata has no general value
    /// labels). Raw codes are always kept; the labels are an additional column.
    fn render_tables_with_labels(&self, tab: &Tabulation) -> Result<String, MdError> {
        // Build code -> label maps for the detailed variables we have value labels for.
        let mut label_maps: HashMap<&str, HashMap<&str, &str>> = HashMap::new();
        for doc in &self.variable_docs {
            if doc.general || doc.categories.is_empty() {
                continue;
            }
            let map = doc
                .categories
                .iter()
                .map(|(code, label)| (code.as_str(), label.as_str()))
                .collect();
            label_maps.insert(doc.name.as_str(), map);
        }

        let mut out = String::new();
        for table in &tab.0 {
            out.push_str(&format_table_with_labels(table, &label_maps)?);
            out.push('\n');
        }
        Ok(out)
    }

    fn render_json(&self) -> Result<String, MdError> {
        let tables: Option<serde_json::Value> = match &self.tabulation {
            Some(tab) => {
                let s = tab.output(TableFormat::Json)?;
                Some(serde_json::from_str(&s).map_err(|err| {
                    MdError::Msg(format!("could not re-parse tabulation JSON: {err}"))
                })?)
            }
            None => None,
        };

        let variables: Vec<serde_json::Value> = self
            .variable_docs
            .iter()
            .map(|doc| {
                serde_json::json!({
                    "name": doc.name,
                    "label": doc.label,
                    "record_type": doc.record_type,
                    "categories": doc
                        .categories
                        .iter()
                        .map(|(code, label)| serde_json::json!({ "code": code, "label": label }))
                        .collect::<Vec<_>>(),
                })
            })
            .collect();

        let generated: Option<serde_json::Value> = self
            .generated_request_json
            .as_ref()
            .and_then(|s| serde_json::from_str(s).ok());

        let value = serde_json::json!({
            "request_kind": self.request_kind,
            "model": self.model,
            "explanation": self.explanation,
            "assumptions": self.assumptions,
            "warnings": self.warnings,
            "variables": variables,
            "generated_request": generated,
            "tables": tables,
        });

        serde_json::to_string_pretty(&value)
            .map_err(|err| MdError::Msg(format!("could not serialize result as JSON: {err}")))
    }
}

/// Render a single result table as text, inserting a left-aligned `<VAR>_label` column after each
/// coded variable column that has an entry in `label_maps`. Column widths are derived from the
/// data; numeric/code columns are right-aligned, label columns left-aligned.
fn format_table_with_labels(
    table: &Table,
    label_maps: &HashMap<&str, HashMap<&str, &str>>,
) -> Result<String, MdError> {
    // Plan the display columns: for each original column, decide whether a label column follows it.
    let mut headers: Vec<String> = Vec::new();
    let mut align_left: Vec<bool> = Vec::new();
    // For each ORIGINAL column index, the label map to expand it with (if any).
    let mut label_after: Vec<Option<&HashMap<&str, &str>>> = Vec::new();

    for col in &table.heading {
        headers.push(col.name());
        align_left.push(false);
        let mut lm = None;
        if let OutputColumn::RequestVar(v) = col {
            if !v.is_general() {
                if let Some(map) = label_maps.get(v.name.as_str()) {
                    lm = Some(map);
                    headers.push(format!("{}_label", v.name));
                    align_left.push(true);
                }
            }
        }
        label_after.push(lm);
    }

    // Expand each data row to match, looking up labels for the coded cells.
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

#[cfg(test)]
mod tests {
    use super::*;

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
    fn test_envelope_parses_minimal_request() {
        let json = r#"{
            "request_kind": "tabulation",
            "abacus_request": {
                "uoa": "P",
                "request_variables": [{"variable_mnemonic": "MARST", "general_detailed_selection": ""}]
            },
            "explanation": "Counts persons by marital status.",
            "assumptions": ""
        }"#;
        let env: LlmTabulationResponse =
            serde_json::from_str(json).expect("should parse the envelope");
        let req = env.abacus_request.expect("should have a request");
        assert_eq!(req.request_variables.len(), 1);
        assert_eq!(req.request_variables[0].name(), "MARST");
        assert!(!req.request_variables[0].is_general());
    }
}
