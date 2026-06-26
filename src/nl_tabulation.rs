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
use std::path::Path;

use serde::de::DeserializeOwned;
use serde::Deserialize;

use crate::conventions::Context;
use crate::input_schema_tabulation as ist;
use crate::ipums_metadata_model::{IpumsCategory, IpumsValue, IpumsVariable};
use crate::llm::LlmProvider;
use crate::mderror::MdError;
use crate::request::AbacusRequest;
use crate::tabulate::{self, OutputColumn, Table, TableFormat, Tabulation};

/// How many value labels a variable may have before they are omitted from the prompt catalog.
///
/// The catalog inlines value labels only for variables with at most this many categories; larger
/// variables show just a count, and the second (refinement) pass resolves their codes on demand.
/// This trims the bulk of the catalog tokens (high-cardinality variables) while keeping common
/// demographic variables (SEX, MARST, RACE, ...) inline so the first pass resolves their codes
/// accurately without a second round-trip. Tune via `NlConfig.category_catalog_max`; set 0 to omit
/// all inline labels (maximum trim, but more refinement and lower first-pass accuracy).
const DEFAULT_CATEGORY_CATALOG_MAX: usize = 12;

/// Inputs needed to translate and run a natural-language tabulation request.
pub struct NlConfig {
    /// IPUMS product/collection, e.g. "usa".
    pub product: String,
    /// Path to the data root (containing `parquet/` and `layouts/`). `None` uses product defaults.
    pub data_root: Option<String>,
    /// Dataset(s) whose metadata is offered to the model and which the tabulation runs against.
    /// If empty, the model chooses an appropriate dataset from those available under the data root.
    pub datasets: Vec<String>,
    /// Max value labels to inline per variable in the catalog. `None` uses the default.
    pub category_catalog_max: Option<usize>,
    /// Force detailed categories for every tabulation variable, overriding the model's (general-by-
    /// default) choice. The CLI's `--detailed` flag mirrors the website's "details" checkbox.
    pub detailed: bool,
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
    /// The dataset(s) the tabulation ran against.
    pub datasets: Vec<String>,
    /// If the dataset was chosen by the model (not given by the caller), the model's reason.
    pub dataset_reason: Option<String>,
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
    /// Category bins as an array (one entry per variable) so the whole envelope is expressible as a
    /// JSON Schema for constrained decoding. Converted to the Abacus map form in `build_strict_request`.
    #[serde(default)]
    category_bins: Vec<LlmCategoryBinGroup>,
}

#[derive(Debug, Clone, Deserialize)]
struct LlmCategoryBinGroup {
    #[serde(default)]
    variable: String,
    #[serde(default)]
    bins: Vec<LlmCategoryBin>,
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

#[derive(Debug, Clone, Deserialize)]
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
    // 0. Decide which dataset(s) to use: the caller's, or let the model pick one from those
    // available under the data root (e.g. "in 1900" -> an 1900 sample).
    let (datasets, dataset_reason) = if cfg.datasets.is_empty() {
        let available = list_available_datasets(cfg)?;
        let chosen = choose_datasets(provider, prompt, &available)?;
        (chosen.datasets, Some(chosen.reason))
    } else {
        (cfg.datasets.clone(), None)
    };

    // 1. Load metadata for the catalog and for value-label documentation.
    let ctx = load_catalog_context(cfg, &datasets)?;
    let variables = loaded_variables(&ctx)?;
    let by_name: HashMap<String, &IpumsVariable> = variables
        .iter()
        .map(|v| (v.name.to_uppercase(), v))
        .collect();

    // 2. Build the prompt and ask the model.
    let cat_max = cfg.category_catalog_max.unwrap_or(DEFAULT_CATEGORY_CATALOG_MAX);
    let catalog = build_catalog(variables, cat_max);
    let user_content = build_user_content(&cfg.product, &datasets, &catalog, prompt);

    let envelope: LlmTabulationResponse =
        complete_json_with_retry(provider, SYSTEM_PROMPT, &user_content, "tabulation response")?;

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
            datasets,
            dataset_reason,
            tabulation: None,
        });
    }

    let mut llm_request = envelope.abacus_request.ok_or_else(|| {
        MdError::LlmError(
            "the model classified this as a tabulation but did not provide an abacus_request"
                .to_string(),
        )
    })?;

    let mut warnings = Vec::new();

    // 2b. Second pass: a filter or grouping may reference a variable whose value codes were too
    // numerous to fit in the catalog (so the model picked codes blind). For exactly those
    // variables, send their full value labels and let the model choose the exact codes.
    let targets = refine_targets(&llm_request, &by_name, cat_max);
    let mut refined_vars: Option<String> = None;
    if !targets.is_empty() {
        match refine_codes(provider, prompt, &targets, &by_name) {
            Ok(refined) => {
                merge_refinements(&mut llm_request, refined, &targets);
                refined_vars = Some(targets.all().join(", "));
            }
            Err(err) => warnings.push(format!(
                "could not refine value codes for {} via a second pass ({err}); \
                 used the first-pass codes.",
                targets.all().join(", ")
            )),
        }
    }

    // 3. Validate + repair into a strict request, filling mechanical fields from metadata.
    let strict = build_strict_request(&llm_request, cfg, &datasets, &by_name, &mut warnings)?;

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
    let mut variable_docs = build_variable_docs(&doc_names, &by_name, &general_names);

    // For binned variables the meaningful categories are the bins themselves (e.g. "0-9"), not the
    // variable's raw value labels — so the result table and legend show the bin labels.
    apply_bin_labels(&mut variable_docs, &llm_request);

    // 4. Serialize and run through the normal Abacus path (which loads layout metadata itself).
    let request_json = serde_json::to_string(&strict).map_err(|err| {
        MdError::Msg(format!("could not serialize the generated Abacus request: {err}"))
    })?;
    let pretty = serde_json::to_string_pretty(&strict).ok();

    let (exec_ctx, exec_request) = AbacusRequest::try_from_json(&request_json)?;
    let tabulation = tabulate::tabulate(&exec_ctx, exec_request)?;

    // If a second pass resolved codes, the first-pass assumptions may no longer reflect the codes
    // actually used; note the refinement so the narrative stays honest.
    let mut assumptions = envelope.assumptions;
    if let Some(vars) = refined_vars {
        if !assumptions.trim().is_empty() {
            assumptions.push(' ');
        }
        assumptions.push_str(&format!(
            "Exact value codes for {vars} were resolved from the full code list in a second pass."
        ));
    }

    Ok(NlResult {
        request_kind,
        model: provider.model_name().to_string(),
        explanation: envelope.explanation,
        assumptions,
        warnings,
        variable_docs,
        generated_request_json: pretty,
        datasets,
        dataset_reason,
        tabulation: Some(tabulation),
    })
}

// ---------------------------------------------------------------------------
// Metadata loading
// ---------------------------------------------------------------------------

fn load_catalog_context(cfg: &NlConfig, datasets: &[String]) -> Result<Context, MdError> {
    let mut ctx =
        Context::from_ipums_collection_name(&cfg.product, None, cfg.data_root.clone())?;
    let ds_refs: Vec<&str> = datasets.iter().map(|s| s.as_str()).collect();

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
// Dataset selection (when the caller didn't name a dataset)
// ---------------------------------------------------------------------------

/// A dataset choice made by the model.
struct ChosenDatasets {
    datasets: Vec<String>,
    reason: String,
}

#[derive(Debug, Default, Deserialize)]
struct LlmDatasetChoice {
    #[serde(default)]
    datasets: Vec<String>,
    /// Accept a singular "dataset" too, in case the model uses it.
    #[serde(default)]
    dataset: Option<String>,
    #[serde(default)]
    reason: String,
}

const DATASET_SELECT_SYSTEM_PROMPT: &str = r#"You pick which IPUMS dataset(s) best answer a user's request from a provided list. Each dataset name encodes its sample, and a year is shown next to it (e.g. "us1900m — 1900" is an 1900 sample).

Respond with ONLY a single JSON object (no markdown fences): {"datasets": ["<name>", ...], "reason": "<one short sentence>"}.
- Choose the SINGLE most appropriate dataset unless the user clearly wants several (e.g. a comparison across years).
- Use ONLY names from the provided list, exactly as written.
- If several datasets share the requested year, pick one (prefer the most complete/standard sample) and say which in the reason."#;

/// List datasets available under the data root: each `parquet/<name>` that also has a
/// `layouts/<name>.layout.txt` (so it is executable). Sorted, de-duplicated.
fn list_available_datasets(cfg: &NlConfig) -> Result<Vec<String>, MdError> {
    let data_root = cfg.data_root.as_deref().ok_or_else(|| {
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

/// Ask the model to choose dataset(s) from `available` for `prompt`, validating its answer.
fn choose_datasets(
    provider: &dyn LlmProvider,
    prompt: &str,
    available: &[String],
) -> Result<ChosenDatasets, MdError> {
    let listing = build_dataset_listing(available);
    let user = format!(
        "User request: {prompt}\n\nAvailable datasets (name — year):\n{listing}\n"
    );
    let choice: LlmDatasetChoice = complete_json_with_retry(
        provider,
        DATASET_SELECT_SYSTEM_PROMPT,
        &user,
        "dataset-selection response",
    )?;

    // Validate the chosen names against the available list (case-insensitive).
    let available_by_lower: HashMap<String, &String> =
        available.iter().map(|a| (a.to_lowercase(), a)).collect();
    let mut resolved: Vec<String> = Vec::new();
    let mut invalid: Vec<String> = Vec::new();
    for name in choice.datasets.iter().chain(choice.dataset.iter()) {
        let key = name.trim().to_lowercase();
        if key.is_empty() {
            continue;
        }
        match available_by_lower.get(&key) {
            Some(real) if !resolved.contains(*real) => resolved.push((*real).clone()),
            Some(_) => {}
            None => invalid.push(name.clone()),
        }
    }

    if resolved.is_empty() {
        return Err(MdError::LlmError(format!(
            "the model did not choose a valid dataset (got {invalid:?}); specify one explicitly"
        )));
    }

    let reason = if choice.reason.trim().is_empty() {
        format!("selected {} for the request", resolved.join(", "))
    } else {
        choice.reason
    };
    Ok(ChosenDatasets {
        datasets: resolved,
        reason,
    })
}

fn build_dataset_listing(available: &[String]) -> String {
    available
        .iter()
        .map(|name| match parse_year(name) {
            Some(year) => format!("{name} — {year}"),
            None => name.clone(),
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// Extract a 4-digit year from a dataset name (e.g. "us1900m" -> 1900), if present.
fn parse_year(name: &str) -> Option<u32> {
    let bytes = name.as_bytes();
    let mut i = 0;
    while i + 4 <= bytes.len() {
        let window = &bytes[i..i + 4];
        let is_four_digits = window.iter().all(u8::is_ascii_digit);
        let bounded_left = i == 0 || !bytes[i - 1].is_ascii_digit();
        let bounded_right = i + 4 == bytes.len() || !bytes[i + 4].is_ascii_digit();
        if is_four_digits && bounded_left && bounded_right {
            return std::str::from_utf8(window).ok().and_then(|s| s.parse().ok());
        }
        i += 1;
    }
    None
}

// ---------------------------------------------------------------------------
// Prompt building
// ---------------------------------------------------------------------------

const SYSTEM_PROMPT: &str = r#"You convert an English description of a tabulation of IPUMS census/survey microdata into a JSON request for the "Abacus" tabulation engine.

Respond with ONLY a single JSON object (no markdown fences, no prose outside the JSON) with exactly these top-level keys. The output MUST be strictly valid JSON: write each key exactly once per object, separate every element with a comma, use no trailing commas, and close every object and array.
- "request_kind": "tabulation" for a cross-tabulation or counts table (the usual case), or "microdata_extract" when the user needs row-level microdata records for further processing on their own machine (e.g. attaching characteristics, or constructing time-use variables). Only "tabulation" can be executed right now.
- "abacus_request": the tabulation request object described below. Required when request_kind is "tabulation"; may be null otherwise.
- "explanation": a short plain-English description of what the tabulation does and how you interpreted the request.
- "assumptions": any assumptions or ambiguities (which variable you chose, how you defined a subpopulation, etc.). Use an empty string if there are none.

The "abacus_request" object has these fields:
- "uoa": unit of analysis: "P" to count persons, "H" to count households.
- "request_variables": the variables to tabulate. Each is {"variable_mnemonic": "<NAME>", "general_detailed_selection": "G" or ""}. "G" requests the general (simplified) categories; "" requests detailed. ONLY variables marked "general" in the catalog (shown as "(P; general)" or "(H; general)") have a general form. For those, DEFAULT TO "G" — it is what users expect and keeps results compact. For every variable NOT marked "general", you MUST use "" (it has only detailed categories). Even for a variable that has a general form, use "" when the user explicitly asks for detail ("detailed", "specific", "single year of age", "by country", "by state") or needs a breakdown finer than the general categories can express (individual countries of birth, individual states, single years of age, a particular detailed category).
- "subpopulation": OPTIONAL array of filters restricting which records are counted. Each filter is {"variable_mnemonic": "<NAME>", "case_selection": true, "request_case_selections": [{"low_code": "<code>" or null, "high_code": "<code>" or null}]}. A selection keeps records whose value is between low_code and high_code inclusive; set one bound to null for an open-ended range.
- "category_bins": OPTIONAL array; each entry groups one continuous variable into bins: {"variable": "<NAME>", "bins": [{"code": <int>, "value_label": "<text>", "low": <int>, "high": <int>}]}. For a CLOSED range set BOTH low and high (e.g. the group "10-19" is low 10, high 19). OMIT a bound only for an open-ended end (e.g. "65+" is low 65 with no high; "under 5" is high 4 with no low). Give each bin a distinct integer "code". A binned variable MUST also appear in "request_variables" (the bins regroup that variable's tabulation).

Rules:
- Use ONLY variable mnemonics from the provided catalog. Never invent variable names.
- Request "G" ONLY for tabulation variables marked "general" in the catalog, and prefer "G" for those; use "" for all other tabulation variables and whenever detail is requested or required. Subpopulation FILTERS use detailed value codes (the exact integer codes shown), since that is where precise category selection matters.
- For subpopulation filters and category bins, use the integer value codes shown in the catalog.
- Do NOT include byte offsets, widths, "mnemonic", or "attached_variable_pointer"; those are filled in from metadata.
- Keep the request minimal: only include "subpopulation" or "category_bins" when the user asks for a filter or a grouping.
- COUNTS vs BREAKDOWNS: when the user asks "how many" of a group — possibly with several conditions ("how many Hispanic men graduated from college") — put EVERY condition in "subpopulation". Multiple subpopulation variables are AND-ed together, and a filter may be on a variable you are not otherwise breaking down. Then tabulate exactly ONE variable, chosen so those conditions pin it to a single value (e.g. tabulate SEX when the group is "men"); the result is then one row, reported as a single number. Break a variable into all its categories only when the user wants the full distribution ("by marital status", "for each", "broken down by").

IPUMS conventions (domain knowledge about how these variables encode concepts — follow them when mapping the request to variables):
- Hispanic/Latino origin is an ETHNICITY, captured by the separate variable HISPAN — it is NOT a RACE category. Race and Hispanic ethnicity are independent dimensions (a person of any race may or may not be Hispanic). For "Hispanic"/"Latino", use HISPAN and select its Hispanic categories (i.e. exclude the "Not Hispanic" code), not a race variable. Use RACE (or a race-detail variable) only for racial categories such as White, Black, or Asian. If the user asks for both race and Hispanic origin, use both variables.

Example user request: "Count people by education, but only women, in the 2019 ACS." (Here EDUC is marked "general" in the catalog so it uses "G"; SEX is not, and the filter uses a detailed code.)
Example response:
{"request_kind":"tabulation","abacus_request":{"uoa":"P","request_variables":[{"variable_mnemonic":"EDUC","general_detailed_selection":"G"}],"subpopulation":[{"variable_mnemonic":"SEX","case_selection":true,"request_case_selections":[{"low_code":"2","high_code":"2"}]}],"category_bins":[]},"explanation":"Tabulates persons by educational attainment (EDUC) using general categories, restricted to females (SEX=2).","assumptions":"Interpreted 'women' as SEX=2."}

Example user request: "How many people were divorced?" (A count of one category — filter to it so the result is a single number.)
Example response:
{"request_kind":"tabulation","abacus_request":{"uoa":"P","request_variables":[{"variable_mnemonic":"MARST","general_detailed_selection":""}],"subpopulation":[{"variable_mnemonic":"MARST","case_selection":true,"request_case_selections":[{"low_code":"4","high_code":"4"}]}],"category_bins":[]},"explanation":"Counts divorced persons (MARST=4).","assumptions":"Interpreted 'divorced' as MARST=4."}

Example user request: "How many divorced women were there?" (Two conditions; tabulate SEX pinned to women, and AND the other condition.)
Example response:
{"request_kind":"tabulation","abacus_request":{"uoa":"P","request_variables":[{"variable_mnemonic":"SEX","general_detailed_selection":""}],"subpopulation":[{"variable_mnemonic":"SEX","case_selection":true,"request_case_selections":[{"low_code":"2","high_code":"2"}]},{"variable_mnemonic":"MARST","case_selection":true,"request_case_selections":[{"low_code":"4","high_code":"4"}]}],"category_bins":[]},"explanation":"Counts divorced women (SEX=2 and MARST=4).","assumptions":"Interpreted 'women' as SEX=2 and 'divorced' as MARST=4."}

Example user request: "People by age in 10-year groups." (A binned variable must ALSO be in request_variables; closed ranges set both low and high.)
Example response:
{"request_kind":"tabulation","abacus_request":{"uoa":"P","request_variables":[{"variable_mnemonic":"AGE","general_detailed_selection":""}],"subpopulation":[],"category_bins":[{"variable":"AGE","bins":[{"code":1,"value_label":"0-9","low":0,"high":9},{"code":2,"value_label":"10-19","low":10,"high":19},{"code":3,"value_label":"20-29","low":20,"high":29}]}]},"explanation":"Tabulates persons by age grouped into 10-year bins.","assumptions":"Used 10-year age groups."}"#;

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
        // Mark variables that actually have a general form so the model only requests "G" for them.
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
    datasets: &[String],
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
            datasets.join(", "),
            unknown.join(", ")
        )));
    }

    // Tabulation variables honor the --detailed override; subpopulation filters are always detailed.
    let request_variables = llm
        .request_variables
        .iter()
        .map(|v| build_request_variable(v, by_name, cfg.detailed))
        .collect::<Result<Vec<_>, _>>()?;
    let subpopulation = llm
        .subpopulation
        .iter()
        .map(|v| build_request_variable(v, by_name, false))
        .collect::<Result<Vec<_>, _>>()?;

    let mut category_bins = BTreeMap::new();
    for group in &llm.category_bins {
        let key = group.variable.trim().to_uppercase();
        if key.is_empty() {
            continue;
        }
        let converted = group
            .bins
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

    let request_samples = datasets
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
    force_detailed: bool,
) -> Result<ist::RequestVariable, MdError> {
    let name = v.name();
    let md = by_name.get(&name).copied();

    // Honor a general selection only for variables that actually have a general form (a general
    // width narrower than the detailed width). Variables without one carry only detailed
    // categories, so a "G" request on them quietly becomes detailed. extract_width is unused for a
    // detailed selection; for a general selection it carries the general width that drives the code
    // collapsing, so it must be correct.
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
// Second pass: refine value codes for filters / bins
// ---------------------------------------------------------------------------

/// The variables whose subpopulation/bin codes are worth a second, focused pass because their
/// value labels were capped out of the first-pass catalog.
struct RefineTargets {
    /// Variable names (uppercased) used as a subpopulation filter.
    filters: Vec<String>,
    /// `category_bins` keys (as the model wrote them) used for grouping.
    bins: Vec<String>,
}

impl RefineTargets {
    fn is_empty(&self) -> bool {
        self.filters.is_empty() && self.bins.is_empty()
    }

    /// All target names, de-duplicated, for warning messages.
    fn all(&self) -> Vec<String> {
        let mut names: Vec<String> = self
            .filters
            .iter()
            .cloned()
            .chain(self.bins.iter().map(|b| b.to_uppercase()))
            .collect();
        names.sort();
        names.dedup();
        names
    }
}

/// The shape parsed out of the model's second (refinement) pass.
#[derive(Debug, Default, Deserialize)]
struct RefineResponse {
    #[serde(default)]
    subpopulation: Vec<LlmRequestVariable>,
    #[serde(default)]
    category_bins: Vec<LlmCategoryBinGroup>,
}

/// How many value labels the variable carries in metadata (0 if it has none).
fn category_count(by_name: &HashMap<String, &IpumsVariable>, name: &str) -> usize {
    by_name
        .get(name)
        .and_then(|v| v.categories.as_ref())
        .map(|c| c.len())
        .unwrap_or(0)
}

/// Pick the filter/grouping variables whose codes the catalog hid from the model (more categories
/// than the catalog cap), so a second pass with their full labels can improve the codes.
fn refine_targets(
    llm: &LlmAbacusRequest,
    by_name: &HashMap<String, &IpumsVariable>,
    cat_max: usize,
) -> RefineTargets {
    let mut filters = Vec::new();
    for v in &llm.subpopulation {
        let name = v.name();
        let has_selection = v.case_selection || !v.request_case_selections.is_empty();
        if !name.is_empty()
            && has_selection
            && category_count(by_name, &name) > cat_max
            && !filters.contains(&name)
        {
            filters.push(name);
        }
    }

    let mut bins = Vec::new();
    for group in &llm.category_bins {
        let key = group.variable.clone();
        if category_count(by_name, &key.to_uppercase()) > cat_max && !bins.contains(&key) {
            bins.push(key);
        }
    }

    RefineTargets { filters, bins }
}

const REFINE_SYSTEM_PROMPT: &str = r#"You are refining the exact value codes for specific IPUMS variables in a tabulation request. For each variable you are given its label, whether it is used as a filter (subpopulation) or a grouping (category bins), and its FULL list of integer value codes with labels.

Using the original user request and these full code lists, respond with ONLY a single JSON object (no markdown fences) with these keys:
- "subpopulation": array of filters, each {"variable_mnemonic":"<NAME>","case_selection":true,"request_case_selections":[{"low_code":"<code>" or null,"high_code":"<code>" or null}]}. A selection keeps records whose value is between low_code and high_code inclusive; use several selections to cover a non-contiguous set of codes; set a bound to null for an open-ended range.
- "category_bins": array; each entry is {"variable":"<NAME>","bins":[{"code":<int>,"value_label":"<text>","low":<int> or null,"high":<int> or null}]}.

Use ONLY the integer codes shown in the lists. Include ONLY the variables you are given, each in the section matching its stated role."#;

fn build_refine_content(
    prompt: &str,
    targets: &RefineTargets,
    by_name: &HashMap<String, &IpumsVariable>,
) -> String {
    let mut out = format!("Original user request: {prompt}\n\nVariables to resolve:\n");
    for name in &targets.filters {
        append_var_codes(&mut out, name, "filter (subpopulation)", by_name);
    }
    for key in &targets.bins {
        append_var_codes(&mut out, &key.to_uppercase(), "grouping (category bins)", by_name);
    }
    out
}

fn append_var_codes(
    out: &mut String,
    name: &str,
    role: &str,
    by_name: &HashMap<String, &IpumsVariable>,
) {
    if let Some(var) = by_name.get(name) {
        let label = var.label.as_deref().unwrap_or("(no label)");
        out.push_str(&format!("- {name} — {label} — used as a {role}\n"));
        if let Some(cats) = &var.categories {
            out.push_str("    codes: ");
            out.push_str(&render_categories_inline(cats));
            out.push('\n');
        }
    }
}

/// Run the second pass: ask the model to pick exact codes for the target variables given their full
/// value labels.
fn refine_codes(
    provider: &dyn LlmProvider,
    prompt: &str,
    targets: &RefineTargets,
    by_name: &HashMap<String, &IpumsVariable>,
) -> Result<RefineResponse, MdError> {
    let user = build_refine_content(prompt, targets, by_name);
    complete_json_with_retry(provider, REFINE_SYSTEM_PROMPT, &user, "refinement response")
}

/// Replace the first-pass selections/bins for the target variables with the refined ones, leaving
/// every other part of the request untouched.
fn merge_refinements(llm: &mut LlmAbacusRequest, refined: RefineResponse, targets: &RefineTargets) {
    // Filters: drop the old entries for each target, then add the refined ones back.
    for fname in &targets.filters {
        llm.subpopulation.retain(|v| &v.name() != fname);
    }
    for v in refined.subpopulation {
        if targets.filters.contains(&v.name()) {
            llm.subpopulation.push(v);
        }
    }

    // Bins: overwrite the target variables' bins (matching the variable case-insensitively).
    for key in &targets.bins {
        let upper = key.to_uppercase();
        if let Some(group) = refined
            .category_bins
            .iter()
            .find(|g| g.variable.to_uppercase() == upper)
        {
            llm.category_bins.retain(|g| g.variable.to_uppercase() != upper);
            llm.category_bins.push(group.clone());
        }
    }
}

// ---------------------------------------------------------------------------
// Documentation
// ---------------------------------------------------------------------------

/// Replace a binned variable's documented categories with its bin labels (code -> "0-9", "10-19",
/// ...), so the result table and legend describe the bins rather than the raw value codes.
fn apply_bin_labels(docs: &mut [VariableDoc], llm: &LlmAbacusRequest) {
    for group in &llm.category_bins {
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

fn build_variable_docs(
    names: &[String],
    by_name: &HashMap<String, &IpumsVariable>,
    general_names: &HashSet<String>,
) -> Vec<VariableDoc> {
    names
        .iter()
        .filter_map(|name| by_name.get(name).copied().map(|var| (name, var)))
        .map(|(name, var)| {
            let general = general_names.contains(name);
            // For a general selection the result codes are collapsed groupings, so document the
            // derived general labels; otherwise document the detailed value labels.
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
fn detailed_categories(var: &IpumsVariable) -> Vec<(String, String)> {
    var.categories
        .as_ref()
        .map(|cats| {
            cats.iter()
                .map(|c| (ipums_value_code(&c.value), c.label().to_string()))
                .collect()
        })
        .unwrap_or_default()
}

/// The integer divisor that collapses detailed codes into general codes, mirroring
/// `RequestVariable::try_from` (request.rs): `10^(detailed_width - general_width)`, or 1 when there
/// is no distinct general width. The tabulation engine emits `detailed_code / divisor`, so the
/// general result codes are grouped the same way here.
fn general_divisor(var: &IpumsVariable) -> usize {
    match (var.formatting, var.general_width) {
        (Some((_, w)), Some(gw)) if gw < w => 10usize.pow((w - gw) as u32),
        _ => 1,
    }
}

/// Whether a variable actually has a distinct general form. The parquet loader falls back
/// `general_width = column_width` when the source has no general width, so the presence of a
/// general width alone is not enough — the general width must be strictly narrower than the
/// detailed width (equivalently, the divisor collapses codes, `> 1`).
fn has_general_form(md: Option<&IpumsVariable>) -> bool {
    md.map(|v| general_divisor(v) > 1).unwrap_or(false)
}

/// Derive general (collapsed) category labels from the detailed value labels using the "first label
/// rule": group detailed codes by their general code (`code / divisor`) and take the label of the
/// smallest detailed code in each group. The parquet metadata lacks the explicit general-category
/// markers (indentation / grouping) of the source metadata, but the first detailed label in a
/// grouping is conventionally the general label (e.g. RELATE 301 "Child" labels general code 3).
/// Returns (general_code, label) pairs sorted by code.
fn general_categories(var: &IpumsVariable) -> Vec<(String, String)> {
    let divisor = general_divisor(var) as i64;
    let cats = match &var.categories {
        Some(c) => c,
        None => return Vec::new(),
    };
    // general_code -> (smallest detailed code seen so far, its label)
    let mut groups: BTreeMap<i64, (i64, String)> = BTreeMap::new();
    for c in cats {
        if let IpumsValue::Integer(v) = c.value {
            let entry = groups.entry(v / divisor).or_insert((i64::MAX, String::new()));
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

/// Group an integer string with thousands separators ("1234567" -> "1,234,567"). Non-integer input
/// is returned unchanged.
fn group_thousands(s: &str) -> String {
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

/// How many times to ask the model for a JSON reply before giving up.
const MAX_JSON_ATTEMPTS: usize = 3;

/// Call the model and parse its JSON reply into `T`, re-asking a few times on a recoverable failure.
/// Low-temperature sampling occasionally emits malformed JSON (a stray duplicate key or missing
/// delimiter), and Gemini sometimes returns an empty/filtered candidate (e.g. a `RECITATION`
/// finish); fresh sampling on a re-ask almost always clears both. Fatal provider errors (bad
/// request, auth) are not retried — they propagate immediately.
fn complete_json_with_retry<T: DeserializeOwned>(
    provider: &dyn LlmProvider,
    system: &str,
    user: &str,
    what: &str,
) -> Result<T, MdError> {
    let mut last_error: Option<String> = None;
    for _ in 0..MAX_JSON_ATTEMPTS {
        match provider.complete_json(system, user) {
            Ok(raw) => {
                let cleaned = strip_json_fences(&raw);
                match serde_json::from_str::<T>(&cleaned) {
                    Ok(value) => return Ok(value),
                    Err(err) => last_error = Some(format!("invalid JSON ({err}); reply was: {cleaned}")),
                }
            }
            Err(err) if is_retryable_llm_error(&err) => last_error = Some(err.to_string()),
            Err(err) => return Err(err),
        }
    }
    Err(MdError::LlmError(format!(
        "could not get a valid {what} after {MAX_JSON_ATTEMPTS} attempts: {}",
        last_error.unwrap_or_default()
    )))
}

/// Whether an LLM error looks transient enough to be worth re-asking (an empty/filtered candidate or
/// a rate-limit/server hiccup), as opposed to a fatal error (bad request, auth) that a retry can't fix.
fn is_retryable_llm_error(err: &MdError) -> bool {
    let message = err.to_string();
    ["RECITATION", "did not contain any candidates", "contained no text", "HTTP 429", "HTTP 500", "HTTP 503"]
        .iter()
        .any(|marker| message.contains(marker))
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

        out.push_str(&self.data_source_text());

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
                // General selections show derived general (collapsed) category labels; mark them so
                // it is clear the codes are groupings, not detailed codes.
                let marker = if doc.general {
                    " (general categories)"
                } else {
                    ""
                };
                out.push_str(&format!(
                    "- {} ({}): {}{}\n",
                    doc.name, doc.record_type, label, marker
                ));
                for (code, clabel) in &doc.categories {
                    out.push_str(&format!("    {code} = {clabel}\n"));
                }
            }
        }

        if let Some(tab) = &self.tabulation {
            out.push_str("\n## Result\n");
            if let Some(single) = self.render_single_number(tab) {
                // A one-row result is really a single value; present it as a number, not a table.
                out.push_str(&single);
            } else {
                match format {
                    // For the text table, inline value labels next to the raw codes.
                    TableFormat::TextTable => out.push_str(&self.render_tables_with_labels(tab)?),
                    _ => out.push_str(&tab.output(format)?),
                }
            }
        }

        Ok(out)
    }

    /// A short "Data source" section naming the dataset(s) used (with year), the model's reason if it
    /// chose them, and an IPUMS attribution.
    fn data_source_text(&self) -> String {
        if self.datasets.is_empty() {
            return String::new();
        }
        let listed: Vec<String> = self
            .datasets
            .iter()
            .map(|d| match parse_year(d) {
                Some(year) => format!("{d} ({year})"),
                None => d.clone(),
            })
            .collect();
        let mut out = format!("\n## Data source\nDataset(s): {}\n", listed.join(", "));
        if let Some(reason) = &self.dataset_reason {
            out.push_str(&format!("Chosen for this request: {}\n", reason.trim()));
        }
        out.push_str("Source: IPUMS, University of Minnesota (www.ipums.org).\n");
        out
    }

    /// If the result is a single value (one table, one row), render it as a number rather than a
    /// table. Returns `None` otherwise, so the caller renders the full table.
    fn render_single_number(&self, tab: &Tabulation) -> Option<String> {
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

        // Describe the single cell using the request-variable columns (with labels where available).
        let label_maps = self.build_label_maps();
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

    /// Build code -> label maps from the documented variables' categories (detailed, or derived
    /// general labels for general selections — keyed by the codes that appear in the result).
    fn build_label_maps(&self) -> HashMap<&str, HashMap<&str, &str>> {
        let mut label_maps: HashMap<&str, HashMap<&str, &str>> = HashMap::new();
        for doc in &self.variable_docs {
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

    /// Render every table as text, inserting a `<VAR>_label` column after each detailed coded
    /// variable column. General columns are left as raw codes (the metadata has no general value
    /// labels). Raw codes are always kept; the labels are an additional column.
    fn render_tables_with_labels(&self, tab: &Tabulation) -> Result<String, MdError> {
        let label_maps = self.build_label_maps();
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
            "datasets": self.datasets,
            "dataset_reason": self.dataset_reason,
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
            // Both detailed and general columns can be labeled — the map is keyed by whichever
            // codes (detailed or general) actually appear in this column.
            if let Some(map) = label_maps.get(v.name.as_str()) {
                lm = Some(map);
                headers.push(format!("{}_label", v.name));
                align_left.push(true);
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

    fn general_request(name: &str) -> LlmRequestVariable {
        LlmRequestVariable {
            variable_mnemonic: name.to_string(),
            mnemonic: String::new(),
            general_detailed_selection: "G".to_string(),
            case_selection: false,
            request_case_selections: vec![],
        }
    }

    fn filter_var(name: &str, low: &str, high: &str) -> LlmRequestVariable {
        LlmRequestVariable {
            variable_mnemonic: name.to_string(),
            mnemonic: String::new(),
            general_detailed_selection: String::new(),
            case_selection: true,
            request_case_selections: vec![LlmCaseSelection {
                low_code: Some(serde_json::json!(low)),
                high_code: Some(serde_json::json!(high)),
            }],
        }
    }

    #[test]
    fn test_general_categories_first_label_rule() {
        // RELATE-like: detailed width 4, general width 2 -> divisor 100. General code = code/100,
        // labeled by the smallest detailed code in each group.
        let cats = vec![
            IpumsCategory::new("Head/householder", UniversalCategoryType::Value, IpumsValue::Integer(101)),
            IpumsCategory::new("Spouse", UniversalCategoryType::Value, IpumsValue::Integer(201)),
            IpumsCategory::new("2nd/3rd wife", UniversalCategoryType::Value, IpumsValue::Integer(202)),
            IpumsCategory::new("Child", UniversalCategoryType::Value, IpumsValue::Integer(301)),
            IpumsCategory::new("Adopted child", UniversalCategoryType::Value, IpumsValue::Integer(302)),
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
        var.general_width = Some(3); // same as detailed -> divisor 1
        assert_eq!(general_divisor(&var), 1);
        var.general_width = None;
        assert_eq!(general_divisor(&var), 1);
    }

    #[test]
    fn test_is_retryable_llm_error() {
        // Transient/filtered responses are worth a re-ask.
        assert!(is_retryable_llm_error(&MdError::LlmError(
            "Gemini response did not contain any candidates: {\"finishReason\":\"RECITATION\"}".into()
        )));
        assert!(is_retryable_llm_error(&MdError::LlmError(
            "the LLM API returned HTTP 429 (rate limit): ...".into()
        )));
        // A bad-request / auth failure is fatal — do not retry.
        assert!(!is_retryable_llm_error(&MdError::LlmError(
            "the LLM API returned HTTP 400: invalid argument".into()
        )));
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
        edu.general_width = Some(2); // has a general form
        let mut sex = var_with_n_categories("SEX", 2);
        sex.formatting = Some((1, 1));
        sex.general_width = Some(1); // no general form (width == general width)

        let catalog = build_catalog(&[edu, sex], 25);
        let edu_line = catalog.lines().find(|l| l.starts_with("EDUC")).unwrap();
        let sex_line = catalog.lines().find(|l| l.starts_with("SEX")).unwrap();
        assert!(edu_line.contains("; general"), "EDUC should be marked general: {edu_line}");
        assert!(!sex_line.contains("; general"), "SEX should not be marked general: {sex_line}");
    }

    #[test]
    fn test_catalog_compact_omits_value_labels() {
        // With the default cap (0) the catalog lists a count, not the actual codes/labels — this is
        // the trim: the refine pass resolves codes for the chosen variables instead.
        let marst = var_with_n_categories("MARST", 6);
        let catalog = build_catalog(std::slice::from_ref(&marst), 0);
        let line = catalog.lines().find(|l| l.starts_with("MARST")).unwrap();
        assert!(line.contains("value labels"), "should show a count hint: {line}");
        assert!(!line.contains("label 0"), "should NOT inline the actual labels: {line}");
    }

    #[test]
    fn test_force_detailed_overrides_general_selection() {
        // EDUC with a distinct general width: "G" would normally collapse to general categories.
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
            "--detailed should force the tabulation variable to detailed"
        );
    }

    #[test]
    fn test_general_request_downgrades_without_general_form() {
        // MARST-like: general_width equals the detailed width, so it has no real general form.
        let mut var = var_with_n_categories("MARST", 6);
        var.formatting = Some((1, 1));
        var.general_width = Some(1);
        assert!(!has_general_form(Some(&var)));
        let mut by_name: HashMap<String, &IpumsVariable> = HashMap::new();
        by_name.insert("MARST".to_string(), &var);

        // Even though the model asked for "G", it must come back detailed.
        let rv = build_request_variable(&general_request("MARST"), &by_name, false).unwrap();
        assert!(
            matches!(rv.general_detailed_selection, ist::GeneralDetailedSelection::Detailed),
            "a 'G' request on a variable with no general form should become detailed"
        );
    }

    #[test]
    fn test_refine_targets_flags_only_capped_filter_vars() {
        // BPL has many codes (capped out of the catalog); SEX has few (the model saw them all).
        let bpl = var_with_n_categories("BPL", 60);
        let sex = var_with_n_categories("SEX", 2);
        let mut by_name: HashMap<String, &IpumsVariable> = HashMap::new();
        by_name.insert("BPL".to_string(), &bpl);
        by_name.insert("SEX".to_string(), &sex);

        let llm = LlmAbacusRequest {
            uoa: "P".to_string(),
            request_variables: vec![],
            subpopulation: vec![filter_var("BPL", "1", "1"), filter_var("SEX", "2", "2")],
            category_bins: Vec::new(),
        };

        let targets = refine_targets(&llm, &by_name, 25);
        assert_eq!(targets.filters, vec!["BPL".to_string()]);
        assert!(targets.bins.is_empty());
        assert!(!targets.is_empty());
    }

    #[test]
    fn test_merge_refinements_replaces_filter_codes() {
        let mut llm = LlmAbacusRequest {
            uoa: "P".to_string(),
            request_variables: vec![],
            // First-pass (blind) guess for BPL, plus an untouched filter on another variable.
            subpopulation: vec![filter_var("BPL", "999", "999"), filter_var("SEX", "2", "2")],
            category_bins: Vec::new(),
        };
        let targets = RefineTargets {
            filters: vec!["BPL".to_string()],
            bins: vec![],
        };
        let refined = RefineResponse {
            subpopulation: vec![filter_var("BPL", "200", "210")],
            category_bins: Vec::new(),
        };

        merge_refinements(&mut llm, refined, &targets);

        // SEX filter is preserved; BPL is replaced with the refined codes.
        assert_eq!(llm.subpopulation.len(), 2);
        let bpl = llm
            .subpopulation
            .iter()
            .find(|v| v.name() == "BPL")
            .expect("BPL filter should remain");
        let low = bpl.request_case_selections[0]
            .low_code
            .as_ref()
            .and_then(|v| v.as_str())
            .unwrap();
        assert_eq!(low, "200", "BPL low code should be the refined value");
        assert!(llm.subpopulation.iter().any(|v| v.name() == "SEX"));
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
