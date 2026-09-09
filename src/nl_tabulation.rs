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

use std::collections::{HashMap, HashSet};

use serde::de::DeserializeOwned;
use serde::Deserialize;

use crate::abacus_service::{
    self, apply_bin_labels, build_catalog, build_strict_request, build_variable_docs,
    list_available_datasets, load_catalog_context, loaded_variables, parse_year,
    render_categories_inline, CategoryBinGroup as LlmCategoryBinGroup,
    RequestVar as LlmRequestVariable, TabRequest as LlmAbacusRequest, DEFAULT_CATEGORY_CATALOG_MAX,
};
use crate::input_schema_tabulation as ist;
use crate::ipums_metadata_model::IpumsVariable;
use crate::llm::LlmProvider;
use crate::mderror::MdError;
use crate::request::AbacusRequest;
use crate::tabulate::{self, TableFormat, Tabulation};

// The metadata/repair/catalog/doc/rendering primitives this module relies on live in
// [crate::abacus_service] so the MCP and HTTP servers can reuse them. The intent-envelope types are
// imported above under their original `Llm*` names so the LLM-specific orchestration below reads the
// same as before. Documentation for variables and the JSON-fence stripper keep their public paths
// here for backward compatibility.
pub use crate::abacus_service::{strip_json_fences, VariableDoc};

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
    /// Cache the (system prompt + catalog) prefix server-side so repeated questions about the same
    /// dataset only send the question. Worth it for a server or a burst of queries; for a single
    /// one-shot query it adds a cache-create round-trip, so it's off by default.
    pub use_cache: bool,
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

// ---------------------------------------------------------------------------
// The lenient shape we parse out of the model. The inner request types
// ([LlmAbacusRequest] etc.) live in [crate::abacus_service]; only the outer
// envelope (with the model's prose) is LLM-specific and stays here.
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

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

/// Translate `prompt` into a tabulation, run it, and return the result with documentation.
pub fn run(provider: &dyn LlmProvider, prompt: &str, cfg: &NlConfig) -> Result<NlResult, MdError> {
    // 0. Decide which dataset(s) to use: the caller's, or let the model pick one from those
    // available under the data root (e.g. "in 1900" -> an 1900 sample).
    let (datasets, dataset_reason) = if cfg.datasets.is_empty() {
        let available = list_available_datasets(cfg.data_root.as_deref())?;
        let chosen = choose_datasets(provider, prompt, &available)?;
        (chosen.datasets, Some(chosen.reason))
    } else {
        (cfg.datasets.clone(), None)
    };

    // 1. Load metadata for the catalog and for value-label documentation.
    let ctx = load_catalog_context(&cfg.product, cfg.data_root.clone(), &datasets)?;
    let variables = loaded_variables(&ctx)?;
    let by_name: HashMap<String, &IpumsVariable> = variables
        .iter()
        .map(|v| (v.name.to_uppercase(), v))
        .collect();

    // 2. Build the prompt and ask the model. The static context (product + catalog) is separated
    // from the question so it can be cached server-side and reused across questions.
    let cat_max = cfg
        .category_catalog_max
        .unwrap_or(DEFAULT_CATEGORY_CATALOG_MAX);
    let catalog = build_catalog(variables, cat_max);
    let static_context = build_static_context(&cfg.product, &datasets, &catalog);
    let question = format!("User request: {prompt}");

    let envelope: LlmTabulationResponse = retry_json("tabulation response", || {
        if cfg.use_cache {
            let display_name =
                cache_display_name(provider.model_name(), SYSTEM_PROMPT, &static_context);
            provider.complete_json_cached(&display_name, SYSTEM_PROMPT, &static_context, &question)
        } else {
            provider.complete_json(SYSTEM_PROMPT, &format!("{static_context}\n\n{question}"))
        }
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
    let strict = build_strict_request(
        &llm_request,
        &cfg.product,
        &cfg.data_root,
        cfg.detailed,
        &datasets,
        &by_name,
        &mut warnings,
    )?;

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
        .filter(|v| {
            matches!(
                v.general_detailed_selection,
                ist::GeneralDetailedSelection::General
            )
        })
        .map(|v| v.variable_mnemonic.clone())
        .collect();
    let mut variable_docs = build_variable_docs(&doc_names, &by_name, &general_names);

    // For binned variables the meaningful categories are the bins themselves (e.g. "0-9"), not the
    // variable's raw value labels — so the result table and legend show the bin labels.
    apply_bin_labels(&mut variable_docs, &llm_request);

    // 4. Serialize and run through the normal Abacus path (which loads layout metadata itself).
    let request_json = serde_json::to_string(&strict).map_err(|err| {
        MdError::Msg(format!(
            "could not serialize the generated Abacus request: {err}"
        ))
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

/// Ask the model to choose dataset(s) from `available` for `prompt`, validating its answer.
fn choose_datasets(
    provider: &dyn LlmProvider,
    prompt: &str,
    available: &[String],
) -> Result<ChosenDatasets, MdError> {
    let listing = build_dataset_listing(available);
    let user = format!("User request: {prompt}\n\nAvailable datasets (name — year):\n{listing}\n");
    let choice: LlmDatasetChoice = retry_json("dataset-selection response", || {
        provider.complete_json(DATASET_SELECT_SYSTEM_PROMPT, &user)
    })?;

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
- "subpopulation": OPTIONAL array of filters restricting which records are counted. Each filter is {"variable_mnemonic": "<NAME>", "case_selection": true, "request_case_selections": [{"low_code": "<code>" or null, "high_code": "<code>" or null}]}. A selection keeps records whose value is between low_code and high_code inclusive; set one bound to null for an open-ended range. Filter codes are DETAILED codes. If you only know the general category code for a variable marked "general" in the catalog, add "general_detailed_selection": "G" to that filter and give general codes; each general code is expanded to the detailed range it covers automatically.
- "category_bins": OPTIONAL array; each entry groups one continuous variable into bins: {"variable": "<NAME>", "bins": [{"code": <int>, "value_label": "<text>", "low": <int>, "high": <int>}]}. For a CLOSED range set BOTH low and high (e.g. the group "10-19" is low 10, high 19). OMIT a bound only for an open-ended end (e.g. "65+" is low 65 with no high; "under 5" is high 4 with no low). Give each bin a distinct integer "code". A binned variable MUST also appear in "request_variables" (the bins regroup that variable's tabulation).

Rules:
- Use ONLY variable mnemonics from the provided catalog. Never invent variable names.
- Request "G" ONLY for tabulation variables marked "general" in the catalog, and prefer "G" for those; use "" for all other tabulation variables and whenever detail is requested or required. Subpopulation FILTERS use detailed value codes (the exact integer codes shown) unless the filter itself is marked "G" with general codes.
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

/// The static part of the prompt (product + datasets + catalog) — everything except the user's
/// question. Stable for a given dataset, so it can be cached server-side and reused.
fn build_static_context(product: &str, datasets: &[String], catalog: &str) -> String {
    format!(
        "IPUMS product: {product}\n\
         Dataset(s) to tabulate: {datasets}\n\n\
         Variable catalog (MNEMONIC — label (record type) [code=label; ...]):\n\
         {catalog}",
        datasets = datasets.join(", "),
    )
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
        append_var_codes(
            &mut out,
            &key.to_uppercase(),
            "grouping (category bins)",
            by_name,
        );
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
    retry_json("refinement response", || {
        provider.complete_json(REFINE_SYSTEM_PROMPT, &user)
    })
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
            llm.category_bins
                .retain(|g| g.variable.to_uppercase() != upper);
            llm.category_bins.push(group.clone());
        }
    }
}

// ---------------------------------------------------------------------------
// Helpers + rendering
// ---------------------------------------------------------------------------

/// How many times to ask the model for a JSON reply before giving up.
const MAX_JSON_ATTEMPTS: usize = 3;

/// Call the model and parse its JSON reply into `T`, re-asking a few times on a recoverable failure.
/// Low-temperature sampling occasionally emits malformed JSON (a stray duplicate key or missing
/// delimiter), and Gemini sometimes returns an empty/filtered candidate (e.g. a `RECITATION`
/// finish); fresh sampling on a re-ask almost always clears both. Fatal provider errors (bad
/// request, auth) are not retried — they propagate immediately.
fn retry_json<T, F>(what: &str, mut call: F) -> Result<T, MdError>
where
    T: DeserializeOwned,
    F: FnMut() -> Result<String, MdError>,
{
    let mut last_error: Option<String> = None;
    for _ in 0..MAX_JSON_ATTEMPTS {
        match call() {
            Ok(raw) => {
                let cleaned = strip_json_fences(&raw);
                match serde_json::from_str::<T>(&cleaned) {
                    Ok(value) => return Ok(value),
                    Err(err) => {
                        last_error = Some(format!("invalid JSON ({err}); reply was: {cleaned}"))
                    }
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

/// A stable cache key for a (model, system prompt, static context) triple, so repeated requests with
/// the same catalog reuse one server-side cache. `DefaultHasher` uses fixed keys, so this is stable
/// across processes.
fn cache_display_name(model: &str, system: &str, context: &str) -> String {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    model.hash(&mut hasher);
    system.hash(&mut hasher);
    context.hash(&mut hasher);
    format!("cimdea-{:016x}", hasher.finish())
}

/// Whether an LLM error looks transient enough to be worth re-asking (an empty/filtered candidate or
/// a rate-limit/server hiccup), as opposed to a fatal error (bad request, auth) that a retry can't fix.
fn is_retryable_llm_error(err: &MdError) -> bool {
    let message = err.to_string();
    [
        "RECITATION",
        "did not contain any candidates",
        "contained no text",
        "HTTP 429",
        "HTTP 500",
        "HTTP 503",
    ]
    .iter()
    .any(|marker| message.contains(marker))
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
            if let Some(single) = abacus_service::render_single_number(tab, &self.variable_docs) {
                // A one-row result is really a single value; present it as a number, not a table.
                out.push_str(&single);
            } else {
                match format {
                    // For the text table, inline value labels next to the raw codes.
                    TableFormat::TextTable => out.push_str(
                        &abacus_service::render_tables_with_labels(tab, &self.variable_docs)?,
                    ),
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::abacus_service::CaseSelection as LlmCaseSelection;
    use crate::ipums_metadata_model::{IpumsCategory, IpumsDataType, IpumsValue, UniversalCategoryType};

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
    fn test_is_retryable_llm_error() {
        // Transient/filtered responses are worth a re-ask.
        assert!(is_retryable_llm_error(&MdError::LlmError(
            "Gemini response did not contain any candidates: {\"finishReason\":\"RECITATION\"}"
                .into()
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
    fn test_cache_display_name_is_stable_and_distinct() {
        let a = cache_display_name("gemini-3.5-flash", "sys", "catalog");
        let b = cache_display_name("gemini-3.5-flash", "sys", "catalog");
        assert_eq!(a, b, "same inputs must give the same cache key (across runs)");
        assert_ne!(a, cache_display_name("gemini-3.5-flash", "sys", "catalog2"));
        assert_ne!(a, cache_display_name("other-model", "sys", "catalog"));
        assert!(a.starts_with("cimdea-"));
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
