# `ask`: Natural-language → Abacus tabulation (progress + next steps)

Status as of 2026-06-24. Phase 1 is implemented, builds with `--release`, and is fully tested
(offline mock tests + a live run against production parquet). What remains is a real-LLM smoke
test (needs an API key) and the enhancements/phases below.

## Goal

Take an English description of a census/survey tabulation, use an LLM to convert it into an Abacus
JSON request, run it through the existing tabulation engine, and return the table plus a plain
explanation and documentation of the IPUMS variables (variable labels + value labels) drawn from the
embedded parquet metadata.

Later phases: (2) interactive chat with table formatting/plots; (3) microdata *extracts* (row-level
data) for further local processing, with the LLM distinguishing tabulation vs. extract requests.

## What's done (Phase 1)

New code:
- `src/llm.rs` — `LlmProvider` trait (pluggable); `GeminiProvider` (blocking `ureq`, Gemini
  `generateContent` with JSON response mode); `InteractionsProvider` (Gemini Interactions API, the
  GA "recommended" interface); `MockLlmProvider` (offline tests). Pick at the CLI with
  `--provider gemini` (default) or `--provider gemini-interactions`.
- `src/nl_tabulation.rs` — orchestration: load metadata → build grounded catalog → prompt model →
  parse response envelope → validate & repair against metadata → execute via the existing
  `AbacusRequest::try_from_json` + `tabulate::tabulate` path → assemble explanation + variable docs.
  Public surface: `NlConfig`, `NlResult`, `VariableDoc`, `run()`, `strip_json_fences()`.
- `src/bin/ask.rs` — CLI.
- `tests/test_nl_tabulation.rs` — 4 offline mock tests + 1 `#[ignore]`d production-parquet test.

Wiring:
- `Cargo.toml`: added `ureq = { version = "2", features = ["json", "tls"] }` and the `ask` bin.
- `src/mderror.rs`: added `MdError::LlmError(String)`.
- `src/lib.rs`: declared `pub mod llm;` and `pub mod nl_tabulation;`.

## How it works (key design decisions)

- **The model only chooses intent**: which variables to tabulate, general (`"G"`) vs detailed
  (`""`), subpopulation filters, category bins, and unit of analysis (`uoa`). Product and samples
  are taken from the CLI (not the model), guaranteeing they match the loaded metadata.
- **Mechanical fields are filled from metadata, not the model**: `extract_start` (irrelevant to
  tabulation — only matters for fixed-width extract output), `mnemonic`, and especially
  `extract_width`. For a `"G"` selection, `try_from_json` feeds `extract_width` into the variable's
  `general_width`, which drives `general_divisor` (the code-collapsing divisor, e.g. 100 for
  RELATE vs RELATED). So we set `extract_width` to the variable's true general width from parquet
  metadata; if it's missing, we warn and fall back to detailed. For detailed selections the value
  is unused by tabulation (confirmed: `query_gen.rs` only applies `general_divisor` in the
  `is_general()` branch), so a placeholder of `1` is safe.
- **Grounding**: the prompt includes a catalog of real variable mnemonics + labels, with inline
  value labels for variables that have ≤ 25 codes (bounds prompt size). The model is told to use
  only catalog variables and the shown integer codes.
- **Validation**: unknown variable mnemonics are a hard error (not silently dropped). Subpopulation
  filters get `case_selection = true` if they carry any selections, even if the model omits the flag.
- **Documentation comes from data, not the model**: variable labels and value labels in the output
  are pulled from `IpumsVariable.label` / `.categories`, so the facts are grounded.
- **Phase 3 hook**: the response envelope has `request_kind` ∈ {`tabulation`, `microdata_extract`}.
  Extract requests are recognized and reported as not-yet-implemented (no table), reserving the branch.

## Metadata reality check (important)

- **Variable/value labels live in production parquet embedded metadata** (the `variables`
  key-value blob, ~1110 entries for usa). `parquet_metadata.rs` parses it into
  `IpumsVariable.label` + `.categories` + `general_width`.
- The **committed `tests/data_root` parquet does NOT embed `variables`** — only version metadata.
  Its **layout files contain only `NAME RECTYPE START WIDTH TYPE`** (no labels). So with the
  committed sample, the catalog has variable *names* only (via parquet schema fallback), tabulation
  still works, but there are no labels/value-labels to document.
- `nl_tabulation::load_catalog_context` prefers `load_metadata_for_datasets_from_parquet` and falls
  back to layouts. Execution always goes through `try_from_json`, which independently loads layout
  metadata for the dataset (so the dataset needs a `layouts/<ds>.layout.txt`).

## Test data for labels (re-fetch each session)

A production sample with embedded labels was copied from gp1 into the **session scratchpad**, which
is **not persistent** — re-copy when resuming:

```bash
DR=<some-dir>/data_root
mkdir -p "$DR/parquet/us2019b"
ln -sfn /home/ccd/cimdea/tests/data_root/layouts "$DR/layouts"   # us2019b.layout.txt already exists
scp gp1.pop.umn.edu:/pkg/ipums/usa/output_data/current/parquet/us2019b/us2019b_usa.H.parquet "$DR/parquet/us2019b/"
scp gp1.pop.umn.edu:/pkg/ipums/usa/output_data/current/parquet/us2019b/us2019b_usa.P.parquet "$DR/parquet/us2019b/"
```

us2019b is ~24 MB (6 MB H + 18 MB P), single file per record type. Other samples are under
`/pkg/ipums/usa/output_data/current/parquet/`.

## How to run

Always build/test with `--release` on this machine (debug builds swap and degrade the system).

```bash
# Real run (needs a key):
GEMINI_API_KEY=... cargo run --release --bin ask -- \
  --product usa --data-root <root> --dataset us2019b \
  "How many people are there by marital status?"

# Useful flags: --show-request (print generated Abacus JSON), -f json, --model <id>,
#               --api-key <key>, --dataset can repeat, -o <file>.

# Offline tests:
cargo test --release --test test_nl_tabulation
cargo test --release            # full suite

# Production-label test (needs the re-fetched sample):
CIMDEA_NL_DATA_ROOT=<root> cargo test --release --test test_nl_tabulation -- --ignored
```

## Verified

- Full suite green under `--release`: 124 lib + 17 tabulate + 4 nl_tabulation + 12 abacus CLI +
  doc-tests, 0 fail.
- **Real-LLM smoke test passed** (2026-06-26) against live Gemini `gemini-2.5-flash`: simple
  detailed (MARST), subpopulation filter (married → SEX), general selection (EDUC `"G"` → collapsed
  codes), and category bins (AGE → 10-year groups) all generated valid Abacus JSON and tabulated.
- Value-label result column verified live (detailed MARST shows a `MARST_label` column) and asserted
  in the production-parquet integration test (`MARST_label` present in the rendered table).

### Gemini API key setup (resolved)

The university Cloud key (`GEMINI_KEY.txt`, project `968779483292`) needed two things before it
worked with our existing `generateContent` path: (1) **enable** the *Gemini API*
(`generativelanguage.googleapis.com`) on the project, and (2) lift the **per-key API restriction**
(the key's "API restrictions" list excluded Gemini → `403 API_KEY_SERVICE_BLOCKED`). After
unrestricting the key (or adding "Generative Language API" to its allowed APIs), the default model
works. Note: the "Gemini Analytics API for structured data" (Conversational Analytics,
`geminidataanalytics.googleapis.com`) is a *different* API — it rejects API keys (needs OAuth2) and
is the wrong shape for our generic JSON generation, so we did not use it.

### Rate limits / quota (observed 2026-06-26)

The project is on the Gemini **free tier**, which is low: we hit `429 RESOURCE_EXHAUSTED` with
`GenerateRequestsPerDayPerProjectPerModel-FreeTier, limit: 20, model: gemini-2.5-flash` after a
batch of live tests. Two things matter: (1) the **two-pass** refinement makes a *second* request on
any filter/bin query, so it consumes quota faster; (2) the per-day cap resets daily, and **enabling
billing** on the project raises the limits substantially. The `429` error message now includes a
hint. A future robustness add: a single bounded retry honoring the response `RetryInfo.retryDelay`
(helps per-minute RPM bursts from the two rapid two-pass calls; won't help a daily cap).

### Interactions API shape (empirically verified 2026-06-26)

`POST https://generativelanguage.googleapis.com/v1beta/interactions?key=KEY`, body:
```json
{ "model": "gemini-2.5-flash", "system_instruction": "...", "input": "...", "store": false }
```
The reply text is in the step whose `type == "model_output"`, concatenating `content[].text`;
there is also a leading `type:"thought"` step to skip. The interaction `id` is the handle for
`previous_interaction_id` (Phase 2 chat). Gotchas confirmed by probing: `response_format` enforces a
JSON *schema* and its `type` takes a JSON-schema type directly (`object`, `array`, `string`, ...) —
**not** OpenAI's `{type:"json_schema", json_schema:{...}}` wrapper; a bare `{"type":"object"}`
returns an empty `{}`. `generation_config.response_mime_type` is rejected ("Unknown parameter"). So
for our dynamically-shaped envelope we send **no** `response_format` and rely on the system prompt +
the `strip_json_fences` safety net (clean JSON confirmed without it).

---

## Next steps (resume here)

1. **Real-LLM smoke test (do first when the key arrives).**
   `export GEMINI_API_KEY=...`, re-fetch us2019b (above), then run a few prompts:
   - "How many people by marital status?"
   - "Sex breakdown of married people."
   - "People by educational attainment, general categories." (exercises `"G"`)
   - "Family income in bins of $10k." (exercises `category_bins`)
   Use `--show-request` to inspect the generated JSON. Confirm the default model id in
   `src/llm.rs` (`DEFAULT_GEMINI_MODEL`, currently `gemini-2.5-flash`) is still current; override
   with `--model` if Gemini returns a 404. Tune the system prompt in `nl_tabulation.rs::SYSTEM_PROMPT`
   if requests come back malformed.

2. ~~**Show value labels in the result table.**~~ **DONE.** The text result table now inlines a
   left-aligned `<VAR>_label` column after each *detailed* coded variable column (raw codes kept).
   Implemented entirely in `nl_tabulation` (`format_table_with_labels`) so `tabulate.rs`/`abacus`
   are untouched; labels come from the parquet catalog, the general flag from the executed
   `OutputColumn::RequestVar`. **Limitation discovered:** parquet metadata carries only *detailed*
   value labels plus a `general_width` — there are **no general category labels**. So a `"G"`
   selection shows raw general grouping codes (no label column) and the Variables legend now prints
   a one-line note instead of dumping detailed codes that don't match the general result codes.
   JSON output is unchanged (consumers already get `tables` + a `variables` code→label map). A
   future improvement would be to source general category labels (if/when present in metadata) so
   general columns can be labeled too.

3. ~~**Two-pass prompting for better filters/bins.**~~ **DONE** (generate → conditional refine).
   Pass 1 is unchanged (full request with best-guess codes). `refine_targets` then flags any
   subpopulation-filter or `category_bins` variable whose category count exceeds the catalog cap
   (so the model picked codes blind). For exactly those variables a second pass (`refine_codes` +
   `REFINE_SYSTEM_PROMPT`) sends their **full uncapped** value labels and the original request; the
   reply is merged with `merge_refinements` (surgical replace, everything else untouched). No
   filters/bins (or only small/labelless vars) → no second call, so the common case and the offline
   mock tests stay single-pass. A successful refine appends a note to `assumptions` (the first-pass
   assumptions can otherwise go stale). Verified live: "born in Mexico" flipped BPL from a blind
   `210` to the correct `20000` (confirmed `20000=Mexico` among BPL's 545 codes, not `3500=New
   Mexico`). Unit-tested offline (`test_refine_targets_*`, `test_merge_refinements_*`).
   **Caveat:** this doubles the request count on filter/bin queries — relevant under the low
   free-tier quota (see below).

4. **Validate case-selection / bin codes against metadata.** Right now codes are parsed but not
   checked against the variable's `categories`. Warn (or error) on codes that don't exist.

5. **Dataset/uoa ergonomics.** `--dataset` is currently required (the catalog must be built before
   the LLM call). Consider: infer `uoa` from the chosen variables' record types; let the model
   suggest a dataset from a provided list of available samples; support multiple datasets cleanly.

6. **Phase 2 — interactive chat.** Reuse `nl_tabulation::run` behind a REPL; add table formatting
   and plots. `NlResult` already separates explanation / docs / table for a UI. Prefer the
   `InteractionsProvider`: the Interactions API supports server-side conversation state via
   `previous_interaction_id` (the response `id`), so chat turns don't have to resend history. To use
   it, `complete_json` would need to grow a variant (or the provider gain a `continue_from(id, ...)`
   method) that threads the prior interaction id and omits `store:false`.

7. **Phase 3 — microdata extracts.** `extract.rs` is an empty stub. Build an extract executor
   (DuckDB `SELECT` of requested columns/rows to parquet/CSV), branch on
   `request_kind == "microdata_extract"`, and extend the prompt so the model routes
   "I need row-level data for attached characteristics / time-use construction" to extract.

8. **Provider expansion (optional).** Add an Anthropic/OpenAI `impl LlmProvider`; the trait and the
   `--provider` enum are already in place (`ProviderChoice` in `ask.rs`).

### Loose ends / cleanups
- `output_format` on the generated request is set to `"json"` but `try_from_json` hardcodes JSON
  internally anyway — harmless, just noted.
- Catalog can be large (~1100 vars). If prompt size becomes an issue, filter by record type once
  `uoa` is known, or move to the two-pass flow (#3).
- Nothing has been committed; the untracked `CLAUDE.md` was left alone. Decide what to commit
  (new modules + tests) when ready.
