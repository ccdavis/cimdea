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
- **Dataset inference**: `--dataset` is optional. When omitted, a first model pass picks the
  dataset(s) from those available under the data root (each `parquet/<name>` with a matching
  `layouts/<name>.layout.txt`), shown with their parsed year (e.g. "in 1900" -> `us1900m`). The
  choice and the model's reason appear in a **`## Data source`** section (a legend/citation, with an
  IPUMS attribution), so the user always sees which sample was used. Code: `list_available_datasets`,
  `choose_datasets`, `parse_year` in `nl_tabulation`.
- **Binned-variable labels**: when a variable is grouped via `category_bins`, the result table and
  legend show the bin labels ("0-9", "10-19", …), not the raw bin codes or the variable's underlying
  value labels. `apply_bin_labels` overrides the binned variable's documented categories with the
  bins' (code → value_label) pairs from the request.
- **Single-number results**: post-processing on the table — if the result is one table with exactly
  one row, it's rendered as a number ("MARST = 4 (Divorced): weighted estimate **28,710,122**
  (unweighted sample: 280,092).") instead of a one-row table. The system prompt steers "how many X"
  questions (incl. multi-condition like "Hispanic men who graduated college") to put every condition
  in `subpopulation` (AND-ed across variables) and tabulate one variable the conditions pin to a
  single value, so the result collapses to one row. `render_single_number`, `group_thousands`.
- **Catalog = tabulatable variables only**: `parse_variable_metadata` excludes (a) source variables
  (`is_source_variable`, raw un-integrated inputs) and (b) ALL string-typed variables. The engine
  reads every value as an integer (`isize`), so no string column can be grouped or range-filtered —
  a string variable produced a DuckDB type error (`BETWEEN` on a source var; `Invalid column type
  Text` when the model grabbed the structural record-type variable `RECTYPEP` as a constant to force
  a one-row count). Excluding strings drops source vars, structural record-type vars (`RECTYPE`/
  `RECTYPEP`), and alphanumeric code vars (`INDNAICS`, `OCCSOC`) — none tabulatable today — so the
  model only sees integer/fixed-coded integrated variables. (In us2015a: 615 integer + 20 fixed kept;
  4 string + sources dropped.)
- **JSON robustness (re-ask on recoverable failures)**: every model JSON reply goes through
  `complete_json_with_retry` (up to `MAX_JSON_ATTEMPTS = 3`). `gemini-3.5-flash` intermittently emits
  malformed JSON (duplicate key / missing delimiter) and occasionally an empty/filtered candidate
  (`RECITATION`); fresh sampling on a re-ask clears both. Fatal errors (HTTP 400/auth) fail fast via
  `is_retryable_llm_error`. The prompt also explicitly demands strictly valid JSON.
- **Constrained decoding (`responseSchema`) — tried and shelved.** The provider can do it
  (`LlmProvider::complete_json_schema`, Gemini `generationConfig.responseSchema`), and we restructured
  `category_bins` from a dynamic-keyed map to an **array** (still converted back to the Abacus map in
  `build_strict_request`) so the envelope is schema-expressible. But on `gemini-3.5-flash` constrained
  decoding ran **~3 minutes per call** (≈60× slower; a 2-pass query took ~6 min) and mis-filled bin
  bounds. So it's **not used** — the retry + strict-prompt give well-formed JSON at full speed. The
  capability is left in place for a future faster model. (The `category_bins` array form was kept; it's
  cleaner than the map and works fine.)
- **Catalog trimming**: the catalog inlines value labels only for variables with ≤
  `DEFAULT_CATEGORY_CATALOG_MAX` (=12) categories; bigger variables show just a count and the refine
  pass resolves their codes. Inline labels were ~60% of the catalog tokens (≈10k of 17k for us2015a);
  trimming keeps common demographic variables (SEX/MARST/RACE) inline for first-pass accuracy while
  dropping the long tail. Combined with dropping `responseSchema`, a simple count went from ~6 min to
  **~6 s** (age-bins ~16 s).
- **Server-side context caching (`--cache`, off by default).** Splits the prompt into a stable
  prefix (system instruction + catalog) and the live question. With `--cache`, `GeminiProvider`
  find-or-creates a Gemini `cachedContent` (keyed by a `displayName` = hash of model + system +
  catalog) and references it in `generateContent`, so the catalog is uploaded once and billed at the
  discounted cached rate on every reuse — verified: two runs of the same query reuse a single
  ~25k-token cache. It mainly cuts **cost**, not latency (our wall-clock is dominated by generation,
  not prompt processing). Best for a server / a burst of questions on one dataset; for a one-shot
  query it adds a cache-create round-trip, hence off by default. Required a fix: the catalog is now
  **sorted** (`build_catalog`) so its content — and thus the cache key — is deterministic (metadata
  load order from a `HashMap` was not). Caches use a 1-hour TTL (`CACHE_TTL`) and auto-expire. (For
  Phase 2 chat, `previous_interaction_id` on the Interactions API will cache the catalog across turns
  for free.)
- **IPUMS conventions section in the prompt**: domain facts about how variables encode concepts live
  in a dedicated `IPUMS conventions:` block (separate from request-format `Rules:`), so they're easy
  to grow. First entry: Hispanic/Latino is an ethnicity (`HISPAN`), independent of `RACE` — never
  answer "Hispanic" with a race variable. (More conventions expected.)
- **General categories are the default** for tabulation variables (matching the website: general by
  default, with a "details" checkbox) — but ONLY for variables that actually have a general form.
  Not every variable does: the parquet loader sets `general_width = column_width` when the source
  has none, so the real test is `general_divisor(var) > 1` (a general width strictly narrower than
  the detailed width). E.g. EDUC/RELATE/BPL/RACE have a general form; MARST/SEX/AGE/STATEFIP do not.
  This is enforced at three layers: (1) `build_catalog` marks eligible variables `"(P; general)"` so
  the model knows which can be general; (2) the system prompt tells it to request `"G"` only for
  marked variables (and only when detail isn't asked for / needed); (3) `build_request_variable`
  (`has_general_form`) is the safety net — a `"G"` request on a variable with no general form quietly
  becomes detailed, so the model can't produce a bogus general selection. Subpopulation **filters**
  stay detailed (their schema has no general/detailed field and defaults to detailed codes — that's
  where precise selection matters). The CLI `--detailed` flag forces detailed for every tabulation
  variable deterministically (overriding the model), the equivalent of the website checkbox
  (`NlConfig.detailed`).
- **Mechanical fields are filled from metadata, not the model**: `extract_start` (irrelevant to
  tabulation — only matters for fixed-width extract output), `mnemonic`, and especially
  `extract_width`. For a `"G"` selection, `try_from_json` feeds `extract_width` into the variable's
  `general_width`, which drives `general_divisor` (the code-collapsing divisor, e.g. 100 for
  RELATE vs RELATED). So we set `extract_width` to the variable's true general width from parquet
  metadata; if it's missing, we warn and fall back to detailed. For detailed selections the value
  is unused by tabulation (confirmed: `query_gen.rs` only applies `general_divisor` in the
  `is_general()` branch), so a placeholder of `1` is safe.
- **Grounding**: the prompt includes a catalog of real variable mnemonics + labels, with inline
  value labels for variables that have ≤ `DEFAULT_CATEGORY_CATALOG_MAX` (=12) codes (bounds prompt
  size; see the "Catalog trimming" note above). The model is told to use only catalog variables and
  the shown integer codes.
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

## Environments (`cimdea.toml`)

The dev/prod split lives in `cimdea.toml` (checked in). Each environment names the **file** holding
its Gemini API key (the key files themselves are gitignored) and the IPUMS **data root** to use:

```toml
default_environment = "dev"
[environments.dev]
api_key_file = "GEMINI_BILLED_KEY.txt"   # the developer's own Gemini account key
data_root    = "~/ipums_usa_data"        # ~ → $HOME; relative paths resolve against the config dir
[environments.prod]
api_key_file = "GEMINI_KEY.txt"          # the organization's Cloud Console project key
data_root    = "~/ipums_usa_data"        # set to the deployment's data location
```

`ask --env dev|prod` selects one (default: `default_environment`). The chosen environment supplies
the API key and data root. **Precedence:** an explicit `--api-key` or `--data-root` flag overrides
the environment; for the key, the order is `--api-key` → environment's key file → `GEMINI_API_KEY`.
With no config file present and no `--env`, the legacy path applies (`GEMINI_API_KEY` + `--data-root`).
Path rules: leading `~`, `$VAR`/`${VAR}`, and relative-to-config-dir. Code: `src/app_config.rs`.

## How to run

Always build/test with `--release` on this machine (debug builds swap and degrade the system).

```bash
# Typical run (key + data root come from cimdea.toml):
cargo run --release --bin ask -- --env dev --dataset us2019b \
  "How many people are there by marital status?"

# Without a config (supply key + data root directly):
GEMINI_API_KEY=... cargo run --release --bin ask -- \
  --product usa --data-root <root> --dataset us2019b \
  "How many people are there by marital status?"

# Useful flags: --env dev|prod, --config <path>, --show-request (print generated Abacus JSON),
#               --detailed (force detailed categories instead of the general default), -f json,
#               --model <id>, --api-key <key>, --provider gemini|gemini-interactions,
#               --dataset can repeat, -o <file>.

# Offline tests:
cargo test --release --test test_nl_tabulation
cargo test --release            # full suite

# Production-label test (needs the re-fetched sample):
CIMDEA_NL_DATA_ROOT=<root> cargo test --release --test test_nl_tabulation -- --ignored
```

## Verified

- Full suite green under `--release`: 124 lib + 17 tabulate + 5 nl_tabulation + 12 abacus CLI +
  doc-tests, 0 fail. (The offline `nl_tabulation` fixtures send `category_bins` as a JSON array, the
  current envelope shape — an earlier map form silently failed to deserialize.)
- **Real-LLM smoke test passed** (2026-06-26): simple detailed (MARST), subpopulation filter
  (married → SEX), general selection (EDUC `"G"` → collapsed codes), and category bins (AGE → 10-year
  groups) all generated valid Abacus JSON and tabulated.
- Value-label result column verified live (detailed MARST shows a `MARST_label` column) and asserted
  in the production-parquet integration test (`MARST_label` present in the rendered table).
- **Default model is now `gemini-3.5-flash`** (`DEFAULT_GEMINI_MODEL`), verified live on both the
  `generateContent` and Interactions endpoints with a **paid AI Studio key** (`GEMINI_BILLED_KEY.txt`,
  gitignored). On 3.5 Flash with the paid key the behaviors that the free tier blocked were confirmed
  end-to-end: general-by-default (EDUC → `"G"`, MARST → detailed, honoring the catalog `general`
  marker), two-pass filter refinement (BPL "born in Mexico" → `20000`), and derived general labels in
  the result table (EDUC general 0–11 with an `EDUC_label` column).

### Gemini API key setup (resolved)

The university Cloud key (`GEMINI_KEY.txt`, project `968779483292`) needed two things before it
worked with our existing `generateContent` path: (1) **enable** the *Gemini API*
(`generativelanguage.googleapis.com`) on the project, and (2) lift the **per-key API restriction**
(the key's "API restrictions" list excluded Gemini → `403 API_KEY_SERVICE_BLOCKED`). After
unrestricting the key (or adding "Generative Language API" to its allowed APIs), the default model
works. Note: the "Gemini Analytics API for structured data" (Conversational Analytics,
`geminidataanalytics.googleapis.com`) is a *different* API — it rejects API keys (needs OAuth2) and
is the wrong shape for our generic JSON generation, so we did not use it.

### API keys / quota

There are two keys (both gitignored, read via `GEMINI_API_KEY` env or `--api-key`):
- `GEMINI_BILLED_KEY.txt` — **paid AI Studio key (personal account), now the one to use.** No
  free-tier daily cap, and it can reach `gemini-3.5-flash` (the default). This is what the live
  verification above used.
- `GEMINI_KEY.txt` — the university Cloud key (project `968779483292`), on the **free tier**: we hit
  `429 RESOURCE_EXHAUSTED` with `...-FreeTier, limit: 20, model: gemini-2.5-flash` after a batch of
  tests. Note the **two-pass** refinement makes a *second* request on any filter/bin query, so it
  consumes quota faster; the per-day cap resets daily, and enabling billing raises it.

The `429` error message includes a hint. A future robustness add: a single bounded retry honoring
the response `RetryInfo.retryDelay` (helps per-minute RPM bursts from the two rapid two-pass calls;
won't help a daily cap).

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

1. ~~**Real-LLM smoke test.**~~ **DONE** (2026-06-26, paid key + `gemini-3.5-flash`). All exercised:
   detailed, subpopulation filter, general `"G"`, category bins, two-pass filter refinement, and
   general-by-default selection. To re-run: `export GEMINI_API_KEY=$(cat GEMINI_BILLED_KEY.txt)`,
   re-fetch us2019b (above), then run prompts with `--show-request`. Override the model with
   `--model` if a newer Flash ships; tune `nl_tabulation.rs::SYSTEM_PROMPT` if requests come back
   malformed.

2. ~~**Show value labels in the result table.**~~ **DONE.** The text result table now inlines a
   left-aligned `<VAR>_label` column after each *detailed* coded variable column (raw codes kept).
   Implemented entirely in `nl_tabulation` (`format_table_with_labels`) so `tabulate.rs`/`abacus`
   are untouched; labels come from the parquet catalog, the general flag from the executed
   `OutputColumn::RequestVar`. **General `"G"` selections are labeled too** via the "first label
   rule" (`general_categories` / `general_divisor` in `nl_tabulation`): the parquet metadata lacks
   explicit general-category markers, but general codes are `detailed_code / divisor` (same divisor
   the engine uses, `10^(detailed_width - general_width)`), and the label of the *smallest* detailed
   code in each group is conventionally the general label (e.g. RELATE 301 "Child" → general 3
   "Child"). So a `"G"` column gets a derived general label column and a `(general categories)`
   legend. Verified against production RELATE (general 1–13 = Head/householder … Institutional
   inmates) and unit-tested (`test_general_categories_first_label_rule`). Caveat: it's a heuristic
   ("typically" the first label); a variable that violates the convention could be mislabeled.
   JSON output is unchanged (consumers already get `tables` + a `variables` code→label map).

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
