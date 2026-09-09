# Abacus MCP + HTTP service

Exposes the same capability as the `ask` feature — English question → IPUMS tabulation — to any
MCP-aware AI (e.g. Claude Code), in a free-flowing chat instead of a one-shot CLI. The key difference
from `ask`: **there is no LLM in this code**. The *connecting* model is the intelligence; these servers
serve metadata and execute tabulations, and teach the model how via MCP `instructions` + tool
descriptions.

Because there's no LLM, the whole path is testable offline against `tests/data_root` (no API key).

## Architecture

```
                       ┌─────────────────────────────┐
  MCP client (Claude)──┤ abacus-mcp  (stdio JSON-RPC) ├─┐
                       └─────────────────────────────┘ │   ┌──────────────────────┐
                       ┌─────────────────────────────┐ ├──▶│ abacus_service::Service│──▶ DuckDB / parquet
  HTTP client ─────────┤ abacus-server (tiny_http)    ├─┘   │  (metadata + tabulate) │
                       └─────────────────────────────┘     └──────────────────────┘
```

- `src/abacus_service.rs` — the shared, transport- and LLM-agnostic core: dataset discovery, variable
  search, value-label lookup, and request validation/repair → execution. The `ask` pipeline
  (`src/nl_tabulation.rs`) and both servers all call it, so the repair logic lives in exactly one place.
- `src/mcp/` — the MCP server (`jsonrpc.rs` framing, `server.rs` dispatch). Stdio, newline-delimited
  JSON-RPC 2.0. `McpServer::handle` is transport-free and unit-tested.
- `src/http_api.rs` — the same `Service` exposed as REST/JSON over `tiny_http` (blocking, no async).
- Binaries: `src/bin/abacus_mcp.rs`, `src/bin/abacus_server.rs`.

The `tabulate` input is a *friendly intent envelope* (`abacus_service::TabRequest`): the caller supplies
only intent (which variables, general vs. detailed, subpopulation filters, category bins, unit of
analysis). Product, datasets, byte offsets, and widths are filled in from metadata, so a caller can't
get them wrong — the same guarantee `ask` gives.

## MCP tools

| Tool | Purpose |
|------|---------|
| `list_datasets` | List samples (with year) available under the data root. |
| `search_variables` | Find variables by keyword over mnemonic / label / value-label text. Returns mnemonic, label, record type, value-label count, general-form flag, and a guidance note when nothing matches. |
| `get_variables` | Full integer code → label list for named variables (to resolve filter / bin codes). |
| `tabulate` | Run a cross-tabulation from the intent envelope; returns the labeled table + the generated Abacus request. |

The `initialize` response carries an `instructions` string teaching the workflow
(`list_datasets → search_variables → get_variables → tabulate`), the envelope shape, the
counts-vs-breakdowns rule, and the IPUMS conventions (e.g. Hispanic origin = `HISPAN`, not a race).

## Running

Build (release only on this machine — debug builds swap the system):

```bash
cargo build --release
```

Data root resolution for both binaries: `--data-root`, else `--env`/`--config` reads it from
`cimdea.toml` (the API-key field there is ignored — these servers need no LLM key). `--product`
defaults to `usa`.

### MCP (stdio) with Claude Code

```bash
claude mcp add abacus -- /abs/path/to/target/release/abacus-mcp \
  --data-root /path/to/data_root --product usa
```

Then ask, e.g. *"how many people by marital status in us2015b?"* — Claude calls `search_variables`
then `tabulate` and reports the table. (The committed `tests/data_root` parquet has variable names but
no value labels; point `--data-root` at a production root for labels — see `docs/ask-nl-tabulation.md`
for fetching a labeled sample.)

Quick manual check without a client:

```bash
printf '%s\n' \
 '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}' \
 '{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"tabulate","arguments":{"dataset":"us2015b","uoa":"P","request_variables":[{"variable_mnemonic":"MARST"}]}}}' \
 | ./target/release/abacus-mcp --data-root tests/data_root
```

### HTTP

```bash
./target/release/abacus-server --data-root tests/data_root --product usa --port 8080

curl localhost:8080/datasets
curl 'localhost:8080/variables/search?dataset=us2015b&q=marital'
curl 'localhost:8080/variables?dataset=us2015b&names=MARST,SEX'
curl 'localhost:8080/catalog?dataset=us2015b'
curl -XPOST localhost:8080/tabulate \
  -d '{"datasets":["us2015b"],"uoa":"P","request_variables":[{"variable_mnemonic":"MARST"}]}'
```

Endpoints: `GET /health`, `GET /datasets`, `GET /variables/search?dataset=&q=&max=`,
`GET /variables?dataset=&names=`, `GET /catalog?dataset=`, `POST /tabulate` (body = the intent
envelope, plus optional `datasets`/`dataset` and `detailed`). When exactly one dataset exists under the
data root and none is specified, it is used automatically; otherwise a `dataset` must be given.

## Tests

```bash
cargo test --release                       # full suite (ask's test_nl_tabulation must stay green)
cargo test --release --test test_abacus_service   # Service: discovery, search, repair, execution
cargo test --release --test test_mcp              # MCP dispatcher: initialize / tools/list / tools/call
```

## Relation to `ask`

The `ask` CLI is unchanged in behavior. The metadata/repair/doc/render primitives it used were moved
into `abacus_service` and `nl_tabulation` now imports them (the intent-envelope types are imported under
their original `Llm*` names). `ask`'s own test suite is the guarantee that the extraction didn't change
its behavior.

## Not yet done

- MCP transport is stdio only (a streamable-HTTP MCP transport can wrap the same `McpServer::handle`).
- No microdata extracts (same Phase 3 gap as `ask`).
- No auth on the HTTP server — run it behind a trusted boundary.
