You are implementing a production adapter from a finished design. Two files are the contract:

- `../Alation-Stardog-Design-v1.0.md` — authoritative spec (architecture, schema, stage algorithms, config, build order §8, invariants §2.3).
- `README.md` — this repo's scaffold map and build order.

The repo is a **scaffold**: DDL, Stardog SMS2/TTL, SQL, config, Dockerfile and CronJob are complete; Python stage/matcher/client bodies are skeletons marked `TODO` / `NotImplementedError`. Your job is to fill those in, in the §8 build order, without breaking the scaffold's contracts.

**Workflow**
1. Read both files above, then run `rg -n "TODO|NotImplementedError" src` to enumerate the work.
2. Produce a short PLAN for the phase we're starting (Phase 1: `sync` for a single data source), then wait for my go-ahead before writing code. Show plans before bulk changes; ask before guessing on any judgment call rather than assuming.
3. Implement one phase at a time. After each: `python -m py_compile $(find src tests -name '*.py')` must pass, and `pytest -q` must stay green. Add tests for new logic (use fakes/mocks for Alation, Stardog, and Postgres — no live credentials exist in this environment).
4. Keep each phase to a focused diff; summarize what changed and stop for review before the next phase.

**Build order (design §8)** — Phase 0 (provision/DDL) is done in the scaffold. Start at Phase 1.
1 `sync` tables, one ds, idempotent upsert + deletion reconcile · 2 parallel fan-out + documents + data products + watermarks · 3 verify Stardog VG + mapping (Q1–Q5) · 4 `extract_concepts` + `suggest` (engine `trgm`) + provisional mapping (Q6) · 5 `writeback` path 1 (publish suggestions) · 6 `writeback` path 2 (promote approved) + audit · 7 `suggest` engine `agent` in shadow mode · 8 schedule + dashboards.

**Invariants you must not break (design §2.3)**
1. No Alation HTTP call during SPARQL evaluation — Stardog reads only Postgres.
2. Forward path uses the read-only token; only `writeback` uses the write-scoped token. Never share them.
3. The adapter is the only writer to Postgres (`alation_writer` / `stardog_reader` roles only).
4. The matcher/agent only *proposes*; only a steward `Binding Status=Approved` triggers a write to `Ontology URI`.
5. Write-back never overwrites a non-null human `Ontology URI` (conflict → log to `writeback_audit`, skip).
6. Every catalog mutation is recorded in `writeback_audit` (append-only).
7. Agent output is grounded: a returned `concept_uri` must exist in the supplied candidate set (and in `ontology_concept`), else discard and log. Pin `model_version` + `prompt_version`; temperature 0; cache on `agent_input_hash`; only re-score changed objects.

**Conventions**
- Python 3.11+, async (`httpx`, `asyncpg`); `pyyaml` for config. Agent engine uses the `anthropic` SDK (optional extra).
- `sql/suggest_trgm.sql` uses `:named` params; asyncpg needs `$1..` — translate at load time, don't rewrite the file's logic.
- Respect Alation throttling: `ds_id`-filtered, `limit≤1500`, 504→halve page, 429→`Retry-After` + single-thread fallback. CFV write API is single-threaded with its own `Retry-After`.

**Open inputs — ask me, do not guess**: the four `writeback.field_ids` (resolve via Custom Fields API), the exact Custom Field Values API payload shape for our Alation version, the column-realisation URI scheme, and Document Hubs vs legacy `/v2/term/` (design §11).

Begin by reading the two contract files and the `TODO` list, then give me the Phase 1 plan.
