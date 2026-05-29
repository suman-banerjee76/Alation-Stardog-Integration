# alation-stardog-bridge

A scheduled, stateless **adapter** that overlays your **Alation** data catalog onto a
**Stardog** knowledge graph and grows ontology-binding coverage over time — a closed
suggest → review → write-back loop.

Implements the v1.0 design in `../Alation-Stardog-Design-v1.0.md` (the authoritative contract).

---

## 1. Project scope

The adapter reads Alation (tables, glossary documents, data products) and Stardog (domain
concepts) on a cadence, lands everything in a **PostgreSQL** zone, proposes ontology bindings
for unbound objects, and writes confirmed bindings back to Alation. **Stardog** then exposes the
PostgreSQL zone as a **JDBC virtual graph** that emits DCAT/SKOS/DCTERMS + bridge (`alat:`)
triples on query, with full SQL pushdown.

```
            read-only token                      write-scoped token
Alation  ───────────────►  Adapter (stages)  ──────────────────────►  Alation
  ▲   tables/docs/DPs      sync → extract → suggest → writeback         custom fields
  │                              │                                       (Ontology URI…)
  │ steward approves            ▼                                            ▲
  │                        PostgreSQL  ◄── SPARQL/JDBC pushdown ── Stardog vg_alation
  └──────────────────────────────────────────  alat:realises  ◄────────────┘
```

**Four stages, run in order each cycle:**

| Stage | Token | Does |
|-------|-------|------|
| `sync` | read-only | Mirror tables (parallel by data source), documents, data products → PostgreSQL; reconcile deletions; materialise approved bindings. |
| `extract_concepts` | Stardog reader | SPARQL-pull domain concepts → `ontology_concept` (daily cadence). |
| `suggest` | — | Score unbound objects → `uri_suggestion` (engine `trgm`; optional `agent` shadow). |
| `writeback` | write-scoped | Publish rank-1 suggestions; promote steward-approved bindings to `Ontology URI` (protect-human, audited). |

**Key invariants (design §2.3):** no live Alation call during SPARQL; read vs write tokens never
shared; the adapter is the only PostgreSQL writer; the matcher only *proposes* — a steward
`Approved` triggers the write; write-back never overwrites a non-null human value; every catalog
mutation is recorded in an append-only audit; agent output is grounded to a closed candidate set.

---

## 2. Repository layout

```
alation-stardog-bridge/
├── pyproject.toml                 # package, deps, pytest config (pythonpath=src, asyncio auto)
├── Dockerfile                     # python:3.12-slim; entrypoint `python -m alation_rdf_sync`
├── config.yaml.example            # copy to config.yaml and fill in
│
├── migrations/
│   ├── 001_init.sql               # schema §3: landing zone + binding loop + ops state (+ GIN/BRIN/trgm indexes)
│   └── 002_roles.sql              # alation_writer / stardog_reader grants; writeback_audit append-only
│
├── src/alation_rdf_sync/
│   ├── __main__.py                # stage orchestration; jitter; resolve-fields / resolve-hub / render-sms2
│   ├── config.py                  # YAML load + secret resolution (env shim; Vault = deploy TODO)
│   ├── ids.py                     # UUIDv7 (monotonic run_id)
│   ├── alation.py                 # Alation HTTP client; the single CFV write site; field-id resolver
│   ├── stardog.py                 # SPARQL concept-extract query + fetch
│   ├── db.py                      # all PostgreSQL access (upserts, reconcile, bindings, audit)
│   ├── models.py                  # configurable source field-map; row builders; custom-field shaping
│   ├── stages/
│   │   ├── sync.py                # tables (fan-out + ThrottleGate) · documents · data products · materialise
│   │   ├── extract_concepts.py    # SPARQL → ontology_concept (cadence-gated)
│   │   ├── suggest.py             # primary + optional shadow engine
│   │   └── writeback.py           # path 1 publish · path 2 promote (protect-human + audit)
│   └── matchers/
│       ├── base.py                # Matcher interface
│       ├── trgm.py                # lexical baseline (:named → $n translation)
│       └── agent.py               # grounded LLM matcher (shadow, cached, temp 0)
│
├── stardog/
│   ├── vg_alation.properties      # JDBC virtual-graph connection + pushdown tuning
│   ├── vg_alation.sms2            # 7 SMS2 mapping blocks (tables/realises/suggested/custom/glossary/DP/wraps)
│   └── bridge.ttl                 # alat: vocabulary + DCAT/SKOS/DCTERMS imports
│
├── sql/
│   ├── suggest_trgm.sql           # trgm matcher (loaded by the suggest stage)
│   ├── validation_queries.sparql  # Q1–Q6 (VG sanity / acceptance)
│   ├── kpis.sql                   # dashboard metrics (§9)
│   └── alerts.sql                 # data-quality alert conditions (fire when rows returned)
│
├── deploy/
│   ├── cronjob.yaml               # CronJob (concurrencyPolicy: Forbid, 90-min deadline)
│   └── alerts.yaml                # Prometheus rules (job failed / deadline / no-recent-success)
│
└── tests/                         # pytest, all fakes — no live Alation/Stardog/PG needed
    ├── test_smoke.py  test_sync.py  test_suggest.py  test_writeback.py
    ├── test_agent.py  test_stardog_mappings.py  test_observability.py
```

---

## 3. Prerequisites

- **Python 3.11+**
- **PostgreSQL 15/16** with the `pg_trgm` extension available
- **Stardog** (with the PostgreSQL JDBC driver) for the virtual graph
- **Docker** + a registry, and **kubectl** access to a cluster (for deployment)
- Alation: a **read-only** API token, a **write-scoped** API token, and the four binding
  custom fields created (§4.2)
- Optional: `ANTHROPIC_API_KEY` if you enable the `agent` matcher

---

## 4. Assemble & test (developer setup)

```bash
git clone https://github.com/suman-banerjee76/Alation-Stardog-Integration.git
cd Alation-Stardog-Integration

python3 -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"          # base + pytest/pytest-asyncio/ruff
#   add the agent engine extra if you'll use it:
pip install -e ".[dev,agent]"    # also installs the anthropic SDK

pytest -q                        # 95 tests, all fakes — no live services required
ruff check src tests             # lint (optional)
```

`pyproject.toml` puts `src/` on the path and enables async tests, so no editable-install dance is
needed for testing. The test suite mocks Alation, Stardog, and Postgres end-to-end.

---

## 5. Provision PostgreSQL (Phase 0)

```bash
export DSN="postgresql://localhost:5432/alation_landing"

createdb alation_landing
psql "$DSN" -f migrations/001_init.sql     # schema + indexes (creates pg_trgm)
psql "$DSN" -f migrations/002_roles.sql    # run as superuser: roles + grants + append-only audit
```

`002_roles.sql` creates the least-privilege roles (`alation_writer` for the adapter,
`stardog_reader` for the virtual graph) and revokes UPDATE/DELETE on `writeback_audit`. Uncomment
and set the `CREATE ROLE … PASSWORD` lines for your environment.

---

## 6. Configure

```bash
cp config.yaml.example config.yaml
```

Edit `config.yaml` — key sections:

- **`alation.base_url`**, page sizes, `parallel_workers`, `data_source_ids` (`auto` or a list),
  `data_products_enabled`, `retry`.
- **`postgres.dsn`** — used directly by the adapter. Embed credentials here, or rely on libpq
  (`PG*` env / `.pgpass`).
- **`stardog.sparql_endpoint`**, `concept_extract_cadence_hours`.
- **`matcher`** — `engine: trgm` (production), optional `shadow_engine: agent`, thresholds.
- **`writeback`** — `enabled`, `field_names`, `field_ids` (see §7), `cfv_async`, job-poll knobs.

**Secrets.** Token/credential refs use `vault://<mount>/<path>/<field>`. `config.py` resolves them
through a real **HashiCorp Vault** client (`SecretResolver`):

- **Production** — set the standard Vault env vars; refs are read from Vault KV v2 (the default):
  ```bash
  export VAULT_ADDR="https://vault.internal:8200"
  export VAULT_TOKEN="<vault token>"
  export VAULT_NAMESPACE="team-a"     # optional (Vault Enterprise)
  export VAULT_KV_VERSION=2           # 1 for KV v1
  ```
  `vault://kv/alation/read-token` → `GET $VAULT_ADDR/v1/kv/data/alation`, field `read-token`.
  Postgres credentials inject automatically: if `postgres.dsn` has no userinfo, `user_secret_ref`
  /`pass_secret_ref` are resolved and spliced into the DSN. Resolved values are cached.

- **Local dev / tests** — when `VAULT_ADDR`/`VAULT_TOKEN` are unset, refs fall back to an env var
  `VAULT_<PATH>` (path upper-cased, `/` → `_`); or just embed credentials directly in `postgres.dsn`:
  ```bash
  export VAULT_KV_ALATION_READ-TOKEN="<read token>"
  export VAULT_KV_ALATION_WRITEBACK-TOKEN="<write token>"
  export VAULT_KV_STARDOG_READER-TOKEN="<stardog reader token>"
  ```

An unresolvable `vault://` ref fails loud (`VaultError`) rather than returning empty.

---

## 7. Resolve the custom-field ids

The four binding fields are addressed by numeric id. Resolve them once from their display names
and pin them (or leave `field_ids: 0` to auto-resolve by name at run time):

```bash
python -m alation_rdf_sync resolve-fields
# prints:
#   writeback:
#     field_ids:
#       ontology_uri: 123
#       suggested_ontology_uri: 124
#       suggestion_confidence: 125
#       binding_status: 126
```

Paste the block under `writeback:` in `config.yaml`. Adjust `writeback.field_names` first if your
catalog uses different labels.

---

## 8. Run locally

```bash
# Run all four stages in order (the scheduled behaviour):
python -m alation_rdf_sync

# Run a single stage:
python -m alation_rdf_sync sync
python -m alation_rdf_sync extract_concepts
python -m alation_rdf_sync suggest
python -m alation_rdf_sync writeback
```

Read-only stages (`sync`, `extract_concepts`, `suggest`) are safe to run first and ship before any
catalog write. Set `ALATION_RDF_SYNC_NO_JITTER=1` to skip the startup jitter for manual runs.

---

## 9. Stardog virtual graph

1. Place the PostgreSQL JDBC driver (`postgresql-42.7.x.jar`) in Stardog's `server/dbms`.
2. Load the bridge ontology into your domain DB under `<https://company.com/ns/alation/>`:
   ```bash
   stardog data add db_domain stardog/bridge.ttl
   ```
3. Resolve the Glossary Hub id and render the mapping. `vg_alation.sms2` is a template — its
   glossary block carries a `@GLOSSARY_DOCUMENT_HUB_ID@` placeholder that must be filled in:
   ```bash
   python -m alation_rdf_sync resolve-hub            # lists hubs (id, doc count, sample titles); run after a sync
   # set stardog.glossary_document_hub_id in config.yaml to the Glossary Hub id, then:
   python -m alation_rdf_sync render-sms2 > stardog/vg_alation.rendered.sms2
   ```
4. Register the virtual graph (point `vg_alation.properties` at the landing PG, `stardog_reader`):
   ```bash
   stardog-admin virtual add stardog/vg_alation.properties stardog/vg_alation.rendered.sms2 --name vg_alation
   ```
5. Validate — `sql/validation_queries.sparql` holds Q1–Q6. Q1 should return rows once tables are
   synced and at least one `Ontology URI` is set; confirm pushdown with the query plan / `EXPLAIN`.

---

## 10. Build & deploy the container

**Build and push:**

```bash
docker build -t registry.internal/alation-rdf-sync:1.0.0 .
docker push registry.internal/alation-rdf-sync:1.0.0
```

The image runs `python -m alation_rdf_sync` (all stages) as its entrypoint; `sql/` is copied
alongside the package and resolved at run time.

**Kubernetes** — the CronJob expects a Secret and a ConfigMap:

```bash
# 1. Config (mounted at /app/config.yaml)
kubectl create configmap alation-rdf-sync-config --from-file=config.yaml=./config.yaml

# 2. Secrets — production: point the adapter at Vault (or use a Vault Agent/CSI sidecar)
kubectl create secret generic alation-rdf-sync-secrets \
  --from-literal=VAULT_ADDR="https://vault.internal:8200" \
  --from-literal=VAULT_TOKEN="$VAULT_TOKEN"
  # add ANTHROPIC_API_KEY here if using the agent engine.
  # Local/no-Vault alternative: supply the VAULT_KV_ALATION_READ-TOKEN / *_WRITEBACK-TOKEN /
  # VAULT_KV_STARDOG_READER-TOKEN env vars instead (the env fallback).

# 3. Schedule (CronJob: hourly, concurrencyPolicy: Forbid, 90-min deadline)
kubectl apply -f deploy/cronjob.yaml

# 4. Alerts (Prometheus rules — needs kube-state-metrics)
kubectl apply -f deploy/alerts.yaml      # or load as a PrometheusRule CR
```

Update the image reference in `deploy/cronjob.yaml` to your registry/tag before applying.

---

## 11. Observability

- **`sql/kpis.sql`** — point a dashboard (Grafana/Metabase) at the landing PG (`stardog_reader`
  has SELECT): binding/suggestion coverage, acceptance rate, precision@1 (agent vs trgm in
  shadow), write-back success, conflicts, gap rate, per-stream duration, freshness lag.
- **`sql/alerts.sql`** — each query returns rows only when a condition is firing (stage failure,
  freshness lag, stale concept extract, write-back failures, recent conflicts, CFV API errors);
  wire to a SQL-exporter/scheduled check.
- **`deploy/alerts.yaml`** — infra-level Prometheus alerts (job failed, deadline exceeded, no
  recent success).

Every stage records `sync_state` (success/failed + timing), so failures surface in both the SQL
alerts and a non-zero job exit.

---

## 12. Build order & confirm-before-production

The code follows the design §8 build sequence (each phase independently shippable):
`0` provision → `1` sync(1 ds) → `2` sync(parallel+docs+DP) → `3` Stardog VG+mapping →
`4` extract+suggest(trgm) → `5` writeback publish → `6` writeback promote+audit →
`7` agent matcher (shadow) → `8` schedule+dashboards.

**Still environment-specific — verify against your Alation instance before production:**

- Custom-field **display names** (and that ids resolve) — §7.
- The picker **`value` encoding** for `Binding Status` (scalar string assumed; authoritative
  per-type schema is at `{instance}/openapi/custom_field_value/`). Isolated in
  `alation.py::set_custom_field_values`.
- Glossary **`document_hub_id`** — discover with `resolve-hub`, set `stardog.glossary_document_hub_id`,
  and `render-sms2` before registering the VG (§9).
- Alation table/document/data-product **JSON field names** — resolved through a tolerant,
  config-overridable field map (`alation.field_map`); override any key if your version differs (§6).
- Vault wiring (`config.py` `SecretResolver`): set `VAULT_ADDR`/`VAULT_TOKEN` and confirm the
  `vault://mount/path/field` refs match your KV layout/version (§6).

---

## License / status

Reference implementation of the v1.0 design. `embedding` matcher engine is an intentional v1.0
non-goal. See `../Alation-Stardog-Design-v1.0.md` for the full contract and `CLAUDE_CODE_PROMPT.md`
for the build brief.
