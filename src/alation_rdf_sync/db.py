from __future__ import annotations
import asyncpg, json
from datetime import datetime, timezone

async def create_pool(dsn: str): return await asyncpg.create_pool(dsn)

async def batch_upsert_tables(pool, batch, run_id):
    # last_updated_at ($10) arrives as an ISO text from Alation JSON; cast so PG parses it.
    sql = """
      INSERT INTO alation_table (alation_key,alation_id,ds_id,schema_name,table_name,title,
        description,url,object_type,last_updated_at,custom_fields,run_id)
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10::timestamptz,$11::jsonb,$12)
      ON CONFLICT (alation_key) DO UPDATE SET
        alation_id=EXCLUDED.alation_id, title=EXCLUDED.title, description=EXCLUDED.description,
        url=EXCLUDED.url, object_type=EXCLUDED.object_type, last_updated_at=EXCLUDED.last_updated_at,
        custom_fields=EXCLUDED.custom_fields, ingested_at=now(), run_id=EXCLUDED.run_id"""
    async with pool.acquire() as c:
        await c.executemany(sql, batch)

async def reconcile_table_deletions(pool, ds_id, run_id):
    async with pool.acquire() as c:
        await c.execute("DELETE FROM alation_table WHERE ds_id=$1 AND run_id<>$2", ds_id, run_id)

async def batch_upsert_documents(pool, batch, run_id):
    sql = """
      INSERT INTO alation_document (alation_id,document_hub_id,folder_id,template_id,title,
        description,url,last_updated_at,custom_fields,run_id)
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8::timestamptz,$9::jsonb,$10)
      ON CONFLICT (alation_id) DO UPDATE SET
        document_hub_id=EXCLUDED.document_hub_id, folder_id=EXCLUDED.folder_id,
        template_id=EXCLUDED.template_id, title=EXCLUDED.title, description=EXCLUDED.description,
        url=EXCLUDED.url, last_updated_at=EXCLUDED.last_updated_at,
        custom_fields=EXCLUDED.custom_fields, ingested_at=now(), run_id=EXCLUDED.run_id"""
    async with pool.acquire() as c:
        await c.executemany(sql, batch)

async def reconcile_document_deletions(pool, run_id):
    # Documents are not ds-scoped: global reconcile against the current run.
    async with pool.acquire() as c:
        await c.execute("DELETE FROM alation_document WHERE run_id<>$1", run_id)

async def batch_upsert_data_products(pool, batch, run_id):
    sql = """
      INSERT INTO alation_data_product (product_id,name,short_description,description,product_type,
        visibility,contact_name,publisher,marketplace_id,url,licence,rights,audience,access_request,
        contract,record_sets,delivery_systems,recommended_products,locales,version,published_at,
        updated_at,custom_fields,run_id)
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,
        $11::jsonb,$12::jsonb,$13::jsonb,$14::jsonb,$15::jsonb,$16::jsonb,$17::jsonb,$18::jsonb,$19::jsonb,
        $20,$21::timestamptz,$22::timestamptz,$23::jsonb,$24)
      ON CONFLICT (product_id) DO UPDATE SET
        name=EXCLUDED.name, short_description=EXCLUDED.short_description, description=EXCLUDED.description,
        product_type=EXCLUDED.product_type, visibility=EXCLUDED.visibility, contact_name=EXCLUDED.contact_name,
        publisher=EXCLUDED.publisher, marketplace_id=EXCLUDED.marketplace_id, url=EXCLUDED.url,
        licence=EXCLUDED.licence, rights=EXCLUDED.rights, audience=EXCLUDED.audience,
        access_request=EXCLUDED.access_request, contract=EXCLUDED.contract, record_sets=EXCLUDED.record_sets,
        delivery_systems=EXCLUDED.delivery_systems, recommended_products=EXCLUDED.recommended_products,
        locales=EXCLUDED.locales, version=EXCLUDED.version, published_at=EXCLUDED.published_at,
        updated_at=EXCLUDED.updated_at, custom_fields=EXCLUDED.custom_fields,
        ingested_at=now(), run_id=EXCLUDED.run_id"""
    async with pool.acquire() as c:
        await c.executemany(sql, batch)

async def reconcile_data_product_deletions(pool, run_id):
    async with pool.acquire() as c:
        await c.execute("DELETE FROM alation_data_product WHERE run_id<>$1", run_id)

async def upsert_concepts(pool, batch, run_id):
    sql = """
      INSERT INTO ontology_concept (concept_uri,pref_label,alt_labels,definition,concept_type,run_id)
      VALUES ($1,$2,$3::jsonb,$4,$5,$6)
      ON CONFLICT (concept_uri) DO UPDATE SET
        pref_label=EXCLUDED.pref_label, alt_labels=EXCLUDED.alt_labels,
        definition=EXCLUDED.definition, concept_type=EXCLUDED.concept_type,
        ingested_at=now(), run_id=EXCLUDED.run_id"""
    async with pool.acquire() as c:
        await c.executemany(sql, batch)

async def list_document_hubs(pool):
    """Distinct document hubs in the landing zone with counts + sample titles, so an operator can
    identify the Glossary Hub id (design §11). Requires `sync` to have populated alation_document."""
    sql = """
      SELECT document_hub_id, count(*) AS documents,
             (array_agg(title ORDER BY title))[1:5] AS sample_titles
      FROM alation_document
      GROUP BY document_hub_id
      ORDER BY documents DESC"""
    async with pool.acquire() as c:
        return await c.fetch(sql)

async def get_last_success_at(pool, endpoint, ds_id=0):
    async with pool.acquire() as c:
        return await c.fetchval(
            "SELECT last_success_at FROM sync_state WHERE endpoint=$1 AND ds_id=$2", endpoint, ds_id)

async def record_sync_state(pool, endpoint, ds_id, run_id, status, seen=None, err=None,
                            started_at=None, duration_ms=None):
    async with pool.acquire() as c:
        await c.execute("""
          INSERT INTO sync_state (endpoint,ds_id,last_run_id,last_success_at,last_started_at,
            last_duration_ms,last_error,objects_seen)
          VALUES ($1,$2,$3, CASE WHEN $4='success' THEN now() END, $5,$6,$7,$8)
          ON CONFLICT (endpoint,ds_id) DO UPDATE SET last_run_id=EXCLUDED.last_run_id,
            last_success_at=COALESCE(EXCLUDED.last_success_at, sync_state.last_success_at),
            last_started_at=EXCLUDED.last_started_at, last_duration_ms=EXCLUDED.last_duration_ms,
            last_error=EXCLUDED.last_error, objects_seen=EXCLUDED.objects_seen""",
          endpoint, ds_id, run_id, status, started_at, duration_ms, err, seen)

_AGENT_SHORTLIST_SQL = """
  WITH unbound AS (
    SELECT alation_key AS object_key, title, description, lower(title) AS lbl
    FROM alation_table
    WHERE COALESCE(custom_fields->'Ontology URI'->>'value','') = ''
      AND COALESCE(custom_fields->'Binding Status'->>'value','Unreviewed') NOT IN ('Approved','Rejected')
      AND length(title) >= $2)
  SELECT 'table' AS object_kind, u.object_key, u.title, u.description,
         c.concept_uri, c.pref_label, c.definition, similarity(u.lbl, c.label_norm) AS sim
  FROM unbound u CROSS JOIN LATERAL (
    SELECT concept_uri, pref_label, definition, label_norm
    FROM ontology_concept WHERE u.lbl % label_norm
    ORDER BY similarity(u.lbl, label_norm) DESC LIMIT $1) c
  ORDER BY u.object_key, sim DESC"""

async def fetch_agent_shortlist(pool, max_candidates, min_label_length):
    """Unbound objects with their top-N trgm candidate concepts (the closed set the agent
    must choose from). Returns rows ordered by object then descending similarity."""
    async with pool.acquire() as c:
        return await c.fetch(_AGENT_SHORTLIST_SQL, max_candidates, min_label_length)

async def agent_suggestion_cached(pool, object_kind, object_key, agent_input_hash):
    """True if this exact (object, candidate-set) was already scored -> only re-score changed."""
    async with pool.acquire() as c:
        return bool(await c.fetchval("""
          SELECT 1 FROM uri_suggestion
          WHERE object_kind=$1 AND object_key=$2 AND method='agent' AND agent_input_hash=$3
          LIMIT 1""", object_kind, object_key, agent_input_hash))

async def upsert_agent_suggestion(pool, *, object_kind, object_key, object_label, concept_uri,
                                  score, rationale, model_version, prompt_version,
                                  agent_input_hash, status, run_id, rank=1):
    """Persist an agent suggestion. The conflict guard `WHERE method='agent'` ensures a shadow
    agent row never overwrites a trgm/production row sharing the same (object, concept)."""
    async with pool.acquire() as c:
        await c.execute("""
          INSERT INTO uri_suggestion (object_kind,object_key,object_label,concept_uri,score,rank,
            method,rationale,model_version,prompt_version,agent_input_hash,status,run_id)
          VALUES ($1,$2,$3,$4,$5,$6,'agent',$7,$8,$9,$10,$11,$12)
          ON CONFLICT (object_kind,object_key,concept_uri) DO UPDATE SET
            score=EXCLUDED.score, rank=EXCLUDED.rank, rationale=EXCLUDED.rationale,
            model_version=EXCLUDED.model_version, prompt_version=EXCLUDED.prompt_version,
            agent_input_hash=EXCLUDED.agent_input_hash, status=EXCLUDED.status, run_id=EXCLUDED.run_id
          WHERE uri_suggestion.method='agent'""",
          object_kind, object_key, object_label, concept_uri, score, rank,
          rationale, model_version, prompt_version, agent_input_hash, status, run_id)

async def upsert_gap_candidate(pool, object_kind, object_key, object_label, rationale, run_id):
    async with pool.acquire() as c:
        await c.execute("""
          INSERT INTO gap_candidate (object_kind,object_key,object_label,rationale,run_id)
          VALUES ($1,$2,$3,$4,$5)
          ON CONFLICT (object_kind,object_key) DO UPDATE SET
            object_label=EXCLUDED.object_label, rationale=EXCLUDED.rationale, run_id=EXCLUDED.run_id""",
          object_kind, object_key, object_label, rationale, run_id)

async def fetch_publishable_suggestions(pool, threshold):
    """Path-1 candidates: pending rank-1 suggestions >= publish_threshold, joined to the
    landing table for the Alation object id + current binding-field values (idempotency check).
    Tables only — suggest produces table suggestions in v1.0."""
    sql = """
      SELECT s.id, s.object_kind, s.object_key, s.concept_uri, s.score, t.alation_id,
             t.custom_fields->'Suggested Ontology URI'->>'value' AS cur_sugg,
             t.custom_fields->'Suggestion Confidence' ->>'value' AS cur_conf,
             t.custom_fields->'Binding Status'        ->>'value' AS cur_status
      FROM uri_suggestion s
      JOIN alation_table t ON s.object_kind='table' AND t.alation_key = s.object_key
      WHERE s.status='pending' AND s.rank=1 AND s.score >= $1
      ORDER BY s.id"""
    async with pool.acquire() as c:
        return await c.fetch(sql, threshold)

async def mark_suggestion_published(pool, suggestion_id):
    async with pool.acquire() as c:
        await c.execute("UPDATE uri_suggestion SET status='published' WHERE id=$1", suggestion_id)

async def materialise_approved_bindings(pool, run_id):
    """Forward-path step (runs in `sync`): when the catalog shows Binding Status=Approved with an
    empty Ontology URI, the steward has approved the suggestion -> stage a pending uri_binding whose
    concept_uri is the approved Suggested Ontology URI (design §5.4 path 2). Naturally idempotent:
    once promoted, Ontology URI is non-empty so the row no longer qualifies; an existing binding is
    only refreshed while still pending (never clobbers written/skipped state)."""
    sql = """
      INSERT INTO uri_binding (object_kind, object_key, alation_otype, alation_object_id,
                               concept_uri, source, writeback_state)
      SELECT 'table', t.alation_key, 'table', t.alation_id,
             t.custom_fields->'Suggested Ontology URI'->>'value', 'suggested-approved', 'pending'
      FROM alation_table t
      WHERE COALESCE(t.custom_fields->'Binding Status'->>'value','') = 'Approved'
        AND COALESCE(t.custom_fields->'Ontology URI'->>'value','') = ''
        AND COALESCE(t.custom_fields->'Suggested Ontology URI'->>'value','') <> ''
      ON CONFLICT (object_kind, object_key) DO UPDATE
        SET concept_uri = EXCLUDED.concept_uri, alation_object_id = EXCLUDED.alation_object_id
        WHERE uri_binding.writeback_state = 'pending'"""
    async with pool.acquire() as c:
        await c.execute(sql)

async def fetch_pending_bindings(pool):
    """Pending bindings joined to the latest landing zone for the current Ontology URI
    (the protect-human comparison value)."""
    sql = """
      SELECT b.object_kind, b.object_key, b.alation_otype, b.alation_object_id, b.concept_uri,
             t.custom_fields->'Ontology URI'->>'value' AS cur_uri
      FROM uri_binding b
      JOIN alation_table t ON b.object_kind='table' AND t.alation_key = b.object_key
      WHERE b.writeback_state='pending'
      ORDER BY b.object_key"""
    async with pool.acquire() as c:
        return await c.fetch(sql)

async def set_binding_state(pool, object_kind, object_key, state, *,
                            field_id=None, last_written_value=None, last_error=None):
    async with pool.acquire() as c:
        await c.execute("""
          UPDATE uri_binding SET writeback_state=$3,
            alation_field_id=COALESCE($4, alation_field_id),
            last_written_value=COALESCE($5, last_written_value),
            last_written_at=CASE WHEN $3='written' THEN now() ELSE last_written_at END,
            last_error=$6
          WHERE object_kind=$1 AND object_key=$2""",
          object_kind, object_key, state, field_id, last_written_value, last_error)

async def append_audit(pool, actor, object_kind, alation_object_id, field_id,
                       old_value, new_value, api_status, note=None):
    """Append-only mutation log (invariant §2.3.6). Table revokes UPDATE/DELETE."""
    async with pool.acquire() as c:
        await c.execute("""
          INSERT INTO writeback_audit
            (actor,object_kind,alation_object_id,field_id,old_value,new_value,api_status,note)
          VALUES ($1,$2,$3,$4,$5,$6,$7,$8)""",
          actor, object_kind, alation_object_id, field_id, old_value, new_value, api_status, note)
