from __future__ import annotations
import json, time
from datetime import datetime, timezone, timedelta
from ..stardog import fetch_concepts
from .. import db

ALT_SEP = "␞"  # U+241E: GROUP_CONCAT separator for altLabels (design §5.2)

async def run(cfg, pool, run_id):
    """Stage 2. Pull domain concepts from Stardog (daily cadence) -> ontology_concept.
    Upsert-only: the design does not reconcile concept deletions in v1.0 (§5.2).
    Records failed sync_state (and re-raises) so failures are observable, not just a crashed run."""
    cadence_h = cfg["stardog"].get("concept_extract_cadence_hours", 24)
    last = await db.get_last_success_at(pool, "concept_extract", 0)
    if last is not None and _within(last, cadence_h):
        return  # cadence gate: extracted recently, skip this run
    started, t0 = datetime.now(timezone.utc), time.monotonic()
    try:
        token = cfg["stardog"].get("reader_token_secret_ref", "")
        bindings = await fetch_concepts(cfg["stardog"]["sparql_endpoint"], token)
        rows = _concept_rows(bindings, run_id)
        if rows:
            await db.upsert_concepts(pool, rows, run_id)
        await db.record_sync_state(pool, "concept_extract", 0, run_id, "success",
                                   seen=len(rows), started_at=started, duration_ms=_ms(t0))
    except Exception as e:  # noqa: BLE001 - record failure state, then propagate
        await db.record_sync_state(pool, "concept_extract", 0, run_id, "failed",
                                   err=str(e), started_at=started, duration_ms=_ms(t0))
        raise

def _ms(t0: float) -> int:
    return int((time.monotonic() - t0) * 1000)

def _within(last: datetime, hours: float) -> bool:
    if last.tzinfo is None:
        last = last.replace(tzinfo=timezone.utc)
    return datetime.now(timezone.utc) - last < timedelta(hours=hours)

def _concept_rows(bindings, run_id):
    """SPARQL results bindings -> ontology_concept row tuples. Skips rows missing the
    PK (concept_uri) or the NOT NULL pref_label; splits alt_labels on U+241E."""
    rows = []
    for b in bindings:
        uri, label = _v(b, "concept_uri"), _v(b, "pref_label")
        if not uri or not label:
            continue
        alts = [a for a in (_v(b, "alt_labels") or "").split(ALT_SEP) if a]
        rows.append((uri, label, json.dumps(alts), _v(b, "definition"), _v(b, "concept_type"), run_id))
    return rows

def _v(b, k):
    cell = b.get(k)
    return cell.get("value") if cell else None
