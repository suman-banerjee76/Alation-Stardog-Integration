from __future__ import annotations
import asyncio, time
from datetime import datetime, timezone
from ..alation import AlationClient, resolve_field_ids, BINDING_FIELD_NAMES
from .. import db

# Alation object type (otype) per landing object_kind. Only tables carry suggestions in v1.0.
# ASSUMPTION: the table otype string for the CFV API is "table" (confirm per Alation version).
_OTYPE = {"table": "table"}

async def run(cfg, pool, run_id):
    """Stage 4 (WRITE-SCOPED, single-threaded). Path 1 publish suggestions; Path 2 promote approved.

    Invariant §2.3.2: this stage is the only holder of the write-scoped token."""
    wb = cfg["writeback"]
    if not wb.get("enabled"):
        return
    al = AlationClient(cfg["alation"]["base_url"], cfg.write_token,
                       cfv_async=wb.get("cfv_async", True),
                       job_poll_interval=wb.get("job_poll_interval", 2.0),
                       job_poll_max=wb.get("job_poll_max", 30))
    started, t0 = datetime.now(timezone.utc), time.monotonic()
    exc = None
    try:
        # Resolve the four binding-field ids: pinned ids win; the rest are looked up by name
        # via the Custom Fields API (design §4.1). Cached back onto cfg for this run.
        cfg["writeback"]["field_ids"] = await _resolved_field_ids(al, cfg)
        await _publish_suggestions(al, pool, cfg, run_id)
        await _promote_approved(al, pool, cfg, run_id)
    except Exception as e:  # noqa: BLE001 - record failure state below, then propagate
        exc = e
    finally:
        await al.aclose()
    dur = int((time.monotonic() - t0) * 1000)
    if exc is None:
        await db.record_sync_state(pool, "writeback", 0, run_id, "success",
                                   started_at=started, duration_ms=dur)
    else:
        await db.record_sync_state(pool, "writeback", 0, run_id, "failed",
                                   err=str(exc), started_at=started, duration_ms=dur)
        raise exc

async def _resolved_field_ids(al, cfg):
    wb = cfg["writeback"]
    names = wb.get("field_names") or BINDING_FIELD_NAMES
    return await resolve_field_ids(al, names, pinned=wb.get("field_ids"))

async def _publish_suggestions(al, pool, cfg, run_id):
    """rank-1 & score>=publish_threshold -> set Suggested URI + Confidence + Status=Suggested.
    Idempotent: skip the write when the object already carries the same values. Per-object
    isolation: one object's failure leaves it `pending` (retried next run) without blocking
    the rest. (Write-back audit is wired in Phase 6.)"""
    ids = cfg["writeback"]["field_ids"]
    threshold = cfg["matcher"]["publish_threshold"]
    rows = await db.fetch_publishable_suggestions(pool, threshold)
    for r in rows:
        otype = _OTYPE.get(r["object_kind"])
        if otype is None:
            continue
        concept_uri, score = r["concept_uri"], float(r["score"])
        try:
            if (r["cur_sugg"] == concept_uri and r["cur_status"] == "Suggested"
                    and _conf_eq(r["cur_conf"], score)):
                await db.mark_suggestion_published(pool, r["id"])   # already equal -> no API call
                continue
            res = await _cfv_put(al, cfg, otype, r["alation_id"], [
                # ASSUMPTION: Binding Status picker accepts the string "Suggested".
                (ids["suggested_ontology_uri"], concept_uri),
                (ids["suggestion_confidence"], score),
                (ids["binding_status"], "Suggested"),
            ])
            if not res.ok:
                raise RuntimeError(res.detail or f"CFV status {res.status}")
            await db.mark_suggestion_published(pool, r["id"])
        except Exception:  # noqa: BLE001 - isolate per-object; leave pending for retry
            continue

async def _promote_approved(al, pool, cfg, run_id):
    """Path 2: promote each pending uri_binding to the authoritative Ontology URI.
    Protect-human (§2.3.5): never overwrite a non-null human value that differs -> skip + audit.
    Converged (cur == concept): mark written, no API call, no audit (no mutation).
    Every actual catalog mutation (and every conflict skip) is appended to writeback_audit
    (§2.3.6). Per-binding isolation; single-threaded."""
    field_id = cfg["writeback"]["field_ids"]["ontology_uri"]
    bindings = await db.fetch_pending_bindings(pool)
    for b in bindings:
        kind, key = b["object_kind"], b["object_key"]
        oid, concept, cur = b["alation_object_id"], b["concept_uri"], b["cur_uri"]
        has_human = cur not in (None, "")
        try:
            if has_human and cur != concept:
                await db.set_binding_state(pool, kind, key, "skipped_conflict", field_id=field_id)
                await db.append_audit(pool, "writeback:promote", kind, oid, field_id,
                                      cur, concept, None, "human value present")
                continue
            if cur == concept:
                await db.set_binding_state(pool, kind, key, "written",
                                           field_id=field_id, last_written_value=concept)
                continue  # already converged -> no call, no mutation to audit
            res = await _cfv_put(al, cfg, b["alation_otype"], oid, [(field_id, concept)])
            if res.ok:
                await db.set_binding_state(pool, kind, key, "written",
                                           field_id=field_id, last_written_value=concept)
            else:
                await db.set_binding_state(pool, kind, key, "failed",
                                           field_id=field_id, last_error=res.detail or f"HTTP {res.status}")
            await db.append_audit(pool, "writeback:promote", kind, oid, field_id,
                                  cur, concept, res.status)
        except Exception as e:  # noqa: BLE001 - isolate per-binding; record + audit the failure
            await db.set_binding_state(pool, kind, key, "failed", field_id=field_id, last_error=str(e))
            await db.append_audit(pool, "writeback:promote", kind, oid, field_id,
                                  cur, concept, None, f"error: {e}")
            continue

async def _cfv_put(al, cfg, otype, oid, field_values):
    """Single CFV write call site; honours the CFV API Retry-After (single-threaded, §5.4).
    Returns a CFVResult (ok already reflects async job confirmation)."""
    retry = cfg["alation"].get("retry", {})
    attempts = retry.get("max_attempts", 5)
    backoff = retry.get("backoff_initial_seconds", 2)
    backoff_max = retry.get("backoff_max_seconds", 60)
    for _ in range(attempts):
        res = await al.set_custom_field_values(otype, oid, field_values)
        if res.status == 429:
            await asyncio.sleep(res.retry_after if res.retry_after is not None else backoff)
            backoff = min(backoff * 2, backoff_max)
            continue
        return res
    raise RuntimeError(f"CFV write to {otype}:{oid}: exhausted {attempts} attempts (throttled)")

def _conf_eq(cur, target: float) -> bool:
    if cur is None:
        return False
    try:
        return abs(float(cur) - target) < 1e-9
    except (TypeError, ValueError):
        return False
