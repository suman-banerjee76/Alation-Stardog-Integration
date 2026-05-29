from __future__ import annotations
import asyncio, contextlib, time
from datetime import datetime, timezone
from ..alation import AlationClient
from .. import db, models

async def run(cfg, pool, run_id):
    """Stage 1 (read-only). Tables fan out by ds_id (N<=parallel_workers); docs + DP single-threaded.
    Each source is isolated: a partial failure records `failed` state and never reconciles or
    contaminates siblings (design §5.1)."""
    al = AlationClient(cfg["alation"]["base_url"], cfg.read_token)
    gate = ThrottleGate()
    try:
        ds_ids = await _discover_data_sources(al, cfg)
        sem = asyncio.Semaphore(cfg["alation"]["parallel_workers"])
        await asyncio.gather(*[_sync_tables_for_ds(al, pool, cfg, sem, gate, d, run_id) for d in ds_ids])
        await _sync_documents(al, pool, cfg, run_id)
        if cfg["alation"].get("data_products_enabled"):
            await _sync_data_products(al, pool, cfg, run_id)
        # Stage steward-approved bindings from this run's landing zone for writeback path 2.
        try:
            await db.materialise_approved_bindings(pool, run_id)
        except Exception:  # noqa: BLE001 - best-effort; pending bindings re-materialise next run
            pass
    finally:
        await al.aclose()

PAGE_FLOOR = 200  # don't halve table pages below this on repeated 504s

class ThrottleGate:
    """Shared across table workers. The first 429 trips it; once tripped, every table
    fetch serialises through one lock, dropping the pool to single-thread (design §5.1)."""
    def __init__(self):
        self._tripped = False
        self._lock = asyncio.Lock()
    def trip(self): self._tripped = True
    @property
    def tripped(self): return self._tripped
    def guard(self):
        return self._lock if self._tripped else contextlib.nullcontext()

def _ms(t0: float) -> int:
    return int((time.monotonic() - t0) * 1000)

async def _discover_data_sources(al, cfg):
    ids = cfg["alation"]["data_source_ids"]
    if ids != "auto":
        return ids
    # auto: enumerate every data source once and cache for the run.
    out, skip, limit = [], 0, 100
    while True:
        r = await al.get_data_sources(skip, limit)
        r.raise_for_status()
        batch = r.json()
        if not batch:
            break
        out.extend(d["id"] for d in batch)
        if len(batch) < limit:
            break
        skip += limit
    return out

async def _sync_tables_for_ds(al, pool, cfg, sem, gate, ds_id, run_id):
    """Sync one data source. A failure here is isolated: it records `failed`
    state and never reconciles (so a partial/empty fetch can't delete live rows)
    and never propagates to sibling sources."""
    async with sem:
        started, t0 = datetime.now(timezone.utc), time.monotonic()
        try:
            seen = await _page_and_upsert_tables(al, pool, cfg, gate, ds_id, run_id)
            await db.reconcile_table_deletions(pool, ds_id, run_id)
            await db.record_sync_state(pool, "table", ds_id, run_id, "success",
                                       seen=seen, started_at=started, duration_ms=_ms(t0))
        except Exception as e:  # noqa: BLE001 - isolate per-source failure
            await db.record_sync_state(pool, "table", ds_id, run_id, "failed",
                                       err=str(e), started_at=started, duration_ms=_ms(t0))

async def _page_and_upsert_tables(al, pool, cfg, gate, ds_id, run_id) -> int:
    fmap = models.field_map(cfg["alation"])
    skip, page, seen = 0, cfg["alation"]["table_page_size"], 0
    while True:
        batch, page = await _fetch_table_page(al, cfg, gate, ds_id, skip, page)
        if not batch:
            break
        rows = [models.table_row(t, fmap, run_id) for t in batch]
        await db.batch_upsert_tables(pool, rows, run_id)
        seen += len(rows)
        if len(batch) < page:
            break
        skip += page
    return seen

async def _fetch_table_page(al, cfg, gate, ds_id, skip, page):
    """Fetch one page with throttling defence: 504 -> halve page (to PAGE_FLOOR);
    429 -> trip the shared gate (single-thread), honour Retry-After, then retry.
    Returns (batch, possibly-reduced page)."""
    retry = cfg["alation"].get("retry", {})
    max_attempts = retry.get("max_attempts", 5)
    backoff = retry.get("backoff_initial_seconds", 2)
    backoff_max = retry.get("backoff_max_seconds", 60)
    for _ in range(max_attempts):
        async with gate.guard():                      # serialised once any worker hits 429
            r = await al.get_tables(ds_id, skip, page)
        if r.status_code == 504 and page > PAGE_FLOOR:
            page = max(PAGE_FLOOR, page // 2)
            continue
        if r.status_code == 429:
            gate.trip()
            await asyncio.sleep(_retry_after(r, backoff))
            backoff = min(backoff * 2, backoff_max)
            continue
        r.raise_for_status()
        return r.json(), page
    raise RuntimeError(f"ds_id={ds_id} skip={skip}: exhausted {max_attempts} attempts (throttled/timeout)")

def _retry_after(r, default: float) -> float:
    val = r.headers.get("Retry-After")
    if val is None:
        return default
    try:
        return float(val)
    except ValueError:
        return default

async def _sync_documents(al, pool, cfg, run_id):
    """Single-threaded; global reconcile. Endpoint chosen by the scaffold: Document Hubs
    (`/integration/v2/document/`). Legacy `/v2/term/` is the alternative — confirm (design §11)."""
    started, t0 = datetime.now(timezone.utc), time.monotonic()
    try:
        fmap = models.field_map(cfg["alation"])
        skip, page, seen = 0, cfg["alation"]["document_page_size"], 0
        while True:
            r = await al.get_documents(skip, page)
            r.raise_for_status()
            batch = r.json()
            if not batch:
                break
            rows = [models.document_row(d, fmap, run_id) for d in batch]
            await db.batch_upsert_documents(pool, rows, run_id)
            seen += len(rows)
            if len(batch) < page:
                break
            skip += page
        await db.reconcile_document_deletions(pool, run_id)
        await db.record_sync_state(pool, "document", 0, run_id, "success",
                                   seen=seen, started_at=started, duration_ms=_ms(t0))
    except Exception as e:  # noqa: BLE001 - isolate this source
        await db.record_sync_state(pool, "document", 0, run_id, "failed",
                                   err=str(e), started_at=started, duration_ms=_ms(t0))

async def _sync_data_products(al, pool, cfg, run_id):
    """Single-threaded; global reconcile. Skipped entirely when data_products_enabled is false."""
    started, t0 = datetime.now(timezone.utc), time.monotonic()
    try:
        fmap = models.field_map(cfg["alation"])
        skip, page, seen = 0, cfg["alation"]["data_product_page_size"], 0
        while True:
            r = await al.get_data_products(skip, page)
            r.raise_for_status()
            batch = r.json()
            if not batch:
                break
            rows = [models.data_product_row(p, fmap, run_id) for p in batch]
            await db.batch_upsert_data_products(pool, rows, run_id)
            seen += len(rows)
            if len(batch) < page:
                break
            skip += page
        await db.reconcile_data_product_deletions(pool, run_id)
        await db.record_sync_state(pool, "data_product", 0, run_id, "success",
                                   seen=seen, started_at=started, duration_ms=_ms(t0))
    except Exception as e:  # noqa: BLE001 - isolate this source
        await db.record_sync_state(pool, "data_product", 0, run_id, "failed",
                                   err=str(e), started_at=started, duration_ms=_ms(t0))
