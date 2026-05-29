"""Phase 1: single-ds table sync — paging, idempotent upsert, deletion reconcile,
504 halving, 429 Retry-After, per-source error isolation. All fakes; no live PG/Alation."""
from __future__ import annotations
import asyncio
import pytest

from alation_rdf_sync.ids import uuid7
from alation_rdf_sync.stages import sync as syncmod


# ---- fakes -----------------------------------------------------------------

class FakeResp:
    def __init__(self, status=200, body=None, headers=None):
        self.status_code = status
        self._body = body if body is not None else []
        self.headers = headers or {}

    def json(self):
        return self._body

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")


class FakeAlation:
    """Scripts endpoint responses; records the (skip, limit) of each table call."""
    def __init__(self, table_responses=None, ds_responses=None,
                 doc_responses=None, dp_responses=None):
        self._tables = list(table_responses or [])
        self._ds = list(ds_responses or [])
        self._docs = list(doc_responses or [])
        self._dps = list(dp_responses or [])
        self.calls = []

    async def get_tables(self, ds_id, skip, limit=1500):
        self.calls.append((ds_id, skip, limit))
        return self._tables.pop(0)

    async def get_data_sources(self, skip, limit=100):
        return self._ds.pop(0)

    async def get_documents(self, skip, limit=100):
        return self._docs.pop(0)

    async def get_data_products(self, skip, limit=100):
        return self._dps.pop(0)


class DBRecorder:
    """Stand-in for the db module; records calls instead of touching Postgres."""
    def __init__(self):
        self.upserts = []          # table row-batches
        self.doc_upserts = []      # document row-batches
        self.dp_upserts = []       # data-product row-batches
        self.reconciled = []       # (ds_id, run_id) — tables
        self.doc_reconciled = []   # run_id — documents
        self.dp_reconciled = []    # run_id — data products
        self.materialised = []     # run_id — uri_binding materialise
        self.states = []           # list of dicts

    async def batch_upsert_tables(self, pool, rows, run_id):
        self.upserts.append(rows)

    async def reconcile_table_deletions(self, pool, ds_id, run_id):
        self.reconciled.append((ds_id, run_id))

    async def batch_upsert_documents(self, pool, rows, run_id):
        self.doc_upserts.append(rows)

    async def reconcile_document_deletions(self, pool, run_id):
        self.doc_reconciled.append(run_id)

    async def batch_upsert_data_products(self, pool, rows, run_id):
        self.dp_upserts.append(rows)

    async def reconcile_data_product_deletions(self, pool, run_id):
        self.dp_reconciled.append(run_id)

    async def record_sync_state(self, pool, endpoint, ds_id, run_id, status,
                                seen=None, err=None, started_at=None, duration_ms=None):
        self.states.append({"endpoint": endpoint, "ds_id": ds_id, "status": status,
                            "seen": seen, "err": err, "started_at": started_at,
                            "duration_ms": duration_ms})

    async def materialise_approved_bindings(self, pool, run_id):
        self.materialised.append(run_id)


@pytest.fixture
def rec(monkeypatch):
    r = DBRecorder()
    monkeypatch.setattr(syncmod, "db", r)
    return r


def _table(key, name="t", ds_id=7):
    return {"key": key, "id": int(key.split("/")[-1]), "ds_id": ds_id,
            "schema_name": "public", "name": name, "custom_fields": []}


def _cfg(**over):
    al = {"table_page_size": 2, "document_page_size": 2, "data_product_page_size": 2,
          "data_source_ids": [7], "parallel_workers": 1,
          "retry": {"max_attempts": 5, "backoff_initial_seconds": 0, "backoff_max_seconds": 0}}
    al.update(over)
    return {"alation": al}

def _gate():
    return syncmod.ThrottleGate()


# ---- discovery -------------------------------------------------------------

async def test_discover_explicit_list_passthrough():
    al = FakeAlation()
    assert await syncmod._discover_data_sources(al, _cfg()) == [7]

async def test_discover_auto_pages():
    full = [{"id": i} for i in range(1, 101)]   # full page (== limit 100) -> fetch again
    al = FakeAlation(ds_responses=[
        FakeResp(body=full),
        FakeResp(body=[{"id": 101}]),           # short page -> stop
    ])
    assert await syncmod._discover_data_sources(al, _cfg(data_source_ids="auto")) == list(range(1, 102))


# ---- paging ----------------------------------------------------------------

async def test_paging_terminates_on_short_page(rec):
    al = FakeAlation(table_responses=[
        FakeResp(body=[_table("d/1"), _table("d/2")]),  # full page of 2
        FakeResp(body=[_table("d/3")]),                 # short page -> stop
    ])
    seen = await syncmod._page_and_upsert_tables(al, None, _cfg(), _gate(), 7, uuid7())
    assert seen == 3
    assert [len(b) for b in rec.upserts] == [2, 1]
    assert al.calls == [(7, 0, 2), (7, 2, 2)]


# ---- idempotent re-run + deletion reconcile --------------------------------

async def test_full_sync_reruns_idempotently_and_reconciles(rec):
    run1 = uuid7()
    al = FakeAlation(table_responses=[FakeResp(body=[_table("d/1")])])
    sem = asyncio.Semaphore(1)
    await syncmod._sync_tables_for_ds(al, None, _cfg(), sem, _gate(), 7, run1)

    run2 = uuid7()
    al2 = FakeAlation(table_responses=[FakeResp(body=[_table("d/1")])])
    await syncmod._sync_tables_for_ds(al2, None, _cfg(), sem, _gate(), 7, run2)

    # Both runs upserted the same row and each reconciled its own ds with its run_id;
    # reconcile deletes rows whose run_id != current -> deletions propagate.
    assert rec.reconciled == [(7, run1), (7, run2)]
    assert [(s["endpoint"], s["ds_id"], s["status"], s["seen"]) for s in rec.states] == [
        ("table", 7, "success", 1), ("table", 7, "success", 1)]


# ---- 504 halving -----------------------------------------------------------

async def test_504_halves_page_then_succeeds(rec):
    al = FakeAlation(table_responses=[
        FakeResp(status=504),                       # 1500 -> 750
        FakeResp(status=504),                       # 750 -> 375
        FakeResp(body=[_table("d/1")]),             # short page at 375
    ])
    batch, newpage = await syncmod._fetch_table_page(al, _cfg(table_page_size=1500), _gate(), 7, 0, 1500)
    assert [c[2] for c in al.calls] == [1500, 750, 375]
    assert newpage == 375 and len(batch) == 1


async def test_504_floors_then_raises(rec):
    # All 504 at/under the floor must not loop forever; exhausts attempts -> raise.
    al = FakeAlation(table_responses=[FakeResp(status=504) for _ in range(5)])
    with pytest.raises(RuntimeError):
        await syncmod._fetch_table_page(al, _cfg(), _gate(), 7, 0, syncmod.PAGE_FLOOR)


# ---- 429 Retry-After -------------------------------------------------------

async def test_429_honours_retry_after_then_succeeds(rec):
    al = FakeAlation(table_responses=[
        FakeResp(status=429, headers={"Retry-After": "0"}),
        FakeResp(body=[_table("d/1")]),
    ])
    gate = _gate()
    batch, _ = await syncmod._fetch_table_page(al, _cfg(), gate, 7, 0, 2)
    assert len(batch) == 1 and len(al.calls) == 2
    assert gate.tripped  # 429 drops the pool to single-thread for the rest of the run


def test_retry_after_parsing():
    assert syncmod._retry_after(FakeResp(headers={"Retry-After": "3"}), 9) == 3.0
    assert syncmod._retry_after(FakeResp(headers={}), 9) == 9          # default
    assert syncmod._retry_after(FakeResp(headers={"Retry-After": "soon"}), 9) == 9  # unparsable


# ---- per-source error isolation --------------------------------------------

async def test_failed_source_records_failed_and_does_not_reconcile(rec):
    al = FakeAlation(table_responses=[FakeResp(status=500)])  # raise_for_status -> error
    sem = asyncio.Semaphore(1)
    run = uuid7()
    await syncmod._sync_tables_for_ds(al, None, _cfg(), sem, _gate(), 7, run)
    assert rec.reconciled == []                               # no destructive reconcile
    assert rec.states[-1]["status"] == "failed" and rec.states[-1]["err"]


# ---- run() materialises approved bindings (Phase 6) ------------------------

async def test_run_materialises_approved_bindings(rec, monkeypatch):
    class FakeClient:
        def __init__(self, base_url, token): pass
        async def get_tables(self, ds_id, skip, limit=1500): return FakeResp(body=[])
        async def get_documents(self, skip, limit=100): return FakeResp(body=[])
        async def aclose(self): pass
    monkeypatch.setattr(syncmod, "AlationClient", FakeClient)

    class Cfg(dict):
        read_token = "READ"
    cfg = Cfg(_cfg(data_products_enabled=False))
    cfg["alation"]["base_url"] = "http://x"
    run = uuid7()
    await syncmod.run(cfg, None, run)
    assert rec.materialised == [run]   # staged for writeback path 2 within the same run


# ---- documents -------------------------------------------------------------

async def test_documents_page_upsert_and_global_reconcile(rec):
    al = FakeAlation(doc_responses=[
        FakeResp(body=[{"id": 1, "document_hub_id": 9, "title": "A"},
                       {"id": 2, "document_hub_id": 9, "title": "B"}]),
        FakeResp(body=[{"id": 3, "document_hub_id": 9, "title": "C"}]),
    ])
    run = uuid7()
    await syncmod._sync_documents(al, None, _cfg(), run)
    assert [len(b) for b in rec.doc_upserts] == [2, 1]
    assert rec.doc_reconciled == [run]                        # global reconcile ran
    st = rec.states[-1]
    assert (st["endpoint"], st["ds_id"], st["status"], st["seen"]) == ("document", 0, "success", 3)
    assert st["started_at"] is not None and st["duration_ms"] is not None  # watermark


async def test_documents_failure_isolated_no_reconcile(rec):
    al = FakeAlation(doc_responses=[FakeResp(status=500)])
    await syncmod._sync_documents(al, None, _cfg(), uuid7())
    assert rec.doc_reconciled == []
    assert rec.states[-1]["status"] == "failed"


# ---- data products ---------------------------------------------------------

async def test_data_products_store_subobjects_as_jsonb_and_reconcile(rec):
    al = FakeAlation(dp_responses=[FakeResp(body=[
        {"product_id": "p1", "name": "Sales", "contract": {"sla": "P1D"},
         "record_sets": [{"tableKey": "d/1"}], "custom_fields": []},
    ])])
    run = uuid7()
    await syncmod._sync_data_products(al, None, _cfg(), run)
    assert rec.dp_reconciled == [run]
    row = rec.dp_upserts[0][0]
    assert row[0] == "p1" and row[1] == "Sales"
    # contract sub-object serialised to JSON text for ::jsonb cast
    assert '"sla"' in row[_DP_COL["contract"]]
    assert rec.states[-1]["status"] == "success"


async def test_data_products_missing_subobject_is_null(rec):
    al = FakeAlation(dp_responses=[FakeResp(body=[
        {"product_id": "p2", "name": "Inv", "custom_fields": []},
    ])])
    await syncmod._sync_data_products(al, None, _cfg(), uuid7())
    row = rec.dp_upserts[0][0]
    assert row[_DP_COL["contract"]] is None  # absent sub-object -> SQL NULL, not 'null'


# Column offsets into the data-product row tuple (see models.data_product_row):
# product_id,name,short_description,description,product_type,visibility,contact_name,
# publisher,marketplace_id,url, then the 9 _DP_JSONB sub-objects.
from alation_rdf_sync import models as _models
_DP_COL = {"contract": 10 + _models._DP_JSONB.index("contract")}
