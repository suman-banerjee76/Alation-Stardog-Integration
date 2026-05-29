"""Phase 5: writeback path 1 (publish suggestions). Fakes only — no live Alation/PG.
Covers field mapping, idempotency, CFV Retry-After, per-object isolation, token separation,
and the single CFV write site's payload shape."""
from __future__ import annotations
import pytest

from alation_rdf_sync.ids import uuid7
from alation_rdf_sync.stages import writeback as wb
from alation_rdf_sync.alation import AlationClient, CFVResult, resolve_field_ids, BINDING_FIELD_NAMES

def _ok(): return CFVResult(True, 200)
def _err(status, retry_after=None): return CFVResult(False, status, retry_after=retry_after)

IDS = {"ontology_uri": 10, "suggested_ontology_uri": 11,
       "suggestion_confidence": 12, "binding_status": 13}

def _cfg(**over):
    c = {"writeback": {"enabled": True, "field_ids": IDS},
         "matcher": {"publish_threshold": 0.62},
         "alation": {"base_url": "http://x", "retry": {"max_attempts": 5,
                     "backoff_initial_seconds": 0, "backoff_max_seconds": 0}}}
    c.update(over)
    return c

def _row(sid=1, kind="table", key="d/1", uri="o/A", score=0.90,
         cur_sugg=None, cur_conf=None, cur_status=None, alation_id=100):
    return {"id": sid, "object_kind": kind, "object_key": key, "concept_uri": uri,
            "score": score, "alation_id": alation_id, "cur_sugg": cur_sugg,
            "cur_conf": cur_conf, "cur_status": cur_status}


# ---- fakes -----------------------------------------------------------------

class FakeAl:
    def __init__(self, responses=None):
        self.calls = []
        self._responses = list(responses or [])
    async def set_custom_field_values(self, otype, oid, field_values):
        self.calls.append((otype, oid, list(field_values)))
        return self._responses.pop(0) if self._responses else _ok()
    async def aclose(self): pass

class WBRecorder:
    def __init__(self, rows=None, bindings=None):
        self.rows = rows or []
        self.bindings = bindings or []
        self.published = []
        self.states = []
        self.binding_states = []   # (kind, key, state, kwargs)
        self.audits = []           # (actor, kind, oid, field_id, old, new, api_status, note)
    async def fetch_publishable_suggestions(self, pool, threshold):
        self.threshold = threshold
        return self.rows
    async def mark_suggestion_published(self, pool, sid):
        self.published.append(sid)
    async def fetch_pending_bindings(self, pool):
        return self.bindings
    async def set_binding_state(self, pool, kind, key, state, **kw):
        self.binding_states.append((kind, key, state, kw))
    async def append_audit(self, pool, actor, kind, oid, field_id, old, new, api_status, note=None):
        self.audits.append((actor, kind, oid, field_id, old, new, api_status, note))
    async def record_sync_state(self, *a, **k):
        self.states.append((a, k))

@pytest.fixture
def rec_factory(monkeypatch):
    def make(rows):
        r = WBRecorder(rows)
        monkeypatch.setattr(wb, "db", r)
        return r
    return make


# ---- publish: field mapping ------------------------------------------------

async def test_publish_sets_three_fields_and_marks_published(rec_factory):
    rec = rec_factory([_row(uri="o/A", score=0.9)])
    al = FakeAl()
    await wb._publish_suggestions(al, None, _cfg(), uuid7())
    assert len(al.calls) == 1
    otype, oid, fvs = al.calls[0]
    assert otype == "table" and oid == 100
    assert fvs == [(11, "o/A"), (12, 0.9), (13, "Suggested")]
    assert rec.published == [1]
    assert rec.threshold == 0.62


# ---- publish: idempotency --------------------------------------------------

async def test_publish_idempotent_when_values_already_equal(rec_factory):
    rec = rec_factory([_row(uri="o/A", score=0.9, cur_sugg="o/A",
                            cur_conf="0.9", cur_status="Suggested")])
    al = FakeAl()
    await wb._publish_suggestions(al, None, _cfg(), uuid7())
    assert al.calls == []            # no API call when already equal
    assert rec.published == [1]      # still marked published

async def test_publish_writes_when_confidence_differs(rec_factory):
    rec = rec_factory([_row(uri="o/A", score=0.9, cur_sugg="o/A",
                            cur_conf="0.5", cur_status="Suggested")])
    al = FakeAl()
    await wb._publish_suggestions(al, None, _cfg(), uuid7())
    assert len(al.calls) == 1 and rec.published == [1]


# ---- publish: CFV Retry-After ----------------------------------------------

async def test_publish_retries_on_429_then_succeeds(rec_factory):
    rec = rec_factory([_row()])
    al = FakeAl(responses=[_err(429, retry_after=0), _ok()])
    await wb._publish_suggestions(al, None, _cfg(), uuid7())
    assert len(al.calls) == 2 and rec.published == [1]


# ---- publish: per-object isolation -----------------------------------------

async def test_publish_failure_isolated_does_not_block_others(rec_factory):
    rec = rec_factory([_row(sid=1, alation_id=100), _row(sid=2, alation_id=200)])
    al = FakeAl(responses=[_err(500), _ok()])  # first object fails
    await wb._publish_suggestions(al, None, _cfg(), uuid7())
    assert rec.published == [2]       # sid 1 left pending, sid 2 published
    assert len(al.calls) == 2

async def test_publish_skips_unknown_object_kind(rec_factory):
    rec = rec_factory([_row(kind="document")])
    al = FakeAl()
    await wb._publish_suggestions(al, None, _cfg(), uuid7())
    assert al.calls == [] and rec.published == []


# ---- run(): gating + token separation --------------------------------------

async def test_run_disabled_short_circuits(monkeypatch):
    rec = WBRecorder([])
    monkeypatch.setattr(wb, "db", rec)
    await wb.run(_cfg(writeback={"enabled": False, "field_ids": IDS}), None, uuid7())
    assert rec.states == []           # nothing ran

async def test_run_uses_write_scoped_token(monkeypatch):
    captured = {}
    class _Client:
        def __init__(self, base_url, token, **kw): captured["token"] = token
        async def aclose(self): pass
    monkeypatch.setattr(wb, "AlationClient", _Client)
    rec = WBRecorder([]); monkeypatch.setattr(wb, "db", rec)
    class Cfg(dict):
        write_token = "WRITE"; read_token = "READ"
    await wb.run(Cfg(_cfg()), None, uuid7())
    assert captured["token"] == "WRITE"   # invariant §2.3.2


# ---- promote (path 2) ------------------------------------------------------

def _binding(kind="table", key="d/1", uri="o/A", cur_uri=None, otype="table", oid=100):
    return {"object_kind": kind, "object_key": key, "concept_uri": uri,
            "cur_uri": cur_uri, "alation_otype": otype, "alation_object_id": oid}

async def test_promote_writes_ontology_uri_when_empty_and_audits(monkeypatch):
    rec = WBRecorder(bindings=[_binding(uri="o/A", cur_uri=None)])
    monkeypatch.setattr(wb, "db", rec)
    al = FakeAl()
    await wb._promote_approved(al, None, _cfg(), uuid7())
    # CFV PUT to the Ontology URI field (id 10)
    assert al.calls == [("table", 100, [(10, "o/A")])]
    assert rec.binding_states == [("table", "d/1", "written",
                                   {"field_id": 10, "last_written_value": "o/A"})]
    # mutation audited: old=None new=o/A api_status=200
    assert rec.audits == [("writeback:promote", "table", 100, 10, None, "o/A", 200, None)]

async def test_promote_protect_human_skips_conflict_and_audits(monkeypatch):
    rec = WBRecorder(bindings=[_binding(uri="o/A", cur_uri="o/HUMAN")])
    monkeypatch.setattr(wb, "db", rec)
    al = FakeAl()
    await wb._promote_approved(al, None, _cfg(), uuid7())
    assert al.calls == []                                   # never overwrite human value
    assert rec.binding_states[0][2] == "skipped_conflict"
    assert rec.audits[0][-1] == "human value present" and rec.audits[0][4] == "o/HUMAN"

async def test_promote_converged_marks_written_without_call_or_audit(monkeypatch):
    rec = WBRecorder(bindings=[_binding(uri="o/A", cur_uri="o/A")])
    monkeypatch.setattr(wb, "db", rec)
    al = FakeAl()
    await wb._promote_approved(al, None, _cfg(), uuid7())
    assert al.calls == []                                   # converged -> no API call
    assert rec.binding_states[0][2] == "written"
    assert rec.audits == []                                 # no mutation -> no audit

async def test_promote_failed_put_records_failed_and_audits_status(monkeypatch):
    rec = WBRecorder(bindings=[_binding(uri="o/A", cur_uri=None)])
    monkeypatch.setattr(wb, "db", rec)
    al = FakeAl(responses=[_err(500)])
    await wb._promote_approved(al, None, _cfg(), uuid7())
    assert rec.binding_states[0][2] == "failed"
    assert rec.audits[0][6] == 500                          # api_status logged

async def test_promote_retries_on_429(monkeypatch):
    rec = WBRecorder(bindings=[_binding(uri="o/A", cur_uri=None)])
    monkeypatch.setattr(wb, "db", rec)
    al = FakeAl(responses=[_err(429, retry_after=0), _ok()])
    await wb._promote_approved(al, None, _cfg(), uuid7())
    assert len(al.calls) == 2 and rec.binding_states[0][2] == "written"

async def test_promote_isolates_per_binding(monkeypatch):
    rec = WBRecorder(bindings=[_binding(key="d/1", oid=100, cur_uri=None),
                               _binding(key="d/2", oid=200, cur_uri=None)])
    monkeypatch.setattr(wb, "db", rec)
    al = FakeAl(responses=[_err(500), _ok()])
    await wb._promote_approved(al, None, _cfg(), uuid7())
    states = {k: s for (_, k, s, _) in rec.binding_states}
    assert states == {"d/1": "failed", "d/2": "written"}


# ---- stage failure observability -------------------------------------------

async def test_run_records_failed_closes_client_and_reraises(monkeypatch):
    rec = WBRecorder([])
    monkeypatch.setattr(wb, "db", rec)
    closed = {"v": False}
    class _Client:
        def __init__(self, base_url, token, **kw): pass
        async def aclose(self): closed["v"] = True
    monkeypatch.setattr(wb, "AlationClient", _Client)
    async def boom(*a, **k): raise RuntimeError("cfv exploded")
    monkeypatch.setattr(wb, "_publish_suggestions", boom)
    class Cfg(dict):
        write_token = "WRITE"
    with pytest.raises(RuntimeError):
        await wb.run(Cfg(_cfg()), None, uuid7())
    assert closed["v"] is True                                  # client still closed
    assert rec.states and rec.states[-1][1].get("err") == "cfv exploded"  # failed recorded


# ---- field-id resolution ---------------------------------------------------

class FieldsAl:
    """Custom Fields API stub. Raises if listed when it shouldn't be (all pinned)."""
    def __init__(self, fields, should_call=True):
        self._fields = fields
        self._should_call = should_call
        self.listed = False
    async def list_custom_fields(self):
        assert self._should_call, "list_custom_fields called despite all ids pinned"
        self.listed = True
        return FakeFieldsResp(self._fields)
    async def aclose(self): pass

class FakeFieldsResp:
    def __init__(self, fields): self._fields = fields
    def raise_for_status(self): pass
    def json(self): return self._fields

_FIELDS = [
    {"id": 101, "name_singular": "Ontology URI"},
    {"id": 102, "name_singular": "Suggested Ontology URI"},
    {"id": 103, "name_singular": "Suggestion Confidence"},
    {"id": 104, "name_singular": "Binding Status"},
    {"id": 999, "name_singular": "Some Other Field"},
]

async def test_resolve_all_by_name():
    al = FieldsAl(_FIELDS)
    ids = await resolve_field_ids(al, BINDING_FIELD_NAMES, pinned=None)
    assert ids == {"ontology_uri": 101, "suggested_ontology_uri": 102,
                   "suggestion_confidence": 103, "binding_status": 104}
    assert al.listed

async def test_resolve_pinned_wins_no_api_call():
    al = FieldsAl(_FIELDS, should_call=False)
    pinned = {"ontology_uri": 10, "suggested_ontology_uri": 11,
              "suggestion_confidence": 12, "binding_status": 13}
    ids = await resolve_field_ids(al, BINDING_FIELD_NAMES, pinned=pinned)
    assert ids == pinned and not al.listed       # fully pinned -> no discovery

async def test_resolve_partial_pinned_looks_up_rest():
    al = FieldsAl(_FIELDS)
    pinned = {"ontology_uri": 10, "binding_status": 0}   # 0 is falsy -> still resolved
    ids = await resolve_field_ids(al, BINDING_FIELD_NAMES, pinned=pinned)
    assert ids["ontology_uri"] == 10                      # pinned kept
    assert ids["binding_status"] == 104 and ids["suggestion_confidence"] == 103

async def test_resolve_tolerates_alternate_name_key():
    fields = [{"id": 7, "name": "Ontology URI"}]          # 'name' instead of 'name_singular'
    ids = await resolve_field_ids(FieldsAl(fields), {"ontology_uri": "Ontology URI"}, pinned=None)
    assert ids == {"ontology_uri": 7}

async def test_resolve_missing_name_raises():
    al = FieldsAl([{"id": 1, "name_singular": "Wrong Name"}])
    with pytest.raises(ValueError) as ei:
        await resolve_field_ids(al, {"ontology_uri": "Ontology URI"}, pinned=None)
    assert "Ontology URI" in str(ei.value)

async def test_writeback_run_auto_resolves_when_unpinned(monkeypatch):
    # field_ids all 0 -> run() resolves via Custom Fields API and publishes with resolved ids.
    rec = WBRecorder(rows=[_row(uri="o/A", score=0.9, alation_id=100)])
    monkeypatch.setattr(wb, "db", rec)
    captured = {}
    class _Client:
        def __init__(self, base_url, token, **kw): pass
        async def list_custom_fields(self): return FakeFieldsResp(_FIELDS)
        async def set_custom_field_values(self, otype, oid, fvs):
            captured["fvs"] = list(fvs); return _ok()
        async def aclose(self): pass
    monkeypatch.setattr(wb, "AlationClient", _Client)
    class Cfg(dict):
        write_token = "WRITE"
    cfg = Cfg(_cfg(writeback={"enabled": True,
                              "field_ids": {"ontology_uri": 0, "suggested_ontology_uri": 0,
                                            "suggestion_confidence": 0, "binding_status": 0}}))
    await wb.run(cfg, None, uuid7())
    # published using the RESOLVED ids (102 suggested, 103 confidence, 104 status)
    assert captured["fvs"] == [(102, "o/A"), (103, 0.9), (104, "Suggested")]


# ---- the single CFV write site: payload shape + endpoint + async job poll ---

class FakeJsonResp:
    def __init__(self, status=200, body=None, headers=None):
        self.status_code = status
        self._body = body
        self.headers = headers or {}
        self.text = ""
    def json(self): return self._body
    def raise_for_status(self):
        if self.status_code >= 400: raise RuntimeError(f"HTTP {self.status_code}")

async def test_cfv_sync_endpoint_builds_array_payload():
    al = AlationClient("http://x", "tok", cfv_async=False)   # legacy sync endpoint
    sent = {}
    class _C:
        async def put(self, url, json):
            sent["url"], sent["json"] = url, json
            return FakeJsonResp(200)
        async def aclose(self): pass
    al._c = _C()
    res = await al.set_custom_field_values("table", 100, [(11, "o/A"), (13, "Suggested")])
    assert res.ok and sent["url"] == "/integration/v2/custom_field_value/"
    assert sent["json"] == [
        {"field_id": 11, "otype": "table", "oid": 100, "value": "o/A"},
        {"field_id": 13, "otype": "table", "oid": 100, "value": "Suggested"}]
    await al.aclose()

async def test_cfv_async_targets_async_endpoint_and_confirms_job():
    al = AlationClient("http://x", "tok", cfv_async=True)
    seen = {}
    class _C:
        async def put(self, url, json):
            seen["url"] = url
            return FakeJsonResp(200, body={"job_id": 42})
        async def get(self, url, params):
            seen["job_url"], seen["job_id"] = url, params["id"]
            return FakeJsonResp(200, body=[{"status": "successful"}])
        async def aclose(self): pass
    al._c = _C()
    res = await al.set_custom_field_values("table", 100, [(11, "o/A")])
    assert seen["url"] == "/integration/v2/custom_field_value/async/"
    assert seen["job_url"] == "/api/v1/bulk_metadata/job/" and seen["job_id"] == 42
    assert res.ok                                            # confirmed via Jobs API

async def test_cfv_async_failed_job_is_not_ok():
    al = AlationClient("http://x", "tok", cfv_async=True)
    class _C:
        async def put(self, url, json): return FakeJsonResp(200, body={"job_id": 7})
        async def get(self, url, params):
            return FakeJsonResp(200, body=[{"status": "failed", "msg": "boom"}])
        async def aclose(self): pass
    al._c = _C()
    res = await al.set_custom_field_values("table", 100, [(11, "o/A")])
    assert not res.ok and "failed" in res.detail             # 200 accepted, but job failed

async def test_cfv_429_returns_retry_after():
    al = AlationClient("http://x", "tok", cfv_async=True)
    class _C:
        async def put(self, url, json): return FakeJsonResp(429, headers={"Retry-After": "5"})
        async def aclose(self): pass
    al._c = _C()
    res = await al.set_custom_field_values("table", 100, [(11, "o/A")])
    assert res.status == 429 and res.retry_after == 5.0 and not res.ok
