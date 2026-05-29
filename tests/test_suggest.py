"""Phase 4: extract_concepts mapping + cadence gate, and the trgm matcher's
:named -> $n translation + param binding. All fakes; no live Stardog/PG."""
from __future__ import annotations
from datetime import datetime, timezone, timedelta
from pathlib import Path
import pytest

from alation_rdf_sync.ids import uuid7
from alation_rdf_sync.stages import extract_concepts as ec
from alation_rdf_sync.stages import suggest as sg
from alation_rdf_sync.matchers.trgm import translate_named, TrgmMatcher

ROOT = Path(__file__).resolve().parent.parent
SUGGEST_SQL = (ROOT / "sql" / "suggest_trgm.sql").read_text()


# ---- fake pool/conn --------------------------------------------------------

class FakeConn:
    def __init__(self):
        self.executed = []          # (sql, args)
        self.upserts = []
        self._fetchval = None

    async def execute(self, sql, *args):
        self.executed.append((sql, args))

    async def executemany(self, sql, batch):
        self.upserts.append((sql, batch))

    async def fetchval(self, sql, *args):
        return self._fetchval


class _Acq:
    def __init__(self, c): self.c = c
    async def __aenter__(self): return self.c
    async def __aexit__(self, *a): return False


class FakePool:
    def __init__(self): self.conn = FakeConn()
    def acquire(self): return _Acq(self.conn)


def _binding(uri, label, alt=None, defn=None, ctype=None):
    b = {"concept_uri": {"value": uri}, "pref_label": {"value": label}}
    if alt is not None: b["alt_labels"] = {"value": alt}
    if defn is not None: b["definition"] = {"value": defn}
    if ctype is not None: b["concept_type"] = {"value": ctype}
    return b


# ---- extract_concepts: binding -> row mapping ------------------------------

def test_concept_rows_splits_alt_labels_on_u241e():
    rows = ec._concept_rows([_binding("o/A", "Customer", alt="Client␞Account", defn="d", ctype="owl:Class")],
                            uuid7())
    uri, label, alts_json, defn, ctype, _ = rows[0]
    assert uri == "o/A" and label == "Customer" and defn == "d"
    assert alts_json == '["Client", "Account"]'   # JSON array for ::jsonb

def test_concept_rows_empty_alt_labels_is_empty_array():
    rows = ec._concept_rows([_binding("o/A", "Customer", alt="")], uuid7())
    assert rows[0][2] == "[]"

def test_concept_rows_skips_missing_uri_or_label():
    rows = ec._concept_rows([
        {"pref_label": {"value": "no uri"}},            # missing concept_uri (PK)
        {"concept_uri": {"value": "o/X"}},              # missing pref_label (NOT NULL)
        _binding("o/A", "ok"),
    ], uuid7())
    assert [r[0] for r in rows] == ["o/A"]

def test_concept_rows_handles_absent_optional_keys():
    rows = ec._concept_rows([_binding("o/A", "ok")], uuid7())
    assert rows[0][2] == "[]" and rows[0][3] is None and rows[0][4] is None


# ---- extract_concepts: cadence gate ----------------------------------------

async def test_cadence_gate_skips_when_recent(monkeypatch):
    pool = FakePool()
    pool.conn._fetchval = datetime.now(timezone.utc) - timedelta(hours=1)   # 1h ago, cadence 24h
    called = {"fetched": False}
    async def fake_fetch(*a, **k):
        called["fetched"] = True; return []
    monkeypatch.setattr(ec, "fetch_concepts", fake_fetch)
    cfg = {"stardog": {"sparql_endpoint": "x", "concept_extract_cadence_hours": 24}}
    await ec.run(cfg, pool, uuid7())
    assert called["fetched"] is False                  # gated: no Stardog call

async def test_cadence_gate_runs_when_stale(monkeypatch):
    pool = FakePool()
    pool.conn._fetchval = datetime.now(timezone.utc) - timedelta(hours=48)  # stale
    async def fake_fetch(*a, **k):
        return [_binding("o/A", "Customer", alt="Client")]
    monkeypatch.setattr(ec, "fetch_concepts", fake_fetch)
    cfg = {"stardog": {"sparql_endpoint": "x", "concept_extract_cadence_hours": 24}}
    await ec.run(cfg, pool, uuid7())
    assert pool.conn.upserts and len(pool.conn.upserts[0][1]) == 1   # concept upserted

async def test_cadence_gate_runs_on_first_ever(monkeypatch):
    pool = FakePool()
    pool.conn._fetchval = None                          # never run
    async def fake_fetch(*a, **k):
        return [_binding("o/A", "Customer")]
    monkeypatch.setattr(ec, "fetch_concepts", fake_fetch)
    cfg = {"stardog": {"sparql_endpoint": "x"}}         # default cadence 24h
    await ec.run(cfg, pool, uuid7())
    assert pool.conn.upserts


# ---- trgm: :named -> $n translation ----------------------------------------

def test_translate_named_reuses_positionals_and_orders_args():
    sql, args = translate_named("SELECT :a, :b, :a", {"a": 1, "b": 2})
    assert sql == "SELECT $1, $2, $1"
    assert args == [1, 2]

def test_translate_strips_comments_before_translation():
    sql, args = translate_named("-- uses :a :b\nSELECT :b", {"a": 9, "b": 5})
    assert ":a" not in sql and ":b" not in sql
    assert sql.strip() == "SELECT $1" and args == [5]   # comment params don't leak in

def test_translate_real_suggest_sql_has_no_named_left():
    run = uuid7()
    sql, args = translate_named(SUGGEST_SQL, {"run_id": run, "top_n": 5,
                                              "min_similarity": 0.45, "min_label_length": 3})
    assert not __import__("re").search(r":\w+", sql)    # all named placeholders translated
    assert run in args and 5 in args and 0.45 in args and 3 in args
    assert len(args) == 4


# ---- trgm matcher binds params and executes --------------------------------

async def test_trgm_matcher_executes_translated_sql():
    pool = FakePool()
    run = uuid7()
    cfg = {"matcher": {"engine": "trgm", "top_n": 5, "min_similarity": 0.45, "min_label_length": 3}}
    await TrgmMatcher().run(pool, cfg, run, suggest_sql=SUGGEST_SQL)
    sql, args = pool.conn.executed[0]
    assert "uri_suggestion" in sql and "$1" in sql
    assert run in args and len(args) == 4


# ---- suggest stage: primary + shadow engine wiring -------------------------

class _SpyMatcher:
    instances = []
    def __init__(self, tag): self.tag = tag; self.ran = False
    async def run(self, pool, cfg, run_id, suggest_sql): self.ran = True

async def test_suggest_runs_primary_only_without_shadow(monkeypatch):
    ran = []
    monkeypatch.setattr(sg, "get_matcher", lambda eng, cfg: _rec(ran, eng))
    monkeypatch.setattr(sg.db, "record_sync_state", _async_noop)
    await sg.run({"matcher": {"engine": "trgm"}}, None, uuid7())
    assert ran == ["trgm"]

async def test_suggest_runs_primary_then_shadow(monkeypatch):
    ran = []
    monkeypatch.setattr(sg, "get_matcher", lambda eng, cfg: _rec(ran, eng))
    monkeypatch.setattr(sg.db, "record_sync_state", _async_noop)
    await sg.run({"matcher": {"engine": "trgm", "shadow_engine": "agent"}}, None, uuid7())
    assert ran == ["trgm", "agent"]      # primary first, then shadow

async def test_suggest_skips_shadow_when_same_as_primary(monkeypatch):
    ran = []
    monkeypatch.setattr(sg, "get_matcher", lambda eng, cfg: _rec(ran, eng))
    monkeypatch.setattr(sg.db, "record_sync_state", _async_noop)
    await sg.run({"matcher": {"engine": "agent", "shadow_engine": "agent"}}, None, uuid7())
    assert ran == ["agent"]              # no double-run

def _rec(ran, eng):
    class M:
        async def run(self, pool, cfg, run_id, suggest_sql): ran.append(eng)
    return M()

async def _async_noop(*a, **k):
    return None


# ---- stage failure observability -------------------------------------------

async def test_suggest_records_failed_and_reraises(monkeypatch):
    states = []
    async def rec(pool, endpoint, ds_id, run_id, status, **k):
        states.append((endpoint, status, k.get("err")))
    monkeypatch.setattr(sg.db, "record_sync_state", rec)
    def boom_matcher(eng, cfg):
        class M:
            async def run(self, *a, **k): raise RuntimeError("pg down")
        return M()
    monkeypatch.setattr(sg, "get_matcher", boom_matcher)
    with pytest.raises(RuntimeError):
        await sg.run({"matcher": {"engine": "trgm"}}, None, uuid7())
    assert states[-1] == ("suggest", "failed", "pg down")

async def test_extract_records_failed_and_reraises(monkeypatch):
    pool = FakePool(); pool.conn._fetchval = None      # gate: never run -> proceed
    async def boom(*a, **k): raise RuntimeError("stardog down")
    monkeypatch.setattr(ec, "fetch_concepts", boom)
    states = []
    async def rec(pool, endpoint, ds_id, run_id, status, **k):
        states.append((endpoint, status, k.get("err")))
    monkeypatch.setattr(ec.db, "record_sync_state", rec)
    with pytest.raises(RuntimeError):
        await ec.run({"stardog": {"sparql_endpoint": "x"}}, pool, uuid7())
    assert states[-1] == ("concept_extract", "failed", "stardog down")

async def test_extract_gate_skip_records_nothing(monkeypatch):
    pool = FakePool()
    pool.conn._fetchval = datetime.now(timezone.utc)   # just ran
    states = []
    async def rec(*a, **k): states.append(a)
    monkeypatch.setattr(ec.db, "record_sync_state", rec)
    await ec.run({"stardog": {"sparql_endpoint": "x", "concept_extract_cadence_hours": 24}},
                 pool, uuid7())
    assert states == []   # gated: no success and no failed row written
