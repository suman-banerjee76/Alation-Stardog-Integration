"""Phase 7: AgentMatcher — shortlist grouping, grounding validation, caching/only-changed,
no_match -> gap, shadow status. Injected adjudicator; no anthropic SDK / PG / network."""
from __future__ import annotations
import pytest

from alation_rdf_sync.ids import uuid7
from alation_rdf_sync.matchers import agent as agentmod
from alation_rdf_sync.matchers.agent import AgentMatcher, _parse_json


# ---- fake db ---------------------------------------------------------------

class DBStub:
    def __init__(self, rows, cached=False):
        self.rows = rows
        self._cached = cached
        self.suggestions = []   # kwargs of upsert_agent_suggestion
        self.gaps = []          # (kind, key, label, rationale)
        self.cache_checks = []
    async def fetch_agent_shortlist(self, pool, max_candidates, min_label_length):
        self.max_candidates = max_candidates
        return self.rows
    async def agent_suggestion_cached(self, pool, kind, key, h):
        self.cache_checks.append((kind, key, h))
        return self._cached
    async def upsert_agent_suggestion(self, pool, **kw):
        self.suggestions.append(kw)
    async def upsert_gap_candidate(self, pool, kind, key, label, rationale, run_id):
        self.gaps.append((kind, key, label, rationale))


def _row(key, uri, label="t", pref="P", defn="d", desc="desc"):
    return {"object_kind": "table", "object_key": key, "title": label, "description": desc,
            "concept_uri": uri, "pref_label": pref, "definition": defn}

def _agent(adjudicate, **over):
    cfg = {"model": "claude-sonnet-4-6", "prompt_version": "bind-v1",
           "max_candidates": 5, "only_changed": True, "shadow": True}
    cfg.update(over)
    return AgentMatcher(cfg, adjudicate=adjudicate)

CFG = {"matcher": {"min_label_length": 3}}


@pytest.fixture
def stub(monkeypatch):
    def make(rows, cached=False):
        s = DBStub(rows, cached=cached)
        monkeypatch.setattr(agentmod, "db", s)
        return s
    return make


# ---- grounding + persistence -----------------------------------------------

async def test_grounded_pick_persisted_as_shadow_agent_row(stub):
    s = stub([_row("d/1", "o/A"), _row("d/1", "o/B")])   # 2 candidates, one object
    async def adj(obj, cands):
        assert obj["key"] == "d/1" and len(cands) == 2     # grouped into one call
        return {"concept_uri": "o/B", "confidence": 0.81, "rationale": "best fit", "no_match": False}
    await _agent(adj).run(None, CFG, uuid7(), suggest_sql="")
    assert len(s.suggestions) == 1
    sug = s.suggestions[0]
    assert sug["concept_uri"] == "o/B" and sug["score"] == 0.81
    assert sug["status"] == "shadow" and sug["model_version"] == "claude-sonnet-4-6"
    assert sug["prompt_version"] == "bind-v1" and sug["rationale"] == "best fit"
    assert s.gaps == []

async def test_shadow_false_writes_pending(stub):
    s = stub([_row("d/1", "o/A")])
    async def adj(o, c): return {"concept_uri": "o/A", "confidence": 0.7, "no_match": False}
    await _agent(adj, shadow=False).run(None, CFG, uuid7(), suggest_sql="")
    assert s.suggestions[0]["status"] == "pending"


# ---- grounding rejection ----------------------------------------------------

async def test_ungrounded_uri_discarded_not_persisted(stub):
    s = stub([_row("d/1", "o/A"), _row("d/1", "o/B")])
    async def adj(o, c): return {"concept_uri": "o/HALLUCINATED", "confidence": 0.99, "no_match": False}
    await _agent(adj).run(None, CFG, uuid7(), suggest_sql="")
    assert s.suggestions == [] and s.gaps == []      # discarded + logged, never persisted


# ---- no_match -> gap_candidate ---------------------------------------------

async def test_no_match_inserts_gap_candidate(stub):
    s = stub([_row("d/1", "o/A")])
    async def adj(o, c): return {"concept_uri": None, "no_match": True, "rationale": "no fit"}
    await _agent(adj).run(None, CFG, uuid7(), suggest_sql="")
    assert s.suggestions == []
    assert s.gaps == [("table", "d/1", "t", "no fit")]

async def test_null_uri_without_flag_also_treated_as_gap(stub):
    s = stub([_row("d/1", "o/A")])
    async def adj(o, c): return {"concept_uri": None, "no_match": False}
    await _agent(adj).run(None, CFG, uuid7(), suggest_sql="")
    assert len(s.gaps) == 1 and s.suggestions == []


# ---- caching / only-changed -------------------------------------------------

async def test_cached_object_is_not_rescored(stub):
    s = stub([_row("d/1", "o/A")], cached=True)
    called = {"n": 0}
    async def adj(o, c): called["n"] += 1; return {"concept_uri": "o/A", "no_match": False}
    await _agent(adj, only_changed=True).run(None, CFG, uuid7(), suggest_sql="")
    assert called["n"] == 0 and s.suggestions == []      # skipped via agent_input_hash cache

async def test_only_changed_false_rescores_even_if_cached(stub):
    s = stub([_row("d/1", "o/A")], cached=True)
    async def adj(o, c): return {"concept_uri": "o/A", "confidence": 0.5, "no_match": False}
    await _agent(adj, only_changed=False).run(None, CFG, uuid7(), suggest_sql="")
    assert len(s.suggestions) == 1


# ---- input hash + json parsing ---------------------------------------------

def test_input_hash_is_candidate_order_insensitive():
    assert AgentMatcher.input_hash("t", "d", ["b", "a"]) == AgentMatcher.input_hash("t", "d", ["a", "b"])

def test_build_input_shape():
    out = AgentMatcher.build_input({"kind": "table", "key": "d/1"}, [{"concept_uri": "o/A"}])
    assert out == {"object": {"kind": "table", "key": "d/1"}, "candidates": [{"concept_uri": "o/A"}]}

def test_parse_json_tolerates_surrounding_prose():
    assert _parse_json('here you go: {"concept_uri": "o/A", "no_match": false} thanks') \
        == {"concept_uri": "o/A", "no_match": False}

def test_parse_json_unparsable_falls_back_to_no_match():
    assert _parse_json("not json at all")["no_match"] is True
