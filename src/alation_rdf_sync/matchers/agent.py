from __future__ import annotations
import hashlib, json, re
from itertools import groupby
from .base import Matcher
from .. import db

PROMPT = """You bind a data catalog object to AT MOST ONE ontology concept.
Choose strictly from the provided candidates. If none fit, set no_match=true.
Return ONLY JSON: {"concept_uri": <uri-from-candidates|null>, "confidence": 0..1,
"rationale": <short>, "no_match": <bool>}."""

class AgentMatcher(Matcher):
    """Shortlist with trgm/embedding (max_candidates), then one model call per CHANGED object.
    Grounded (§2.3.7): a returned concept_uri MUST be in the candidate set (which is sourced from
    ontology_concept), else discard + log. Temperature 0; cache on agent_input_hash; pin
    model_version + prompt_version. Shadow by default: rows are written status='shadow' so the
    publish path (status='pending') never publishes them — promotion to production is operational
    (flip matcher.engine / status) once precision@1 beats trgm.

    `adjudicate` is injectable (object, candidates) -> result dict, so tests need no SDK/network.
    """
    def __init__(self, agent_cfg: dict, adjudicate=None):
        self.cfg = agent_cfg
        self._adj = adjudicate
        self._client = None

    async def run(self, pool, cfg, run_id, suggest_sql: str):
        max_candidates = self.cfg.get("max_candidates", 5)
        min_label_length = cfg["matcher"].get("min_label_length", 3)
        only_changed = self.cfg.get("only_changed", True)
        status = "shadow" if self.cfg.get("shadow", True) else "pending"
        model_version = self.cfg.get("model")
        prompt_version = self.cfg.get("prompt_version")

        rows = await db.fetch_agent_shortlist(pool, max_candidates, min_label_length)
        for obj, candidates in self._group(rows):
            cand_uris = [c["concept_uri"] for c in candidates]
            h = self.input_hash(obj["title"], obj.get("description"), cand_uris)
            if only_changed and await db.agent_suggestion_cached(pool, obj["kind"], obj["key"], h):
                continue
            result = await self._adjudicate(obj, candidates)
            await self._persist(pool, run_id, obj, candidates, h, result,
                                status, model_version, prompt_version)

    async def _persist(self, pool, run_id, obj, candidates, h, result,
                       status, model_version, prompt_version):
        uri = result.get("concept_uri")
        if result.get("no_match") or not uri:
            await db.upsert_gap_candidate(pool, obj["kind"], obj["key"], obj["title"],
                                          result.get("rationale") or "agent no_match", run_id)
            return
        if uri not in {c["concept_uri"] for c in candidates}:
            # ungrounded hallucination: discard + log, never persist (§2.3.7)
            print(f"[agent] discard ungrounded concept_uri={uri!r} for {obj['kind']}:{obj['key']}")
            return
        await db.upsert_agent_suggestion(
            pool, object_kind=obj["kind"], object_key=obj["key"], object_label=obj["title"],
            concept_uri=uri, score=float(result.get("confidence") or 0.0),
            rationale=result.get("rationale"), model_version=model_version,
            prompt_version=prompt_version, agent_input_hash=h, status=status, run_id=run_id)

    @staticmethod
    def _group(rows):
        """Shortlist rows (ordered by object) -> (object, candidates) pairs."""
        def key(r): return (r["object_kind"], r["object_key"])
        for (kind, okey), grp in groupby(rows, key=key):
            grp = list(grp)
            obj = {"kind": kind, "key": okey, "title": grp[0]["title"],
                   "description": grp[0]["description"],
                   "columns": [], "glossary_context": [], "data_product": None}
            candidates = [{"concept_uri": r["concept_uri"], "pref_label": r["pref_label"],
                           "definition": r["definition"]} for r in grp]
            yield obj, candidates

    async def _adjudicate(self, obj, candidates):
        if self._adj is not None:
            return await self._adj(obj, candidates)
        return await self._adjudicate_anthropic(obj, candidates)

    async def _adjudicate_anthropic(self, obj, candidates):
        try:
            import anthropic
        except ImportError as e:
            raise RuntimeError("engine=agent needs the 'anthropic' extra (pip install '.[agent]')") from e
        if self._client is None:
            self._client = anthropic.AsyncAnthropic()
        msg = await self._client.messages.create(
            model=self.cfg["model"], max_tokens=512, temperature=0.0, system=PROMPT,
            messages=[{"role": "user",
                       "content": json.dumps(self.build_input(obj, candidates))}])
        text = "".join(getattr(b, "text", "") for b in msg.content)
        return _parse_json(text)

    @staticmethod
    def input_hash(label: str, description: str, candidate_uris: list[str]) -> str:
        return hashlib.sha256((label + "|" + (description or "") + "|"
                               + ",".join(sorted(candidate_uris))).encode()).hexdigest()

    @staticmethod
    def build_input(obj: dict, candidates: list[dict]) -> dict:
        return {"object": obj, "candidates": candidates}


def _parse_json(text: str) -> dict:
    try:
        return json.loads(text)
    except (json.JSONDecodeError, TypeError):
        m = re.search(r"\{.*\}", text or "", re.S)   # tolerate prose around the JSON
        return json.loads(m.group(0)) if m else {"no_match": True, "rationale": "unparsable model output"}
