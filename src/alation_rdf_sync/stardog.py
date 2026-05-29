from __future__ import annotations
import httpx
from pathlib import Path

# The glossary SMS2 mapping filters documents to the Glossary Hub. The hub id is
# environment-specific (design §11), so vg_alation.sms2 carries this placeholder and is rendered
# with the resolved id (config stardog.glossary_document_hub_id) before `virtual add`.
GLOSSARY_HUB_PLACEHOLDER = "@GLOSSARY_DOCUMENT_HUB_ID@"

def _sms2_candidates():
    return [Path(__file__).resolve().parents[2] / "stardog" / "vg_alation.sms2",
            Path.cwd() / "stardog" / "vg_alation.sms2"]

def load_sms2() -> str:
    for p in _sms2_candidates():
        if p.exists():
            return p.read_text()
    raise FileNotFoundError("vg_alation.sms2 not found in: "
                            + ", ".join(str(p) for p in _sms2_candidates()))

def render_sms2(hub_id, text: str | None = None) -> str:
    """Substitute the resolved Glossary Hub id into the SMS2 mapping. Raises if the placeholder
    is absent (already rendered / hand-edited) to avoid silently registering the wrong hub."""
    if text is None:
        text = load_sms2()
    if GLOSSARY_HUB_PLACEHOLDER not in text:
        raise ValueError(f"{GLOSSARY_HUB_PLACEHOLDER} not found in SMS2 — already rendered?")
    return text.replace(GLOSSARY_HUB_PLACEHOLDER, str(int(hub_id)))

CONCEPTS_SPARQL = """
SELECT ?concept_uri ?pref_label (GROUP_CONCAT(DISTINCT ?alt;SEPARATOR="\\u241E") AS ?alt_labels)
       ?definition ?concept_type WHERE {
  ?concept_uri a ?concept_type . VALUES ?concept_type { owl:Class skos:Concept }
  OPTIONAL { ?concept_uri rdfs:label ?rl } OPTIONAL { ?concept_uri skos:prefLabel ?pl }
  BIND (COALESCE(?pl,?rl) AS ?pref_label)
  OPTIONAL { ?concept_uri skos:altLabel ?alt } OPTIONAL { ?concept_uri skos:definition ?definition }
  FILTER (BOUND(?pref_label)) }
GROUP BY ?concept_uri ?pref_label ?definition ?concept_type
"""

async def fetch_concepts(endpoint: str, token: str):
    async with httpx.AsyncClient(timeout=120.0) as c:
        r = await c.post(endpoint, headers={"Authorization": f"bearer {token}",
            "Accept": "application/sparql-results+json"}, data={"query": CONCEPTS_SPARQL})
        r.raise_for_status()
        return r.json()["results"]["bindings"]
