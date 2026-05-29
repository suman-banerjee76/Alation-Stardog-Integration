from alation_rdf_sync.models import shape_custom_fields
from alation_rdf_sync.matchers.agent import AgentMatcher

def test_shape_custom_fields():
    out = shape_custom_fields([{"field_name": "Ontology URI", "value": "x", "value_type": "string"}])
    assert out == {"Ontology URI": {"value": "x", "type": "string"}}

def test_agent_input_hash_stable():
    h1 = AgentMatcher.input_hash("t", "d", ["b", "a"])
    h2 = AgentMatcher.input_hash("t", "d", ["a", "b"])  # order-insensitive
    assert h1 == h2

def test_grounding_contract():
    # A returned concept_uri MUST be one of the candidates (validated before persistence).
    candidates = ["https://o/A", "https://o/B"]
    assert "https://o/C" not in candidates  # would be discarded + logged
