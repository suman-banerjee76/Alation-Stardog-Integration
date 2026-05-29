"""Phase 3: static verification of the Stardog virtual-graph contract.

No live Stardog/PG here, so we verify the invariant that *can* be checked offline:
every table and bare column the SMS2 `FROM SQL` blocks read must exist in the DDL.
This catches a migration column rename silently breaking the virtual graph — the
class of bug a live Q1 would surface, but as a fast unit test."""
from __future__ import annotations
import re
from pathlib import Path
import pytest

from alation_rdf_sync.stardog import GLOSSARY_HUB_PLACEHOLDER, render_sms2, load_sms2

ROOT = Path(__file__).resolve().parent.parent
DDL = (ROOT / "migrations" / "001_init.sql").read_text()
SMS2 = (ROOT / "stardog" / "vg_alation.sms2").read_text()
QUERIES = (ROOT / "sql" / "validation_queries.sparql").read_text()

_CONSTRAINT_KW = {"PRIMARY", "UNIQUE", "FOREIGN", "CONSTRAINT", "CHECK"}


def _ddl_columns() -> dict[str, set[str]]:
    """{table_name: {column, ...}} parsed from CREATE TABLE bodies."""
    out: dict[str, set[str]] = {}
    for tbl, body in re.findall(r"CREATE TABLE(?:\s+IF NOT EXISTS)?\s+(\w+)\s*\((.*?)\);", DDL, re.S):
        cols: set[str] = set()
        for frag in body.split(","):
            m = re.match(r"\s*([a-zA-Z_]\w*)", frag)
            if not m:
                continue
            tok = m.group(1)
            if tok.upper() in _CONSTRAINT_KW:
                continue
            cols.add(tok.lower())
        out[tbl] = cols
    return out


def _mapping_blocks():
    """[(name, from_sql_body), ...] one per MAPPING in the SMS2 file."""
    blocks = []
    for chunk in re.split(r"MAPPING\s+<", SMS2)[1:]:
        name = chunk.split(">", 1)[0]
        m = re.search(r"FROM SQL\s*\{(.*?)\}\s*TO", chunk, re.S | re.I)
        blocks.append((name, m.group(1) if m else ""))
    return blocks


DDL_COLS = _ddl_columns()
BLOCKS = _mapping_blocks()


def test_ddl_parsed_expected_tables():
    for t in ("alation_table", "alation_document", "alation_data_product",
              "ontology_concept", "uri_suggestion", "uri_binding", "writeback_audit",
              "gap_candidate", "sync_state"):
        assert t in DDL_COLS and DDL_COLS[t], f"DDL parse missed {t}"


def test_seven_mapping_blocks_present():
    names = [n for n, _ in BLOCKS]
    assert names == ["urn:alation:tables", "urn:alation:tables:realises",
                     "urn:alation:tables:suggested", "urn:alation:tables:custom",
                     "urn:alation:glossary", "urn:alation:data_products", "urn:alation:dp:wraps"]


@pytest.mark.parametrize("name,body", BLOCKS)
def test_mapping_from_tables_exist(name, body):
    tables = re.findall(r"FROM\s+(alation_\w+)", body, re.I)
    assert tables, f"{name}: no FROM table found"
    for t in tables:
        assert t in DDL_COLS, f"{name}: unknown table {t}"


@pytest.mark.parametrize("name,body", BLOCKS)
def test_mapping_bare_columns_exist(name, body):
    tables = re.findall(r"FROM\s+(alation_\w+)", body, re.I)
    known = set().union(*(DDL_COLS[t] for t in tables))
    select = re.search(r"SELECT\s+(.*?)\s+FROM\s", body, re.S | re.I)
    assert select, f"{name}: no SELECT..FROM"
    for item in select.group(1).split(","):
        item = item.strip()
        if not item or "->" in item or "(" in item or re.search(r"\s+as\s+", item, re.I):
            continue  # JSONB / function / aliased expression — not a bare column
        col = item.split(".")[-1].strip().lower()   # drop table qualifier
        assert col in known, f"{name}: column '{col}' not in {sorted(known)}"


def test_validation_queries_q1_through_q5_present():
    for q in ("Q1", "Q2", "Q3", "Q4", "Q5"):
        assert re.search(rf"#\s*{q}\b", QUERIES), f"missing {q}"
    assert "vg_alation" in QUERIES


def test_uri_templates_follow_design_scheme():
    # design §6.3: tbl/{alation_key}, dp/{product_id}, doc/{alation_id}
    assert "alation/tbl/{alation_key}" in SMS2
    assert "alation/dp/{product_id}" in SMS2
    assert "alation/doc/{alation_id}" in SMS2


# ---- glossary hub id is a rendered parameter, not a magic number ------------

def test_glossary_hub_is_placeholder_not_hardcoded():
    assert GLOSSARY_HUB_PLACEHOLDER in SMS2                 # template, not a baked id
    assert "document_hub_id = 1 " not in SMS2

def test_render_sms2_substitutes_real_file():
    rendered = render_sms2(42, text=load_sms2())
    assert "document_hub_id = 42" in rendered
    assert GLOSSARY_HUB_PLACEHOLDER not in rendered

def test_render_sms2_rejects_nonnumeric_hub_id():
    with pytest.raises(ValueError):                         # int() guard blocks SQL injection
        render_sms2("1; DROP TABLE", text=f"x {GLOSSARY_HUB_PLACEHOLDER}")

def test_render_sms2_raises_if_placeholder_absent():
    with pytest.raises(ValueError):
        render_sms2(1, text="already rendered: document_hub_id = 7")
