from __future__ import annotations
import json
from dataclasses import dataclass, field
from typing import Any

@dataclass
class TableRow:
    alation_key: str; alation_id: int; ds_id: int; schema_name: str; table_name: str
    title: str | None = None; description: str | None = None; url: str | None = None
    object_type: str = "table"; last_updated_at: str | None = None
    custom_fields: dict[str, Any] = field(default_factory=dict)

@dataclass
class Concept:
    concept_uri: str; pref_label: str; alt_labels: list[str] = field(default_factory=list)
    definition: str | None = None; concept_type: str | None = None

@dataclass
class Suggestion:
    object_kind: str; object_key: str; object_label: str; concept_uri: str | None
    score: float; rank: int; method: str = "trgm"; rationale: str | None = None
    no_match: bool = False; model_version: str | None = None; prompt_version: str | None = None
    agent_input_hash: str | None = None

# ---- Source payload field mapping ------------------------------------------
# The exact JSON keys in Alation's table/document/data-product payloads vary by version and by
# the ODPS spec. Rather than hardcode one set of names, every key the sync reads is resolved
# through this map: logical column -> ordered list of candidate source keys (first present wins).
# Defaults follow the documented Alation shape with common fallbacks; override any of them in
# config under alation.field_map without changing code. Example:
#   alation: { field_map: { table: { last_updated_at: [modified_ts] } } }
DEFAULT_FIELD_MAP: dict[str, dict[str, list[str]]] = {
    "table": {
        "alation_key": ["key"], "alation_id": ["id"], "ds_id": ["ds_id"],
        "schema_name": ["schema_name"], "table_name": ["name"],
        "title": ["title"], "description": ["description"], "url": ["url"],
        "object_type": ["table_type", "object_type"],
        "last_updated_at": ["ts_updated", "last_updated", "ts_last_updated"],
        "custom_fields": ["custom_fields"],
    },
    "document": {
        "alation_id": ["id"], "document_hub_id": ["document_hub_id"],
        "folder_id": ["folder_id"], "template_id": ["template_id"],
        "title": ["title"], "description": ["description"], "url": ["url"],
        "last_updated_at": ["ts_updated", "last_updated"], "custom_fields": ["custom_fields"],
    },
    "data_product": {
        "product_id": ["product_id", "id"], "name": ["name"],
        "short_description": ["short_description"], "description": ["description"],
        "product_type": ["product_type"], "visibility": ["visibility"],
        "contact_name": ["contact_name"], "publisher": ["publisher"],
        "marketplace_id": ["marketplace_id"], "url": ["url"], "version": ["version"],
        "published_at": ["published_at"], "updated_at": ["updated_at"],
        "licence": ["licence", "license"], "rights": ["rights"], "audience": ["audience"],
        "access_request": ["access_request", "accessRequest"], "contract": ["contract"],
        "record_sets": ["record_sets", "recordSets"],
        "delivery_systems": ["delivery_systems", "deliverySystems"],
        "recommended_products": ["recommended_products", "recommendedDataProducts"],
        "locales": ["locales"], "custom_fields": ["custom_fields"],
    },
    # custom_fields array item keys (from the table/document payloads)
    "custom_field": {"name": ["field_name", "name"], "value": ["value"],
                     "type": ["value_type", "type"]},
}

# data-product JSONB sub-object columns, in DDL order (see db.batch_upsert_data_products).
_DP_JSONB = ("licence", "rights", "audience", "access_request", "contract", "record_sets",
             "delivery_systems", "recommended_products", "locales")

def _val(obj: dict, candidates: list[str]):
    for k in candidates:
        if isinstance(obj, dict) and k in obj:
            return obj[k]
    return None

def field_map(cfg_alation: dict | None) -> dict:
    """DEFAULT_FIELD_MAP with per-object-type overrides from alation.field_map merged in.
    Override values may be a single key (str) or a list of candidate keys."""
    out = {otype: {logical: list(keys) for logical, keys in fields.items()}
           for otype, fields in DEFAULT_FIELD_MAP.items()}
    for otype, fields in ((cfg_alation or {}).get("field_map") or {}).items():
        out.setdefault(otype, {})
        for logical, keys in fields.items():
            out[otype][logical] = [keys] if isinstance(keys, str) else list(keys)
    return out

def shape_custom_fields(raw: list[dict], cf_map: dict | None = None) -> dict[str, dict]:
    """Alation custom-field list payload -> {name: {value, type}} JSONB shape. Tolerant of the
    item key names (field_name/name, value_type/type) and overridable via the custom_field map."""
    cf = cf_map or DEFAULT_FIELD_MAP["custom_field"]
    out = {}
    for item in raw or []:
        name = _val(item, cf["name"])
        if name is None:
            continue
        out[name] = {"value": _val(item, cf["value"]), "type": _val(item, cf["type"]) or "string"}
    return out

def table_row(obj: dict, fmap: dict, run_id) -> tuple:
    m = fmap["table"]
    return (_val(obj, m["alation_key"]), _val(obj, m["alation_id"]), _val(obj, m["ds_id"]),
            _val(obj, m["schema_name"]), _val(obj, m["table_name"]), _val(obj, m["title"]),
            _val(obj, m["description"]), _val(obj, m["url"]),
            _val(obj, m["object_type"]) or "table", _val(obj, m["last_updated_at"]),
            json.dumps(shape_custom_fields(_val(obj, m["custom_fields"]) or [], fmap.get("custom_field"))),
            run_id)

def document_row(obj: dict, fmap: dict, run_id) -> tuple:
    m = fmap["document"]
    return (_val(obj, m["alation_id"]), _val(obj, m["document_hub_id"]), _val(obj, m["folder_id"]),
            _val(obj, m["template_id"]), _val(obj, m["title"]), _val(obj, m["description"]),
            _val(obj, m["url"]), _val(obj, m["last_updated_at"]),
            json.dumps(shape_custom_fields(_val(obj, m["custom_fields"]) or [], fmap.get("custom_field"))),
            run_id)

def data_product_row(obj: dict, fmap: dict, run_id) -> tuple:
    m = fmap["data_product"]
    jb = [json.dumps(_val(obj, m[k])) if _val(obj, m[k]) is not None else None for k in _DP_JSONB]
    return (str(_val(obj, m["product_id"])), _val(obj, m["name"]), _val(obj, m["short_description"]),
            _val(obj, m["description"]), _val(obj, m["product_type"]), _val(obj, m["visibility"]),
            _val(obj, m["contact_name"]), _val(obj, m["publisher"]), _val(obj, m["marketplace_id"]),
            _val(obj, m["url"]), *jb, _val(obj, m["version"]), _val(obj, m["published_at"]),
            _val(obj, m["updated_at"]),
            json.dumps(shape_custom_fields(_val(obj, m["custom_fields"]) or [], fmap.get("custom_field"))),
            run_id)
