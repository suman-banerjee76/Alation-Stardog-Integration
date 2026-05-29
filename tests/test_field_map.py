"""Resolve the Alation JSON field-name assumptions: every source key the sync reads goes through
a tolerant, config-overridable field map (models.field_map / *_row / shape_custom_fields)."""
from __future__ import annotations
from alation_rdf_sync import models

R = "RUN"

# table row order: key,id,ds_id,schema,name,title,desc,url,object_type,last_updated,custom_fields,run_id

def test_table_row_uses_default_keys():
    fmap = models.field_map({})
    obj = {"key": "7.s.t", "id": 7, "ds_id": 3, "schema_name": "s", "name": "t", "title": "T",
           "table_type": "VIEW", "ts_updated": "2024-01-01T00:00:00Z", "custom_fields": []}
    row = models.table_row(obj, fmap, R)
    assert row[0] == "7.s.t" and row[1] == 7 and row[2] == 3 and row[4] == "t"
    assert row[8] == "VIEW"                          # object_type <- table_type
    assert row[9] == "2024-01-01T00:00:00Z"          # last_updated_at <- ts_updated

def test_table_row_object_type_defaults_to_table():
    row = models.table_row({"key": "k", "id": 1, "ds_id": 1, "schema_name": "s", "name": "t"},
                           models.field_map({}), R)
    assert row[8] == "table"

def test_table_row_last_updated_fallback_key():
    # payload uses 'last_updated' instead of 'ts_updated' — covered by the default candidate list
    row = models.table_row({"key": "k", "id": 1, "ds_id": 1, "schema_name": "s", "name": "t",
                            "last_updated": "X"}, models.field_map({}), R)
    assert row[9] == "X"

def test_field_map_config_override():
    fmap = models.field_map({"field_map": {"table": {"last_updated_at": ["modified_ts"]}}})
    row = models.table_row({"key": "k", "id": 1, "ds_id": 1, "schema_name": "s", "name": "t",
                            "modified_ts": "Z", "ts_updated": "ignored"}, fmap, R)
    assert row[9] == "Z"                             # override wins over the default key

def test_field_map_override_accepts_scalar():
    fmap = models.field_map({"field_map": {"table": {"table_name": "label"}}})
    row = models.table_row({"key": "k", "id": 1, "ds_id": 1, "schema_name": "s", "label": "t"}, fmap, R)
    assert row[4] == "t"

def test_custom_fields_tolerates_name_and_type_keys():
    out = models.shape_custom_fields([{"name": "Steward", "value": "alice", "type": "string"}])
    assert out == {"Steward": {"value": "alice", "type": "string"}}

def test_custom_fields_default_field_name_key():
    out = models.shape_custom_fields([{"field_name": "Ontology URI", "value": "x", "value_type": "string"}])
    assert out == {"Ontology URI": {"value": "x", "type": "string"}}

def test_custom_fields_override_keys():
    cf = {"name": ["label"], "value": ["val"], "type": ["dtype"]}
    out = models.shape_custom_fields([{"label": "X", "val": 1, "dtype": "int"}], cf)
    assert out == {"X": {"value": 1, "type": "int"}}

def test_document_row_keys():
    row = models.document_row({"id": 9, "document_hub_id": 2, "title": "G",
                               "ts_updated": "T", "custom_fields": []}, models.field_map({}), R)
    assert row[0] == 9 and row[1] == 2 and row[4] == "G" and row[7] == "T"

def test_data_product_row_resolves_camelcase_record_sets():
    fmap = models.field_map({})
    row = models.data_product_row({"product_id": "p1", "name": "N",
                                   "recordSets": [{"tableKey": "k"}]}, fmap, R)
    idx = 10 + models._DP_JSONB.index("record_sets")
    assert row[0] == "p1" and '"tableKey"' in row[idx]   # camelCase fallback resolved -> jsonb text

def test_data_product_row_missing_subobject_is_null():
    row = models.data_product_row({"product_id": "p2", "name": "N"}, models.field_map({}), R)
    assert row[10 + models._DP_JSONB.index("contract")] is None
