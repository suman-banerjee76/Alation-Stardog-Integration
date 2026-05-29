-- Alation -> Stardog bridge: landing zone + binding loop + ops state.
-- Apply: psql "$DSN" -f migrations/001_init.sql
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- ---- Landing zone (forward sync) -------------------------------------------
CREATE TABLE IF NOT EXISTS alation_table (
  alation_key text PRIMARY KEY, alation_id bigint NOT NULL, ds_id int NOT NULL,
  schema_name text NOT NULL, table_name text NOT NULL, title text, description text,
  url text, object_type text, last_updated_at timestamptz,
  custom_fields jsonb NOT NULL DEFAULT '{}',
  ingested_at timestamptz NOT NULL DEFAULT now(), run_id uuid NOT NULL);
CREATE INDEX IF NOT EXISTS alation_table_ds_idx    ON alation_table (ds_id);
CREATE INDEX IF NOT EXISTS alation_table_sname_idx ON alation_table (ds_id, schema_name, table_name);
CREATE INDEX IF NOT EXISTS alation_table_run_brin  ON alation_table USING brin (ingested_at);
CREATE INDEX IF NOT EXISTS alation_table_cf_gin    ON alation_table USING gin (custom_fields jsonb_path_ops);

CREATE TABLE IF NOT EXISTS alation_document (
  alation_id bigint PRIMARY KEY, document_hub_id bigint NOT NULL, folder_id bigint,
  template_id bigint, title text NOT NULL, description text, url text,
  last_updated_at timestamptz, custom_fields jsonb NOT NULL DEFAULT '{}',
  ingested_at timestamptz NOT NULL DEFAULT now(), run_id uuid NOT NULL);
CREATE INDEX IF NOT EXISTS alation_document_hub_idx  ON alation_document (document_hub_id);
CREATE INDEX IF NOT EXISTS alation_document_cf_gin   ON alation_document USING gin (custom_fields jsonb_path_ops);
CREATE INDEX IF NOT EXISTS alation_document_run_brin ON alation_document USING brin (ingested_at);

CREATE TABLE IF NOT EXISTS alation_data_product (
  product_id text PRIMARY KEY, name text NOT NULL, short_description text, description text,
  product_type text, visibility text, contact_name text, publisher text, marketplace_id text,
  url text, licence jsonb, rights jsonb, audience jsonb, access_request jsonb, contract jsonb,
  record_sets jsonb, delivery_systems jsonb, recommended_products jsonb, locales jsonb,
  version text, published_at timestamptz, updated_at timestamptz,
  custom_fields jsonb NOT NULL DEFAULT '{}', ingested_at timestamptz NOT NULL DEFAULT now(),
  run_id uuid NOT NULL);
CREATE INDEX IF NOT EXISTS alation_dp_mkt_idx      ON alation_data_product (marketplace_id);
CREATE INDEX IF NOT EXISTS alation_dp_type_idx     ON alation_data_product (product_type);
CREATE INDEX IF NOT EXISTS alation_dp_contract_gin ON alation_data_product USING gin (contract jsonb_path_ops);
CREATE INDEX IF NOT EXISTS alation_dp_recsets_gin  ON alation_data_product USING gin (record_sets jsonb_path_ops);

-- ---- Binding loop ----------------------------------------------------------
CREATE TABLE IF NOT EXISTS ontology_concept (
  concept_uri text PRIMARY KEY, pref_label text NOT NULL, alt_labels jsonb NOT NULL DEFAULT '[]',
  definition text, concept_type text,
  label_norm text GENERATED ALWAYS AS (lower(pref_label)) STORED,
  ingested_at timestamptz NOT NULL DEFAULT now(), run_id uuid NOT NULL);
CREATE INDEX IF NOT EXISTS ontology_concept_trgm ON ontology_concept USING gin (label_norm gin_trgm_ops);

CREATE TABLE IF NOT EXISTS uri_suggestion (
  id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  object_kind text NOT NULL, object_key text NOT NULL, object_label text NOT NULL,
  concept_uri text NOT NULL, score numeric(5,4) NOT NULL, rank int NOT NULL,
  method text NOT NULL DEFAULT 'trgm',
  rationale text, model_version text, prompt_version text, agent_input_hash text,
  status text NOT NULL DEFAULT 'pending',
  created_at timestamptz NOT NULL DEFAULT now(), run_id uuid NOT NULL,
  UNIQUE (object_kind, object_key, concept_uri));
CREATE INDEX IF NOT EXISTS uri_suggestion_obj_idx ON uri_suggestion (object_kind, object_key, rank);

CREATE TABLE IF NOT EXISTS uri_binding (
  object_kind text NOT NULL, object_key text NOT NULL,
  alation_otype text NOT NULL, alation_object_id bigint NOT NULL,
  concept_uri text NOT NULL, source text NOT NULL,
  approved_by text, approved_at timestamptz,
  writeback_state text NOT NULL DEFAULT 'pending',
  alation_field_id bigint, last_written_value text, last_written_at timestamptz, last_error text,
  PRIMARY KEY (object_kind, object_key));

CREATE TABLE IF NOT EXISTS writeback_audit (
  id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY, ts timestamptz NOT NULL DEFAULT now(),
  actor text NOT NULL, object_kind text NOT NULL, alation_object_id bigint NOT NULL,
  field_id bigint NOT NULL, old_value text, new_value text, api_status int, note text);

CREATE TABLE IF NOT EXISTS gap_candidate (
  object_kind text NOT NULL, object_key text NOT NULL, object_label text NOT NULL,
  rationale text, created_at timestamptz NOT NULL DEFAULT now(), run_id uuid NOT NULL,
  PRIMARY KEY (object_kind, object_key));

-- ---- Operational state -----------------------------------------------------
CREATE TABLE IF NOT EXISTS sync_state (
  endpoint text NOT NULL, ds_id int NOT NULL DEFAULT 0, last_run_id uuid, last_success_at timestamptz,
  last_started_at timestamptz, last_duration_ms bigint, last_error text, objects_seen int,
  PRIMARY KEY (endpoint, ds_id));
