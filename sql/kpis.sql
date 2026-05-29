-- KPI queries for the Alation -> Stardog bridge (design §9). One block per KPI; point a
-- dashboard (Grafana/Metabase) at the landing PG (stardog_reader has SELECT). Read-only.

-- KPI: binding coverage (tables) -- tables with an authoritative Ontology URI
SELECT count(*) FILTER (WHERE COALESCE(custom_fields->'Ontology URI'->>'value','') <> '') AS bound,
       count(*) AS total,
       round(100.0 * count(*) FILTER (WHERE COALESCE(custom_fields->'Ontology URI'->>'value','') <> '')
             / NULLIF(count(*), 0), 2) AS pct
FROM alation_table;

-- KPI: binding coverage (data products)
SELECT count(*) FILTER (WHERE COALESCE(custom_fields->'Ontology URI'->>'value','') <> '') AS bound,
       count(*) AS total,
       round(100.0 * count(*) FILTER (WHERE COALESCE(custom_fields->'Ontology URI'->>'value','') <> '')
             / NULLIF(count(*), 0), 2) AS pct
FROM alation_data_product;

-- KPI: suggestion coverage -- unbound tables that have at least one suggestion
WITH unbound AS (
  SELECT alation_key FROM alation_table
  WHERE COALESCE(custom_fields->'Ontology URI'->>'value','') = '')
SELECT count(*) FILTER (WHERE s.object_key IS NOT NULL) AS with_suggestion,
       count(*) AS unbound_total,
       round(100.0 * count(*) FILTER (WHERE s.object_key IS NOT NULL) / NULLIF(count(*), 0), 2) AS pct
FROM unbound u
LEFT JOIN (SELECT DISTINCT object_key FROM uri_suggestion WHERE object_kind='table') s
  ON s.object_key = u.alation_key;

-- KPI: acceptance rate -- approved / reviewed (Approved + Rejected)
SELECT count(*) FILTER (WHERE st = 'Approved') AS approved,
       count(*) FILTER (WHERE st IN ('Approved','Rejected')) AS reviewed,
       round(100.0 * count(*) FILTER (WHERE st = 'Approved')
             / NULLIF(count(*) FILTER (WHERE st IN ('Approved','Rejected')), 0), 2) AS pct
FROM (SELECT custom_fields->'Binding Status'->>'value' AS st FROM alation_table) x;

-- KPI: precision@1 (agent vs trgm in shadow) -- rank-1 suggestion vs confirmed Ontology URI
SELECT s.method,
       count(*) FILTER (WHERE s.concept_uri = t.cur) AS hits,
       count(*) AS evaluated,
       round(100.0 * count(*) FILTER (WHERE s.concept_uri = t.cur) / NULLIF(count(*), 0), 2) AS precision_at_1
FROM uri_suggestion s
JOIN (SELECT alation_key, custom_fields->'Ontology URI'->>'value' AS cur
      FROM alation_table
      WHERE COALESCE(custom_fields->'Ontology URI'->>'value','') <> '') t
  ON s.object_kind = 'table' AND s.object_key = t.alation_key
WHERE s.rank = 1
GROUP BY s.method;

-- KPI: write-back success rate -- by terminal binding state
SELECT writeback_state, count(*) AS n,
       round(100.0 * count(*) / NULLIF(sum(count(*)) OVER (), 0), 2) AS pct
FROM uri_binding
GROUP BY writeback_state
ORDER BY writeback_state;

-- KPI: conflicts logged -- protect-human skips
SELECT count(*) AS conflicts
FROM uri_binding
WHERE writeback_state = 'skipped_conflict';

-- KPI: no-match (gap) rate -- gap candidates vs unbound tables
SELECT (SELECT count(*) FROM gap_candidate) AS gaps,
       (SELECT count(*) FROM alation_table
        WHERE COALESCE(custom_fields->'Ontology URI'->>'value','') = '') AS unbound_total;

-- KPI: per-stream sync duration -- last run timings per endpoint
SELECT endpoint, ds_id, last_duration_ms, objects_seen, last_success_at, last_error
FROM sync_state
ORDER BY endpoint, ds_id;

-- KPI: freshness lag -- time since last success per endpoint (interval; bound by cadence)
SELECT endpoint, ds_id, now() - last_success_at AS freshness_lag
FROM sync_state
ORDER BY freshness_lag DESC NULLS FIRST;
