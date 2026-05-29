-- Lexical matcher (matcher.engine = trgm). Params: :run_id :top_n :min_similarity :min_label_length
WITH unbound AS (
  SELECT 'table' AS object_kind, alation_key AS object_key, lower(title) AS lbl
  FROM alation_table
  WHERE COALESCE(custom_fields->'Ontology URI'->>'value','') = ''
    AND COALESCE(custom_fields->'Binding Status'->>'value','Unreviewed') NOT IN ('Approved','Rejected')
    AND length(title) >= :min_label_length
)
INSERT INTO uri_suggestion (object_kind,object_key,object_label,concept_uri,score,rank,rationale,method,run_id)
SELECT u.object_kind,u.object_key,u.lbl,s.concept_uri,s.sim,s.rnk,'prefLabel trgm','trgm',:run_id
FROM unbound u CROSS JOIN LATERAL (
  SELECT c.concept_uri, similarity(u.lbl,c.label_norm) AS sim,
         row_number() OVER (ORDER BY similarity(u.lbl,c.label_norm) DESC) AS rnk
  FROM ontology_concept c WHERE u.lbl % c.label_norm ORDER BY sim DESC LIMIT :top_n
) s
WHERE s.sim >= :min_similarity
ON CONFLICT (object_kind,object_key,concept_uri)
DO UPDATE SET score=EXCLUDED.score, rank=EXCLUDED.rank, run_id=EXCLUDED.run_id;
