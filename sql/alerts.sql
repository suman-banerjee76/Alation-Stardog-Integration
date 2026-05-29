-- Alert-condition queries (design §8 row 8: "alerts firing on failure conditions").
-- Each returns ZERO rows when healthy and >=1 row when the condition is firing; wire to a
-- SQL-exporter / scheduled check that pages when rowcount > 0. Thresholds are inline and tunable.

-- ALERT: stage failure -- any endpoint recorded an error on its last run
SELECT endpoint, ds_id, last_error, last_success_at
FROM sync_state
WHERE last_error IS NOT NULL;

-- ALERT: freshness lag exceeded -- no success within ~2x the 60-min cadence (tune interval)
SELECT endpoint, ds_id, last_success_at, now() - last_success_at AS lag
FROM sync_state
WHERE last_success_at IS NULL
   OR now() - last_success_at > interval '2 hours';

-- ALERT: concept extract stale -- daily cadence missed (> 48h)
SELECT endpoint, last_success_at, now() - last_success_at AS lag
FROM sync_state
WHERE endpoint = 'concept_extract'
  AND (last_success_at IS NULL OR now() - last_success_at > interval '48 hours');

-- ALERT: write-back failures -- bindings stuck in failed state
SELECT object_kind, object_key, alation_object_id, last_error, last_written_at
FROM uri_binding
WHERE writeback_state = 'failed';

-- ALERT: write-back conflicts in the last hour -- protect-human skips worth a steward's eyes
SELECT actor, object_kind, alation_object_id, old_value, new_value, ts
FROM writeback_audit
WHERE note = 'human value present'
  AND ts > now() - interval '1 hour';

-- ALERT: CFV API errors in the last hour -- non-2xx promote attempts
SELECT object_kind, alation_object_id, field_id, api_status, ts
FROM writeback_audit
WHERE api_status IS NOT NULL
  AND (api_status < 200 OR api_status >= 300)
  AND ts > now() - interval '1 hour';
