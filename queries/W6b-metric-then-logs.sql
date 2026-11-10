-- W6b: cross-tool walk with a join key. For PromQL steps whose queries filter on namespace="X", does the agent
-- read logs within the next 3 agent steps (same session+stage), and are they logs of the same namespace X?
WITH pos AS (
  SELECT session_id, stage, step_id, row_number() OVER (PARTITION BY session_id, stage ORDER BY step_id) AS n
  FROM steps WHERE source = 'agent'
), mns AS (
  SELECT DISTINCT p.session_id, p.stage, p.n, substr(m, 11) AS ns
  FROM (SELECT session_id, step_id, unnest(promql_matchers(query)) AS m FROM metric_results) q
  JOIN pos p USING (session_id, step_id)
  WHERE m LIKE 'namespace=%' AND m NOT LIKE 'namespace=~%'
), logs AS (
  SELECT DISTINCT p.session_id, p.stage, p.n, tc.namespace AS ns
  FROM telemetry_calls tc JOIN pos p USING (session_id, stage, step_id) WHERE tc.modality = 'logs'
), j AS (
  SELECT m.session_id, m.stage, m.n, m.ns,
    max(CASE WHEN l.n IS NOT NULL THEN 1 ELSE 0 END) AS any_logs,
    max(CASE WHEN l.ns = m.ns THEN 1 ELSE 0 END) AS same_ns_logs
  FROM mns m LEFT JOIN logs l ON l.session_id = m.session_id AND l.stage = m.stage AND l.n BETWEEN m.n + 1 AND m.n + 3
  GROUP BY 1, 2, 3, 4
)
SELECT stage, count(*) AS promql_steps_with_ns,
  round(avg(any_logs) * 100, 1) AS pct_logs_within_3,
  round(avg(same_ns_logs) * 100, 1) AS pct_same_ns_logs_within_3
FROM j GROUP BY stage ORDER BY stage;
