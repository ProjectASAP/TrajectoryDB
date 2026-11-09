-- W4a: stage crossing. Per session that used PromQL in both stages: metrics from diagnosis that are
-- queried again in mitigation (verification), split by mitigation outcome (NULL = unknown).
WITH m AS (
  SELECT session_id, stage, array_distinct(flatten(array_agg(metrics))) AS ms FROM promql_steps GROUP BY 1, 2
), x AS (
  SELECT d.session_id, d.ms AS dm, g.ms AS gm
  FROM m d JOIN m g ON d.session_id = g.session_id AND d.stage = 'diagnosis' AND g.stage = 'mitigation'
)
SELECT s.mitigation_passed, count(*) AS sessions,
  round(avg(cardinality(dm)), 2) AS avg_diag_metrics,
  round(avg(cardinality(gm)), 2) AS avg_mitig_metrics,
  round(avg(cardinality(array_intersect(dm, gm))), 2) AS avg_shared,
  round(avg(CASE WHEN cardinality(array_intersect(dm, gm)) > 0 THEN 100.0 ELSE 0 END), 1) AS pct_sessions_reusing
FROM x JOIN sessions s USING (session_id)
GROUP BY s.mitigation_passed
ORDER BY s.mitigation_passed NULLS LAST;
