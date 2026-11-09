-- T4a: how get_metrics calls turn out, per stage (kind classified from result content, see views.sql).
SELECT kind,
  count(*) FILTER (WHERE stage = 'diagnosis')  AS diagnosis,
  count(*) FILTER (WHERE stage = 'mitigation') AS mitigation,
  count(*) AS total, count(DISTINCT session_id) AS sessions,
  round(count(*) * 100.0 / sum(count(*)) OVER (), 1) AS pct
FROM metric_results
GROUP BY kind
ORDER BY total DESC, kind;
