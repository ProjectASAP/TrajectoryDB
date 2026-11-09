-- T3: where PromQL comes from: the native get_metrics tool vs query= params inside bash (curl).
SELECT via, count(*) AS queries, count(DISTINCT session_id) AS sessions,
  sum(CASE WHEN promql_error(query) IS NULL THEN 1 ELSE 0 END) AS parsed
FROM promql
GROUP BY via
ORDER BY via;
