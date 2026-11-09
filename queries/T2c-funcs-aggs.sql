-- T2c: PromQL functions and aggregations used (aggregation shown with its by/without clause stripped).
SELECT kind, name, count(*) AS uses, count(DISTINCT session_id) AS sessions FROM (
  SELECT session_id, 'func' AS kind, unnest(promql_funcs(query)) AS name FROM promql
  UNION ALL
  SELECT session_id, 'agg', split_part(unnest(promql_aggs(query)), ' ', 1) FROM promql
)
GROUP BY kind, name
ORDER BY kind, uses DESC, name;
