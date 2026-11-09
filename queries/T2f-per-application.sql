-- T2f: PromQL usage per application.
SELECT s.application, count(*) AS queries, count(DISTINCT p.session_id) AS sessions_using,
  count(DISTINCT p.query) AS distinct_queries,
  round(avg(CASE WHEN cardinality(promql_funcs(p.query)) > 0 OR cardinality(promql_aggs(p.query)) > 0 THEN 1.0 ELSE 0 END) * 100, 1) AS pct_with_func_or_agg
FROM promql p JOIN sessions s USING (session_id)
GROUP BY s.application
ORDER BY queries DESC, s.application;
