-- T2a: overall shape of every PromQL the agent sent (promql view = get_metrics + query= inside bash).
WITH p AS (
  SELECT query, promql_error(query) IS NULL AS ok, promql_metrics(query) m, promql_funcs(query) f,
         promql_aggs(query) a, promql_matchers(query) mt, promql_ranges(query) r FROM promql
)
SELECT count(*) AS queries, sum(CAST(ok AS INT)) AS parsed,
  round(avg(CAST(ok AS INT)) * 100, 1) AS parsed_pct,
  sum(CASE WHEN cardinality(f) > 0 THEN 1 ELSE 0 END)  AS with_func,
  sum(CASE WHEN cardinality(a) > 0 THEN 1 ELSE 0 END)  AS with_agg,
  sum(CASE WHEN cardinality(mt) > 0 THEN 1 ELSE 0 END) AS with_matcher,
  sum(CASE WHEN cardinality(r) > 0 THEN 1 ELSE 0 END)  AS with_range,
  sum(CASE WHEN cardinality(m) > 1 THEN 1 ELSE 0 END)  AS multi_metric,
  sum(CASE WHEN cardinality(m) = 0 THEN 1 ELSE 0 END)  AS no_metric_name
FROM p;
