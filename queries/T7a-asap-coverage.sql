-- T7a: how much of the agent's PromQL ASAPQuery's planner could serve (promql_asap_class mirrors its pattern table).
-- Counted over calls, distinct exact queries, and distinct shapes (promql_normalize 'shape').
SELECT promql_asap_class(query) AS class, count(*) AS calls,
  round(count(*) * 100.0 / sum(count(*)) OVER (), 1) AS pct_calls,
  count(DISTINCT query) AS distinct_queries,
  count(DISTINCT promql_normalize(query, 'shape')) AS distinct_shapes,
  count(DISTINCT session_id) AS sessions
FROM promql
GROUP BY 1
ORDER BY calls DESC, class;
