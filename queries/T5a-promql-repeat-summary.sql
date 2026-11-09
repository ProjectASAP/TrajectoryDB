-- T5a: repeated PromQL within a session, after promql_normalize(q, mode). mode = 'exact' for now.
-- repeat_calls = calls whose exact query string was already issued earlier in the same session.
WITH q AS (
  SELECT session_id, promql_normalize(query, 'exact') AS query, count(*) AS k FROM promql GROUP BY 1, 2
)
SELECT sum(k) AS calls, count(*) AS distinct_session_queries, sum(k - 1) AS repeat_calls,
  round(sum(k - 1) * 100.0 / sum(k), 1) AS repeat_pct,
  count(DISTINCT CASE WHEN k > 1 THEN session_id END) AS sessions_with_repeats,
  count(DISTINCT session_id) AS sessions_using_promql
FROM q;
