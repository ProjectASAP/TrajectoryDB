-- T5a: exact-string repeated PromQL (get_metrics) within a session.
-- repeat_calls = calls whose exact query string was already issued earlier in the same session.
WITH q AS (
  SELECT session_id, query, count(*) AS k FROM tool_calls WHERE fn = 'get_metrics' GROUP BY session_id, query
)
SELECT sum(k) AS calls, count(*) AS distinct_session_queries, sum(k - 1) AS repeat_calls,
  round(sum(k - 1) * 100.0 / sum(k), 1) AS repeat_pct,
  count(DISTINCT CASE WHEN k > 1 THEN session_id END) AS sessions_with_repeats,
  count(DISTINCT session_id) AS sessions_using_get_metrics
FROM q;
