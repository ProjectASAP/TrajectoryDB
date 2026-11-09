-- T5b: the most-repeated exact PromQL strings within single sessions (top 15).
SELECT query, count(*) AS sessions_repeating, sum(k) AS calls, max(k) AS max_in_one_session
FROM (SELECT session_id, query, count(*) AS k FROM tool_calls WHERE fn = 'get_metrics' GROUP BY session_id, query HAVING count(*) > 1)
GROUP BY query
ORDER BY calls DESC, query
LIMIT 15;
