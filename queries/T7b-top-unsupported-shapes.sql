-- T7b: the most common query shapes ASAPQuery cannot serve: what the planner would need to add.
-- Shape = promql_normalize(q, 'shape') (label values and windows blanked).
SELECT promql_normalize(query, 'shape') AS shape, count(*) AS calls, count(DISTINCT session_id) AS sessions
FROM promql
WHERE promql_asap_class(query) = 'unsupported'
GROUP BY 1
ORDER BY calls DESC, shape
LIMIT 15;
