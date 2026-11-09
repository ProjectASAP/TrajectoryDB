-- T2g: PromQL the parser rejects, and what the backend said for the same call.
SELECT p.query, promql_error(p.query) AS parse_error, o.status AS backend_status, substr(o.content, 1, 80) AS backend_says
FROM promql p LEFT JOIN observations o ON o.session_id = p.session_id AND o.call_id = p.call_id
WHERE promql_error(p.query) IS NOT NULL
ORDER BY p.query, p.session_id;
